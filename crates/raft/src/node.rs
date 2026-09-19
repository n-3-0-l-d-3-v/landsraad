//! The consensus node: a deterministic, I/O-free state machine.
//!
//! Inputs are `tick(now)`, `handle(now, from, message)` and
//! `propose(now, command)`; outputs are the messages to send. `now` is a
//! virtual tick supplied by the caller (there is no wall clock anywhere).
//! Everything that must survive a crash (term, vote, log) is written to the
//! `Storage` *before* any message that depends on it is returned, so a
//! crash can lose a message but never a promise.
//!
//! Rules implemented (Raft, Ongaro & Ousterhout 2014):
//! - a node grants at most one vote per term, and only to a candidate whose
//!   log is at least as up to date as its own;
//! - a follower truncates its log only on a genuine conflict (a stale or
//!   duplicated AppendEntries never deletes valid entries);
//! - a leader advances its commit index only over entries from its own
//!   term (older entries commit indirectly), which is why each new leader
//!   appends a no-op entry immediately;
//! - failed AppendEntries carry a hint so the leader backs up quickly.

use crate::message::{Entry, Index, Message, NodeId, Term};
use crate::storage::Storage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub id: NodeId,
    pub cluster_size: usize,
    /// Inclusive range of randomized election timeouts, in ticks.
    pub election_timeout: (u64, u64),
    pub heartbeat_interval: u64,
    /// Most entries sent in one AppendEntries.
    pub max_batch: usize,
    /// Seeds this node's timeout randomness (mixed with `id`).
    pub seed: u64,
}

impl Config {
    pub fn new(id: NodeId, cluster_size: usize, seed: u64) -> Self {
        Config {
            id,
            cluster_size,
            election_timeout: (150, 300),
            heartbeat_interval: 50,
            max_batch: 32,
            seed,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing {
    pub to: NodeId,
    pub message: Message,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProposeError {
    #[error("not the leader (last known leader: {0:?})")]
    NotLeader(Option<NodeId>),
}

pub struct Node {
    cfg: Config,
    storage: Box<dyn Storage>,
    term: Term,
    voted_for: Option<NodeId>,
    log: Vec<Entry>,
    role: Role,
    leader_id: Option<NodeId>,
    commit_index: Index,
    applied_index: Index,
    votes: Vec<bool>,
    next_index: Vec<Index>,
    match_index: Vec<Index>,
    election_deadline: u64,
    heartbeat_deadline: u64,
    rng: u64,
}

impl Node {
    /// Starts (or restarts after a crash) a node from whatever `storage`
    /// holds. Volatile state (commit index, role) always restarts fresh.
    pub fn new(cfg: Config, mut storage: Box<dyn Storage>, now: u64) -> Self {
        assert!(cfg.id < cfg.cluster_size, "node id out of range");
        assert!(cfg.election_timeout.0 > 0 && cfg.election_timeout.0 <= cfg.election_timeout.1);
        let p = storage.load();
        let n = cfg.cluster_size;
        let rng = cfg.seed ^ (cfg.id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ p.term;
        let mut node = Node {
            cfg,
            storage,
            term: p.term,
            voted_for: p.voted_for,
            log: p.log,
            role: Role::Follower,
            leader_id: None,
            commit_index: 0,
            applied_index: 0,
            votes: vec![false; n],
            next_index: vec![1; n],
            match_index: vec![0; n],
            election_deadline: 0,
            heartbeat_deadline: 0,
            rng,
        };
        node.reset_election_timer(now);
        node
    }

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> Term {
        self.term
    }
    pub fn leader_id(&self) -> Option<NodeId> {
        self.leader_id
    }
    pub fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }
    pub fn commit_index(&self) -> Index {
        self.commit_index
    }
    pub fn log(&self) -> &[Entry] {
        &self.log
    }
    pub fn last_index(&self) -> Index {
        self.log.len() as Index
    }

    fn quorum(&self) -> usize {
        self.cfg.cluster_size / 2 + 1
    }

    fn next_rand(&mut self) -> u64 {
        // SplitMix64.
        self.rng = self.rng.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn reset_election_timer(&mut self, now: u64) {
        let (lo, hi) = self.cfg.election_timeout;
        let span = hi - lo + 1;
        self.election_deadline = now + lo + self.next_rand() % span;
    }

    fn term_at(&self, index: Index) -> Term {
        if index == 0 {
            0
        } else {
            self.log[index as usize - 1].term
        }
    }

    fn last_term(&self) -> Term {
        self.term_at(self.last_index())
    }

    fn persist_hard_state(&mut self) {
        self.storage.save_hard_state(self.term, self.voted_for);
    }

    /// Adopts a newer term: forget any vote, become a follower.
    fn step_down(&mut self, now: u64, term: Term) {
        self.term = term;
        self.voted_for = None;
        self.persist_hard_state();
        self.role = Role::Follower;
        self.leader_id = None;
        self.reset_election_timer(now);
    }

    fn peers(&self) -> impl Iterator<Item = NodeId> {
        let me = self.cfg.id;
        (0..self.cfg.cluster_size).filter(move |&p| p != me)
    }

    pub fn tick(&mut self, now: u64) -> Vec<Outgoing> {
        match self.role {
            Role::Leader => {
                if now >= self.heartbeat_deadline {
                    self.heartbeat_deadline = now + self.cfg.heartbeat_interval;
                    return self.broadcast_append();
                }
                vec![]
            }
            _ => {
                if now >= self.election_deadline {
                    return self.start_election(now);
                }
                vec![]
            }
        }
    }

    fn start_election(&mut self, now: u64) -> Vec<Outgoing> {
        self.term += 1;
        self.voted_for = Some(self.cfg.id);
        self.persist_hard_state();
        self.role = Role::Candidate;
        self.leader_id = None;
        self.votes = vec![false; self.cfg.cluster_size];
        self.votes[self.cfg.id] = true;
        self.reset_election_timer(now);
        if self.votes.iter().filter(|v| **v).count() >= self.quorum() {
            return self.become_leader(now);
        }
        let msg = Message::RequestVote {
            term: self.term,
            last_log_index: self.last_index(),
            last_log_term: self.last_term(),
        };
        self.peers()
            .map(|to| Outgoing {
                to,
                message: msg.clone(),
            })
            .collect()
    }

    fn become_leader(&mut self, now: u64) -> Vec<Outgoing> {
        self.role = Role::Leader;
        self.leader_id = Some(self.cfg.id);
        let next = self.last_index() + 1;
        self.next_index = vec![next; self.cfg.cluster_size];
        self.match_index = vec![0; self.cfg.cluster_size];
        // The no-op lets this leader commit entries from earlier terms.
        let noop = Entry {
            term: self.term,
            command: vec![],
        };
        self.storage.append(std::slice::from_ref(&noop));
        self.log.push(noop);
        self.advance_commit();
        self.heartbeat_deadline = now + self.cfg.heartbeat_interval;
        self.broadcast_append()
    }

    fn broadcast_append(&mut self) -> Vec<Outgoing> {
        let peers: Vec<NodeId> = self.peers().collect();
        peers.into_iter().map(|p| self.append_for(p)).collect()
    }

    fn append_for(&self, peer: NodeId) -> Outgoing {
        let next = self.next_index[peer];
        let prev_index = next - 1;
        let start = prev_index as usize;
        let end = self.log.len().min(start + self.cfg.max_batch);
        Outgoing {
            to: peer,
            message: Message::AppendEntries {
                term: self.term,
                prev_index,
                prev_term: self.term_at(prev_index),
                entries: self.log[start..end].to_vec(),
                leader_commit: self.commit_index,
            },
        }
    }

    /// Appends a client command to the leader's log and starts replicating
    /// it. Returns the entry's index; it is *not* committed yet.
    pub fn propose(
        &mut self,
        _now: u64,
        command: Vec<u8>,
    ) -> Result<(Index, Vec<Outgoing>), ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader(self.leader_id));
        }
        let e = Entry {
            term: self.term,
            command,
        };
        self.storage.append(std::slice::from_ref(&e));
        self.log.push(e);
        self.advance_commit();
        Ok((self.last_index(), self.broadcast_append()))
    }

    pub fn handle(&mut self, now: u64, from: NodeId, msg: Message) -> Vec<Outgoing> {
        if from == self.cfg.id || from >= self.cfg.cluster_size {
            return vec![];
        }
        if msg.term() > self.term {
            self.step_down(now, msg.term());
        }
        match msg {
            Message::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(now, from, term, last_log_index, last_log_term),
            Message::VoteResponse { term, granted } => {
                self.on_vote_response(now, from, term, granted)
            }
            Message::AppendEntries {
                term,
                prev_index,
                prev_term,
                entries,
                leader_commit,
            } => self.on_append_entries(
                now,
                from,
                term,
                prev_index,
                prev_term,
                entries,
                leader_commit,
            ),
            Message::AppendResponse {
                term,
                success,
                match_index,
            } => self.on_append_response(from, term, success, match_index),
        }
    }

    fn reply(&self, to: NodeId, message: Message) -> Vec<Outgoing> {
        vec![Outgoing { to, message }]
    }

    fn on_request_vote(
        &mut self,
        now: u64,
        from: NodeId,
        term: Term,
        last_log_index: Index,
        last_log_term: Term,
    ) -> Vec<Outgoing> {
        let mut granted = false;
        if term == self.term {
            let up_to_date = last_log_term > self.last_term()
                || (last_log_term == self.last_term() && last_log_index >= self.last_index());
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(from);
            if can_vote && up_to_date {
                self.voted_for = Some(from);
                self.persist_hard_state();
                self.reset_election_timer(now);
                granted = true;
            }
        }
        self.reply(
            from,
            Message::VoteResponse {
                term: self.term,
                granted,
            },
        )
    }

    fn on_vote_response(
        &mut self,
        now: u64,
        from: NodeId,
        term: Term,
        granted: bool,
    ) -> Vec<Outgoing> {
        if self.role != Role::Candidate || term != self.term || !granted {
            return vec![];
        }
        self.votes[from] = true;
        if self.votes.iter().filter(|v| **v).count() >= self.quorum() {
            return self.become_leader(now);
        }
        vec![]
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_entries(
        &mut self,
        now: u64,
        from: NodeId,
        term: Term,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    ) -> Vec<Outgoing> {
        let respond = |node: &Node, success: bool, match_index: Index| {
            node.reply(
                from,
                Message::AppendResponse {
                    term: node.term,
                    success,
                    match_index,
                },
            )
        };
        if term < self.term {
            return respond(self, false, 0);
        }
        debug_assert!(
            self.role != Role::Leader,
            "two leaders in term {}: election safety violated",
            self.term
        );
        // A valid AppendEntries for our term: there is a leader, so any
        // candidacy is over.
        self.role = Role::Follower;
        self.leader_id = Some(from);
        self.reset_election_timer(now);

        if prev_index > self.last_index() {
            return respond(self, false, self.last_index());
        }
        if self.term_at(prev_index) != prev_term {
            return respond(self, false, prev_index.saturating_sub(1));
        }
        for (k, e) in entries.iter().enumerate() {
            let idx = prev_index + 1 + k as Index;
            if idx <= self.last_index() {
                if self.term_at(idx) == e.term {
                    continue; // already have it (duplicate or overlapping resend)
                }
                self.storage.truncate_from(idx);
                self.log.truncate(idx as usize - 1);
            }
            self.storage.append(&entries[k..]);
            self.log.extend_from_slice(&entries[k..]);
            break;
        }
        let matched = prev_index + entries.len() as Index;
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(matched);
        }
        respond(self, true, matched)
    }

    fn on_append_response(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: Index,
    ) -> Vec<Outgoing> {
        if self.role != Role::Leader || term != self.term {
            return vec![];
        }
        if success {
            let m = match_index.min(self.last_index());
            self.match_index[from] = self.match_index[from].max(m);
            self.next_index[from] = self.match_index[from] + 1;
            self.advance_commit();
            if self.next_index[from] <= self.last_index() {
                return vec![self.append_for(from)];
            }
            vec![]
        } else {
            let lowest = self.match_index[from] + 1;
            self.next_index[from] = lowest.max(self.next_index[from].min(match_index + 1));
            vec![self.append_for(from)]
        }
    }

    /// Commits the highest entry of the current term stored on a majority.
    fn advance_commit(&mut self) {
        let quorum = self.quorum();
        for n in (self.commit_index + 1..=self.last_index()).rev() {
            if self.term_at(n) != self.term {
                break;
            }
            let replicas = 1 + self.peers().filter(|&p| self.match_index[p] >= n).count();
            if replicas >= quorum {
                self.commit_index = n;
                break;
            }
        }
    }

    /// Newly committed entries since the last call, in order, with their
    /// indices — what the state machine should apply next.
    pub fn take_committed(&mut self) -> Vec<(Index, Entry)> {
        let out = (self.applied_index + 1..=self.commit_index)
            .map(|i| (i, self.log[i as usize - 1].clone()))
            .collect();
        self.applied_index = self.commit_index;
        out
    }
}
