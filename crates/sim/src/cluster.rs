use std::collections::BTreeMap;

use channel::{Channel, FaultProfile, Tick};
use raft::{
    decode_message, encode_message, Config, Entry, Index, MemStorage, Node, NodeId, Outgoing,
    ProposeError, Role, SietchStorage, Storage, Term,
};

use crate::rng::Rng;

/// Per-tick probabilities of injected cluster events.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Faults {
    /// Crash one random live node.
    pub crash: f64,
    /// Restart one random crashed node.
    pub restart: f64,
    /// Cut the network into two random sides.
    pub partition: f64,
    /// Remove all partitions.
    pub heal: f64,
    /// Per handled event: crash the node right after its handler ran, before
    /// its outgoing messages are sent.
    pub crash_after_event: f64,
    /// Submit a new client command to the current leader.
    pub propose: f64,
}

impl Faults {
    pub const NONE: Faults = Faults {
        crash: 0.0,
        restart: 0.0,
        partition: 0.0,
        heal: 0.0,
        crash_after_event: 0.0,
        propose: 0.0,
    };
}

/// Where nodes keep their persistent state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageKind {
    /// In memory; survives a simulated crash (fast).
    Mem,
    /// Real `sietch` stores under `<dir>/node<i>`; a crash drops the node
    /// and its open store, and a restart reopens the directory from disk.
    Sietch(std::path::PathBuf),
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub storage: StorageKind,
    pub n: usize,
    pub seed: u64,
    pub profile: FaultProfile,
    pub faults: Faults,
    pub election_timeout: (u64, u64),
    pub heartbeat_interval: u64,
}

impl ClusterConfig {
    pub fn new(n: usize, seed: u64) -> Self {
        ClusterConfig {
            storage: StorageKind::Mem,
            n,
            seed,
            profile: FaultProfile::CLEAN,
            faults: Faults::NONE,
            election_timeout: (150, 300),
            heartbeat_interval: 50,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invariant violated at tick {tick}: {what}")]
pub struct Violation {
    pub tick: u64,
    pub what: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    pub sent: u64,
    pub delivered: u64,
    pub rejected_by_codec: u64,
    pub dropped_by_partition: u64,
    pub dropped_to_crashed_node: u64,
    pub crashes: u64,
    pub restarts: u64,
    pub proposals_accepted: u64,
    pub terms_with_a_leader: u64,
}

pub struct Cluster {
    cfg: ClusterConfig,
    now: u64,
    rng: Rng,
    /// `links[from * n + to]`; the diagonal is unused.
    links: Vec<Channel>,
    blocked: Vec<bool>,
    nodes: Vec<Option<Node>>,
    disks: Vec<MemStorage>,
    stats: Stats,
    next_command: u64,
    // Invariant-checker state.
    leaders_by_term: BTreeMap<Term, NodeId>,
    committed: Vec<Entry>,
    max_commit_seen: Vec<Index>,
    max_term_seen: Vec<Term>,
}

impl Cluster {
    pub fn new(cfg: ClusterConfig) -> Self {
        let n = cfg.n;
        let links = (0..n * n)
            .map(|i| {
                Channel::new(
                    cfg.seed ^ (i as u64 + 1).wrapping_mul(0xD6E8_FEB8_6659_FD93),
                    cfg.profile,
                )
            })
            .collect();
        let disks: Vec<MemStorage> = (0..n).map(|_| MemStorage::new()).collect();
        let mut c = Cluster {
            rng: Rng::new(cfg.seed ^ 0xA5A5_A5A5_5A5A_5A5A),
            now: 0,
            links,
            blocked: vec![false; n * n],
            nodes: (0..n).map(|_| None).collect(),
            disks,
            stats: Stats::default(),
            next_command: 0,
            leaders_by_term: BTreeMap::new(),
            committed: Vec::new(),
            max_commit_seen: vec![0; n],
            max_term_seen: vec![0; n],
            cfg,
        };
        for i in 0..n {
            c.boot(i);
        }
        c
    }

    fn boot(&mut self, i: NodeId) {
        let mut cfg = Config::new(i, self.cfg.n, self.cfg.seed);
        cfg.election_timeout = self.cfg.election_timeout;
        cfg.heartbeat_interval = self.cfg.heartbeat_interval;
        let storage: Box<dyn Storage> = match &self.cfg.storage {
            StorageKind::Mem => Box::new(self.disks[i].clone()),
            StorageKind::Sietch(dir) => Box::new(
                SietchStorage::open(dir.join(format!("node{i}")))
                    .expect("fail-stop: cannot open node storage"),
            ),
        };
        self.nodes[i] = Some(Node::new(cfg, storage, self.now));
    }

    pub fn node_count(&self) -> usize {
        self.cfg.n
    }
    pub fn now(&self) -> u64 {
        self.now
    }
    pub fn stats(&self) -> Stats {
        self.stats
    }
    pub fn node(&self, i: NodeId) -> Option<&Node> {
        self.nodes[i].as_ref()
    }
    pub fn is_up(&self, i: NodeId) -> bool {
        self.nodes[i].is_some()
    }
    /// The globally committed log prefix observed so far.
    pub fn committed(&self) -> &[Entry] {
        &self.committed
    }
    pub fn channel_stats(&self) -> channel::FaultStats {
        let mut t = channel::FaultStats::default();
        for c in &self.links {
            let s = c.stats();
            t.sent += s.sent;
            t.delivered += s.delivered;
            t.dropped += s.dropped;
            t.duplicated += s.duplicated;
            t.reordered += s.reordered;
            t.corrupted += s.corrupted;
            t.truncated += s.truncated;
        }
        t
    }

    /// The live leader of the highest term, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .flatten()
            .filter(|n| n.role() == Role::Leader)
            .max_by_key(|n| n.term())
            .map(|n| n.id())
    }

    pub fn crash(&mut self, i: NodeId) {
        if self.nodes[i].take().is_some() {
            self.stats.crashes += 1;
        }
    }

    pub fn restart(&mut self, i: NodeId) -> Result<(), Violation> {
        if self.nodes[i].is_some() {
            return Ok(());
        }
        self.boot(i);
        self.stats.restarts += 1;
        // Durability: everything this node had committed before the crash
        // must still be in the log it recovered from storage.
        let log = self.nodes[i].as_ref().unwrap().log();
        let c = self.max_commit_seen[i] as usize;
        let known = c.min(self.committed.len());
        if log.len() < known || log[..known] != self.committed[..known] {
            return Err(self.violation(format!(
                "node {i} lost committed entries across a crash (had commit index {c})"
            )));
        }
        Ok(())
    }

    /// Ends all injected chaos: no more random events, partitions healed,
    /// every crashed node restarted. The channels keep their fault profile,
    /// so the network stays lossy: liveness is then a statement about
    /// recovering *despite* residual loss.
    pub fn quiesce(&mut self) -> Result<(), Violation> {
        self.cfg.faults = Faults::NONE;
        self.heal();
        for i in 0..self.cfg.n {
            self.restart(i)?;
        }
        Ok(())
    }

    /// `side[i]` says which side node `i` is on; links between different
    /// sides are cut in both directions.
    pub fn partition(&mut self, side: &[bool]) {
        let n = self.cfg.n;
        for a in 0..n {
            for b in 0..n {
                self.blocked[a * n + b] = a != b && side[a] != side[b];
            }
        }
    }

    pub fn isolate(&mut self, i: NodeId) {
        let mut side = vec![false; self.cfg.n];
        side[i] = true;
        self.partition(&side);
    }

    pub fn heal(&mut self) {
        self.blocked.iter_mut().for_each(|b| *b = false);
    }

    /// Submits a command to the live leader of the highest term.
    pub fn propose(&mut self, command: Vec<u8>) -> Result<(NodeId, Index), ProposeError> {
        let Some(l) = self.leader() else {
            return Err(ProposeError::NotLeader(None));
        };
        let now = self.now;
        let (idx, out) = self.nodes[l].as_mut().unwrap().propose(now, command)?;
        self.stats.proposals_accepted += 1;
        self.route(l, out);
        Ok((l, idx))
    }

    fn violation(&self, what: String) -> Violation {
        Violation {
            tick: self.now,
            what,
        }
    }

    fn route(&mut self, from: NodeId, out: Vec<Outgoing>) {
        let n = self.cfg.n;
        for o in out {
            self.stats.sent += 1;
            if self.blocked[from * n + o.to] {
                self.stats.dropped_by_partition += 1;
                continue;
            }
            self.links[from * n + o.to].send(&encode_message(&o.message));
        }
    }

    /// Advances virtual time by one tick.
    pub fn step(&mut self) -> Result<(), Violation> {
        self.now += 1;
        let n = self.cfg.n;
        let now = self.now;

        // Injected cluster events.
        let f = self.cfg.faults;
        if self.rng.chance(f.crash) {
            let i = self.rng.below(n);
            self.crash(i);
        }
        if self.rng.chance(f.restart) {
            let i = self.rng.below(n);
            self.restart(i)?;
        }
        if self.rng.chance(f.partition) {
            let side: Vec<bool> = (0..n).map(|_| self.rng.chance(0.5)).collect();
            self.partition(&side);
        }
        if self.rng.chance(f.heal) {
            self.heal();
        }
        if self.rng.chance(f.propose) {
            let cmd = format!("cmd{}", self.next_command).into_bytes();
            if self.propose(cmd).is_ok() {
                self.next_command += 1;
            }
        }

        // Deliver everything that arrives this tick.
        for from in 0..n {
            for to in 0..n {
                if from == to {
                    continue;
                }
                for datagram in self.links[from * n + to].advance(Tick(now)) {
                    if self.nodes[to].is_none() {
                        self.stats.dropped_to_crashed_node += 1;
                        continue;
                    }
                    let msg = match decode_message(&datagram) {
                        Ok(m) => m,
                        Err(_) => {
                            self.stats.rejected_by_codec += 1;
                            continue;
                        }
                    };
                    self.stats.delivered += 1;
                    let out = self.nodes[to].as_mut().unwrap().handle(now, from, msg);
                    if self.rng.chance(f.crash_after_event) {
                        self.crash(to);
                    } else {
                        self.route(to, out);
                    }
                }
            }
        }

        // Timers.
        for i in 0..n {
            if let Some(node) = self.nodes[i].as_mut() {
                let out = node.tick(now);
                self.route(i, out);
            }
        }

        self.check_invariants()
    }

    pub fn run(&mut self, ticks: u64) -> Result<(), Violation> {
        for _ in 0..ticks {
            self.step()?;
        }
        Ok(())
    }

    fn check_invariants(&mut self) -> Result<(), Violation> {
        let n = self.cfg.n;
        for i in 0..n {
            let Some(node) = self.nodes[i].as_ref() else {
                continue;
            };
            // Terms never go backwards, even across restarts.
            if node.term() < self.max_term_seen[i] {
                let msg = format!(
                    "node {i} term went backwards: {} -> {}",
                    self.max_term_seen[i],
                    node.term()
                );
                return Err(self.violation(msg));
            }
            self.max_term_seen[i] = node.term();

            // Election safety: at most one leader per term.
            if node.role() == Role::Leader {
                let prev = *self.leaders_by_term.entry(node.term()).or_insert(i);
                if prev != i {
                    let msg = format!(
                        "election safety: nodes {prev} and {i} both lead term {}",
                        node.term()
                    );
                    return Err(self.violation(msg));
                }
                self.stats.terms_with_a_leader = self.leaders_by_term.len() as u64;
            }
        }

        // State-machine safety: every node's committed prefix agrees with
        // the global committed log, which only ever grows.
        for i in 0..n {
            let Some(node) = self.nodes[i].as_ref() else {
                continue;
            };
            let c = node.commit_index() as usize;
            let log = node.log();
            if c > log.len() {
                return Err(self.violation(format!("node {i} commit index beyond its log")));
            }
            for (k, e) in log[..c].iter().enumerate() {
                if k < self.committed.len() {
                    if self.committed[k] != *e {
                        let msg = format!(
                            "state machine safety: node {i} committed a different entry at index {}",
                            k + 1
                        );
                        return Err(self.violation(msg));
                    }
                } else {
                    self.committed.push(e.clone());
                }
            }
            self.max_commit_seen[i] = self.max_commit_seen[i].max(c as Index);
        }

        // Leader completeness: a leader's log holds every entry committed
        // in an earlier term than its own.
        for i in 0..n {
            let Some(node) = self.nodes[i].as_ref() else {
                continue;
            };
            if node.role() != Role::Leader {
                continue;
            }
            for (k, e) in self.committed.iter().enumerate() {
                if e.term >= node.term() {
                    break;
                }
                if node.log().get(k) != Some(e) {
                    let msg = format!(
                        "leader completeness: leader {i} (term {}) lacks committed entry {}",
                        node.term(),
                        k + 1
                    );
                    return Err(self.violation(msg));
                }
            }
        }

        // Log matching: same index and term implies identical prefixes.
        for a in 0..n {
            for b in a + 1..n {
                let (Some(x), Some(y)) = (self.nodes[a].as_ref(), self.nodes[b].as_ref()) else {
                    continue;
                };
                let (la, lb) = (x.log(), y.log());
                let m = la.len().min(lb.len());
                if let Some(i) = (0..m).rev().find(|&i| la[i].term == lb[i].term) {
                    if la[..=i] != lb[..=i] {
                        let msg = format!(
                            "log matching: nodes {a} and {b} agree on term at index {} but differ before it",
                            i + 1
                        );
                        return Err(self.violation(msg));
                    }
                }
            }
        }
        Ok(())
    }
}
