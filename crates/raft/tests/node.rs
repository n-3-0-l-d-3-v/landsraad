//! Scripted tests for the node state machine (ticket 002): one test per
//! consensus rule, driving nodes by hand with exact messages.

use raft::*;

fn cfg(id: NodeId, n: usize) -> Config {
    let mut c = Config::new(id, n, 1);
    c.election_timeout = (100, 100); // deterministic: deadline = now + 100
    c.heartbeat_interval = 10;
    c
}

fn boot(id: NodeId, n: usize) -> (Node, MemStorage) {
    let s = MemStorage::new();
    (Node::new(cfg(id, n), Box::new(s.clone()), 0), s)
}

fn e(term: Term, c: &[u8]) -> Entry {
    Entry {
        term,
        command: c.to_vec(),
    }
}

fn vote_req(term: Term, i: Index, t: Term) -> Message {
    Message::RequestVote {
        term,
        last_log_index: i,
        last_log_term: t,
    }
}

fn append(term: Term, prev: Index, prev_term: Term, entries: Vec<Entry>, commit: Index) -> Message {
    Message::AppendEntries {
        term,
        prev_index: prev,
        prev_term,
        entries,
        leader_commit: commit,
    }
}

fn only(out: Vec<Outgoing>) -> Message {
    assert_eq!(out.len(), 1, "{out:?}");
    out.into_iter().next().unwrap().message
}

fn win(node: &mut Node, now: u64, voters: &[NodeId]) {
    let term = node.term();
    for &v in voters {
        node.handle(
            now,
            v,
            Message::VoteResponse {
                term,
                granted: true,
            },
        );
    }
}

#[test]
fn a_follower_times_out_becomes_a_candidate_and_asks_everyone_for_votes() {
    let (mut n, s) = boot(0, 3);
    assert!(n.tick(99).is_empty());
    let out = n.tick(100);
    assert_eq!(n.role(), Role::Candidate);
    assert_eq!(n.term(), 1);
    assert_eq!(n.voted_for(), Some(0));
    assert_eq!(out.len(), 2);
    assert!(out
        .iter()
        .all(|o| o.message == vote_req(1, 0, 0) && o.to != 0));
    let p = s.snapshot();
    assert_eq!(
        (p.term, p.voted_for),
        (1, Some(0)),
        "persisted before sending"
    );
}

#[test]
fn a_candidate_with_a_majority_becomes_leader_and_appends_a_noop() {
    let (mut n, _s) = boot(0, 3);
    n.tick(100);
    win(&mut n, 101, &[1]);
    assert_eq!(n.role(), Role::Leader);
    assert_eq!(n.log(), &[e(1, b"")]);
    assert_eq!(n.leader_id(), Some(0));
}

#[test]
fn a_denied_or_stale_vote_does_not_elect() {
    let (mut n, _s) = boot(0, 3);
    n.tick(100);
    n.handle(
        101,
        1,
        Message::VoteResponse {
            term: 1,
            granted: false,
        },
    );
    n.handle(
        101,
        2,
        Message::VoteResponse {
            term: 0,
            granted: true,
        },
    );
    assert_eq!(n.role(), Role::Candidate);
}

#[test]
fn one_vote_per_term_and_a_higher_term_resets_it() {
    let (mut n, s) = boot(2, 3);
    let r = only(n.handle(1, 0, vote_req(1, 0, 0)));
    assert_eq!(
        r,
        Message::VoteResponse {
            term: 1,
            granted: true
        }
    );
    assert_eq!(
        s.snapshot().voted_for,
        Some(0),
        "vote persisted before the reply"
    );
    let r = only(n.handle(2, 1, vote_req(1, 0, 0)));
    assert_eq!(
        r,
        Message::VoteResponse {
            term: 1,
            granted: false
        }
    );
    let r = only(n.handle(3, 0, vote_req(1, 0, 0)));
    assert_eq!(
        r,
        Message::VoteResponse {
            term: 1,
            granted: true
        },
        "the same candidate may ask again (its first reply may have been lost)"
    );
    let r = only(n.handle(4, 1, vote_req(2, 0, 0)));
    assert_eq!(
        r,
        Message::VoteResponse {
            term: 2,
            granted: true
        }
    );
}

#[test]
fn a_stale_term_vote_request_is_refused_and_reports_our_term() {
    let (mut n, _s) = boot(1, 3);
    n.handle(1, 0, vote_req(5, 0, 0));
    let r = only(n.handle(2, 2, vote_req(3, 0, 0)));
    assert_eq!(
        r,
        Message::VoteResponse {
            term: 5,
            granted: false
        }
    );
}

#[test]
fn votes_are_refused_to_candidates_with_out_of_date_logs() {
    let s = MemStorage::new();
    let mut seed = s.clone();
    seed.save_hard_state(2, None);
    seed.append(&[e(1, b"a"), e(2, b"b")]);
    let mut n = Node::new(cfg(1, 3), Box::new(s), 0);
    // Shorter log, same last term.
    let r = only(n.handle(1, 0, vote_req(3, 1, 1)));
    assert_eq!(
        r,
        Message::VoteResponse {
            term: 3,
            granted: false
        }
    );
    // Longer but older last term loses to a newer last term.
    let r = only(n.handle(2, 2, vote_req(3, 9, 1)));
    assert_eq!(
        r,
        Message::VoteResponse {
            term: 3,
            granted: false
        }
    );
    // Equal log: granted.
    let r = only(n.handle(3, 2, vote_req(3, 2, 2)));
    assert_eq!(
        r,
        Message::VoteResponse {
            term: 3,
            granted: true
        }
    );
}

#[test]
fn a_restarted_node_cannot_vote_twice_in_the_same_term() {
    let (mut n, s) = boot(2, 3);
    n.handle(1, 0, vote_req(1, 0, 0));
    drop(n); // crash
    let mut n = Node::new(cfg(2, 3), Box::new(s), 5);
    assert_eq!((n.term(), n.voted_for()), (1, Some(0)));
    let r = only(n.handle(6, 1, vote_req(1, 0, 0)));
    assert_eq!(
        r,
        Message::VoteResponse {
            term: 1,
            granted: false
        }
    );
}

#[test]
fn a_leader_steps_down_on_seeing_a_higher_term() {
    let (mut n, _s) = boot(0, 3);
    n.tick(100);
    win(&mut n, 101, &[1]);
    n.handle(
        102,
        2,
        Message::AppendResponse {
            term: 9,
            success: false,
            match_index: 0,
        },
    );
    assert_eq!(
        (n.role(), n.term(), n.voted_for()),
        (Role::Follower, 9, None)
    );
}

#[test]
fn a_follower_rejects_a_mismatched_prev_and_hints_where_to_resume() {
    let (mut n, _s) = boot(1, 3);
    // Empty log, leader claims prev_index 5.
    let r = only(n.handle(1, 0, append(1, 5, 1, vec![], 0)));
    assert_eq!(
        r,
        Message::AppendResponse {
            term: 1,
            success: false,
            match_index: 0
        }
    );
    // Have one entry of term 1; leader says entry 1 has term 2.
    n.handle(2, 0, append(1, 0, 0, vec![e(1, b"a")], 0));
    let r = only(n.handle(3, 0, append(2, 1, 2, vec![], 0)));
    assert_eq!(
        r,
        Message::AppendResponse {
            term: 2,
            success: false,
            match_index: 0
        }
    );
}

#[test]
fn a_conflicting_suffix_is_truncated_and_replaced() {
    let (mut n, s) = boot(1, 3);
    n.handle(
        1,
        0,
        append(1, 0, 0, vec![e(1, b"a"), e(1, b"b"), e(1, b"c")], 0),
    );
    let r = only(n.handle(2, 2, append(2, 1, 1, vec![e(2, b"X")], 0)));
    assert_eq!(
        r,
        Message::AppendResponse {
            term: 2,
            success: true,
            match_index: 2
        }
    );
    assert_eq!(n.log(), &[e(1, b"a"), e(2, b"X")]);
    assert_eq!(s.snapshot().log, n.log(), "storage mirrors the log");
}

#[test]
fn a_stale_or_duplicated_append_never_deletes_valid_entries() {
    let (mut n, s) = boot(1, 3);
    let full = append(1, 0, 0, vec![e(1, b"a"), e(1, b"b"), e(1, b"c")], 0);
    n.handle(1, 0, full.clone());
    // An older, shorter resend of the same prefix arrives late.
    let r = only(n.handle(2, 0, append(1, 0, 0, vec![e(1, b"a")], 0)));
    assert_eq!(
        n.log(),
        &[e(1, b"a"), e(1, b"b"), e(1, b"c")],
        "the shorter resend must not truncate the longer, matching log"
    );
    assert_eq!(s.snapshot().log.len(), 3, "nor may storage be truncated");
    assert!(matches!(
        r,
        Message::AppendResponse {
            success: true,
            match_index: 1,
            ..
        }
    ));
    n.handle(3, 0, full);
    assert_eq!(n.log().len(), 3);
}

#[test]
fn a_follower_never_commits_past_what_the_leader_verified() {
    let (mut n, _s) = boot(1, 3);
    n.handle(1, 0, append(1, 0, 0, vec![e(1, b"a"), e(1, b"b")], 9));
    assert_eq!(
        n.commit_index(),
        2,
        "min(leader_commit, last verified index)"
    );
    n.handle(2, 0, append(1, 2, 1, vec![e(1, b"c")], 1));
    assert_eq!(n.commit_index(), 2, "commit index never moves backwards");
    assert_eq!(
        n.take_committed()
            .iter()
            .map(|(i, _)| *i)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(
        n.take_committed().is_empty(),
        "each committed entry is delivered once"
    );
}

#[test]
fn a_valid_append_ends_a_candidacy() {
    let (mut n, _s) = boot(1, 3);
    n.tick(100);
    assert_eq!(n.role(), Role::Candidate);
    n.handle(101, 0, append(1, 0, 0, vec![], 0));
    assert_eq!((n.role(), n.leader_id()), (Role::Follower, Some(0)));
}

#[test]
fn a_stale_term_append_is_rejected_without_changing_state() {
    let (mut n, _s) = boot(1, 3);
    n.handle(1, 0, vote_req(4, 0, 0));
    let r = only(n.handle(2, 2, append(2, 0, 0, vec![e(2, b"z")], 0)));
    assert_eq!(
        r,
        Message::AppendResponse {
            term: 4,
            success: false,
            match_index: 0
        }
    );
    assert!(n.log().is_empty());
}

#[test]
fn a_leader_backs_up_using_the_followers_hint_then_catches_up() {
    let (mut n, _s) = boot(0, 3);
    n.tick(100);
    win(&mut n, 101, &[1]);
    for _ in 0..3 {
        n.propose(102, b"x".to_vec()).unwrap();
    }
    // Follower 2 has nothing: rejects prev_index 3 with hint 0.
    let out = n.handle(
        103,
        2,
        Message::AppendResponse {
            term: 1,
            success: false,
            match_index: 0,
        },
    );
    let Message::AppendEntries {
        prev_index,
        entries,
        ..
    } = only(out)
    else {
        panic!()
    };
    assert_eq!(prev_index, 0);
    assert_eq!(entries.len(), 4, "no-op plus three proposals in one batch");
}

#[test]
fn only_entries_from_the_current_term_commit_by_counting_figure_8() {
    // Node 0 holds [t1, t2] as its log at term 2, in a 5-node cluster.
    let s = MemStorage::new();
    let mut seed = s.clone();
    seed.save_hard_state(2, None);
    seed.append(&[e(1, b"a"), e(2, b"b")]);
    let mut n = Node::new(cfg(0, 5), Box::new(s), 0);
    n.tick(100);
    assert_eq!(n.term(), 3);
    win(&mut n, 101, &[1, 2]);
    assert_eq!(n.role(), Role::Leader);
    assert_eq!(n.log().len(), 3, "log [t1, t2, no-op t3]");

    // Three followers now hold index 2 (a term-2 entry): a majority stores
    // it, but it is not from the leader's term, so it must NOT commit.
    for f in [1, 2, 3] {
        n.handle(
            102,
            f,
            Message::AppendResponse {
                term: 3,
                success: true,
                match_index: 2,
            },
        );
    }
    assert_eq!(n.commit_index(), 0);

    // Once the term-3 no-op reaches a majority, everything before it commits.
    for f in [1, 2] {
        n.handle(
            103,
            f,
            Message::AppendResponse {
                term: 3,
                success: true,
                match_index: 3,
            },
        );
    }
    assert_eq!(n.commit_index(), 3);
}

#[test]
fn a_stale_success_reply_cannot_move_match_index_backwards() {
    let (mut n, _s) = boot(0, 3);
    n.tick(100);
    win(&mut n, 101, &[1]);
    n.propose(102, b"x".to_vec()).unwrap();
    n.handle(
        103,
        1,
        Message::AppendResponse {
            term: 1,
            success: true,
            match_index: 2,
        },
    );
    assert_eq!(n.commit_index(), 2);
    n.handle(
        104,
        1,
        Message::AppendResponse {
            term: 1,
            success: true,
            match_index: 1,
        },
    );
    assert_eq!(n.commit_index(), 2);
}

#[test]
fn propose_on_a_non_leader_is_refused_with_a_leader_hint() {
    let (mut n, _s) = boot(1, 3);
    assert_eq!(n.propose(0, vec![1]), Err(ProposeError::NotLeader(None)));
    n.handle(1, 0, append(1, 0, 0, vec![], 0));
    assert_eq!(n.propose(2, vec![1]), Err(ProposeError::NotLeader(Some(0))));
}

#[test]
fn a_leader_sends_heartbeats_on_schedule() {
    let (mut n, _s) = boot(0, 3);
    n.tick(100);
    win(&mut n, 101, &[1]);
    assert!(n.tick(105).is_empty());
    assert_eq!(n.tick(111).len(), 2);
    assert!(n.tick(112).is_empty());
}

#[test]
fn a_single_node_cluster_elects_itself_and_commits_immediately() {
    let (mut n, _s) = boot(0, 1);
    assert!(n.tick(100).is_empty());
    assert_eq!(n.role(), Role::Leader);
    assert_eq!(n.commit_index(), 1, "its own no-op");
    let (idx, out) = n.propose(101, b"go".to_vec()).unwrap();
    assert!((idx, out.len(), n.commit_index()) == (2, 0, 2));
}

#[test]
fn election_timeouts_are_deterministic_per_seed_and_differ_across_nodes() {
    let timeouts = |seed: u64, id: NodeId| {
        let mut c = Config::new(id, 3, seed);
        c.election_timeout = (100, 200);
        let mut n = Node::new(c, Box::new(MemStorage::new()), 0);
        (0..400).find(|&t| !n.tick(t).is_empty()).unwrap()
    };
    assert_eq!(timeouts(7, 0), timeouts(7, 0));
    let all: Vec<u64> = (0..3).map(|i| timeouts(7, i)).collect();
    assert!(
        all.iter().any(|t| *t != all[0]),
        "nodes should not all time out together: {all:?}"
    );
    assert!(all.iter().all(|t| (100..=200).contains(t)));
}

#[test]
fn messages_from_self_or_an_unknown_node_are_ignored() {
    let (mut n, _s) = boot(0, 3);
    assert!(n.handle(1, 0, vote_req(9, 0, 0)).is_empty());
    assert!(n.handle(1, 7, vote_req(9, 0, 0)).is_empty());
    assert_eq!(n.term(), 0);
}
