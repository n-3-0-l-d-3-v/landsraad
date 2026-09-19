//! Safety under chaos (ticket 003). Scripted scenarios first, then a property
//! test over arbitrary seeds, cluster sizes and fault intensities: no
//! invariant may ever be violated, however hostile the run.

use channel::FaultProfile;
use proptest::prelude::*;
use raft::Role;
use sim::{Cluster, ClusterConfig, Faults};

fn wait_for_leader(c: &mut Cluster, max_ticks: u64) -> usize {
    for _ in 0..max_ticks {
        c.step().unwrap();
        if let Some(l) = c.leader() {
            return l;
        }
    }
    panic!("no leader within {max_ticks} ticks");
}

#[test]
fn a_clean_cluster_elects_a_leader_and_commits_client_commands() {
    let mut c = Cluster::new(ClusterConfig::new(3, 1));
    let leader = wait_for_leader(&mut c, 1000);
    for i in 0..5 {
        c.propose(format!("v{i}").into_bytes()).unwrap();
    }
    c.run(500).unwrap();
    let committed: Vec<&[u8]> = c.committed().iter().map(|e| e.command.as_slice()).collect();
    assert_eq!(
        committed.len(),
        6,
        "no-op plus five commands: {committed:?}"
    );
    assert_eq!(committed[1], b"v0");
    for i in 0..3 {
        assert_eq!(
            c.node(i).unwrap().commit_index(),
            6,
            "node {i} (leader is {leader})"
        );
    }
    assert_eq!(
        (0..3)
            .filter(|&i| c.node(i).unwrap().role() == Role::Leader)
            .count(),
        1
    );
}

#[test]
fn the_same_seed_reproduces_the_same_run() {
    let run = || {
        let mut cfg = ClusterConfig::new(5, 42);
        cfg.profile = FaultProfile {
            loss: 0.1,
            duplication: 0.1,
            reorder_max_delay: 20,
            corruption: 0.05,
            truncation: 0.05,
            base_delay: 2,
        };
        cfg.faults = Faults {
            crash: 0.002,
            restart: 0.01,
            partition: 0.003,
            heal: 0.01,
            crash_after_event: 0.0005,
            propose: 0.05,
        };
        let mut c = Cluster::new(cfg);
        c.run(4000).unwrap();
        (c.stats(), c.committed().to_vec(), c.channel_stats())
    };
    assert_eq!(run(), run());
}

#[test]
fn an_isolated_leader_cannot_commit_and_its_entries_are_overwritten_after_healing() {
    let mut c = Cluster::new(ClusterConfig::new(3, 7));
    let old = wait_for_leader(&mut c, 1000);
    c.propose(b"before".to_vec()).unwrap();
    c.run(300).unwrap();
    let committed_before = c.committed().len();
    assert!(committed_before >= 2);

    c.isolate(old);
    // The stranded leader still accepts writes but can never commit them.
    c.propose(b"doomed".to_vec()).ok();
    c.run(1500).unwrap();
    let new = (0..3)
        .filter(|&i| i != old)
        .find(|&i| c.node(i).unwrap().role() == Role::Leader)
        .expect("the majority side elects a new leader");
    assert!(c.node(new).unwrap().term() > c.node(old).unwrap().term());
    assert!(
        !c.committed().iter().any(|e| e.command == b"doomed"),
        "an entry only the isolated leader had must never commit"
    );
    c.propose(b"after".to_vec()).unwrap();
    c.run(300).unwrap();

    c.heal();
    c.run(1000).unwrap();
    for i in 0..3 {
        let log = c.node(i).unwrap().log();
        assert!(
            !log.iter().any(|e| e.command == b"doomed"),
            "node {i} kept a doomed entry"
        );
        assert!(
            log.iter().any(|e| e.command == b"after"),
            "node {i} missed a committed entry"
        );
    }
    assert_eq!(c.node(old).unwrap().role(), Role::Follower);
}

#[test]
fn committed_entries_survive_crashing_every_node() {
    let mut c = Cluster::new(ClusterConfig::new(3, 3));
    wait_for_leader(&mut c, 1000);
    for i in 0..4 {
        c.propose(format!("k{i}").into_bytes()).unwrap();
    }
    c.run(400).unwrap();
    let before = c.committed().len();
    assert!(before >= 5);
    for i in 0..3 {
        c.crash(i);
    }
    c.run(50).unwrap();
    for i in 0..3 {
        c.restart(i).unwrap(); // the durability check runs inside restart
    }
    wait_for_leader(&mut c, 2000);
    c.propose(b"post-crash".to_vec()).unwrap();
    c.run(500).unwrap();
    assert!(c.committed().len() > before);
    assert_eq!(c.committed()[..before].len(), before);
}

#[test]
fn a_crash_between_persisting_and_sending_never_breaks_safety() {
    let mut cfg = ClusterConfig::new(3, 11);
    cfg.faults = Faults {
        crash_after_event: 0.05,
        restart: 0.05,
        propose: 0.05,
        ..Faults::NONE
    };
    let mut c = Cluster::new(cfg);
    c.run(6000).unwrap();
    assert!(c.stats().crashes > 20, "the test must actually crash nodes");
}

#[test]
fn hostile_runs_still_make_progress_so_the_safety_checks_are_not_vacuous() {
    let mut total_commits = 0;
    let mut total_terms = 0;
    for seed in 0..8u64 {
        let mut cfg = ClusterConfig::new(3, seed);
        cfg.profile = FaultProfile {
            loss: 0.1,
            duplication: 0.1,
            reorder_max_delay: 15,
            corruption: 0.03,
            truncation: 0.03,
            base_delay: 2,
        };
        cfg.faults = Faults {
            crash: 0.001,
            restart: 0.01,
            partition: 0.002,
            heal: 0.01,
            crash_after_event: 0.0002,
            propose: 0.05,
        };
        let mut c = Cluster::new(cfg);
        c.run(8000).unwrap();
        total_commits += c.committed().len();
        total_terms += c.stats().terms_with_a_leader;
        let s = c.channel_stats();
        assert!(
            s.dropped > 0 && s.duplicated > 0 && s.corrupted > 0,
            "{s:?}"
        );
    }
    assert!(total_commits > 200, "commits: {total_commits}");
    assert!(total_terms >= 8);
}

/// Short election timeouts, frequent crashes (including right after a vote is
/// granted, before the reply is sent) and partitions, lots of proposals, many
/// seeds: the interleavings that expose unpersisted votes and the "commit an
/// older-term entry by counting" bug of Raft's Figure 8.
#[test]
fn heavy_churn_across_many_seeds_never_violates_safety() {
    let mut commits = 0;
    for seed in 0..100u64 {
        let mut cfg = ClusterConfig::new(if seed % 3 == 0 { 5 } else { 3 }, seed);
        cfg.election_timeout = (20, 45);
        cfg.heartbeat_interval = 6;
        cfg.profile = FaultProfile {
            loss: 0.15,
            duplication: 0.1,
            reorder_max_delay: 12,
            corruption: 0.02,
            truncation: 0.02,
            base_delay: 1,
        };
        cfg.faults = Faults {
            crash: 0.01,
            restart: 0.06,
            partition: 0.02,
            heal: 0.05,
            crash_after_event: 0.01,
            propose: 0.2,
        };
        let mut c = Cluster::new(cfg);
        if let Err(v) = c.run(3000) {
            panic!("seed {seed}: {v}");
        }
        commits += c.committed().len();
    }
    assert!(commits > 300, "commits: {commits}");
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn no_invariant_is_ever_violated_under_arbitrary_faults(
        seed in any::<u64>(),
        n in prop::sample::select(vec![3usize, 5]),
        loss in 0.0f64..0.35,
        dup in 0.0f64..0.3,
        reorder in 0u64..40,
        corrupt in 0.0f64..0.15,
        trunc in 0.0f64..0.15,
        crash in 0.0f64..0.01,
        partition in 0.0f64..0.01,
        after_event in 0.0f64..0.002,
    ) {
        let mut cfg = ClusterConfig::new(n, seed);
        cfg.profile = FaultProfile {
            loss, duplication: dup, reorder_max_delay: reorder,
            corruption: corrupt, truncation: trunc, base_delay: 1,
        };
        cfg.faults = Faults {
            crash, restart: 0.02, partition, heal: 0.02,
            crash_after_event: after_event, propose: 0.05,
        };
        let mut c = Cluster::new(cfg);
        prop_assert_eq!(c.run(3000), Ok(()));
    }
}
