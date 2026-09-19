//! Liveness, real-disk recovery and the replicated key-value store
//! (ticket 004).
//!
//! Liveness is not a theorem of Raft under arbitrary faults (an adversary
//! can starve elections forever), so it is stated the way it can honestly
//! be: once injected chaos stops (partitions healed, crashed nodes
//! restarted), a leader emerges and new commands commit on every node
//! within a bound, across many seeds.

use channel::FaultProfile;
use kv::{replay, Command};
use proptest::prelude::*;
use sim::{Cluster, ClusterConfig, Faults, StorageKind};

const HOSTILE: FaultProfile = FaultProfile {
    loss: 0.15,
    duplication: 0.1,
    reorder_max_delay: 15,
    corruption: 0.03,
    truncation: 0.03,
    base_delay: 2,
};

const CHAOS: Faults = Faults {
    crash: 0.004,
    restart: 0.02,
    partition: 0.004,
    heal: 0.02,
    crash_after_event: 0.001,
    propose: 0.05,
};

fn marker_committed_everywhere(c: &Cluster, marker: &[u8]) -> bool {
    (0..c.node_count()).all(|i| {
        c.node(i).is_some_and(|n| {
            n.log()[..n.commit_index() as usize]
                .iter()
                .any(|e| e.command == marker)
        })
    })
}

/// Proposes `marker` (retrying if a leader change swallows it) and returns
/// how many ticks it took to commit on every node, or None past `limit`.
fn ticks_to_commit_everywhere(c: &mut Cluster, marker: &[u8], limit: u64) -> Option<u64> {
    let start = c.now();
    while c.now() - start < limit {
        let in_a_log = (0..c.node_count())
            .filter_map(|i| c.node(i))
            .any(|n| n.log().iter().any(|e| e.command == marker));
        if !in_a_log && (c.now() - start).is_multiple_of(50) {
            let _ = c.propose(marker.to_vec());
        }
        c.step().unwrap();
        if marker_committed_everywhere(c, marker) {
            return Some(c.now() - start);
        }
    }
    None
}

fn liveness_run(profile: FaultProfile, seed: u64, chaos_ticks: u64, limit: u64) -> Option<u64> {
    let mut cfg = ClusterConfig::new(if seed.is_multiple_of(2) { 3 } else { 5 }, seed);
    cfg.profile = profile;
    cfg.faults = CHAOS;
    let mut c = Cluster::new(cfg);
    c.run(chaos_ticks).unwrap();
    c.quiesce().unwrap();
    ticks_to_commit_everywhere(&mut c, b"after-the-storm", limit)
}

#[test]
fn after_chaos_on_a_clean_network_a_command_commits_everywhere_quickly() {
    let mut worst = 0;
    for seed in 0..40 {
        let t = liveness_run(FaultProfile::CLEAN, seed, 3000, 1500)
            .unwrap_or_else(|| panic!("seed {seed}: no recovery within 1500 ticks"));
        worst = worst.max(t);
    }
    eprintln!("clean network: worst recovery = {worst} ticks");
}

#[test]
fn after_chaos_despite_residual_loss_a_command_still_commits_everywhere() {
    let mut worst = 0;
    for seed in 0..40 {
        let t = liveness_run(HOSTILE, seed, 3000, 6000)
            .unwrap_or_else(|| panic!("seed {seed}: no recovery within 6000 ticks"));
        worst = worst.max(t);
    }
    eprintln!("15% loss network: worst recovery = {worst} ticks");
}

fn arb_command() -> impl Strategy<Value = Command> {
    let key = prop::collection::vec(0u8..5, 1..3);
    prop_oneof![
        3 => (key.clone(), prop::collection::vec(any::<u8>(), 0..5))
            .prop_map(|(key, value)| Command::Put { key, value }),
        1 => key.prop_map(|key| Command::Delete { key }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn replicated_kv_matches_a_hashmap_in_a_clean_cluster(
        cmds in prop::collection::vec(arb_command(), 0..40),
        seed in any::<u64>(),
    ) {
        let mut c = Cluster::new(ClusterConfig::new(3, seed));
        while c.leader().is_none() {
            c.step().unwrap();
        }
        let mut model = std::collections::HashMap::new();
        for cmd in &cmds {
            match cmd {
                Command::Put { key, value } => { model.insert(key.clone(), value.clone()); }
                Command::Delete { key } => { model.remove(key); }
            }
            let (l, idx) = c.propose(cmd.encode()).unwrap();
            while c.node(l).unwrap().commit_index() < idx {
                c.step().unwrap();
            }
        }
        c.run(400).unwrap();
        for i in 0..3 {
            let n = c.node(i).unwrap();
            let store = replay(&n.log()[..n.commit_index() as usize]);
            prop_assert_eq!(store.len(), model.len(), "node {}", i);
            for (k, v) in &model {
                prop_assert_eq!(store.get(k), Some(v.as_slice()), "node {} key {:?}", i, k);
            }
        }
    }
}

#[test]
fn every_node_converges_to_the_same_kv_state_after_chaos() {
    for seed in 0..12u64 {
        let mut cfg = ClusterConfig::new(3, seed);
        cfg.profile = HOSTILE;
        cfg.faults = Faults {
            propose: 0.0,
            ..CHAOS
        };
        let mut c = Cluster::new(cfg);
        for t in 0..4000u64 {
            if t % 7 == 0 {
                let cmd = Command::Put {
                    key: format!("k{}", t % 9).into_bytes(),
                    value: format!("v{t}").into_bytes(),
                };
                let _ = c.propose(cmd.encode());
            }
            c.step().unwrap();
        }
        c.quiesce().unwrap();
        ticks_to_commit_everywhere(&mut c, b"sync", 8000)
            .unwrap_or_else(|| panic!("seed {seed}: did not converge"));
        c.run(500).unwrap();
        let global = replay(c.committed());
        assert!(
            !global.is_empty(),
            "seed {seed}: chaos should have committed some writes"
        );
        for i in 0..3 {
            let n = c.node(i).unwrap();
            let mine = replay(&n.log()[..n.commit_index() as usize]);
            assert_eq!(mine, global, "seed {seed}: node {i} diverged");
        }
    }
}

#[test]
fn a_cluster_on_real_disks_survives_crashes_and_a_full_restart() {
    let dir = tempfile::tempdir().unwrap();
    let make = |seed: u64| {
        let mut cfg = ClusterConfig::new(3, seed);
        cfg.storage = StorageKind::Sietch(dir.path().to_path_buf());
        cfg
    };

    let mut cfg = make(5);
    cfg.faults = Faults {
        crash: 0.003,
        restart: 0.02,
        crash_after_event: 0.002,
        propose: 0.0,
        ..Faults::NONE
    };
    let mut c = Cluster::new(cfg);
    for t in 0..2500u64 {
        if t % 10 == 0 {
            let cmd = Command::Put {
                key: format!("k{}", t % 5).into_bytes(),
                value: format!("v{t}").into_bytes(),
            };
            let _ = c.propose(cmd.encode());
        }
        c.step().unwrap(); // durability and safety are checked on every restart and tick
    }
    assert!(
        c.stats().crashes > 3,
        "the test must really crash nodes: {:?}",
        c.stats()
    );
    c.quiesce().unwrap();
    ticks_to_commit_everywhere(&mut c, b"pre-shutdown", 4000).expect("recovers on real disks");
    let before = c.committed().to_vec();
    let state_before = replay(&before);
    drop(c); // every node and every open store goes away

    // Cold start: brand-new nodes, brand-new checker, same directories.
    let mut c = Cluster::new(make(6));
    ticks_to_commit_everywhere(&mut c, b"post-restart", 4000).expect("recovers after a cold start");
    assert_eq!(
        &c.committed()[..before.len()],
        &before[..],
        "everything committed before the shutdown is still committed, in order"
    );
    let state_after = replay(&c.committed()[..before.len()]);
    assert_eq!(state_after, state_before);
}
