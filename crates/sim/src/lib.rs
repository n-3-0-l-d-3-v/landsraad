//! A deterministic simulated cluster (ticket 003): N `raft::Node`s wired by
//! one distrans `Channel` per directed link, with partitions, crashes and
//! restarts, all in virtual time and reproducible from a seed. After every
//! tick an invariant checker verifies Raft's safety properties; any
//! violation is reported with the tick it happened on.
//!
//! What is simulated and what is not: the channel faults (loss, duplication,
//! reordering, corruption, truncation) are distrans's real fault model;
//! messages are real distrans frames; node storage is real
//! (`raft::MemStorage` survives a crash, exactly as a disk would). Crashes
//! happen between events, or immediately after an event's handler ran but
//! before its outgoing messages were sent (state persisted, messages lost).

mod cluster;
mod rng;

pub use cluster::{Cluster, ClusterConfig, Faults, Stats, Violation};
