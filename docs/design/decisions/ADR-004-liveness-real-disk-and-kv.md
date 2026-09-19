# ADR-004: Liveness stated honestly, real-disk recovery, and a replicated key-value store

## Status
Accepted

## Decisions
- **Liveness is a bounded-recovery claim, not a theorem.** Raft cannot guarantee progress under arbitrary faults (an adversary can starve elections indefinitely), so the tests state what can be honestly measured: after injected chaos stops (`Cluster::quiesce`: partitions healed, every crashed node restarted, no more random events), a leader emerges and a fresh command commits on *every* node within a bound, over 40 seeds (3 and 5 node clusters). Two settings: a clean network, and one that keeps losing 15% of datagrams, duplicating 10%, reordering and corrupting after the chaos ends.
  - Clean network: worst recovery observed **257 ticks** (election timeouts are 150-300 ticks, heartbeat 50).
  - 15% loss / 10% duplication / reordering / corruption: worst recovery observed **1,253 ticks**, bound asserted at 6,000.
  Both are single measured runs of a deterministic simulator, not proofs; they show the bound holds with a wide margin over 40 seeds each.
- **Real disks.** `StorageKind::Sietch` gives every node its own `sietch::Store` directory. A crash drops the node and closes its store; a restart reopens the directory from disk, exercising sietch's real recovery path. The test runs chaos (crashes, including crash-after-persist, and restarts) on real stores with the invariant checker on, then drops the *entire* cluster, builds a brand-new one (new nodes, new checker) over the same directories, and requires that everything committed before the shutdown is still committed, in order, with an identical key-value state.
- **A replicated key-value store** (`crates/kv`): `Put`/`Delete` commands with a compact encoding; `apply` is deterministic and total (empty commands, which include the leadership no-op, and malformed ones are ignored identically on every node); `replay(log)` is a pure fold. Properties: encoding round-trips, decoding arbitrary bytes never panics, replay equals a `HashMap` model, and replay composes over any prefix/suffix split (incremental apply equals full replay).
- **Differential tests.** A clean cluster given a random command sequence ends on every node with exactly the `HashMap` model's contents (24 cases). After chaos on a hostile network, every node's key-value state equals the replay of the global committed log (12 seeds, 4,000 ticks of churn each).

## Findings
No consensus bugs surfaced in this ticket; the tests passed once written. Reported as-is. The simulator does not model torn writes at the storage layer: sietch's own suite already injects corruption at every byte offset of a real log (Phase 2), and Raft's persist-before-reply discipline means only a write that was never acknowledged can be torn, which is equivalent to crashing before it happened.

## Limitations
Reads are not implemented as a linearizable operation (a client would propose a read through the log, or read the leader's applied state and accept staleness). No log compaction: a long-lived cluster's log and store grow without bound. No membership changes.
