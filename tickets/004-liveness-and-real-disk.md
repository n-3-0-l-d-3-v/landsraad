---
status: done
phase: 7
---

# 004 — Liveness after healing, real-disk crash recovery, replicated KV

## Scope
- Liveness: after faults stop and the network heals, a leader emerges and new commands commit on every node within a bounded number of ticks.
- Sietch-backed cluster: crash and restart nodes against real on-disk storage.
- A small replicated key-value state machine applied from committed entries; differential test against a serial reference model.

## Done
- [x] Bounded-recovery liveness tests (clean and 15%-loss networks), measured
- [x] Sietch-backed cluster: chaos on real disks and a cold restart of the whole cluster
- [x] `crates/kv` state machine; differential tests against a HashMap and cross-node convergence; mutation-checked
- [x] ADR-004
