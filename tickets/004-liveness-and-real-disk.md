---
status: open
phase: 7
---

# 004 — Liveness after healing, real-disk crash recovery, replicated KV

## Scope
- Liveness: after faults stop and the network heals, a leader emerges and new commands commit on every node within a bounded number of ticks.
- Sietch-backed cluster: crash and restart nodes against real on-disk storage.
- A small replicated key-value state machine applied from committed entries; differential test against a serial reference model.
