---
status: open
phase: 7
---

# 002 — Leader election and log replication (the node state machine)

## Scope
- `raft::Node`: a deterministic, I/O-free state machine. Inputs: `tick(now)`, `handle(now, from, msg)`, `propose(now, cmd)`. Outputs: messages to send. Virtual ticks only; no wall clock.
- Randomized election timeouts from a seeded PRNG, heartbeats, term handling, vote granting with the up-to-date-log rule, log matching with conflict truncation, commit advancement restricted to current-term entries, a no-op entry at the start of each leadership.
- Persist-before-reply: all persistent state is saved before any message that depends on it is returned.
- Scripted unit tests for each rule, including Raft's Figure 8 scenario.
