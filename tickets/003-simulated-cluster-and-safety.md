---
status: done
phase: 7
---

# 003 — Simulated cluster, fault injection, and safety invariants

## Scope
- `crates/sim`: N nodes wired by one distrans `Channel` per directed link (loss, duplication, reordering, corruption, truncation), plus network partitions and node crash/restart (in-memory state lost, storage kept), all in virtual time and reproducible from a seed.
- An invariant checker run on every tick: election safety, log matching, leader completeness, state-machine safety, committed-entry durability across crashes.
- Property test: for arbitrary seeds and fault schedules, no invariant is ever violated.

## Done
- [x] `crates/sim`: cluster over distrans channels, partitions, crash/restart (incl. crash after persist), seeded and reproducible
- [x] Six invariants checked every tick
- [x] Scripted scenarios, non-vacuity test, property test, churn test
- [x] Checker validated by injecting five real bugs: three caught, two not (reported honestly; unit tests catch both)
- [x] ADR-003
