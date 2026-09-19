# LANDSRAAD — THE COLONY

> A 3-node replicated, consensus-driven distributed store with no shared memory or global clock.

## Why "LANDSRAAD"

The governing council of every Great House in the Imperium — independent, mutually distrustful parties who nonetheless have to reach binding collective decisions. That is distributed consensus described politically instead of technically: many nodes, no shared trust, still forced to agree.

Part of **[ARRAKIS](https://github.com/n-3-0-l-d-3-v/arrakis)** — a constrained computing
ecosystem built by removing assumptions ordinary computers depend on. This
repository is developed standalone and mirrored into the combined ecosystem
repo commit-for-commit.

## Status

**Phase 7 — COMPLETE.** A Raft-style consensus stack in virtual time, over
distrans's hostile channel, persisted on sietch:
- `raft`: message codec inside distrans frames (garbage never panics), a
  `Storage` trait with in-memory and real sietch implementations, and a
  deterministic I/O-free `Node` (elections, replication, Figure 8 commit rule).
- `sim`: a cluster of nodes over per-link distrans channels with partitions and
  crash/restart (including crash after persisting, before sending), plus a
  checker for election safety, log matching, leader completeness, state-machine
  safety and durability, run every tick.
- `kv`: a replicated key-value state machine, differentially tested.
- `landsraad-chaos`: reproducible randomized fault schedules
  (`cargo run --release -p sim --bin landsraad-chaos -- --runs 200`) and
  `--measure` for latency distributions.

Honest results: the checker caught 4 of 5 deliberately injected Raft bugs
(one only at a 1,500-run budget); the fifth, Raft's Figure 8 commit bug, is
caught by a scripted test but not by random search. Measuring exposed a real
simulator bug (stale link clocks), now fixed. See
[ADR-005](docs/design/decisions/ADR-005-chaos-runner-and-measurements.md).

See [tickets/](tickets/) for the live phase-by-phase ticket board and
[docs/design/](docs/design/) for constraints, invariants and architecture
decision records.

## The constraint

Nodes communicate only over distrans, storage is still append-only, there is no global clock, and any node may crash at any time.

## What the constraint forces

Leader election, log replication, quorum commitment, and recovery that never silently produces an invalid committed state.

## Research question

> How does consensus behave when storage is immutable, messages are unordered/lossy, and wall-clock time is unavailable?

## Sibling repositories

- [mentat](https://github.com/n-3-0-l-d-3-v/mentat) — THE MACHINE (COMPLETE)
- [chakobsa](https://github.com/n-3-0-l-d-3-v/chakobsa) — THE LANGUAGE (QUEUED)
- [muaddib](https://github.com/n-3-0-l-d-3-v/muaddib) — THE KERNEL (QUEUED)
- [sietch](https://github.com/n-3-0-l-d-3-v/sietch) — THE VAULT (ACTIVE)
- [choam](https://github.com/n-3-0-l-d-3-v/choam) — THE DATABASE (QUEUED)
- [distrans](https://github.com/n-3-0-l-d-3-v/distrans) — THE WIRE (QUEUED)
- [ghola](https://github.com/n-3-0-l-d-3-v/ghola) — THE HISTORY (QUEUED)
- [shai-hulud](https://github.com/n-3-0-l-d-3-v/shai-hulud) — THE ARTIFACT (STRETCH)

## Development

This is a real, tested, benchmarked systems component — not a demo. See
[docs/DEFINITION_OF_DONE.md](docs/DEFINITION_OF_DONE.md) for the acceptance
bar every piece of this repo must clear before it is considered complete.

```bash
cargo build
cargo test
cargo bench
```
