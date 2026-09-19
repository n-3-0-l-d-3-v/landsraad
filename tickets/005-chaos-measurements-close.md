---
status: done
phase: 7
---

# 005 — Chaos runner, measurements, phase close

## Scope
- `landsraad-chaos` binary: many seeds, prints violations with a replayable seed.
- Measurements: election time in ticks, commit latency, behaviour across fault intensities.
- ADR, README, phase closure.

## Done
- [x] `landsraad-chaos` with reproducible seeds, --churn, --measure
- [x] Large-budget hunt: unpersisted-vote bug caught (seed 823); Figure 8 not found by random search (scripted test covers it)
- [x] Simulator clock-staleness bug found by measurement, fixed, regression-tested
- [x] Measurements; ADR-005; Phase 7 closed
