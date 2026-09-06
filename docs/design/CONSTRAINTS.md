# Constraints — THE COLONY

## Primary constraint

Nodes communicate only over impossible-wire, storage is still append-only, there is no global clock, and any node may crash at any time.

## What it forces

Leader election, log replication, quorum commitment, and recovery that never silently produces an invalid committed state.

## Research question

How does consensus behave when storage is immutable, messages are unordered/lossy, and wall-clock time is unavailable?

## What is explicitly out of scope

See the root [SCOPE.md](../../SCOPE.md) for the CORE / EXTENSION / EXPERIMENT
classification that applies to this repo.
