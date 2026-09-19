# Scope — landsraad

## CORE
- Raft-style leader election and log replication over distrans's hostile channel, persisted on sietch, in virtual time.
- Safety invariants checked continuously under partitions, crashes, loss, duplication, reordering and corruption.

## EXTENSION
- Replicated key-value state machine; chaos runner; measurements.

## EXPERIMENT (only if CORE and EXTENSION are healthy)
- Membership changes, log compaction/snapshots, linearizable reads.
