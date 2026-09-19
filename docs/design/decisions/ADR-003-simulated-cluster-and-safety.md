# ADR-003: A deterministic simulated cluster over distrans channels, with continuous invariant checking

## Status
Accepted

## Decisions
- **One distrans `Channel` per directed link**, carrying real distrans frames. Loss, duplication, reordering, corruption and truncation come straight from Phase 5's fault model, and corrupted datagrams are rejected by the frame CRC (counted in `Stats::rejected_by_codec`) before consensus sees them. Time is virtual ticks; a run is reproducible from `(seed, profile, faults)` (tested: two runs with the same config produce identical stats, committed logs and channel statistics).
- **Crashes are honest.** A crash drops the node and keeps its `MemStorage` (a disk that survives), and a restart rebuilds the node from storage. Crashes happen between events, or (`crash_after_event`) immediately after a handler ran and persisted but before its messages were sent, which is the nastiest realistic timing.
- **Partitions are directional link cuts** applied at send time; datagrams already in flight still arrive, as on a real network.
- **An invariant checker runs after every tick:** election safety (one leader per term), state-machine safety (every node's committed prefix agrees with a single global committed log that only grows), leader completeness (a leader holds every entry committed in an earlier term), log matching (same index and term implies identical prefixes), term monotonicity across restarts, and durability (a restarted node still has everything it had committed). A violation is a typed error carrying the tick.

## Testing
Scripted: clean cluster commits five commands on all three nodes; an isolated leader's entries never commit and are overwritten after healing while a new leader (higher term) serves clients; crashing and restarting every node loses no committed entry; heavy crash-after-persist. Non-vacuity: hostile runs must actually make progress (hundreds of commits) and actually inject drops, duplicates and corruption. Property test: 48 random (seed, size 3 or 5, fault intensities up to 35% loss / 30% duplication / 15% corruption / partitions / crashes) x 3000 ticks, no violation. Churn test: 100 seeds with short timeouts, frequent crashes and partitions.

## Does the checker actually detect bugs? (five injected Raft bugs)
| Injected bug | Detected by the simulator? |
|---|---|
| Grant votes ignoring the up-to-date-log rule | Yes: leader completeness violation, e.g. "leader 2 (term 6) lacks committed entry 136" |
| Grant more than one vote per term | Yes: election safety, "nodes 1 and 2 both lead term 1" |
| Truncate the log on any overlap | Yes: leader completeness violation |
| Commit older-term entries by counting (Figure 8) | **No**, not in 48 property cases, 8 hostile seeds, or 300 churn-heavy seeds |
| Do not persist a granted vote | **No**, same budget |

The last two are honest gaps in *randomized* search, not in coverage overall: both are caught deterministically by ticket 002's scripted node tests (the Figure 8 scenario and the restarted-node double-vote test). Each needs a specific interleaving (five nodes, a specific crash, two same-term candidates, a voter crashing between votes) that random schedules hit too rarely at this budget. Ticket 005's chaos runner is where a much larger budget is spent and reported.

## Limitations
Storage in the simulator is in-memory (real-disk crash recovery is ticket 004). No Byzantine faults. The checker observes committed state through node commit indices, so it can only judge what nodes report.
