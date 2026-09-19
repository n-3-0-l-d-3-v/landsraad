# ADR-005: A reproducible chaos runner, what a large budget found, and measurements

## Status
Accepted (closes Phase 7)

## Decisions
- **`landsraad-chaos`** runs many randomized fault schedules and prints any violation with the seed that replays it: `landsraad-chaos --seed S --runs 1 --ticks T [--churn]`. Cluster size (3 or 5), loss, duplication, reordering, corruption, truncation, crash, partition and crash-after-persist rates all derive from the seed. `--churn` uses short election timeouts and doubled crash/partition rates, the hardest setting. `--measure` prints latency distributions. Exits 1 on any violation.
- Baseline on correct code: 200 runs x 4,000 ticks, 0 violations, 54,954 entries committed, 905 leader terms, 3,773 crashes, 626,819 datagrams sent (84,255 dropped, 56,306 duplicated, 31,197 corrupted).

## What a large budget found (validating the checker, continued from ADR-003)
1,500 churn runs x 3,000 ticks = 4.5 million ticks, about 14,700 leader terms, 42,000 crashes, 4.6 million datagrams per hunt, against the two bugs the smaller budgets missed:
- **Vote not persisted: caught** (seed 823, tick 1408, "election safety: nodes 2 and 0 both lead term 15"). Replays deterministically from that seed on the broken code and passes on the correct code. About 1 run in 1,500, so the earlier 300-seed search was simply too small.
- **Commit older-term entries by counting (Raft's Figure 8): not found** at this budget. The anomaly needs a five-node interleaving (a leader replicates an old entry to a minority and crashes; a second leader overwrites its index; the first returns, counts a majority on the old entry, commits it, and crashes again; the second wins and overwrites a committed entry). Random schedules do not reach it. The scripted node test reproduces exactly that reasoning and does catch it. So the honest position: this bug is covered by a deterministic test, not by random search, and the simulator's random search should not be cited as evidence against it.

## Simulator bug found by measuring
Measuring propose-to-commit latency on a clean network with 1-tick links returned 1 tick; a round trip cannot take less than 2. Messages sent while handling an arrival were stamped with the destination link's stale clock (that link had not yet been advanced this tick), so a reply could arrive in the same tick it was sent. The fix advances every link's clock before any arrival is handled. All tests still passed on the fixed simulator; a regression test now pins the round trip at exactly 2 ticks. Latencies before the fix were under-reported by one tick.

## Measurements (virtual ticks, deterministic; election timeout 150-300, heartbeat 50; 200 clusters each)
| Network, nodes | First leader elected (median / p99 / max) | Propose -> committed at leader (median / p99 / max) |
|---|---|---|
| clean, 3 | 182 / 271 / 279 | 2 / 2 / 2 |
| clean, 5 | 171 / 247 / 279 | 2 / 2 / 2 |
| 10% loss, 5% dup, reorder, 3 | 192 / 409 / 470 | 9 / 19 / 86 |
| 10% loss, 5% dup, reorder, 5 | 182 / 434 / 448 | 10 / 18 / 26 |

Reading them: on a clean network a commit costs one round trip, and first election lands in the 150-300 timeout window as designed. Ten percent loss multiplies commit latency roughly fivefold at the median (retries wait for the next heartbeat or reply) and pushes worst-case election time to about 1.6x. Ticks are simulated time; no claim is made about wall-clock speed.

## Phase 7 summary and limitations
Election, replication, persistence on sietch, a replicated key-value store, invariant checking, a reproducible chaos runner, and measurements are done. Not built: log compaction/snapshots (logs and stores grow forever), membership changes, linearizable reads, and any non-simulated network (nodes talk only through distrans's simulated channel; there are no real sockets). Bounded recovery is measured, not proven.
