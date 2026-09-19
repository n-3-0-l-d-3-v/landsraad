# ADR-002: A deterministic, I/O-free Raft node with persist-before-reply

## Status
Accepted

## Decisions
- **Pure state machine.** `Node` takes `tick(now)`, `handle(now, from, msg)` and `propose(now, cmd)` and returns the messages to send. No sockets, threads or clocks: time is a virtual tick the caller supplies, which is what lets the simulator (ticket 003) reproduce any run from a seed and is the direct answer to "no wall-clock time" in the phase constraint.
- **Persist before reply.** Term, vote and log writes happen inside the handler, before it returns any message that depends on them, so a crash can lose a message but never a promise (tested: a restarted node cannot vote twice in a term; storage is checked immediately after a granted vote).
- **Randomized election timeouts** come from a per-node SplitMix64 seeded from the config seed and node id, so timeouts are reproducible but differ across nodes (tested).
- **Commit only current-term entries by counting** (Raft's Figure 8 rule), which is why every new leader appends a no-op entry: without it a leader could not commit earlier-term entries until a client happened to write. The Figure 8 scenario is a scripted test.
- **Truncate only on a genuine conflict.** A stale or duplicated AppendEntries carrying a prefix of what the follower already has must not delete the longer, matching log. `match_index` on the leader only ever moves forward, so a late duplicated reply cannot regress it.
- **Fast backup.** A rejected AppendEntries carries a hint (the follower's last index, or `prev_index - 1` on a term mismatch) so the leader does not step back one entry per round trip; the leader clamps `next_index` above what the follower has already acknowledged.
- **Defensive input handling.** Messages from self or from outside the cluster are ignored; a message claiming `prev_index 0` with a non-zero `prev_term` cannot underflow (`saturating_sub`); a same-term AppendEntries reaching a leader trips a `debug_assert` because it would mean election safety is already broken, and the simulator's invariant check is designed to catch that independently.

## Testing
22 scripted tests, one per rule: candidacy and persisted self-vote, election, denied/stale votes, one vote per term, stale-term refusal, up-to-date-log rule (three cases), restart, step-down on higher term, mismatch and hints, conflict truncation, stale-append safety, commit clamping and once-only delivery, candidacy ending, leader backing up, Figure 8, stale success replies, propose refusal with leader hint, heartbeat schedule, single-node cluster, seeded timeouts, ignored senders.

Mutation-checked, three rules: allowing commit of any-term entries (caught by Figure 8), truncating on any overlap, and not persisting a granted vote.

**A weak test caught by its own mutation check.** The first version of the stale-append test resent the full log at the end, which healed the damage a buggy truncation had done, so the mutation went undetected. The test now asserts the log and storage immediately after the short resend. Reported because it is exactly the sort of test that looks fine until you break the code on purpose.

## Limitations
No snapshots or log compaction, no membership changes, no read-index / lease reads (reads would go through the log). Randomized timeouts make livelock unlikely, not impossible; liveness is measured, not proven (tickets 004-005).
