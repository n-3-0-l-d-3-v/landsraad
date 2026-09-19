# ADR-001: Consensus messages inside distrans frames, and fail-stop durable storage on sietch

## Status
Accepted

## Decisions
- **Messages ride in distrans `Frame`s** (frame type = message kind). The CRC-32C from Phase 5 rejects corrupted or truncated datagrams before consensus sees them, so this layer only has to be robust against *well-formed frames with bad payloads*: unknown type, short payload, trailing bytes, non-0/1 booleans, and an entry count larger than the payload could hold (a declared count of 4 billion cannot force a huge allocation). All are typed errors; arbitrary bytes never panic (property-tested).
- **The sender is not in the message.** It is the link the datagram arrived on, supplied by the (simulated) network. This matches how distrans channels work and avoids trusting a spoofable field.
- **`Storage` is a trait** so the deterministic simulator can use an in-memory store whose clones share state (a crash drops the node, not the disk) while real tests use `SietchStorage`.
- **Fail-stop persistence.** A failed write to term, vote or log panics with an explicit message rather than returning an error the consensus code could ignore. A node that cannot record a promise must not keep making promises.
- **Sietch layout.** Hard state at key `h`; each log entry at `e` + big-endian index, so `scan` returns the log in order. Appends and multi-entry truncations are single `apply_batch` calls, so a crash leaves the old or the new state, never a mixture (sietch discards a torn tail on reopen). Load asserts the log is gap-free, so corruption fails loudly.

## Testing
Unit tests for every message, every single-byte flip and every truncation of every sample; property tests (512 cases) for round trip, arbitrary bytes, and any single bit flip being rejected; a storage property test (64 cases) applying arbitrary hard-state/append/truncate sequences to a reference model, the in-memory storage and the sietch storage, then reopening the sietch one from disk. Mutation-checked: an off-by-one in sietch truncation is caught.

## Research question (immutable storage)
Truncating a log is a *delete plus tombstones* on an append-only store, not an in-place shrink. Raft only truncates on a genuine conflict, so this is rare; measurements come in ticket 005.

## Limitations
No log compaction or snapshots (a log only grows), no membership changes.
