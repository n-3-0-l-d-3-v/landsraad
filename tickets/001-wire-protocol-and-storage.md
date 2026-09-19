---
status: open
phase: 7
---

# 001 — Consensus messages over distrans frames, and durable storage on sietch

## Scope
- `crates/raft`: the four Raft messages (RequestVote, VoteResponse, AppendEntries, AppendResponse), a hand-written binary codec carried inside distrans `frame` frames (so corruption is rejected by distrans's CRC before consensus ever sees it). Decoding arbitrary bytes never panics.
- A `Storage` trait for a node's persistent state (current term, vote, log) with an in-memory implementation (survives a simulated crash) and a real `sietch::Store`-backed implementation (survives a real close/reopen, including a torn tail).
- Property tests: message round trip; garbage never panics; both storages agree with a reference model under arbitrary append/truncate/vote sequences.
