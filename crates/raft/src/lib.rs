//! Raft-style consensus building blocks: the wire messages and their codec
//! (over distrans frames), durable storage on sietch, and (ticket 002) the
//! node state machine. See `docs/design/decisions/`.

mod message;
mod storage;

pub use message::{
    decode_message, encode_message, CodecError, Entry, Index, Message, NodeId, Term,
};
pub use storage::{MemStorage, Persistent, SietchStorage, Storage};
