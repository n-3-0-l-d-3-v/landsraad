//! The four consensus messages and their wire codec. A message travels as
//! the payload of a distrans `frame::Frame` (frame type = message kind), so
//! a corrupted or truncated datagram is rejected by distrans's CRC before
//! the consensus layer sees a single byte. Decoding arbitrary input never
//! panics.
//!
//! Payload layouts (little-endian):
//! ```text
//! RequestVote      (1): term u64, last_log_index u64, last_log_term u64
//! VoteResponse     (2): term u64, granted u8
//! AppendEntries    (3): term u64, prev_index u64, prev_term u64,
//!                       leader_commit u64, count u32,
//!                       count x (entry_term u64, len u32, command bytes)
//! AppendResponse   (4): term u64, success u8, match_index u64
//! ```
//! The sender's identity is not in the message: it is the link the
//! datagram arrived on, which the simulated network supplies.

use frame::{decode, encode, DecodeError, Frame};

pub type NodeId = usize;
pub type Term = u64;
/// 1-based log position; 0 means "before the first entry".
pub type Index = u64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    /// Opaque to consensus. An empty command is the leadership no-op.
    pub command: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    RequestVote {
        term: Term,
        last_log_index: Index,
        last_log_term: Term,
    },
    VoteResponse {
        term: Term,
        granted: bool,
    },
    AppendEntries {
        term: Term,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
    },
    AppendResponse {
        term: Term,
        success: bool,
        match_index: Index,
    },
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote { term, .. }
            | Message::VoteResponse { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendResponse { term, .. } => *term,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CodecError {
    #[error(transparent)]
    Frame(#[from] DecodeError),
    #[error("unknown message type {0}")]
    UnknownType(u8),
    #[error("message payload truncated")]
    Truncated,
    #[error("message payload has trailing bytes")]
    TrailingBytes,
    #[error("boolean field has value {0}, expected 0 or 1")]
    BadBool(u8),
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], CodecError> {
        if self.0.len() < n {
            return Err(CodecError::Truncated);
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn bool(&mut self) -> Result<bool, CodecError> {
        match self.take(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(CodecError::BadBool(other)),
        }
    }
}

/// Encodes a message as a complete distrans datagram.
pub fn encode_message(msg: &Message) -> Vec<u8> {
    let mut p = Vec::new();
    let kind = match msg {
        Message::RequestVote {
            term,
            last_log_index,
            last_log_term,
        } => {
            p.extend(term.to_le_bytes());
            p.extend(last_log_index.to_le_bytes());
            p.extend(last_log_term.to_le_bytes());
            1
        }
        Message::VoteResponse { term, granted } => {
            p.extend(term.to_le_bytes());
            p.push(u8::from(*granted));
            2
        }
        Message::AppendEntries {
            term,
            prev_index,
            prev_term,
            entries,
            leader_commit,
        } => {
            p.extend(term.to_le_bytes());
            p.extend(prev_index.to_le_bytes());
            p.extend(prev_term.to_le_bytes());
            p.extend(leader_commit.to_le_bytes());
            p.extend((entries.len() as u32).to_le_bytes());
            for e in entries {
                p.extend(e.term.to_le_bytes());
                p.extend((e.command.len() as u32).to_le_bytes());
                p.extend(&e.command);
            }
            3
        }
        Message::AppendResponse {
            term,
            success,
            match_index,
        } => {
            p.extend(term.to_le_bytes());
            p.push(u8::from(*success));
            p.extend(match_index.to_le_bytes());
            4
        }
    };
    encode(&Frame {
        frame_type: kind,
        sequence: 0,
        flags: 0,
        payload: p,
    })
}

/// Decodes a datagram. Any corruption, truncation or malformed payload is a
/// typed error; nothing here can panic on arbitrary input.
pub fn decode_message(datagram: &[u8]) -> Result<Message, CodecError> {
    let f = decode(datagram)?;
    let mut r = Reader(&f.payload);
    let msg = match f.frame_type {
        1 => Message::RequestVote {
            term: r.u64()?,
            last_log_index: r.u64()?,
            last_log_term: r.u64()?,
        },
        2 => Message::VoteResponse {
            term: r.u64()?,
            granted: r.bool()?,
        },
        3 => {
            let term = r.u64()?;
            let prev_index = r.u64()?;
            let prev_term = r.u64()?;
            let leader_commit = r.u64()?;
            let count = r.u32()? as usize;
            // Each entry needs at least 12 bytes, so a huge declared count
            // cannot make us allocate more than the payload can justify.
            if count > r.0.len() / 12 {
                return Err(CodecError::Truncated);
            }
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let term = r.u64()?;
                let len = r.u32()? as usize;
                entries.push(Entry {
                    term,
                    command: r.take(len)?.to_vec(),
                });
            }
            Message::AppendEntries {
                term,
                prev_index,
                prev_term,
                entries,
                leader_commit,
            }
        }
        4 => Message::AppendResponse {
            term: r.u64()?,
            success: r.bool()?,
            match_index: r.u64()?,
        },
        other => return Err(CodecError::UnknownType(other)),
    };
    if !r.0.is_empty() {
        return Err(CodecError::TrailingBytes);
    }
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn samples() -> Vec<Message> {
        vec![
            Message::RequestVote {
                term: 7,
                last_log_index: 3,
                last_log_term: 6,
            },
            Message::VoteResponse {
                term: 7,
                granted: true,
            },
            Message::AppendEntries {
                term: 9,
                prev_index: 0,
                prev_term: 0,
                entries: vec![],
                leader_commit: 0,
            },
            Message::AppendEntries {
                term: u64::MAX,
                prev_index: 5,
                prev_term: 4,
                entries: vec![
                    Entry {
                        term: 4,
                        command: vec![],
                    },
                    Entry {
                        term: 5,
                        command: vec![1, 2, 3],
                    },
                ],
                leader_commit: 5,
            },
            Message::AppendResponse {
                term: 1,
                success: false,
                match_index: 0,
            },
        ]
    }

    #[test]
    fn every_message_round_trips() {
        for m in samples() {
            assert_eq!(decode_message(&encode_message(&m)), Ok(m));
        }
    }

    #[test]
    fn corruption_is_caught_by_the_frame_crc() {
        for m in samples() {
            let good = encode_message(&m);
            for i in 0..good.len() {
                let mut bad = good.clone();
                bad[i] ^= 0x01;
                assert!(decode_message(&bad).is_err(), "flip at byte {i} of {m:?}");
            }
        }
    }

    #[test]
    fn every_truncation_is_an_error() {
        for m in samples() {
            let good = encode_message(&m);
            for cut in 0..good.len() {
                assert!(decode_message(&good[..cut]).is_err());
            }
        }
    }

    fn framed(kind: u8, payload: Vec<u8>) -> Vec<u8> {
        encode(&Frame {
            frame_type: kind,
            sequence: 0,
            flags: 0,
            payload,
        })
    }

    #[test]
    fn valid_frames_with_bad_payloads_are_typed_errors() {
        assert_eq!(
            decode_message(&framed(99, vec![])),
            Err(CodecError::UnknownType(99))
        );
        assert_eq!(
            decode_message(&framed(2, vec![0; 9])),
            Ok(Message::VoteResponse {
                term: 0,
                granted: false
            })
        );
        let mut p = vec![0; 8];
        p.push(2);
        assert_eq!(decode_message(&framed(2, p)), Err(CodecError::BadBool(2)));
        assert_eq!(
            decode_message(&framed(2, vec![0; 10])),
            Err(CodecError::TrailingBytes)
        );
        assert_eq!(
            decode_message(&framed(1, vec![0; 3])),
            Err(CodecError::Truncated)
        );
    }

    #[test]
    fn a_huge_declared_entry_count_cannot_force_a_huge_allocation() {
        let mut p = vec![0u8; 32];
        p.extend(u32::MAX.to_le_bytes());
        assert_eq!(decode_message(&framed(3, p)), Err(CodecError::Truncated));
    }
}
