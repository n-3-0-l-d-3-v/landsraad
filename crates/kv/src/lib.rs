//! A replicated key-value state machine (ticket 004). Commands are opaque
//! bytes to consensus; this crate defines their meaning. Applying the same
//! committed log always yields the same map, on every node, in any order of
//! restarts: `apply` is deterministic and total, and malformed or empty
//! commands (the leadership no-op is empty) are ignored the same way
//! everywhere.

use std::collections::BTreeMap;

use raft::Entry;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Command {
    /// `1, key_len u32 LE, key, value` for Put; `2, key` for Delete.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Command::Put { key, value } => {
                let mut v = vec![1];
                v.extend((key.len() as u32).to_le_bytes());
                v.extend(key);
                v.extend(value);
                v
            }
            Command::Delete { key } => {
                let mut v = vec![2];
                v.extend(key);
                v
            }
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<Command> {
        match bytes.split_first()? {
            (1, rest) => {
                let len = u32::from_le_bytes(rest.get(..4)?.try_into().ok()?) as usize;
                let body = rest.get(4..)?;
                let key = body.get(..len)?.to_vec();
                Some(Command::Put {
                    key,
                    value: body[len..].to_vec(),
                })
            }
            (2, key) => Some(Command::Delete { key: key.to_vec() }),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Store {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Store {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, entry: &Entry) {
        match Command::decode(&entry.command) {
            Some(Command::Put { key, value }) => {
                self.map.insert(key, value);
            }
            Some(Command::Delete { key }) => {
                self.map.remove(&key);
            }
            None => {}
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        self.map.get(key).map(|v| v.as_slice())
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.map.iter().map(|(k, v)| (k.as_slice(), v.as_slice()))
    }
}

/// The state after applying `entries` in order to an empty store.
pub fn replay(entries: &[Entry]) -> Store {
    let mut s = Store::new();
    entries.iter().for_each(|e| s.apply(e));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(c: Command) -> Entry {
        Entry {
            term: 1,
            command: c.encode(),
        }
    }

    #[test]
    fn put_get_overwrite_delete() {
        let mut s = Store::new();
        let put = |k: &str, v: &str| {
            entry(Command::Put {
                key: k.into(),
                value: v.into(),
            })
        };
        s.apply(&put("a", "1"));
        s.apply(&put("a", "2"));
        s.apply(&put("b", ""));
        assert_eq!(s.get(b"a"), Some(&b"2"[..]));
        assert_eq!(s.get(b"b"), Some(&b""[..]));
        s.apply(&entry(Command::Delete { key: b"a".to_vec() }));
        assert_eq!(s.get(b"a"), None);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn empty_and_malformed_commands_are_ignored() {
        let mut s = Store::new();
        for c in [
            vec![],
            vec![9],
            vec![1],
            vec![1, 255, 255, 255, 255, 1],
            vec![1, 3, 0, 0, 0, 1],
        ] {
            s.apply(&Entry {
                term: 1,
                command: c,
            });
        }
        assert!(s.is_empty());
    }

    #[test]
    fn commands_round_trip_including_awkward_keys() {
        for c in [
            Command::Put {
                key: vec![],
                value: vec![],
            },
            Command::Put {
                key: vec![0, 1, 2],
                value: vec![9; 40],
            },
            Command::Delete { key: vec![] },
            Command::Delete { key: vec![1, 2, 3] },
        ] {
            assert_eq!(Command::decode(&c.encode()), Some(c));
        }
    }
}
