//! A node's persistent state (current term, vote, log) behind a trait, with
//! an in-memory implementation that outlives a simulated crash and a real
//! implementation on `sietch::Store`.
//!
//! Persistence failure is fail-stop: a node that cannot durably record its
//! term, vote or log must not keep participating (it could later
//! contradict a promise it made), so these operations panic with a clear
//! message rather than return an error the consensus code might ignore.

use std::sync::{Arc, Mutex};

use storage::{LogOp, Store};

use crate::message::{Entry, Index, NodeId, Term};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Persistent {
    pub term: Term,
    pub voted_for: Option<NodeId>,
    /// `log[i]` is the entry at index `i + 1`.
    pub log: Vec<Entry>,
}

pub trait Storage: Send {
    fn load(&mut self) -> Persistent;
    fn save_hard_state(&mut self, term: Term, voted_for: Option<NodeId>);
    /// Appends entries after the current last index.
    fn append(&mut self, entries: &[Entry]);
    /// Removes the entry at `index` and everything after it.
    fn truncate_from(&mut self, index: Index);
}

/// In-memory storage. Clones share the same state, so a test harness can
/// keep one handle, give another to a node, drop the node to simulate a
/// crash, and hand a fresh clone to the restarted node.
#[derive(Debug, Clone, Default)]
pub struct MemStorage(Arc<Mutex<Persistent>>);

impl MemStorage {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn snapshot(&self) -> Persistent {
        self.0.lock().unwrap().clone()
    }
}

impl Storage for MemStorage {
    fn load(&mut self) -> Persistent {
        self.snapshot()
    }
    fn save_hard_state(&mut self, term: Term, voted_for: Option<NodeId>) {
        let mut p = self.0.lock().unwrap();
        p.term = term;
        p.voted_for = voted_for;
    }
    fn append(&mut self, entries: &[Entry]) {
        self.0.lock().unwrap().log.extend_from_slice(entries);
    }
    fn truncate_from(&mut self, index: Index) {
        let mut p = self.0.lock().unwrap();
        p.log.truncate(index.saturating_sub(1) as usize);
    }
}

const HARD_STATE_KEY: &[u8] = b"h";
const ENTRY_PREFIX: u8 = b'e';
const NO_VOTE: u64 = u64::MAX;

fn entry_key(index: Index) -> Vec<u8> {
    let mut k = vec![ENTRY_PREFIX];
    k.extend(index.to_be_bytes());
    k
}

/// Durable storage on a `sietch::Store`. Each mutation is one durable
/// append (a multi-key truncation is one atomic batch), so a crash leaves
/// either the old state or the new state, never a mixture; sietch's own
/// recovery discards a torn tail on reopen.
pub struct SietchStorage {
    store: Store,
    last_index: Index,
}

impl SietchStorage {
    pub fn open(dir: impl Into<std::path::PathBuf>) -> Result<Self, storage::StoreError> {
        let store = Store::open(dir)?;
        let last_index = store.scan(&[ENTRY_PREFIX]).len() as Index;
        Ok(Self { store, last_index })
    }
}

impl Storage for SietchStorage {
    fn load(&mut self) -> Persistent {
        let (term, voted_for) = match self.store.get(HARD_STATE_KEY) {
            Some(b) if b.len() == 16 => {
                let term = u64::from_le_bytes(b[..8].try_into().unwrap());
                let vote = u64::from_le_bytes(b[8..].try_into().unwrap());
                (term, (vote != NO_VOTE).then_some(vote as NodeId))
            }
            Some(_) => panic!("fail-stop: corrupt raft hard state"),
            None => (0, None),
        };
        let mut log = Vec::new();
        for (i, (key, val)) in self.store.scan(&[ENTRY_PREFIX]).into_iter().enumerate() {
            assert_eq!(
                key,
                entry_key(i as Index + 1),
                "fail-stop: raft log has a gap or misordered entry"
            );
            assert!(val.len() >= 8, "fail-stop: corrupt raft log entry");
            log.push(Entry {
                term: u64::from_le_bytes(val[..8].try_into().unwrap()),
                command: val[8..].to_vec(),
            });
        }
        self.last_index = log.len() as Index;
        Persistent {
            term,
            voted_for,
            log,
        }
    }

    fn save_hard_state(&mut self, term: Term, voted_for: Option<NodeId>) {
        let mut v = term.to_le_bytes().to_vec();
        v.extend(voted_for.map_or(NO_VOTE, |n| n as u64).to_le_bytes());
        self.store
            .put(HARD_STATE_KEY.to_vec(), v)
            .expect("fail-stop: cannot persist raft hard state");
    }

    fn append(&mut self, entries: &[Entry]) {
        let ops: Vec<LogOp> = entries
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let mut v = e.term.to_le_bytes().to_vec();
                v.extend(&e.command);
                LogOp::Put(entry_key(self.last_index + 1 + i as Index), v)
            })
            .collect();
        if ops.is_empty() {
            return;
        }
        self.store
            .apply_batch(ops)
            .expect("fail-stop: cannot persist raft log");
        self.last_index += entries.len() as Index;
    }

    fn truncate_from(&mut self, index: Index) {
        if index > self.last_index {
            return;
        }
        let ops: Vec<LogOp> = (index..=self.last_index)
            .map(|i| LogOp::Delete(entry_key(i)))
            .collect();
        self.store
            .apply_batch(ops)
            .expect("fail-stop: cannot truncate raft log");
        self.last_index = index.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(term: Term, c: &[u8]) -> Entry {
        Entry {
            term,
            command: c.to_vec(),
        }
    }

    fn exercise(s: &mut dyn Storage) {
        assert_eq!(s.load(), Persistent::default());
        s.save_hard_state(3, Some(2));
        s.append(&[e(1, b"a"), e(1, b""), e(2, b"c")]);
        s.truncate_from(3);
        s.append(&[e(3, b"d")]);
        s.save_hard_state(4, None);
        assert_eq!(
            s.load(),
            Persistent {
                term: 4,
                voted_for: None,
                log: vec![e(1, b"a"), e(1, b""), e(3, b"d")]
            }
        );
        s.truncate_from(1);
        assert!(s.load().log.is_empty());
        s.truncate_from(1);
        s.append(&[e(9, b"z")]);
        assert_eq!(s.load().log, vec![e(9, b"z")]);
    }

    #[test]
    fn mem_storage_behaves() {
        exercise(&mut MemStorage::new());
    }

    #[test]
    fn mem_storage_clones_share_state() {
        let mut a = MemStorage::new();
        let b = a.clone();
        a.save_hard_state(5, Some(1));
        assert_eq!(b.snapshot().term, 5);
    }

    #[test]
    fn sietch_storage_behaves() {
        let d = tempfile::tempdir().unwrap();
        exercise(&mut SietchStorage::open(d.path()).unwrap());
    }

    #[test]
    fn sietch_storage_survives_a_real_close_and_reopen() {
        let d = tempfile::tempdir().unwrap();
        {
            let mut s = SietchStorage::open(d.path()).unwrap();
            s.save_hard_state(8, Some(0));
            s.append(&[e(1, b"x"), e(8, b"")]);
            s.truncate_from(2);
            s.append(&[e(8, b"y")]);
        }
        let mut s = SietchStorage::open(d.path()).unwrap();
        assert_eq!(
            s.load(),
            Persistent {
                term: 8,
                voted_for: Some(0),
                log: vec![e(1, b"x"), e(8, b"y")]
            }
        );
        s.append(&[e(9, b"more")]);
        assert_eq!(
            s.load().log.len(),
            3,
            "indexing continues correctly after reopen"
        );
    }
}
