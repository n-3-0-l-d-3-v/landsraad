use kv::*;
use proptest::prelude::*;
use raft::Entry;
use std::collections::HashMap;

fn arb_cmd() -> impl Strategy<Value = Command> {
    let key = prop::collection::vec(0u8..4, 0..3);
    prop_oneof![
        (key.clone(), prop::collection::vec(any::<u8>(), 0..4))
            .prop_map(|(key, value)| Command::Put { key, value }),
        key.prop_map(|key| Command::Delete { key }),
    ]
}

proptest! {
    #[test]
    fn encode_decode_round_trips(c in arb_cmd()) {
        prop_assert_eq!(Command::decode(&c.encode()), Some(c));
    }

    #[test]
    fn decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..40)) {
        let _ = Command::decode(&bytes);
    }

    #[test]
    fn replay_matches_a_hashmap_and_composes_over_prefixes(cmds in prop::collection::vec(arb_cmd(), 0..40), cut in any::<prop::sample::Index>()) {
        let entries: Vec<Entry> = cmds.iter().map(|c| Entry { term: 1, command: c.encode() }).collect();
        let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for c in &cmds {
            match c {
                Command::Put { key, value } => { model.insert(key.clone(), value.clone()); }
                Command::Delete { key } => { model.remove(key); }
            }
        }
        let full = replay(&entries);
        prop_assert_eq!(full.len(), model.len());
        for (k, v) in &model {
            prop_assert_eq!(full.get(k), Some(v.as_slice()));
        }
        let at = cut.index(entries.len() + 1);
        let mut incremental = replay(&entries[..at]);
        entries[at..].iter().for_each(|e| incremental.apply(e));
        prop_assert_eq!(incremental, full);
    }
}
