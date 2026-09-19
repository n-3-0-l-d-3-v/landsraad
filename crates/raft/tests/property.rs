//! Property tests (ticket 001): the codec round-trips and never panics, and
//! both storages agree with a plain reference model under arbitrary
//! operation sequences (the sietch one also across a real reopen).

use proptest::prelude::*;
use raft::*;

fn arb_entry() -> impl Strategy<Value = Entry> {
    (any::<u64>(), prop::collection::vec(any::<u8>(), 0..12))
        .prop_map(|(term, command)| Entry { term, command })
}

fn arb_message() -> impl Strategy<Value = Message> {
    prop_oneof![
        (any::<u64>(), any::<u64>(), any::<u64>()).prop_map(|(term, i, t)| Message::RequestVote {
            term,
            last_log_index: i,
            last_log_term: t
        }),
        (any::<u64>(), any::<bool>())
            .prop_map(|(term, granted)| Message::VoteResponse { term, granted }),
        (
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            prop::collection::vec(arb_entry(), 0..6),
            any::<u64>()
        )
            .prop_map(|(term, prev_index, prev_term, entries, leader_commit)| {
                Message::AppendEntries {
                    term,
                    prev_index,
                    prev_term,
                    entries,
                    leader_commit,
                }
            }),
        (any::<u64>(), any::<bool>(), any::<u64>()).prop_map(|(term, success, match_index)| {
            Message::AppendResponse {
                term,
                success,
                match_index,
            }
        }),
    ]
}

#[derive(Debug, Clone)]
enum Op {
    Hard(u64, Option<usize>),
    Append(Vec<Entry>),
    Truncate(u64),
}

fn arb_ops() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(
        prop_oneof![
            (any::<u64>(), prop::option::of(0usize..5)).prop_map(|(t, v)| Op::Hard(t, v)),
            prop::collection::vec(arb_entry(), 0..4).prop_map(Op::Append),
            (0u64..12).prop_map(Op::Truncate),
        ],
        0..25,
    )
}

fn apply_reference(p: &mut Persistent, op: &Op) {
    match op {
        Op::Hard(t, v) => {
            p.term = *t;
            p.voted_for = *v;
        }
        Op::Append(es) => p.log.extend(es.iter().cloned()),
        Op::Truncate(i) => p.log.truncate(i.saturating_sub(1) as usize),
    }
}

fn apply(s: &mut dyn Storage, op: &Op) {
    match op {
        Op::Hard(t, v) => s.save_hard_state(*t, *v),
        Op::Append(es) => s.append(es),
        Op::Truncate(i) => s.truncate_from(*i),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn messages_round_trip(m in arb_message()) {
        prop_assert_eq!(decode_message(&encode_message(&m)), Ok(m));
    }

    #[test]
    fn arbitrary_bytes_never_panic_the_decoder(bytes in prop::collection::vec(any::<u8>(), 0..80)) {
        let _ = decode_message(&bytes);
    }

    #[test]
    fn any_single_bit_flip_is_rejected(m in arb_message(), bit in any::<prop::sample::Index>()) {
        let mut d = encode_message(&m);
        let i = bit.index(d.len() * 8);
        d[i / 8] ^= 1 << (i % 8);
        prop_assert!(decode_message(&d).is_err());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn both_storages_match_the_reference_and_sietch_survives_reopen(ops in arb_ops()) {
        let dir = tempfile::tempdir().unwrap();
        let mut reference = Persistent::default();
        let mut mem = MemStorage::new();
        let mut disk = SietchStorage::open(dir.path()).unwrap();
        for op in &ops {
            apply_reference(&mut reference, op);
            apply(&mut mem, op);
            apply(&mut disk, op);
        }
        prop_assert_eq!(&mem.load(), &reference);
        prop_assert_eq!(&disk.load(), &reference);
        drop(disk);
        let mut reopened = SietchStorage::open(dir.path()).unwrap();
        prop_assert_eq!(&reopened.load(), &reference);
    }
}
