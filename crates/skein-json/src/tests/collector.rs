//! Selective collection: dispositions, counts, duplicate admission and lifetime.
use super::{boxed, env, key, number, string};
use crate::collector::{self, Cap, Collector, Counts, Error, Event, Filter, Keep, Key, Limits, Node, Request, Waiting};
use crate::document;
use crate::tokenizer;
use crate::{Document, Kind, Token};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Intake, Queue};

const LIMITS: Limits = Limits {
    tokenizer: tokenizer::Limits { depth: 8, string: 16, number: 8, chunk: 4, length: 4096 },
    tokens: 32,
    text: 64,
    skip: 4096,
};
const FIELDS: &[Node] = &[
    Node { key: Key::Field(b"keep"), keep: Keep::Value },
    Node { key: Key::Field(b"text"), keep: Keep::Text(Cap::new(0)) },
    Node { key: Key::Field(b"array"), keep: Keep::Into(&[Node { key: Key::Each, keep: Keep::Text(Cap::new(1)) }]) },
];
const FILTER: Filter = Filter { root: Keep::Into(FIELDS) };

struct Machine {
    collector: Collector,
    env: Env<Limits>,
    above: Queue<Event>,
    below: Queue<Down>,
}
impl Machine {
    fn new(filter: Filter, limits: Limits) -> Machine {
        Machine::with_caps(filter, limits, &[3, 2])
    }
    fn with_caps(filter: Filter, limits: Limits, caps: &[u32]) -> Machine {
        let basic = env(limits.tokenizer);
        Machine {
            collector: Collector::new(filter, &limits, caps).expect("valid filter"),
            env: Env { now: basic.now, wall: basic.wall, limits },
            above: Queue::with_capacity(4),
            below: Queue::with_capacity(4),
        }
    }
    fn down(&mut self, request: Request) {
        collector::down(&mut self.collector, &self.env, request, &mut self.above, &mut self.below);
        assert!(self.above.len() <= 1 && self.below.len() <= 1);
    }
    fn up(&mut self, event: Up) {
        collector::up(&mut self.collector, &self.env, event, &mut self.above, &mut self.below);
        assert!(self.above.len() <= 1 && self.below.len() <= 1);
    }
    fn read(&mut self, bytes: &[u8]) -> Event {
        let mut intake = Intake::with_capacity(
            u32::try_from(bytes.len()).unwrap().max(tokenizer::largest_demand(&self.env.limits.tokenizer)),
        );
        intake.append(bytes).unwrap();
        self.down(Request::Collect);
        for _ in 0..bytes.len().checked_mul(4).unwrap().checked_add(16).unwrap() {
            if let Some(event) = self.above.pop() {
                drop(self.below.pop());
                return event;
            }
            match self.below.pop().expect("an unfinished collect demands bytes") {
                Down::Demand { read, room: 0 } => match intake.meet(read) {
                    Some(bytes) => self.up(Up::Bytes(bytes)),
                    None => self.up(Up::End),
                },
                other @ (Down::Demand { .. } | Down::Send(_) | Down::Finish) => panic!("unexpected {other:?}"),
            }
        }
        panic!("bounded progress");
    }
}
fn collect(bytes: &[u8], filter: Filter, limits: Limits) -> (Event, Counts) {
    let mut machine = Machine::new(filter, limits);
    let event = machine.read(bytes);
    (event, machine.collector.counts())
}
fn expected(tokens: &[Token]) -> Event {
    Event::Collected(Document::from_tokens(tokens, &document::Limits { tokens: 32, text: 64 }).unwrap())
}

#[test]
fn named_and_unnamed_fields_and_each_disposition() {
    let (event, counts) =
        collect(br#"{"absent":{"x":[1,false]},"keep":{"x":7},"array":["a","ab"],"text":"abc"}"#, FILTER, LIMITS);
    assert_eq!(
        event,
        expected(&[
            Token::ObjectStart,
            key(b"keep"),
            Token::ObjectStart,
            key(b"x"),
            number(b"7"),
            Token::ObjectEnd,
            key(b"array"),
            Token::ArrayStart,
            string(b"a"),
            string(b"ab"),
            Token::ArrayEnd,
            key(b"text"),
            string(b"abc"),
            Token::ObjectEnd
        ])
    );
    assert_eq!(counts.skipped, 15);
    let empty = Filter { root: Keep::Into(&[]) };
    assert_eq!(collect(b"[1,false,{},[],null]", empty, LIMITS).0, expected(&[Token::ArrayStart, Token::ArrayEnd]));
    assert_eq!(collect(b"17", empty, LIMITS).0, expected(&[number(b"17")]));
}
#[test]
fn long_values_keep_only_their_decoded_length_and_unknown_long_keys_skip() {
    let (event, counts) = collect(br#"{"text":"abcd","a_very_long_unknown_name":"huge"}"#, FILTER, LIMITS);
    let Event::Collected(document) = event else { panic!("collected") };
    assert_eq!(document.token(2).unwrap().kind, Kind::Long);
    assert_eq!(document.token(2).unwrap().len, 4);
    assert_eq!(document.text_len(), 4);
    assert_eq!(counts.skipped, 6);
    assert_eq!(
        collect(br#"{"text":"abc"}"#, FILTER, LIMITS).0,
        expected(&[Token::ObjectStart, key(b"text"), string(b"abc"), Token::ObjectEnd])
    );
}
#[test]
fn only_a_named_duplicate_is_rejected() {
    assert_eq!(collect(br#"{"keep":1,"keep":2}"#, FILTER, LIMITS).0, Event::Failed(Error::Duplicate));
    assert_eq!(
        collect(br#"{"other":1,"other":2}"#, FILTER, LIMITS).0,
        expected(&[Token::ObjectStart, Token::ObjectEnd])
    );
    assert_eq!(
        collect(br#"{"keep":{"x":1,"x":2}}"#, FILTER, LIMITS).0,
        expected(&[
            Token::ObjectStart,
            key(b"keep"),
            Token::ObjectStart,
            key(b"x"),
            number(b"1"),
            key(b"x"),
            number(b"2"),
            Token::ObjectEnd,
            Token::ObjectEnd
        ])
    );
}
#[test]
fn each_retained_count_and_skip_count_accepts_its_edge_then_fails() {
    let limits = Limits { tokens: 4, text: 5, skip: 3, ..LIMITS };
    let bytes = br#"{"keep":1,"omit":123}"#;
    let (_, counts) = collect(bytes, FILTER, limits);
    assert_eq!(counts, Counts { tokens: 4, text: 5, skipped: 3 });
    assert_eq!(
        collect(bytes, FILTER, limits).0,
        expected(&[Token::ObjectStart, key(b"keep"), number(b"1"), Token::ObjectEnd])
    );
    assert_eq!(collect(bytes, FILTER, Limits { tokens: 3, ..limits }).0, Event::Failed(Error::TooManyTokens));
    assert_eq!(collect(bytes, FILTER, Limits { text: 4, ..limits }).0, Event::Failed(Error::TooMuchText { cap: None }));
    assert_eq!(collect(bytes, FILTER, Limits { skip: 2, ..limits }).0, Event::Failed(Error::SkippedTooLong));
}
#[test]
fn dropped_text_is_still_validated() {
    assert_eq!(
        collect(br#"{"omit":"bad\q"}"#, FILTER, LIMITS).0,
        Event::Failed(Error::Tokenizer(tokenizer::Error::Escape))
    );
    assert_eq!(
        collect(br#"{"text":"long\q"}"#, FILTER, LIMITS).0,
        Event::Failed(Error::Tokenizer(tokenizer::Error::Escape))
    );
}
#[test]
fn close_in_idle_reading_and_over_and_restart_after_each_outcome() {
    for state in 0_u32..3 {
        let mut machine = Machine::new(FILTER, LIMITS);
        if state == 1 {
            machine.down(Request::Collect);
            drop(machine.below.pop());
        }
        if state == 2 {
            drop(machine.read(b"{}"));
        }
        machine.down(Request::Close);
        assert_eq!(machine.above.pop(), Some(Event::Closed));
        assert_eq!(machine.collector.waiting(), Waiting::Nothing);
        if state == 1 {
            assert_eq!(machine.below.pop(), Some(Down::Demand { read: Read::Nothing, room: 0 }));
        }
        machine.up(Up::Bytes(boxed(b"{")));
        machine.up(Up::End);
        assert!(machine.above.is_empty());
    }
    let mut machine = Machine::new(FILTER, LIMITS);
    for bytes in [b"{}".as_slice(), br#"{"keep":1,"keep":2}"#, br#"{"omit":"bad\q"}"#] {
        drop(machine.read(bytes));
        machine.collector.restart();
        assert_eq!(machine.collector.waiting(), Waiting::Collect);
        assert_eq!(machine.collector.counts(), Counts { tokens: 0, text: 0, skipped: 0 });
        assert_eq!(
            machine.read(br#"{"keep":7}"#),
            expected(&[Token::ObjectStart, key(b"keep"), number(b"7"), Token::ObjectEnd])
        );
        machine.collector.restart();
    }
}

#[test]
fn skip_count_failure_stops_before_a_large_values_end() {
    let limits = Limits { skip: 3, ..LIMITS };
    assert_eq!(collect(br#"{"omit":"abcdefgh"#, FILTER, limits).0, Event::Failed(Error::SkippedTooLong));
}

#[test]
#[should_panic(expected = "a Close after Closed")]
fn a_second_close_is_an_owner_bug() {
    let mut machine = Machine::new(FILTER, LIMITS);
    machine.down(Request::Close);
    drop(machine.above.pop());
    machine.down(Request::Close);
}

#[test]
fn one_static_filter_uses_the_named_cap_supplied_at_construction() {
    const FILTER: Filter = Filter { root: Keep::Text(Cap::new(0)) };
    for cap in 0..=4 {
        let mut machine = Machine::with_caps(FILTER, LIMITS, &[cap]);
        let event = machine.read(br#""a\nb""#);
        let Event::Collected(document) = event else { panic!("a valid string") };
        let record = document.token(0).unwrap();
        assert_eq!(record.len, 3);
        assert_eq!(record.kind, if cap < 3 { Kind::Long } else { Kind::String });
        assert_eq!(document.text_len(), if cap < 3 { 0 } else { 3 });
    }
    assert_eq!(Cap::new(0).index(), 0);
}

#[test]
fn cap_values_are_owned_and_restarts_reuse_the_same_table() {
    const FILTER: Filter = Filter { root: Keep::Text(Cap::new(0)) };
    let mut caps = [2];
    let mut machine = Machine::with_caps(FILTER, LIMITS, &caps);
    caps[0] = 0;
    assert_eq!(caps, [0]);
    assert_eq!(machine.read(br#""ab""#), expected(&[string(b"ab")]));
    machine.collector.restart();
    assert_eq!(machine.read(br#""ab""#), expected(&[string(b"ab")]));
    assert_eq!(collector::worst_case(&LIMITS, &[0; 257], &FILTER), None);
}

const TAGGED: collector::Tagged = collector::Tagged {
    tag: b"type",
    known: &[
        collector::Variant { value: b"small", children: &[Node { key: Key::Field(b"id"), keep: Keep::Value }] },
        collector::Variant { value: b"large", children: &[Node { key: Key::Field(b"body"), keep: Keep::Value }] },
    ],
    unknown: Cap::new(0),
};
const TAG_FILTER: Filter = Filter { root: Keep::Tagged(&TAGGED) };

#[test]
fn tagged_projection_is_independent_of_tag_position() {
    let limits = Limits { tokenizer: tokenizer::Limits { string: 32, ..LIMITS.tokenizer }, ..LIMITS };
    for bytes in
        [br#"{"type":"small","body":"discard","id":1}"#.as_slice(), br#"{"body":"discard","id":1,"type":"small"}"#]
    {
        let mut machine = Machine::with_caps(TAG_FILTER, limits, &[64]);
        let Event::Collected(document) = machine.read(bytes) else { panic!("collected") };
        assert_eq!(document.len(), 6);
        assert_eq!(document.text_len(), 12);
        assert_eq!(machine.collector.counts().skipped, 9);
    }
}

#[test]
fn a_late_tag_only_fails_the_selected_variants_count() {
    let limits =
        Limits { tokens: 6, text: 12, tokenizer: tokenizer::Limits { string: 32, ..LIMITS.tokenizer }, ..LIMITS };
    let bytes = br#"{"body":"01234567890123456789","id":1,"type":"small"}"#;
    let mut machine = Machine::with_caps(TAG_FILTER, limits, &[2]);
    let Event::Collected(_) = machine.read(bytes) else { panic!("collected") };
    let mut machine = Machine::with_caps(TAG_FILTER, limits, &[2]);
    assert_eq!(
        machine.read(br#"{"body":"01234567890123456789","id":1,"type":"large"}"#),
        Event::Failed(Error::TooMuchText { cap: None })
    );
}

#[test]
fn unknown_and_missing_tags_keep_the_bounded_whole_copy() {
    for bytes in [br#"{"type":"other","body":7}"#.as_slice(), br#"{"body":7}"#] {
        let mut machine = Machine::with_caps(TAG_FILTER, LIMITS, &[64]);
        let Event::Collected(document) = machine.read(bytes) else { panic!("collected") };
        assert!(document.len() >= 4);
        let mut machine = Machine::with_caps(TAG_FILTER, LIMITS, &[2]);
        assert_eq!(machine.read(bytes), Event::Failed(Error::TooMuchText { cap: Some(Cap::new(0)) }));
    }
}

#[test]
fn tags_are_strings_and_cannot_repeat() {
    for bytes in [br#"{"type":7}"#.as_slice(), br#"{"type":{}}"#] {
        let mut machine = Machine::with_caps(TAG_FILTER, LIMITS, &[64]);
        assert_eq!(machine.read(bytes), Event::Failed(Error::NotTagged));
    }
    let mut machine = Machine::with_caps(TAG_FILTER, LIMITS, &[64]);
    assert_eq!(machine.read(br#"{"type":"small","type":"small"}"#), Event::Failed(Error::Duplicate));
}

#[test]
fn an_ambiguous_shared_field_is_refused_at_construction() {
    const BAD: collector::Tagged = collector::Tagged {
        tag: b"type",
        known: &[
            collector::Variant { value: b"a", children: &[Node { key: Key::Field(b"id"), keep: Keep::Value }] },
            collector::Variant {
                value: b"b",
                children: &[Node { key: Key::Field(b"id"), keep: Keep::Text(Cap::new(0)) }],
            },
        ],
        unknown: Cap::new(0),
    };
    assert_eq!(
        Collector::new(Filter { root: Keep::Tagged(&BAD) }, &LIMITS, &[64]).unwrap_err(),
        collector::AmbiguousFilter { field: b"id" }
    );
}

#[test]
fn shared_nested_tag_is_projected_once_before_its_outer_tag() {
    const OUTER: collector::Tagged = collector::Tagged {
        tag: b"type",
        known: &[
            collector::Variant {
                value: b"a",
                children: &[Node { key: Key::Field(b"nested"), keep: Keep::Tagged(&TAGGED) }],
            },
            collector::Variant {
                value: b"b",
                children: &[Node { key: Key::Field(b"nested"), keep: Keep::Tagged(&TAGGED) }],
            },
        ],
        unknown: Cap::new(0),
    };
    let limits =
        Limits { tokens: 11, text: 32, tokenizer: tokenizer::Limits { string: 32, ..LIMITS.tokenizer }, ..LIMITS };
    let mut machine = Machine::with_caps(Filter { root: Keep::Tagged(&OUTER) }, limits, &[2]);
    let Event::Collected(document) = machine.read(br#"{"nested":{"body":"ignored","id":1,"type":"small"},"type":"b"}"#)
    else {
        panic!("collected")
    };
    assert_eq!(document.len(), 11);
    assert_eq!(document.text_len(), 23);
    assert_eq!(machine.collector.counts().skipped, 9);
}

#[test]
fn a_large_losing_container_does_not_leave_partial_structure() {
    let limits = Limits { tokens: 6, text: 32, ..LIMITS };
    let mut machine = Machine::with_caps(TAG_FILTER, limits, &[2]);
    let Event::Collected(document) = machine.read(br#"{"body":[[1,2,3,4,5,6]],"id":1,"type":"small"}"#) else {
        panic!("collected")
    };
    assert_eq!(document.len(), 6);
    assert_eq!(machine.collector.counts().skipped, 15);
}

#[test]
fn tagged_array_elements_reuse_candidates_and_restart_keeps_the_filter() {
    let filter = Filter { root: Keep::Into(&[Node { key: Key::Each, keep: Keep::Tagged(&TAGGED) }]) };
    let mut machine = Machine::with_caps(filter, LIMITS, &[64]);
    for _ in 0_u32..2 {
        let Event::Collected(document) = machine.read(br#"[{"type":"small","id":1},{"type":"large","body":"a"}]"#)
        else {
            panic!("collected")
        };
        assert_eq!(document.len(), 14);
        machine.collector.restart();
    }
}

#[test]
fn a_losing_outer_variant_does_not_inherit_its_nested_failure_or_skip() {
    const OUTER: collector::Tagged = collector::Tagged {
        tag: b"type",
        known: &[
            collector::Variant {
                value: b"a",
                children: &[Node { key: Key::Field(b"nested"), keep: Keep::Tagged(&TAGGED) }],
            },
            collector::Variant { value: b"b", children: &[Node { key: Key::Field(b"id"), keep: Keep::Value }] },
        ],
        unknown: Cap::new(0),
    };
    let limits =
        Limits { tokens: 6, text: 12, tokenizer: tokenizer::Limits { string: 32, ..LIMITS.tokenizer }, ..LIMITS };
    let mut machine = Machine::with_caps(Filter { root: Keep::Tagged(&OUTER) }, limits, &[2]);
    let bytes = br#"{"nested":{"body":"01234567890123456789","type":"large"},"id":1,"type":"b"}"#;
    let Event::Collected(document) = machine.read(bytes) else { panic!("collected") };
    assert_eq!(document.len(), 6);
    assert_eq!(machine.collector.counts().skipped, 46);
}

#[test]
fn a_strict_string_reports_the_first_retention_bound_it_passes() {
    for (text, expected) in
        [(64, Error::Tokenizer(tokenizer::Error::StringTooLong)), (12, Error::TooMuchText { cap: None })]
    {
        let mut machine = Machine::with_caps(TAG_FILTER, Limits { text, ..LIMITS }, &[2]);
        assert_eq!(
            machine.read(br#"{"type":"large","body":"0123456789012345678901234567890123456789"}"#),
            Event::Failed(expected)
        );
    }
}

#[test]
fn provisional_skip_counts_exclude_numeric_lookahead_whitespace() {
    let mut machine = Machine::with_caps(TAG_FILTER, LIMITS, &[64]);
    let Event::Collected(_) = machine.read(br#"{"body": 123 , "id": 1 , "type": "small"}"#) else {
        panic!("collected")
    };
    assert_eq!(machine.collector.counts().skipped, 3);
}

#[test]
fn a_later_long_value_preserves_the_whole_candidates_first_failure() {
    let mut machine = Machine::with_caps(TAG_FILTER, Limits { tokens: 1, ..LIMITS }, &[64]);
    assert_eq!(
        machine.read(br#"{"type":"other","body":"012345678901234567890123456789"}"#),
        Event::Failed(Error::TooManyTokens)
    );
}

#[test]
fn an_unknown_copy_names_a_string_bound_smaller_than_its_own_cap() {
    let mut machine = Machine::with_caps(TAG_FILTER, LIMITS, &[64]);
    assert_eq!(
        machine.read(br#"{"type":"other","body":"012345678901234567890123456789"}"#),
        Event::Failed(Error::Tokenizer(tokenizer::Error::StringTooLong))
    );
}

#[test]
fn value_keeps_every_key_under_the_strict_string_bound() {
    let limits = Limits { tokenizer: tokenizer::Limits { string: 0, ..LIMITS.tokenizer }, ..LIMITS };
    let mut machine = Machine::new(Filter { root: Keep::Value }, limits);
    assert_eq!(machine.read(br#"{"a":[]}"#), Event::Failed(Error::Tokenizer(tokenizer::Error::StringTooLong)));
}
