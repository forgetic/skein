//! The tokenizer (json.md, 3): its demands, its answers, every limit, every
//! escape, and closing in each state.

#![expect(clippy::disallowed_types, reason = "a test collects what it reads in a Vec")]
#![expect(clippy::disallowed_macros, reason = "a test writes what it expects with vec!")]

use alloc::vec;
use alloc::vec::Vec;

use skein_lib::stream::{Delimiter, Fault, Read, Up};

use super::{LIMITS, Machine, boxed, demand, failure, key, number, read, string, tokens};
use crate::Token;
use crate::tokenizer::{self as json, Error, Event, Limits, Request, Waiting};

const QUOTE: Delimiter = Delimiter::new(b"\"").expect("one byte");

fn scan(max: u32) -> Read {
    Read::Scan { until: QUOTE, max }
}

#[test]
fn every_kind_of_token_is_read() {
    let document = br#" { "a" : [ true , false , null , -1.5e+3 , "x" , { } , [ ] ] } "#;
    assert_eq!(
        tokens(document),
        vec![
            Token::ObjectStart,
            key(b"a"),
            Token::ArrayStart,
            Token::True,
            Token::False,
            Token::Null,
            number(b"-1.5e+3"),
            string(b"x"),
            Token::ObjectStart,
            Token::ObjectEnd,
            Token::ArrayStart,
            Token::ArrayEnd,
            Token::ArrayEnd,
            Token::ObjectEnd,
        ]
    );
}

#[test]
fn a_document_may_be_a_single_scalar() {
    assert_eq!(tokens(b"\"x\""), vec![string(b"x")]);
    assert_eq!(tokens(b"true"), vec![Token::True]);
    assert_eq!(tokens(b"null\n"), vec![Token::Null]);
    assert_eq!(tokens(b"0"), vec![number(b"0")]);
    assert_eq!(tokens(b" 12 "), vec![number(b"12")]);
}

#[test]
fn nothing_is_demanded_until_the_side_above_asks() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.tokenizer.waiting(), Waiting::Next);
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.tokenizer.waiting(), Waiting::Bytes);
}

#[test]
fn each_reading_demands_only_what_it_needs() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b" "), (None, demand(Read::Fill(1))), "whitespace is skipped a byte at a time");
    assert_eq!(machine.bytes(b"["), (Some(Event::Token(Token::ArrayStart)), None), "a token leaves nothing demanded");
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"f"), (None, demand(Read::Fill(4))), "the rest of false");
    assert_eq!(machine.bytes(b"alse"), (Some(Event::Token(Token::False)), None));
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b","), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"\""), (None, demand(scan(LIMITS.chunk))), "a string is scanned to its quote");
    assert_eq!(machine.bytes(b"abcd"), (None, demand(scan(LIMITS.chunk))), "a scan that met no quote");
    assert_eq!(machine.bytes(b"e\""), (Some(Event::Token(string(b"abcde"))), None));
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b","), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"1"), (None, demand(Read::Fill(1))), "a number is read a byte at a time");
    assert_eq!(machine.bytes(b"2"), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"]"), (Some(Event::Token(number(b"12"))), None), "the byte after it ends it");
    assert_eq!(machine.down(Request::Next), (Some(Event::Token(Token::ArrayEnd)), None), "and is held for the next");
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))), "the trailing whitespace, to the end");
    assert_eq!(machine.up(Up::End), (Some(Event::Done), None));
    assert_eq!(machine.tokenizer.waiting(), Waiting::Close);
    assert_eq!(machine.down(Request::Close), (Some(Event::Closed), None));
    assert_eq!(machine.tokenizer.waiting(), Waiting::Nothing);
}

#[test]
fn an_escaped_quote_ends_a_scan_but_not_the_string() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"\""), (None, demand(scan(LIMITS.chunk))));
    assert_eq!(machine.bytes(b"a\\\""), (None, demand(scan(LIMITS.chunk))));
    assert_eq!(machine.bytes(b"\\"), (None, demand(scan(LIMITS.chunk))), "a backslash at the scan's maximum");
    assert_eq!(machine.bytes(b"\"b\""), (Some(Event::Token(string(b"a\"\"b"))), None));
}

#[test]
fn the_end_of_the_stream_ends_a_number_that_is_the_whole_document() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"7"), (None, demand(Read::Fill(1))));
    assert_eq!(machine.up(Up::End), (Some(Event::Token(number(b"7"))), None));
    assert_eq!(machine.down(Request::Next), (Some(Event::Done), None), "the end is held for the next");
    assert_eq!(failure(b"-", LIMITS), Error::Number);
    assert_eq!(failure(b"1.", LIMITS), Error::Number);
    assert_eq!(failure(b"[1", LIMITS), Error::Truncated, "within an array, more was due");
}

#[test]
fn an_end_or_a_failure_while_idle_is_held_for_the_next_demand() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"["), (Some(Event::Token(Token::ArrayStart)), None));
    assert_eq!(machine.up(Up::End), (None, None));
    assert_eq!(machine.down(Request::Next), (Some(Event::Failed(Error::Truncated)), None));

    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.up(Up::End), (None, None), "an empty stream");
    assert_eq!(machine.down(Request::Next), (Some(Event::Failed(Error::Truncated)), None));

    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"{"), (Some(Event::Token(Token::ObjectStart)), None));
    assert_eq!(machine.up(Up::Failed(Fault::Reset)), (None, None));
    assert_eq!(machine.down(Request::Next), (Some(Event::Failed(Error::Stream(Fault::Reset))), None));
    assert_eq!(machine.up(Up::End), (None, None), "nothing follows the outcome");
}

#[test]
fn a_byte_held_after_a_number_is_read_before_an_end_held_after_it() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"["), (Some(Event::Token(Token::ArrayStart)), None));
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"3"), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"]"), (Some(Event::Token(number(b"3"))), None));
    assert_eq!(machine.up(Up::End), (None, None));
    assert_eq!(machine.down(Request::Next), (Some(Event::Token(Token::ArrayEnd)), None));
    assert_eq!(machine.down(Request::Next), (Some(Event::Done), None));

    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"["), (Some(Event::Token(Token::ArrayStart)), None));
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"3"), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b","), (Some(Event::Token(number(b"3"))), None));
    assert_eq!(machine.up(Up::End), (None, None));
    assert_eq!(machine.down(Request::Next), (Some(Event::Failed(Error::Truncated)), None), "a comma wants more");
}

#[test]
fn the_stream_failing_fails_the_document_in_any_reading() {
    for prefix in [&b""[..], b"[", b"[\"ab", b"[1", b"[t"] {
        let mut machine = Machine::new(LIMITS);
        let mut demanded = machine.down(Request::Next).1;
        for &byte in prefix {
            demanded = match machine.bytes(&[byte]) {
                (Some(Event::Token(_)), None) => machine.down(Request::Next).1,
                (None, down) => down,
                other => panic!("{other:?} reading {}", prefix.escape_ascii()),
            };
        }
        assert!(demanded.is_some(), "{} is read on", prefix.escape_ascii());
        assert_eq!(machine.up(Up::Failed(Fault::Invalid)), (Some(Event::Failed(Error::Stream(Fault::Invalid))), None));
        assert_eq!(machine.tokenizer.waiting(), Waiting::Close);
    }
}

#[test]
fn a_close_withdraws_what_was_demanded_and_answers_once() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.down(Request::Close), (Some(Event::Closed), None), "idle: nothing to withdraw");
    assert_eq!(machine.up(Up::End), (None, None), "the stream's end, after the close");

    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"\""), (None, demand(scan(LIMITS.chunk))));
    assert_eq!(machine.down(Request::Close), (Some(Event::Closed), demand(Read::Nothing)), "reading: withdrawn");
    assert_eq!(machine.bytes(b"abc\""), (None, None), "a delivery already on its way is dropped");

    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
    assert_eq!(machine.bytes(b"x"), (Some(Event::Failed(Error::Unexpected)), None));
    assert_eq!(machine.down(Request::Close), (Some(Event::Closed), None), "over: nothing to withdraw");
}

#[test]
#[should_panic(expected = "a Next before the last one was answered")]
fn a_second_next_before_the_first_is_answered_is_a_bug() {
    let mut machine = Machine::new(LIMITS);
    drop(machine.down(Request::Next));
    drop(machine.down(Request::Next));
}

#[test]
#[should_panic(expected = "a Next after the document's outcome")]
fn a_next_after_the_outcome_is_a_bug() {
    let mut machine = Machine::new(LIMITS);
    drop(machine.up(Up::End));
    drop(machine.down(Request::Next));
    drop(machine.down(Request::Next));
}

#[test]
fn nesting_past_the_depth_is_refused_at_the_first_container_past_it() {
    assert_eq!(tokens(b"[[{\"a\":[]}]]").len(), 9, "four deep is the limit");
    let events = read(b"[[{\"a\":[[]]}]]", LIMITS);
    assert_eq!(events.len(), 6);
    assert_eq!(events.last(), Some(&Event::Failed(Error::TooDeep)));
    assert_eq!(failure(b"[[[[[", LIMITS), Error::TooDeep);
    let flat = Limits { depth: 0, ..LIMITS };
    assert_eq!(read(b"1", flat).pop(), Some(Event::Done), "a scalar needs no depth");
    assert_eq!(failure(b"[]", flat), Error::TooDeep);
}

#[test]
fn strings_and_keys_are_held_to_their_limit_once_unescaped() {
    let limits = Limits { string: 4, ..LIMITS };
    assert_eq!(read(b"\"abcd\"", limits).pop(), Some(Event::Done));
    assert_eq!(failure(b"\"abcde\"", limits), Error::StringTooLong);
    assert_eq!(failure(b"{\"abcde\":1}", limits), Error::StringTooLong);
    assert_eq!(read(b"\"\\u0041\\n\\t\\\\\"", limits).pop(), Some(Event::Done), "six escapes' bytes, four out");
    assert_eq!(read(b"\"\xF0\x9F\x98\x80\"", limits).pop(), Some(Event::Done), "a four-byte character fits four");
    assert_eq!(failure(b"\"a\xF0\x9F\x98\x80\"", limits), Error::StringTooLong);
    assert_eq!(failure(b"\"a\\ud83d\\ude00\"", limits), Error::StringTooLong, "a pair is four bytes");
    assert_eq!(
        failure(b"\"abc\xF0\x28\"", limits),
        Error::StringTooLong,
        "a character that would not fit is too long before it is checked"
    );
    assert_eq!(failure(b"\"abc\xF0\x28\"", LIMITS), Error::Utf8);
    let empty = Limits { string: 0, ..LIMITS };
    assert_eq!(read(b"{\"\":\"\"}", empty).pop(), Some(Event::Done));
}

#[test]
fn numbers_are_held_to_their_limit() {
    let limits = Limits { number: 3, ..LIMITS };
    assert_eq!(
        tokens(b"[123,-12,1e5]"),
        vec![Token::ArrayStart, number(b"123"), number(b"-12"), number(b"1e5"), Token::ArrayEnd]
    );
    assert_eq!(read(b"[123,-12,1e5]", limits).pop(), Some(Event::Done));
    assert_eq!(failure(b"1234", limits), Error::NumberTooLong);
    assert_eq!(failure(b"[-1.5]", limits), Error::NumberTooLong);
    assert_eq!(failure(b"[01]", limits), Error::Number, "a number's own error before its length");
}

#[test]
fn every_escape_is_undone() {
    assert_eq!(tokens(br#""\" \\ \/ \b \f \n \r \t""#), vec![string(b"\" \\ / \x08 \x0C \n \r \t")]);
    assert_eq!(tokens(b"\"\\u0000\\u001f\\u0041\\u00e9\""), vec![string(b"\0\x1FA\xC3\xA9")]);
    assert_eq!(tokens(b"\"\\u20AC\\uFFFF\""), vec![string(b"\xE2\x82\xAC\xEF\xBF\xBF")]);
    assert_eq!(tokens(b"\"\\ud83d\\ude00\\uDBFF\\uDFFF\""), vec![string(b"\xF0\x9F\x98\x80\xF4\x8F\xBF\xBF")]);
}

#[test]
fn bad_escapes_and_lone_surrogates_are_refused() {
    assert_eq!(failure(br#""\x""#, LIMITS), Error::Escape);
    assert_eq!(failure(br#""\U0041""#, LIMITS), Error::Escape);
    assert_eq!(failure(br#""\u00G1""#, LIMITS), Error::Escape);
    assert_eq!(failure(br#""\u004""#, LIMITS), Error::Escape, "the quote is not a hex digit");
    assert_eq!(failure(br#""\ud800""#, LIMITS), Error::Surrogate, "a high surrogate, then the end");
    assert_eq!(failure(br#""\ud800x""#, LIMITS), Error::Surrogate);
    assert_eq!(failure(br#""\ud800\n""#, LIMITS), Error::Surrogate, "an escape that is not a \\u");
    assert_eq!(failure(b"\"\\ud800\\u0041\"", LIMITS), Error::Surrogate, "a \\u that is not low");
    assert_eq!(failure(br#""\ud800\ud800""#, LIMITS), Error::Surrogate);
    assert_eq!(failure(br#""\udc00""#, LIMITS), Error::Surrogate, "a low surrogate alone");
    assert_eq!(failure(br#""\ud800\u12""#, LIMITS), Error::Escape);
}

#[test]
fn text_must_be_utf8() {
    assert_eq!(tokens("\"é€😀\"".as_bytes()), vec![string("é€😀".as_bytes())]);
    assert_eq!(tokens(b"\"\x7F\""), vec![string(b"\x7F")], "DEL is not a control character in JSON");
    for bad in [
        &b"\"\x80\""[..],            // a continuation byte alone
        b"\"\xC0\x80\"",             // overlong
        b"\"\xC1\xBF\"",             // overlong
        b"\"\xE0\x80\x80\"",         // overlong
        b"\"\xED\xA0\x80\"",         // a surrogate
        b"\"\xF0\x80\x80\x80\"",     // overlong
        b"\"\xF4\x90\x80\x80\"",     // past U+10FFFF
        b"\"\xF5\x80\x80\x80\"",     // past U+10FFFF
        b"\"\xFF\"",                 // never in UTF-8
        b"\"\xC3\"",                 // cut by the quote
        b"\"\xE2\x82\"",             // cut by the quote
        b"{\"\xE2\x82\xAC\xE2\":1}", // a key
    ] {
        assert_eq!(failure(bad, LIMITS), Error::Utf8, "{}", bad.escape_ascii());
    }
}

#[test]
fn a_control_character_must_be_escaped() {
    assert_eq!(failure(b"\"a\nb\"", LIMITS), Error::Control);
    assert_eq!(failure(b"\"\x00\"", LIMITS), Error::Control);
    assert_eq!(failure(b"\"\x1F\"", LIMITS), Error::Control);
}

#[test]
fn numbers_follow_the_grammar() {
    for good in ["0", "-0", "1", "-1", "10", "0.5", "-0.0", "1e5", "1E+5", "1e-05", "2.5E3", "99999999"] {
        assert_eq!(tokens(good.as_bytes()), vec![number(good.as_bytes())], "{good}");
    }
    for bad in ["01", "-01", "1.", "-", ".5", "+1", "1e", "1e+", "1.e5", "--1", "1-2", "1.2.3", "1e5e", "0x1"] {
        let error = failure(bad.as_bytes(), LIMITS);
        assert!(error == Error::Number || error == Error::Unexpected || error == Error::Trailing, "{bad}: {error:?}");
    }
    assert_eq!(failure(b"01", LIMITS), Error::Number);
    assert_eq!(failure(b".5", LIMITS), Error::Unexpected, "no number starts with a point");
    assert_eq!(failure(b"0x1", LIMITS), Error::Trailing, "a zero, then something else");
    assert_eq!(failure(b"[0x1]", LIMITS), Error::Unexpected);
    assert_eq!(failure(b"[NaN]", LIMITS), Error::Unexpected);
    assert_eq!(failure(b"-Infinity", LIMITS), Error::Number);
}

#[test]
fn the_grammar_is_checked_between_tokens() {
    for (bad, error) in [
        (&b"{\"a\" 1}"[..], Error::Unexpected),
        (b"{\"a\":1,}", Error::Unexpected),
        (b"[1,]", Error::Unexpected),
        (b"[,1]", Error::Unexpected),
        (b"[1 2]", Error::Unexpected),
        (b"{a:1}", Error::Unexpected),
        (b"{'a':1}", Error::Unexpected),
        (b"[}", Error::Unexpected),
        (b"{]", Error::Unexpected),
        (b"[1}", Error::Unexpected),
        (b"{\"a\":1]", Error::Unexpected),
        (b"[tru]", Error::Unexpected),
        (b"[nulL]", Error::Unexpected),
        (b"{} {}", Error::Trailing),
        (b"1 2", Error::Trailing),
        (b"\"a\" x", Error::Trailing),
        (b"", Error::Truncated),
        (b"   ", Error::Truncated),
        (b"{", Error::Truncated),
        (b"{\"a\"", Error::Truncated),
        (b"{\"a\":", Error::Truncated),
        (b"[1,", Error::Truncated),
        (b"\"abc", Error::Truncated),
        (b"tr", Error::Truncated),
    ] {
        assert_eq!(failure(bad, LIMITS), error, "{}", bad.escape_ascii());
    }
}

#[test]
fn a_string_cut_at_every_scan_maximum_reads_the_same() {
    let document = "[\"a\\\"b\\\\\\u00e9\\ud83d\\ude00é😀\", \"\"]".as_bytes();
    let expected = vec![Token::ArrayStart, string("a\"b\\é😀é😀".as_bytes()), string(b""), Token::ArrayEnd];
    for chunk in 1..=24 {
        let limits = Limits { chunk, string: 64, ..LIMITS };
        let mut events = read(document, limits);
        assert_eq!(events.pop(), Some(Event::Done), "chunk {chunk}");
        let mut tokens = Vec::new();
        for token in &expected {
            tokens.push(Event::Token(token.clone()));
        }
        assert_eq!(events, tokens, "chunk {chunk}");
    }
}

#[test]
fn bytes_lost_at_the_end_of_a_scan_are_never_seen() {
    // The control character sits in a scan the end of the stream leaves
    // unmet: the document is cut short, whatever the bytes held.
    assert_eq!(failure(b"\"ab\x01", Limits { chunk: 8, ..LIMITS }), Error::Truncated);
    assert_eq!(failure(b"\"ab\x01", Limits { chunk: 1, ..LIMITS }), Error::Control);
}

#[test]
fn the_largest_demand_is_the_chunk_or_a_literal() {
    assert_eq!(json::largest_demand(&Limits { chunk: 1, ..LIMITS }), 4, "the rest of false");
    assert_eq!(json::largest_demand(&Limits { chunk: 4096, ..LIMITS }), 4096);
}

#[test]
fn the_worst_case_is_the_stack_and_the_longest_text() {
    let limits = Limits { depth: 10, string: 100, number: 40, chunk: 16 };
    assert_eq!(json::worst_case(&limits), Some(10 + 100), "a container is a byte");
    assert_eq!(json::worst_case(&Limits { number: 200, ..limits }), Some(10 + 200));
    assert_eq!(json::worst_case(&Limits { chunk: 0, ..limits }), None, "a scan of nothing");
}

#[test]
fn deliveries_cut_anywhere_in_a_character_read_the_same() {
    // Each scan delivers what the side below held, so a test of the cuts
    // is a test of the scan's maximum; the step is fed by hand here.
    let text = "aé€😀".as_bytes();
    for cut in 1..text.len() {
        let mut machine = Machine::new(Limits { chunk: 16, ..LIMITS });
        assert_eq!(machine.down(Request::Next), (None, demand(Read::Fill(1))));
        assert_eq!(machine.bytes(b"\""), (None, demand(scan(16))));
        let (first, second) = text.split_at_checked(cut).unwrap();
        assert_eq!(machine.bytes(first), (None, demand(scan(16))), "cut at {cut}");
        let mut last = Vec::from(second);
        last.push(b'"');
        let (event, down) = machine.up(Up::Bytes(boxed(&last)));
        assert_eq!(event, Some(Event::Token(string(text))), "cut at {cut}");
        assert_eq!(down, None);
    }
}
