//! The transcripts (testing-strategy.md, 4.1): documents kept as files in
//! `transcripts/`, each `<name>.json` beside `<name>.expect`, what it must
//! decode to.
//!
//! An expectation lists one token a line, then the outcome:
//!
//! ```text
//! # a comment
//! limits depth 16 string 256 length 4096 chunk 64 (optional; any of them)
//! object
//! key "name"
//! string "caf\xc3\xa9"                            (or the UTF-8 itself)
//! number -1.5e3
//! true
//! false
//! null
//! end object
//! array
//! end array
//! done                                            (or: failed too-deep)
//! ```
//!
//! Text is quoted, with `\\`, `\"`, `\n`, `\r`, `\t` and `\xHH` escapes.
//! The limits not given are [`LIMITS`]'; a `chunk` given pins the scans'
//! maximum, which the tests otherwise vary.

use std::fmt::Write as _;
use std::fs;
use std::path::PathBuf;

use skein_json::Token;
use skein_json::tokenizer::{Error, Limits};

use crate::{Decoded, Outcome};

/// The limits a transcript is read with, unless its expectation says
/// otherwise.
pub const LIMITS: Limits = Limits { depth: 32, string: 4096, number: 64, chunk: 64, length: 1 << 20 };

/// A transcript and what it must decode to.
#[derive(Clone, Debug)]
pub struct Transcript {
    pub name: String,
    pub document: Vec<u8>,
    pub limits: Limits,
    /// Whether the expectation pins the scans' maximum.
    pub pinned_chunk: bool,
    /// What it must decode to: `None` while it has no expectation yet.
    pub expected: Option<Decoded>,
}

/// Every transcript, in the order of their names.
#[must_use]
pub fn all() -> Vec<Transcript> {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("transcripts");
    let mut names = Vec::new();
    for entry in fs::read_dir(&directory).expect("the transcripts directory") {
        let path = entry.expect("a directory entry").path();
        if path.extension().is_some_and(|extension| extension == "json") {
            names.push(path.file_stem().expect("a name").to_string_lossy().into_owned());
        }
    }
    names.sort();
    let mut transcripts = Vec::new();
    for name in names {
        let document = fs::read(directory.join(format!("{name}.json"))).expect("the document");
        let (limits, pinned_chunk, expected) = match fs::read(directory.join(format!("{name}.expect"))) {
            Ok(text) => {
                let (limits, pinned, decoded) = parse(&text).unwrap_or_else(|error| panic!("{name}.expect: {error}"));
                (limits, pinned, Some(decoded))
            }
            Err(_) => (LIMITS, false, None),
        };
        transcripts.push(Transcript { name, document, limits, pinned_chunk, expected });
    }
    transcripts
}

/// An expectation's limits, whether it pins the chunk, and the decoding.
///
/// # Errors
///
/// What is wrong with the expectation, and on which line.
pub fn parse(text: &[u8]) -> Result<(Limits, bool, Decoded), String> {
    let mut limits = LIMITS;
    let mut pinned = false;
    let mut tokens = Vec::new();
    let mut outcome = None;
    for (number, line) in text.split(|&byte| byte == b'\n').enumerate() {
        let line = line.trim_ascii();
        if line.is_empty() || line.starts_with(b"#") {
            continue;
        }
        let at = |message: &str| format!("line {}: {message}", number + 1);
        if outcome.is_some() {
            return Err(at("nothing follows the outcome"));
        }
        let (word, rest) = match line.iter().position(|&byte| byte == b' ') {
            Some(space) => (&line[..space], line[space + 1..].trim_ascii()),
            None => (line, &b""[..]),
        };
        match word {
            b"limits" => {
                let words: Vec<&[u8]> = rest.split(|&byte| byte == b' ').filter(|word| !word.is_empty()).collect();
                for pair in words.chunks(2) {
                    let [name, value] = pair else { return Err(at("a limit without a value")) };
                    let value: u32 = std::str::from_utf8(value)
                        .ok()
                        .and_then(|value| value.parse().ok())
                        .ok_or_else(|| at("a limit is a number"))?;
                    match *name {
                        b"depth" => limits.depth = value,
                        b"string" => limits.string = value,
                        b"number" => limits.number = value,
                        b"length" => limits.length = value,
                        b"chunk" => {
                            limits.chunk = value;
                            pinned = true;
                        }
                        _ => return Err(at("an unknown limit")),
                    }
                }
            }
            b"object" => tokens.push(Token::ObjectStart),
            b"array" => tokens.push(Token::ArrayStart),
            b"end" => match rest {
                b"object" => tokens.push(Token::ObjectEnd),
                b"array" => tokens.push(Token::ArrayEnd),
                _ => return Err(at("end object, or end array")),
            },
            b"key" => tokens.push(Token::Key(unquote(rest).ok_or_else(|| at("a quoted key"))?)),
            b"string" => tokens.push(Token::String(unquote(rest).ok_or_else(|| at("a quoted string"))?)),
            b"number" => tokens.push(Token::Number(rest.into())),
            b"true" => tokens.push(Token::True),
            b"false" => tokens.push(Token::False),
            b"null" => tokens.push(Token::Null),
            b"done" => outcome = Some(Outcome::Done),
            b"failed" => outcome = Some(Outcome::Failed(error(rest).ok_or_else(|| at("an error's name"))?)),
            _ => return Err(at("an unknown line")),
        }
    }
    let outcome = outcome.ok_or("no outcome")?;
    Ok((limits, pinned, Decoded { tokens, outcome }))
}

/// An expectation of `decoded`, as a draft for a transcript that has none
/// yet: to be read, and checked against the document, before it is kept.
#[must_use]
pub fn render(limits: &Limits, decoded: &Decoded) -> String {
    let mut out = String::new();
    if *limits != LIMITS {
        let Limits { depth, string, number, chunk, length } = *limits;
        writeln!(out, "limits depth {depth} string {string} number {number} chunk {chunk} length {length}")
            .expect("writing to a String");
    }
    for token in &decoded.tokens {
        match token {
            Token::ObjectStart => out.push_str("object\n"),
            Token::ObjectEnd => out.push_str("end object\n"),
            Token::ArrayStart => out.push_str("array\n"),
            Token::ArrayEnd => out.push_str("end array\n"),
            Token::Key(text) => writeln!(out, "key {}", quote(text)).expect("writing to a String"),
            Token::String(text) => writeln!(out, "string {}", quote(text)).expect("writing to a String"),
            Token::Number(text) => {
                writeln!(out, "number {}", String::from_utf8_lossy(text)).expect("writing to a String");
            }
            Token::True => out.push_str("true\n"),
            Token::False => out.push_str("false\n"),
            Token::Null => out.push_str("null\n"),
        }
    }
    match decoded.outcome {
        Outcome::Done => out.push_str("done\n"),
        Outcome::Failed(error) => writeln!(out, "failed {}", error_name(error)).expect("writing to a String"),
    }
    out
}

const ERRORS: [(Error, &str); 12] = [
    (Error::Unexpected, "unexpected"),
    (Error::Trailing, "trailing"),
    (Error::TooLong, "too-long"),
    (Error::TooDeep, "too-deep"),
    (Error::StringTooLong, "string-too-long"),
    (Error::NumberTooLong, "number-too-long"),
    (Error::Number, "number"),
    (Error::Escape, "escape"),
    (Error::Surrogate, "surrogate"),
    (Error::Utf8, "utf8"),
    (Error::Control, "control"),
    (Error::Truncated, "truncated"),
];

fn error(name: &[u8]) -> Option<Error> {
    ERRORS.iter().find(|(_, known)| known.as_bytes() == name).map(|(error, _)| *error)
}

fn error_name(error: Error) -> &'static str {
    ERRORS.iter().find(|(known, _)| *known == error).map_or("stream", |(_, name)| name)
}

/// Text in quotes, as an expectation writes it: UTF-8 as it is, and the
/// rest escaped.
fn quote(text: &[u8]) -> String {
    let mut out = String::from("\"");
    for chunk in text.utf8_chunks() {
        for character in chunk.valid().chars() {
            match character {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                _ if character.is_control() => {
                    let mut buffer = [0; 4];
                    for byte in character.encode_utf8(&mut buffer).as_bytes() {
                        write!(out, "\\x{byte:02x}").expect("writing to a String");
                    }
                }
                _ => out.push(character),
            }
        }
        for byte in chunk.invalid() {
            write!(out, "\\x{byte:02x}").expect("writing to a String");
        }
    }
    out.push('"');
    out
}

/// The text of `"..."`, unescaped.
fn unquote(quoted: &[u8]) -> Option<Box<[u8]>> {
    let inner = quoted.strip_prefix(b"\"")?.strip_suffix(b"\"")?;
    let mut text = Vec::new();
    let mut bytes = inner.iter();
    while let Some(&byte) = bytes.next() {
        if byte != b'\\' {
            text.push(byte);
            continue;
        }
        match bytes.next()? {
            b'\\' => text.push(b'\\'),
            b'"' => text.push(b'"'),
            b'n' => text.push(b'\n'),
            b'r' => text.push(b'\r'),
            b't' => text.push(b'\t'),
            b'x' => {
                let high = char::from(*bytes.next()?).to_digit(16)?;
                let low = char::from(*bytes.next()?).to_digit(16)?;
                text.push(u8::try_from(high * 16 + low).ok()?);
            }
            _ => return None,
        }
    }
    Some(text.into_boxed_slice())
}
