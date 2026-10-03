//! Documents from a seed (testing-strategy.md, 2.4): token sequences that
//! are valid documents, rendered as text with whitespace and escapes drawn
//! at random, and that text mutated.

use skein_json::Token;
use skein_lib::Rng;

/// How large a generated document grows.
#[derive(Clone, Copy, Debug)]
pub struct Shape {
    /// How deep it nests, at most.
    pub depth: u32,
    /// The most members or elements in one object or array.
    pub width: u32,
    /// The longest string or key, in characters.
    pub string: u32,
}

/// A valid document's tokens.
#[must_use]
pub fn tokens(rng: &mut Rng, shape: Shape) -> Vec<Token> {
    let mut tokens = Vec::new();
    value(rng, shape, 0, &mut tokens);
    tokens
}

fn value(rng: &mut Rng, shape: Shape, depth: u32, tokens: &mut Vec<Token>) {
    // Mostly an object or an array at the top, as documents are.
    let nests = depth < shape.depth && rng.chance(if depth == 0 { 900 } else { 500 });
    match rng.below(if nests { 2 } else { 6 }) {
        0 if nests => container(rng, shape, depth, tokens, true),
        1 if nests => container(rng, shape, depth, tokens, false),
        0 | 1 => tokens.push(Token::String(text(rng, shape.string))),
        2 => tokens.push(Token::Number(number(rng))),
        3 => tokens.push(Token::True),
        4 => tokens.push(Token::False),
        _ => tokens.push(Token::Null),
    }
}

fn container(rng: &mut Rng, shape: Shape, depth: u32, tokens: &mut Vec<Token>, object: bool) {
    tokens.push(if object { Token::ObjectStart } else { Token::ArrayStart });
    // Now and then empty; otherwise one member or more, up to the width.
    let members = if rng.chance(100) { 0 } else { rng.between(1, u64::from(shape.width)) };
    for _ in 0..members {
        if object {
            tokens.push(Token::Key(text(rng, shape.string)));
        }
        value(rng, shape, depth + 1, tokens);
    }
    tokens.push(if object { Token::ObjectEnd } else { Token::ArrayEnd });
}

/// Characters a string is made of: ASCII, the bytes JSON escapes, and
/// characters of two, three and four bytes, the edges of each among them.
const CHARACTERS: &[char] = &[
    'a',
    'b',
    'z',
    'A',
    '0',
    ' ',
    '/',
    '"',
    '\\',
    '\n',
    '\r',
    '\t',
    '\u{0}',
    '\u{1}',
    '\u{8}',
    '\u{c}',
    '\u{1f}',
    '\u{7f}',
    '\u{80}',
    'é',
    '\u{7ff}',
    '\u{800}',
    '€',
    '\u{d7ff}',
    '\u{e000}',
    '\u{fffd}',
    '\u{ffff}',
    '\u{10000}',
    '😀',
    '\u{10ffff}',
];

/// A string's text: UTF-8 of up to `longest` characters.
#[must_use]
pub fn text(rng: &mut Rng, longest: u32) -> Box<[u8]> {
    let mut text = String::new();
    for _ in 0..rng.below(u64::from(longest) + 1) {
        text.push(CHARACTERS[usize::try_from(rng.below(CHARACTERS.len() as u64)).expect("fits a usize")]);
    }
    text.into_bytes().into_boxed_slice()
}

/// A number's text, valid.
#[must_use]
pub fn number(rng: &mut Rng) -> Box<[u8]> {
    let mut text = Vec::new();
    if rng.chance(300) {
        text.push(b'-');
    }
    if rng.chance(200) {
        text.push(b'0');
    } else {
        digits(rng, &mut text, true);
    }
    if rng.chance(300) {
        text.push(b'.');
        digits(rng, &mut text, false);
    }
    if rng.chance(300) {
        text.push(if rng.chance(500) { b'e' } else { b'E' });
        match rng.below(3) {
            0 => text.push(b'+'),
            1 => text.push(b'-'),
            _ => {}
        }
        digits(rng, &mut text, false);
    }
    text.into_boxed_slice()
}

fn digits(rng: &mut Rng, text: &mut Vec<u8>, leading: bool) {
    for at in 0..rng.between(1, 6) {
        let low = u64::from(leading && at == 0);
        text.push(b'0' + u8::try_from(rng.between(low, 9)).expect("a digit"));
    }
}

/// `tokens` as JSON text, with whitespace between them and an escape for
/// each character drawn at random among those that spell it.
#[must_use]
pub fn render(rng: &mut Rng, tokens: &[Token]) -> Vec<u8> {
    let mut out = Vec::new();
    // Whether the next value is not the first in its container.
    let mut after_value = false;
    whitespace(rng, &mut out);
    for token in tokens {
        let ends = matches!(token, Token::ObjectEnd | Token::ArrayEnd);
        if after_value && !ends {
            out.push(b',');
            whitespace(rng, &mut out);
        }
        match token {
            Token::ObjectStart => out.push(b'{'),
            Token::ObjectEnd => out.push(b'}'),
            Token::ArrayStart => out.push(b'['),
            Token::ArrayEnd => out.push(b']'),
            Token::Key(text) => {
                string(rng, text, &mut out);
                whitespace(rng, &mut out);
                out.push(b':');
            }
            Token::String(text) => string(rng, text, &mut out),
            Token::Number(text) => out.extend_from_slice(text),
            Token::True => out.extend_from_slice(b"true"),
            Token::False => out.extend_from_slice(b"false"),
            Token::Null => out.extend_from_slice(b"null"),
        }
        whitespace(rng, &mut out);
        after_value = !matches!(token, Token::ObjectStart | Token::ArrayStart | Token::Key(_));
    }
    out
}

fn whitespace(rng: &mut Rng, out: &mut Vec<u8>) {
    if rng.chance(700) {
        return;
    }
    for _ in 0..rng.between(1, 4) {
        out.push(b" \t\n\r"[usize::try_from(rng.below(4)).expect("fits a usize")]);
    }
}

fn string(rng: &mut Rng, text: &[u8], out: &mut Vec<u8>) {
    out.push(b'"');
    for character in std::str::from_utf8(text).expect("generated text is UTF-8").chars() {
        let short = match character {
            '"' => Some(b'"'),
            '\\' => Some(b'\\'),
            '/' => Some(b'/'),
            '\u{8}' => Some(b'b'),
            '\u{c}' => Some(b'f'),
            '\n' => Some(b'n'),
            '\r' => Some(b'r'),
            '\t' => Some(b't'),
            _ => None,
        };
        let must_escape = character < ' ' || character == '"' || character == '\\';
        match rng.below(3) {
            0 if short.is_some() => {
                out.push(b'\\');
                out.push(short.unwrap_or_default());
            }
            1 => unicode(rng, character, out),
            _ if must_escape => unicode(rng, character, out),
            _ => {
                let mut buffer = [0; 4];
                out.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
            }
        }
    }
    out.push(b'"');
}

/// A character as `\u` escapes, a surrogate pair past the basic plane, its
/// hex digits in either case.
fn unicode(rng: &mut Rng, character: char, out: &mut Vec<u8>) {
    let mut units = [0; 2];
    for unit in character.encode_utf16(&mut units) {
        let digits = if rng.chance(500) { format!("{unit:04x}") } else { format!("{unit:04X}") };
        out.push(b'\\');
        out.push(b'u');
        out.extend_from_slice(digits.as_bytes());
    }
}

/// Bytes a mutation inserts or writes over another: JSON's structure,
/// escapes, the starts of literals and numbers, and bytes that are not
/// UTF-8 or begin a character at an edge.
const BYTES: &[u8] = b"{}[],:\"\\ \n-+.eE0179tfnux/\x00\x01\x1f\x7f\x80\xbf\xc0\xc3\xe0\xed\xf0\xf4\xf5\xff";

/// `document` with a few edits drawn at random.
#[must_use]
pub fn mutate(rng: &mut Rng, document: &[u8]) -> Vec<u8> {
    let mut out = document.to_vec();
    for _ in 0..rng.between(1, 3) {
        let at = usize::try_from(rng.below(out.len() as u64 + 1)).expect("fits a usize");
        let byte = BYTES[usize::try_from(rng.below(BYTES.len() as u64)).expect("fits a usize")];
        match rng.below(6) {
            0 | 1 => out.insert(at, byte),
            2 if at < out.len() => out[at] = byte,
            3 if at < out.len() => {
                out.remove(at);
            }
            4 => {
                let end = (at + usize::try_from(rng.between(1, 16)).expect("fits a usize")).min(out.len());
                let copied = out[at..end].to_vec();
                let to = usize::try_from(rng.below(out.len() as u64 + 1)).expect("fits a usize");
                out.splice(to..to, copied);
            }
            _ => out.truncate(at),
        }
    }
    out
}

/// A document nested `depth` deep, arrays and objects alternating.
#[must_use]
pub fn nested(depth: u32) -> Vec<u8> {
    let mut out = Vec::new();
    for level in 0..depth {
        out.extend_from_slice(if level % 2 == 0 { b"[" } else { b"{\"k\":" });
    }
    out.extend_from_slice(b"null");
    for level in (0..depth).rev() {
        out.push(if level % 2 == 0 { b']' } else { b'}' });
    }
    out
}
