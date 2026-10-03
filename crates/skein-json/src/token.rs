//! What the tokenizer reads and the writer writes (json.md, 3 and 4).

use alloc::boxed::Box;

/// One token of a JSON document (RFC 8259): the start or end of an object
/// or an array, a key, or a value that holds no other.
///
/// The commas and colons between them, and the whitespace, are the
/// tokenizer's to check and the writer's to put; they are not tokens.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Token {
    /// `{`
    ObjectStart,
    /// `}`
    ObjectEnd,
    /// `[`
    ArrayStart,
    /// `]`
    ArrayEnd,
    /// An object member's key: its text, unescaped, valid UTF-8, in a box of
    /// exactly its length. Its value is the token after it.
    Key(Box<[u8]>),
    /// A string: its text, unescaped, valid UTF-8, in a box of exactly its
    /// length.
    String(Box<[u8]>),
    /// A number, as the text of the document: validated against JSON's
    /// grammar, never converted. The consumer parses the integers it
    /// expects, with checks (json.md, 3).
    Number(Box<[u8]>),
    /// `true`
    True,
    /// `false`
    False,
    /// `null`
    Null,
}
