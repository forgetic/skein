//! The names that cross layer boundaries (programming-model.md, 4.2 and 4.4).

/// An opaque name for an entity of another layer, or of this layer as another
/// layer knows it.
///
/// A layer makes tokens from its own handles (`Id::token`) and turns back into
/// handles only the tokens it issued, in the record variant it issued them for.
/// The layer that holds a token stores it without interpreting it, and echoes
/// it on every record about that entity.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Token(u64);

impl Token {
    #[must_use]
    pub const fn new(raw: u64) -> Token {
        Token(raw)
    }

    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// The right to answer one call.
///
/// A call that comes up to a layer carries a `ReplyTo`, and the layer answers
/// with exactly one reply, which consumes it, now or later. It is neither
/// `Copy` nor `Clone`, so a second reply does not compile; the simulator
/// catches a missing one.
#[derive(PartialEq, Eq, Hash, Debug)]
#[must_use = "every call is answered exactly once"]
pub struct ReplyTo(Token);

impl ReplyTo {
    /// Issued by the layer the call came from, naming the call in its own terms.
    pub const fn new(token: Token) -> ReplyTo {
        ReplyTo(token)
    }

    /// The token of the call, for the layer that issued it, once the reply
    /// has reached it.
    #[must_use]
    pub const fn into_token(self) -> Token {
        self.0
    }
}
