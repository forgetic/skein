//! Typed handles (4.1).

#![expect(
    clippy::disallowed_types,
    reason = "PhantomData ties a handle to its entity type; this is the one place that needs it"
)]

use core::cmp::Ordering;
use core::fmt;
use core::hash::{Hash, Hasher};
use core::marker::PhantomData;

use crate::Token;

/// The name of an entity of type `T`: a slot index in the entity's slab plus
/// the slot's generation, so a handle is never reused for another entity.
///
/// `Id<A>` and `Id<B>` are different types. A lookup checks the generation, so
/// a stale handle finds nothing rather than the entity that took its slot.
pub struct Id<T> {
    slot: u32,
    generation: u32,
    entity: PhantomData<T>,
}

impl<T> Id<T> {
    pub(crate) const fn new(slot: u32, generation: u32) -> Id<T> {
        Id { slot, generation, entity: PhantomData }
    }

    pub(crate) const fn slot(self) -> u32 {
        self.slot
    }

    pub(crate) const fn generation(self) -> u32 {
        self.generation
    }

    /// This handle as a token, to pass to another layer.
    #[must_use]
    pub const fn token(self) -> Token {
        let [s0, s1, s2, s3] = self.slot.to_be_bytes();
        let [g0, g1, g2, g3] = self.generation.to_be_bytes();
        Token::new(u64::from_be_bytes([s0, s1, s2, s3, g0, g1, g2, g3]))
    }

    /// The handle a token was made from. Only for tokens this layer issued
    /// for entities of type `T`: nothing else checks it.
    #[must_use]
    pub const fn from_token(token: Token) -> Id<T> {
        let [s0, s1, s2, s3, g0, g1, g2, g3] = token.raw().to_be_bytes();
        Id::new(u32::from_be_bytes([s0, s1, s2, s3]), u32::from_be_bytes([g0, g1, g2, g3]))
    }
}

// By hand, because derives would require `T` to implement each trait too.

impl<T> Clone for Id<T> {
    fn clone(&self) -> Id<T> {
        *self
    }
}

impl<T> Copy for Id<T> {}

impl<T> PartialEq for Id<T> {
    fn eq(&self, other: &Id<T>) -> bool {
        self.slot == other.slot && self.generation == other.generation
    }
}

impl<T> Eq for Id<T> {}

impl<T> PartialOrd for Id<T> {
    fn partial_cmp(&self, other: &Id<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Id<T> {
    fn cmp(&self, other: &Id<T>) -> Ordering {
        self.slot.cmp(&other.slot).then(self.generation.cmp(&other.generation))
    }
}

impl<T> Hash for Id<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.slot.hash(state);
        self.generation.hash(state);
    }
}

impl<T> fmt::Debug for Id<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Id").field("slot", &self.slot).field("generation", &self.generation).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Id;

    #[test]
    fn a_handle_survives_the_round_trip_through_a_token() {
        let id: Id<u8> = Id::new(0xDEAD_BEEF, 7);
        assert_eq!(Id::<u8>::from_token(id.token()), id);
        assert_ne!(Id::<u8>::new(1, 2).token(), Id::<u8>::new(2, 1).token());
    }
}
