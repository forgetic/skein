use crate::{Time, Wall};

/// What a step reads besides its own state: the times of the current
/// iteration and its layer's limits. A step receives it behind a shared
/// borrow, so it is read-only (section 2).
#[derive(Debug)]
pub struct Env<L> {
    /// Read once per iteration by the shell (or the simulator), and the same
    /// for every step in the iteration.
    pub now: Time,
    /// The wall-clock time, read with `now`: for things about the world, never
    /// for a deadline (section 8).
    pub wall: Wall,
    /// The layer's configured limits.
    pub limits: L,
}
