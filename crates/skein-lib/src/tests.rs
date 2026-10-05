//! lib's step tests (lib.md, 10; testing-strategy.md, 2.1): each container
//! driven through its operations, at and past its capacity, and each value
//! type through its edges, one module per area of lib.md.

mod bytes;
mod containers;
mod handles;
mod held;
mod streams;
mod time;

/// The random cases a comparison with a naive function runs here. The
/// fuzzy suite runs the same comparisons from the same seeds, over many
/// more, in tests/lib (testing-strategy.md, 8).
const ROUNDS: u32 = 300;
