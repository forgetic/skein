//! lib's comparisons with naive functions, run long in the fuzzy suite
//! (testing-strategy.md, 8): the byte search against one that compares at
//! every position, and the intake against a plain reference, each over
//! many random cases from a seed. lib's step tests run them short.

pub mod intake;
pub mod search;
