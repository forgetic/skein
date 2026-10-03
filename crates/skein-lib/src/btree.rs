//! The standard library's B-trees, priced: what a bounded map or set takes
//! (programming-model.md, 6.3), counted in tree nodes.

use core::mem::size_of;

/// The standard library's B-tree nodes (`B = 6` in `alloc::collections::btree`)
/// hold at most 11 entries, and every node but the root at least 5. Nothing
/// checks these against the library but a test with a counting allocator.
const NODE_CAPACITY: usize = 11;
const NODE_MIN: u64 = 5;

/// The most heap a B-tree of `entries` keys and values of `key` and `value`
/// bytes, aligned to `align`, takes: a node for every `NODE_MIN` entries and
/// one for the root, each priced as an internal node, the larger kind. `None`
/// past a `u64`.
pub(crate) fn worst_case(entries: u32, key: usize, value: usize, align: usize) -> Option<u64> {
    let word = size_of::<usize>();
    let align = align.max(word);
    // A leaf node: its parent link, its index in the parent and its length,
    // then its keys and values. Each part is rounded up to the alignment, so
    // the bound holds whatever order the fields are laid out in.
    let header = word.checked_add(size_of::<u16>().checked_mul(2)?)?;
    let leaf = round_up(header, align)?
        .checked_add(round_up(key.checked_mul(NODE_CAPACITY)?, align)?)?
        .checked_add(round_up(value.checked_mul(NODE_CAPACITY)?, align)?)?;
    // An internal node: a leaf and an edge either side of each entry.
    let edges = word.checked_mul(NODE_CAPACITY.checked_add(1)?)?;
    let internal = u64::try_from(leaf.checked_add(round_up(edges, align)?)?).ok()?;
    let nodes = (u64::from(entries) / NODE_MIN).checked_add(1)?;
    nodes.checked_mul(internal)
}

fn round_up(bytes: usize, align: usize) -> Option<usize> {
    bytes.checked_next_multiple_of(align)
}
