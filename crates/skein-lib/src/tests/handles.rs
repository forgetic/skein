//! Handles and slabs (lib.md, 3): a handle's round trip through a token, a
//! full slab, and a retired entity reachable until it is reclaimed, its
//! handle stale after.

use crate::{Id, Slab};

#[test]
fn a_handle_survives_the_round_trip_through_a_token() {
    let id: Id<u8> = Id::new(0xDEAD_BEEF, 7);
    assert_eq!(Id::<u8>::from_token(id.token()), id);
    assert_ne!(Id::<u8>::new(1, 2).token(), Id::<u8>::new(2, 1).token());
}

#[test]
fn a_full_slab_hands_the_value_back() {
    let mut slab = Slab::with_capacity(1);
    let id = slab.insert('a').expect("room for one");
    assert_eq!(slab.insert('b'), Err('b'));
    assert_eq!(slab.get(id), Some(&'a'));
    assert!(slab.is_full());
}

#[test]
fn a_retired_entity_is_reachable_until_reclaimed_and_its_handle_goes_stale() {
    let mut slab = Slab::with_capacity(1);
    let old = slab.insert('a').expect("room for one");
    slab.retire(old);
    assert_eq!(slab.get(old), Some(&'a'));
    assert_eq!(slab.len(), 1);
    slab.reclaim();
    assert_eq!(slab.get(old), None);
    assert!(slab.is_empty());
    let new = slab.insert('b').expect("the slot was freed");
    assert_ne!(new, old);
    assert_eq!(slab.get(old), None);
    assert_eq!(slab.get_mut(new), Some(&mut 'b'));
}

#[test]
#[should_panic(expected = "an entity is retired once")]
fn retiring_twice_is_a_bug() {
    let mut slab = Slab::with_capacity(1);
    let id = slab.insert(()).expect("room for one");
    slab.retire(id);
    slab.retire(id);
}

#[test]
fn a_zero_capacity_slab_refuses_everything() {
    let mut slab = Slab::with_capacity(0);
    assert_eq!(slab.insert(1_u8), Err(1));
}
