//! The bounded containers (lib.md, 5): each refuses past its capacity and
//! keeps its order, and a B-tree is priced in its nodes.

use alloc::boxed::Box;

use crate::btree::worst_case;
use crate::{List, Map, Queue, Set, Stack};

#[test]
fn a_list_refuses_past_its_capacity() {
    let mut list = List::with_capacity(2);
    assert_eq!(list.push(1_u8), Ok(()));
    assert_eq!(list.push(2), Ok(()));
    assert_eq!(list.push(3), Err(3));
    assert_eq!(list.get(1), Some(&2));
    assert_eq!(list.get(2), None);
    assert_eq!(&*list.to_boxed(), &[1, 2]);
    assert_eq!(&*list.into_boxed(), &[1, 2]);
}

#[test]
fn slots_pushed_in_order_are_filled_in_any_order() {
    let mut results = List::with_capacity(3);
    for _ in 0_u32..3 {
        results.push(None).expect("room");
    }
    for (index, result) in [(2, 'c'), (0, 'a'), (1, 'b')] {
        let slot = results.get_mut(index).expect("a slot per call");
        *slot = Some(result);
    }
    assert_eq!(results.get_mut(3), None);
    assert_eq!(&*results.into_boxed(), &[Some('a'), Some('b'), Some('c')]);
}

#[test]
fn a_list_cleared_is_refilled_to_its_capacity() {
    let mut list = List::with_capacity(2);
    assert_eq!(list.push(1_u8), Ok(()));
    assert_eq!(list.push(2), Ok(()));
    list.clear();
    assert!(list.is_empty());
    assert_eq!(list.room(), 2, "its capacity kept");
    assert_eq!(list.push(3), Ok(()));
    assert_eq!(&*list.into_boxed(), &[3]);
}

#[test]
fn a_full_map_refuses_new_keys_but_replaces_values() {
    let mut map = Map::with_capacity(2);
    assert_eq!(map.insert('b', 2_u8), Ok(None));
    assert_eq!(map.insert('a', 1), Ok(None));
    assert_eq!(map.insert('c', 3), Err(('c', 3)));
    assert_eq!(map.insert('a', 10), Ok(Some(1)));
    assert_eq!(map.len(), 2);
    assert_eq!(map.get(&'a'), Some(&10));
    assert!(!map.contains_key(&'c'));
    assert_eq!(map.remove(&'b'), Some(2));
    assert_eq!(map.remove(&'b'), None);
    assert_eq!(map.insert('c', 3), Ok(None), "removing makes room");
}

#[test]
fn entries_come_out_in_key_order() {
    let mut map = Map::with_capacity(4);
    for key in [3_u8, 1, 4, 2] {
        map.insert(key, u32::from(key) * 10).expect("room");
    }
    for (expected, (key, value)) in (1_u8..).zip(&map) {
        assert_eq!((*key, *value), (expected, u32::from(expected) * 10));
    }
    assert_eq!(map.first(), Some((&1, &10)));
    assert_eq!(map.last(), Some((&4, &40)));
    if let Some(value) = map.get_mut(&4) {
        *value = 0;
    }
    assert_eq!(map.iter().last(), Some((&4, &0)));
}

#[test]
fn a_map_keyed_by_bytes_is_queried_by_a_slice() {
    let mut map: Map<Box<[u8]>, u8> = Map::with_capacity(1);
    map.insert(Box::from(&b"src/lib.rs"[..]), 7).expect("room");
    assert_eq!(map.get(&b"src/lib.rs"[..]), Some(&7));
    assert!(map.contains_key(&b"src/lib.rs"[..]));
    assert_eq!(map.get(&b"src"[..]), None);
    assert_eq!(map.remove(&b"src/lib.rs"[..]), Some(7));
}

#[test]
fn a_zero_capacity_map_refuses_everything() {
    let mut map = Map::with_capacity(0);
    assert_eq!(map.insert(1_u8, ()), Err((1, ())));
    assert!(map.is_empty());
    assert_eq!(map.first(), None);
}

#[test]
fn a_full_set_refuses_new_keys_only() {
    let mut set = Set::with_capacity(2);
    assert_eq!(set.insert(2_u8), Ok(true));
    assert_eq!(set.insert(1), Ok(true));
    assert_eq!(set.insert(3), Err(3));
    assert_eq!(set.insert(1), Ok(false), "a key already present takes no new room");
    assert!(set.contains(&2));
    assert_eq!((set.first(), set.last()), (Some(&1), Some(&2)));
    for (expected, key) in (1_u8..).zip(&set) {
        assert_eq!(*key, expected);
    }
    assert!(set.remove(&1));
    assert!(!set.remove(&1));
    assert_eq!(set.insert(3), Ok(true), "removing makes room");
    assert_eq!(set.len(), 2);
}

#[test]
fn the_least_key_is_taken_first_and_makes_room() {
    let mut set = Set::with_capacity(2);
    assert_eq!(set.pop_first(), None);
    assert_eq!(set.insert(5_u8), Ok(true));
    assert_eq!(set.insert(4), Ok(true));
    assert_eq!(set.pop_first(), Some(4));
    assert_eq!(set.insert(6), Ok(true), "taking a key makes room");
    assert_eq!(set.pop_first(), Some(5));
    assert_eq!(set.pop_first(), Some(6));
    assert!(set.is_empty());
}

#[test]
fn a_zero_capacity_set_refuses_everything() {
    let mut set = Set::with_capacity(0);
    assert_eq!(set.insert('a'), Err('a'));
    assert!(set.is_empty());
}

#[test]
fn a_queue_is_fifo_and_bounded() {
    let mut queue = Queue::with_capacity(2);
    queue.push(1_u8);
    assert_eq!(queue.try_push(2), Ok(()));
    assert_eq!(queue.try_push(3), Err(3));
    assert_eq!(queue.room(), 0);
    assert_eq!(queue.pop(), Some(1));
    assert_eq!(queue.pop(), Some(2));
    assert_eq!(queue.pop(), None);
}

#[test]
#[should_panic(expected = "the loop reserves room for every output")]
fn pushing_past_the_reservation_is_a_bug() {
    let mut queue = Queue::with_capacity(0);
    queue.push(());
}

#[test]
fn a_stack_is_lifo_and_bounded() {
    let mut stack = Stack::with_capacity(2);
    assert_eq!(stack.push('a'), Ok(()));
    assert_eq!(stack.push('b'), Ok(()));
    assert_eq!(stack.push('c'), Err('c'));
    assert_eq!((stack.len(), stack.capacity()), (2, 2));
    assert_eq!(stack.top(), Some(&'b'));
    assert_eq!(stack.pop(), Some('b'));
    assert_eq!(stack.push('c'), Ok(()));
    assert_eq!(stack.pop(), Some('c'));
    assert_eq!(stack.pop(), Some('a'));
    assert_eq!(stack.pop(), None);
    assert!(stack.is_empty());
}

#[test]
fn the_top_changes_in_place() {
    let mut stack = Stack::with_capacity(3);
    assert_eq!(stack.top_mut(), None);
    assert_eq!(stack.push(1_u32), Ok(()));
    assert_eq!(stack.push(10), Ok(()));
    *stack.top_mut().expect("two items") += 1;
    assert_eq!(stack.pop(), Some(11));
    assert_eq!(stack.top(), Some(&1));
}

#[test]
fn a_zero_capacity_stack_refuses_everything() {
    let mut stack = Stack::with_capacity(0);
    assert_eq!(stack.push(()), Err(()));
    assert_eq!(Stack::<u64>::worst_case(4), Some(32));
    assert_eq!(Stack::<u64>::worst_case(u32::MAX), Some(8 * u64::from(u32::MAX)));
}

#[test]
fn the_price_grows_with_the_entries_and_their_size() {
    let empty = worst_case(0, 8, 8, 8).expect("fits");
    assert!(empty > 0, "the root is priced even before it is allocated");
    assert!(worst_case(5, 8, 8, 8).expect("fits") > empty);
    assert!(worst_case(5, 8, 16, 8).expect("fits") > worst_case(5, 8, 8, 8).expect("fits"));
    assert_eq!(worst_case(u32::MAX, usize::MAX, 0, 8), None);
}
