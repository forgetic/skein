//! Each container's `worst_case` against the counting allocator (lib.md, 10;
//! programming-model.md, 6.3): built at a capacity, tiny ones among them,
//! filled to it, emptied and filled again, every operation a step of the
//! meter, and what it held of its own never more than `worst_case(capacity)`.
//!
//! The items own no heap: what an item owns is its owner's to count (lib.md,
//! 5). They come in several sizes and alignments, up to 64 bytes, as a
//! container's price depends on both. What an operation hands out (an item
//! taken, a delivery, a box of a list's items) is its receiver's, dropped
//! before the check.
//!
//! A slab, a queue, a list, a stack and an intake are allocated once, at their
//! capacity, and hold exactly their worst case from the start. A map, a set
//! and a deadline table allocate tree nodes as they fill, priced at the most
//! nodes the tree can have, each as the larger kind: they are filled in
//! order, each leaf left behind holding six entries, then thinned to five a
//! leaf, the fewest a leaf holds, and driven at random.

use std::any::type_name;

use skein_heap::{Counting, Meter};
use skein_lib::stream::{Delimiter, Read};
use skein_lib::{Deadlines, Id, Intake, List, Map, Queue, Rng, Set, Slab, Stack, Time};

#[global_allocator]
static HEAP: Counting = Counting;

/// Tiny capacities, a B-tree's (a leaf is full at 11 and splits at 12), and
/// enough for a tree of three levels.
const CAPACITIES: [u32; 9] = [0, 1, 2, 3, 5, 11, 12, 64, 300];

/// A container's operations, each a step of the meter, checked against its
/// worst case.
struct Metered {
    meter: Meter,
    bound: u64,
    what: (&'static str, &'static str, u32),
    /// The most it held of its own in a step.
    most: u64,
}

impl Metered {
    /// Meters a container of `capacity`, priced at `bound`, made from now on.
    /// What the test keeps besides is made before.
    fn new(container: &'static str, item: &'static str, capacity: u32, bound: Option<u64>) -> Metered {
        let bound = bound.expect("a test's capacity is priced");
        Metered { meter: Meter::new(), bound, what: (container, item, capacity), most: 0 }
    }

    fn start(&self) {
        self.meter.start();
    }

    /// Ends the step: what it handed out, `()` for nothing, is dropped as its
    /// receiver's, and what the container held of its own is checked.
    fn end<H>(&mut self, handed: H) {
        let measured = self.meter.end();
        drop(handed);
        let own = self.meter.check(measured, self.bound, &self.what);
        self.most = self.most.max(own);
    }

    /// For a container allocated once, at its capacity.
    fn held_exactly_its_worst_case(&self) {
        assert_eq!(self.most, self.bound, "{:?}: allocated at its capacity, its worst case", self.what);
    }
}

/// An item that owns no heap, the `n`th of its kind: distinct for each `n`
/// it can hold, and in their order, so that it can be a key.
trait Item: Ord + Copy {
    fn nth(n: u32) -> Self;
}

impl Item for u8 {
    fn nth(n: u32) -> u8 {
        n.to_le_bytes()[0]
    }
}

impl Item for u16 {
    fn nth(n: u32) -> u16 {
        u16::try_from(n).expect("a key the tests draw fits")
    }
}

impl Item for u64 {
    fn nth(n: u32) -> u64 {
        u64::from(n)
    }
}

impl Item for u128 {
    fn nth(n: u32) -> u128 {
        u128::from(n)
    }
}

/// The largest item: 64 bytes.
impl Item for [u64; 8] {
    fn nth(n: u32) -> [u64; 8] {
        [u64::from(n); 8]
    }
}

fn index(n: u32) -> usize {
    usize::try_from(n).expect("a u32 fits a usize")
}

/// A number in `0..bound`.
fn below(rng: &mut Rng, bound: u32) -> u32 {
    u32::try_from(rng.below(u64::from(bound))).expect("below a u32")
}

/// The keys a test of a B-tree of `capacity` draws from: as many again as it
/// holds, and some.
fn keys(capacity: u32) -> u32 {
    2 * capacity + 2
}

/// Filled in order with the keys below `next`, a B-tree's leaf splits as it
/// fills past 11, keeping six: the `j`th leaf holds 7j to 7j + 5, once
/// 7j + 11 is in. Taking each leaf's first key off leaves five, the fewest a
/// leaf holds, and makes room for the next key in order, which fills more
/// leaves: the keys taken off, each with the key put in after it.
fn thinning(next: u32) -> impl Iterator<Item = (u32, u32)> {
    (0..).map(move |leaf| (7 * leaf, next + leaf)).take_while(|&(taken, put)| taken + 11 < put)
}

/// Fills a slab to its capacity, refuses one more, and does it again after
/// half of it, then all of it, is retired and reclaimed.
fn slab<T: Item>(capacity: u32) {
    let mut ids: Vec<Id<T>> = Vec::with_capacity(index(capacity));
    let mut metered = Metered::new("Slab", type_name::<T>(), capacity, Slab::<T>::worst_case(capacity));
    metered.start();
    let mut slab = Slab::with_capacity(capacity);
    metered.end(());
    for every in [2, 1, 1] {
        for n in 0..=capacity {
            metered.start();
            let inserted = slab.insert(T::nth(n));
            metered.end(());
            match inserted {
                Ok(id) => ids.push(id),
                Err(_) => assert!(slab.is_full(), "a slab refuses only when full"),
            }
        }
        assert_eq!(slab.len(), capacity);
        let mut at = 0;
        ids.retain(|&id| {
            let retire = at % every == 0;
            at += 1;
            if retire {
                metered.start();
                slab.retire(id);
                metered.end(());
            }
            !retire
        });
        metered.start();
        slab.reclaim();
        metered.end(());
    }
    metered.held_exactly_its_worst_case();
}

/// Fills a queue to its capacity, refuses one more, and takes half of it
/// before filling it again, so that it wraps around; then takes all of it.
fn queue<T: Item>(capacity: u32) {
    let mut metered = Metered::new("Queue", type_name::<T>(), capacity, Queue::<T>::worst_case(capacity));
    metered.start();
    let mut queue = Queue::with_capacity(capacity);
    metered.end(());
    let mut next = 0;
    for _ in 0..3 {
        while queue.room() > 0 {
            metered.start();
            queue.push(T::nth(next));
            metered.end(());
            next += 1;
        }
        metered.start();
        let refused = queue.try_push(T::nth(next));
        metered.end(refused);
        for _ in 0..=capacity / 2 {
            metered.start();
            let taken = queue.pop();
            metered.end(taken);
        }
    }
    while !queue.is_empty() {
        metered.start();
        let taken = queue.pop();
        metered.end(taken);
    }
    metered.held_exactly_its_worst_case();
}

/// Fills a list to its capacity, refuses one more and copies it out; then
/// fills half of it again, and moves that out.
fn list<T: Item>(capacity: u32) {
    let mut metered = Metered::new("List", type_name::<T>(), capacity, List::<T>::worst_case(capacity));
    metered.start();
    let mut list = List::with_capacity(capacity);
    metered.end(());
    for n in 0..=capacity {
        metered.start();
        let pushed = list.push(T::nth(n));
        metered.end(pushed);
    }
    assert_eq!(list.room(), 0);
    metered.start();
    let copy = list.to_boxed();
    metered.end(copy);
    metered.start();
    list.clear();
    metered.end(());
    for n in 0..capacity / 2 {
        metered.start();
        let pushed = list.push(T::nth(n));
        metered.end(pushed);
    }
    // Moved into a box of exactly its length: shrunk, which copies.
    metered.start();
    let moved = list.into_boxed();
    metered.end(moved);
    metered.held_exactly_its_worst_case();
}

/// Fills a stack to its capacity, refuses one more, pops half of it and fills
/// it again; then pops all of it.
fn stack<T: Item>(capacity: u32) {
    let mut metered = Metered::new("Stack", type_name::<T>(), capacity, Stack::<T>::worst_case(capacity));
    metered.start();
    let mut stack = Stack::with_capacity(capacity);
    metered.end(());
    for _ in 0..2 {
        for n in 0..=capacity {
            metered.start();
            let pushed = stack.push(T::nth(n));
            metered.end(pushed);
        }
        assert_eq!(stack.len(), capacity);
        for _ in 0..=capacity / 2 {
            metered.start();
            let popped = stack.pop();
            metered.end(popped);
        }
    }
    while !stack.is_empty() {
        metered.start();
        let popped = stack.pop();
        metered.end(popped);
    }
    metered.held_exactly_its_worst_case();
}

/// Bytes enough for the largest capacity.
const BYTES: [u8; 512] = [b'a'; 512];

/// Fills an intake to its cap and delivers all of it: in a fill, in a scan
/// to a delimiter that ends the cap, and in a scan that reaches its maximum
/// with none; then appends and meets at random, wrapping around its buffer.
fn intake(capacity: u32) {
    let cap = index(capacity);
    let mut metered = Metered::new("Intake", "u8", capacity, Intake::worst_case(capacity));
    metered.start();
    let mut intake = Intake::with_capacity(capacity);
    metered.end(());
    let append = |metered: &mut Metered, intake: &mut Intake, bytes: &[u8]| {
        metered.start();
        let appended = intake.append(bytes);
        metered.end(());
        appended.is_ok()
    };
    // What it delivers is handed out: its length is all the test keeps.
    let meet = |metered: &mut Metered, intake: &mut Intake, read: Read| {
        metered.start();
        let delivered = intake.meet(read);
        let len = delivered.as_deref().map(<[u8]>::len);
        metered.end(delivered);
        len
    };
    assert!(append(&mut metered, &mut intake, &BYTES[..cap]));
    assert!(!append(&mut metered, &mut intake, b"a"), "a full intake refuses a byte more");
    assert_eq!(meet(&mut metered, &mut intake, Read::Fill(capacity)), Some(cap));
    if capacity >= 1 {
        assert!(append(&mut metered, &mut intake, &BYTES[..cap - 1]));
        assert!(append(&mut metered, &mut intake, b"\n"));
        let read = Read::Scan { until: Delimiter::LF, max: capacity };
        assert_eq!(meet(&mut metered, &mut intake, read), Some(cap));
    }
    if capacity >= 4 {
        assert!(append(&mut metered, &mut intake, &BYTES[..cap]));
        let read = Read::Scan { until: Delimiter::CRLF_CRLF, max: capacity };
        assert_eq!(meet(&mut metered, &mut intake, read), Some(cap));
    }
    let delimiters = [Delimiter::LF, Delimiter::CRLF, Delimiter::CRLF_CRLF];
    let mut rng = Rng::new(0x1_47A4E ^ u64::from(capacity));
    for _ in 0..8 * capacity + 8 {
        if rng.chance(500) {
            let mut piece = [0_u8; 16];
            let len = index(below(&mut rng, intake.room().min(16) + 1));
            for byte in &mut piece[..len] {
                *byte = [b'a', b'\r', b'\n'][index(below(&mut rng, 3))];
            }
            assert!(append(&mut metered, &mut intake, &piece[..len]));
        } else {
            let until = delimiters[index(below(&mut rng, 3))];
            let shortest = u32::try_from(until.as_bytes().len()).expect("a delimiter is a few bytes");
            let read = if rng.chance(500) || capacity < shortest {
                Read::Fill(below(&mut rng, capacity + 1))
            } else {
                Read::Scan { until, max: shortest + below(&mut rng, capacity - shortest + 1) }
            };
            meet(&mut metered, &mut intake, read);
        }
    }
    metered.held_exactly_its_worst_case();
}

/// Fills a map to its capacity in order, refuses a new key and replaces a
/// value; thins it to leaves of five; empties it, fills it in reverse and
/// empties it again; then drives it at random.
fn map<K: Item, V: Item>(capacity: u32) {
    let bound = Map::<K, V>::worst_case(capacity);
    let mut metered = Metered::new("Map", type_name::<(K, V)>(), capacity, bound);
    metered.start();
    let mut map = Map::<K, V>::with_capacity(capacity);
    metered.end(());
    let insert = |metered: &mut Metered, map: &mut Map<K, V>, n: u32| {
        metered.start();
        let inserted = map.insert(K::nth(n), V::nth(n));
        metered.end(inserted);
    };
    let remove = |metered: &mut Metered, map: &mut Map<K, V>, n: u32| {
        metered.start();
        let removed = map.remove(&K::nth(n));
        metered.end(removed);
    };
    for n in 0..=capacity {
        insert(&mut metered, &mut map, n);
    }
    assert!(!map.contains_key(&K::nth(capacity)), "a full map refuses a new key");
    if capacity > 0 {
        insert(&mut metered, &mut map, 0);
    }
    for (taken, put) in thinning(capacity) {
        remove(&mut metered, &mut map, taken);
        insert(&mut metered, &mut map, put);
    }
    for n in 0..keys(capacity) {
        remove(&mut metered, &mut map, n);
    }
    for n in (0..capacity).rev() {
        insert(&mut metered, &mut map, n);
    }
    for n in (0..capacity).rev() {
        remove(&mut metered, &mut map, n);
    }
    let mut rng = Rng::new(0x3E_E7 ^ u64::from(capacity));
    for _ in 0..8 * capacity + 8 {
        let n = below(&mut rng, keys(capacity));
        if rng.chance(600) {
            insert(&mut metered, &mut map, n);
        } else {
            remove(&mut metered, &mut map, n);
        }
    }
    assert!(map.len() <= capacity);
}

/// A set, driven as a map is, and emptied from its least key.
fn set<K: Item>(capacity: u32) {
    let mut metered = Metered::new("Set", type_name::<K>(), capacity, Set::<K>::worst_case(capacity));
    metered.start();
    let mut set = Set::<K>::with_capacity(capacity);
    metered.end(());
    let insert = |metered: &mut Metered, set: &mut Set<K>, n: u32| {
        metered.start();
        let inserted = set.insert(K::nth(n));
        metered.end(inserted);
    };
    let remove = |metered: &mut Metered, set: &mut Set<K>, n: u32| {
        metered.start();
        let removed = set.remove(&K::nth(n));
        metered.end(removed);
    };
    for n in 0..=capacity {
        insert(&mut metered, &mut set, n);
    }
    assert!(!set.contains(&K::nth(capacity)), "a full set refuses a new key");
    for (taken, put) in thinning(capacity) {
        remove(&mut metered, &mut set, taken);
        insert(&mut metered, &mut set, put);
    }
    while !set.is_empty() {
        metered.start();
        let first = set.pop_first();
        metered.end(first);
    }
    for n in (0..capacity).rev() {
        insert(&mut metered, &mut set, n);
    }
    for n in (0..capacity).rev() {
        remove(&mut metered, &mut set, n);
    }
    let mut rng = Rng::new(0x5E7 ^ u64::from(capacity));
    for _ in 0..8 * capacity + 8 {
        let n = below(&mut rng, keys(capacity));
        if rng.chance(600) {
            insert(&mut metered, &mut set, n);
        } else {
            remove(&mut metered, &mut set, n);
        }
    }
    assert!(set.len() <= capacity);
}

/// A deadline table, its two trees driven as a map is: armed in order, by
/// key and by time, thinned, moved and expired; then at random.
fn deadlines<K: Item>(capacity: u32) {
    let bound = Deadlines::<K>::worst_case(capacity);
    let mut metered = Metered::new("Deadlines", type_name::<K>(), capacity, bound);
    metered.start();
    let mut table = Deadlines::<K>::with_capacity(capacity);
    metered.end(());
    let arm = |metered: &mut Metered, table: &mut Deadlines<K>, n: u32, at: u32| {
        metered.start();
        let armed = table.arm(K::nth(n), Time::from_nanos(u64::from(at)));
        metered.end(armed);
    };
    let cancel = |metered: &mut Metered, table: &mut Deadlines<K>, n: u32| {
        metered.start();
        table.cancel(K::nth(n));
        metered.end(());
    };
    let expire = |metered: &mut Metered, table: &mut Deadlines<K>, now: u32| {
        metered.start();
        let expired = table.expire(Time::from_nanos(u64::from(now)));
        metered.end(expired);
        expired.is_some()
    };
    for n in 0..=capacity {
        arm(&mut metered, &mut table, n, n);
    }
    assert_eq!(table.len(), capacity, "a full table refuses a new key");
    for (taken, put) in thinning(capacity) {
        cancel(&mut metered, &mut table, taken);
        arm(&mut metered, &mut table, put, put);
    }
    // Each armed one moved later (the full table refuses the others), then
    // all of them expired.
    let keys = keys(capacity);
    for n in 0..keys {
        arm(&mut metered, &mut table, n, keys + n);
    }
    while expire(&mut metered, &mut table, 2 * keys) {}
    assert!(table.is_empty());
    let mut rng = Rng::new(0xDEAD ^ u64::from(capacity));
    for _ in 0..8 * capacity + 8 {
        let n = below(&mut rng, keys);
        let at = below(&mut rng, keys);
        match below(&mut rng, 3) {
            0 => arm(&mut metered, &mut table, n, at),
            1 => cancel(&mut metered, &mut table, n),
            _ => {
                expire(&mut metered, &mut table, at);
            }
        }
    }
    assert!(table.len() <= capacity);
}

#[test]
fn a_slab_holds_exactly_its_worst_case() {
    for capacity in CAPACITIES {
        slab::<u8>(capacity);
        slab::<u64>(capacity);
        slab::<u128>(capacity);
        slab::<[u64; 8]>(capacity);
    }
}

#[test]
fn a_queue_holds_exactly_its_worst_case() {
    for capacity in CAPACITIES {
        queue::<u8>(capacity);
        queue::<u64>(capacity);
        queue::<u128>(capacity);
        queue::<[u64; 8]>(capacity);
    }
}

#[test]
fn a_list_holds_exactly_its_worst_case() {
    for capacity in CAPACITIES {
        list::<u8>(capacity);
        list::<u64>(capacity);
        list::<u128>(capacity);
        list::<[u64; 8]>(capacity);
    }
}

#[test]
fn a_stack_holds_exactly_its_worst_case() {
    for capacity in CAPACITIES {
        stack::<u8>(capacity);
        stack::<u64>(capacity);
        stack::<u128>(capacity);
        stack::<[u64; 8]>(capacity);
    }
}

#[test]
fn an_intake_holds_exactly_its_worst_case() {
    for capacity in CAPACITIES {
        intake(capacity);
    }
}

#[test]
fn a_map_holds_no_more_than_its_worst_case() {
    for capacity in CAPACITIES {
        map::<u16, u8>(capacity);
        map::<u64, u64>(capacity);
        map::<u128, u8>(capacity);
        map::<[u64; 8], [u64; 8]>(capacity);
    }
}

#[test]
fn a_set_holds_no_more_than_its_worst_case() {
    for capacity in CAPACITIES {
        set::<u16>(capacity);
        set::<u64>(capacity);
        set::<u128>(capacity);
        set::<[u64; 8]>(capacity);
    }
}

#[test]
fn a_deadline_table_holds_no_more_than_its_worst_case() {
    for capacity in CAPACITIES {
        deadlines::<u16>(capacity);
        deadlines::<u64>(capacity);
        deadlines::<u128>(capacity);
    }
}
