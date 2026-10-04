//! The bytes held for rustls in one slice: appended within the room,
//! discarded and taken from the front, written in place.

use skein_lib::Overflow;

use crate::held::Held;

#[test]
fn bytes_are_appended_within_the_room_and_refused_whole_past_it() {
    let mut held = Held::with_capacity(8);
    assert!(held.is_empty());
    assert_eq!(held.append(b"abcde"), Ok(()));
    assert_eq!((held.len(), held.room()), (5, 3));
    assert_eq!(held.append(b"fghi"), Err(Overflow), "refused whole");
    assert_eq!(held.filled(), b"abcde");
    assert_eq!(held.append(b"fgh"), Ok(()));
    assert_eq!(held.room(), 0);
    assert_eq!(held.append(b""), Ok(()));
}

#[test]
fn what_is_discarded_or_taken_moves_the_rest_to_the_front() {
    let mut held = Held::with_capacity(8);
    held.append(b"abcdefgh").unwrap();
    held.discard(3);
    assert_eq!(held.filled(), b"defgh");
    assert_eq!(&*held.take(2), b"de");
    assert_eq!(held.filled(), b"fgh");
    // No more than is held.
    assert_eq!(&*held.take(10), b"fgh");
    assert!(held.is_empty());
    held.discard(0);
    assert_eq!(held.room(), 8);
}

#[test]
fn rustls_writes_in_place_and_counts_what_it_wrote() {
    let mut held = Held::with_capacity(8);
    held.append(b"ab").unwrap();
    let spare = held.spare_mut();
    assert_eq!(spare.len(), 6);
    for (to, from) in spare.iter_mut().zip(b"cde") {
        *to = *from;
    }
    held.wrote(3);
    assert_eq!(held.filled(), b"abcde");
    held.filled_mut()[0] = b'A';
    assert_eq!(held.filled(), b"Abcde");
    held.clear();
    assert!(held.is_empty());
}

#[test]
#[should_panic(expected = "written within the room")]
fn writing_past_the_room_is_a_bug() {
    let mut held = Held::with_capacity(2);
    held.wrote(3);
}

#[test]
#[should_panic(expected = "rustls discards only what it was given")]
fn discarding_past_what_is_held_is_a_bug() {
    let mut held = Held::with_capacity(4);
    held.append(b"ab").unwrap();
    held.discard(3);
}
