//! Focused step tests.

#![expect(clippy::disallowed_macros, reason = "tests use literal collections and variant assertions")]
#![expect(clippy::disallowed_types, reason = "tests enumerate a small fixed set of key tuples")]
#![expect(clippy::disallowed_methods, reason = "tests copy fixed byte vectors for comparisons")]

use crate::{Event, KeyReader, KeyWriter, Limits, Op, Page, Range, Request, Store, down, up};
use crate::{crc, frame};
use alloc::boxed::Box;
use alloc::vec;
use skein_io::file;
use skein_io::kernel::Fd;
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};

fn limits() -> Limits {
    Limits {
        key: 100,
        value: 100,
        ops: 8,
        commit: 4096,
        queued: 8,
        queued_bytes: 32768,
        budget: 4096,
        segment: 8192,
        snapshot_after: 16384,
        chunk: 4096,
        page: Page { rows: 10, bytes: 4096 },
        deadline: Duration::from_secs(1),
    }
}

fn env() -> Env<Limits> {
    Env { now: Time::ZERO, wall: Wall::EPOCH, limits: limits() }
}

fn put(key: &[u8], value: &[u8]) -> Op {
    Op::Put { key: Box::from(key), value: Box::from(value) }
}

#[test]
fn crc_castagnoli_vector() {
    assert_eq!(crc::crc32c(b"123456789"), 0xe306_9283);
}

#[test]
fn keys_roundtrip_and_order() {
    let mut encoded = vec![];
    for tag in [0_u8, 1, 255] {
        for bytes in [&b""[..], &b"a"[..], &b"a\0"[..], &b"a\xff"[..], &b"b"[..]] {
            for number in [0_u64, 1, 256, u64::MAX] {
                let mut writer = KeyWriter::new(100);
                writer.tag(tag).bytes(bytes).u64(number);
                let key = writer.finish().expect("within limit");
                let mut reader = KeyReader::new(&key);
                assert_eq!(reader.tag(), Some(tag));
                assert_eq!(reader.bytes().as_deref(), Some(bytes));
                assert_eq!(reader.u64(), Some(number));
                assert!(reader.is_empty());
                encoded.push((tag, bytes.to_vec(), number, key));
            }
        }
    }
    for a in &encoded {
        for b in &encoded {
            assert_eq!(a.3.cmp(&b.3), (&a.0, &a.1, &a.2).cmp(&(&b.0, &b.1, &b.2)));
        }
    }
}

#[test]
fn frame_rejects_every_single_byte_corruption() {
    let bytes =
        frame::encode(7, &[put(b"a", b"one"), Op::Erase { key: Box::from(&b"b"[..]) }], 4096).expect("frame fits");
    let decoded = frame::decode(&bytes, 4096).expect("valid frame");
    assert_eq!(decoded.number, 7);
    assert_eq!(decoded.ops.len(), 2);
    for at in 0..bytes.len() {
        let mut bad = bytes.clone();
        *bad.get_mut(at).expect("byte in frame") ^= 1;
        assert!(frame::decode(&bad, 4096).is_none(), "corruption at {at}");
    }
}

#[test]
fn memory_commits_reads_and_pages() {
    let env = env();
    let mut store = Store::memory(&env.limits);
    let mut above = Queue::with_capacity(20);
    let mut below = Queue::with_capacity(20);
    down(
        &mut store,
        &env,
        Request::Commit {
            owner: Token::new(1),
            ops: vec![put(b"a", b"1"), put(b"b", b"2"), put(b"c", b"3")].into_boxed_slice(),
        },
        &mut above,
        &mut below,
    );
    assert_eq!(above.pop(), Some(Event::Committed { owner: Token::new(1), number: 1 }));
    down(
        &mut store,
        &env,
        Request::Load { owner: Token::new(2), range: Range::prefix(b""), max: Page { rows: 2, bytes: 10 } },
        &mut above,
        &mut below,
    );
    let Event::Loaded { rows, next, .. } = above.pop().expect("page") else {
        panic!("expected page");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(next.as_deref(), Some(&b"c"[..]));
    down(&mut store, &env, Request::Get { owner: Token::new(3), key: Box::from(&b"b"[..]) }, &mut above, &mut below);
    assert_eq!(above.pop(), Some(Event::Got { owner: Token::new(3), value: Some(Box::from(&b"2"[..])) }));
}

#[test]
fn a_page_too_small_for_one_row_is_refused() {
    let env = env();
    let mut store = Store::memory(&env.limits);
    let mut above = Queue::with_capacity(8);
    let mut below = Queue::with_capacity(8);
    down(
        &mut store,
        &env,
        Request::Commit { owner: Token::new(1), ops: vec![put(b"a", b"value")].into_boxed_slice() },
        &mut above,
        &mut below,
    );
    drop(above.pop());
    down(
        &mut store,
        &env,
        Request::Load { owner: Token::new(2), range: Range::prefix(b""), max: Page { rows: 1, bytes: 1 } },
        &mut above,
        &mut below,
    );
    assert_eq!(above.pop(), Some(Event::Refused { owner: Token::new(2), refusal: crate::Refusal::TooLarge }));
    down(
        &mut store,
        &env,
        Request::Load {
            owner: Token::new(3),
            range: Range { start: Box::from(&b"z"[..]), end: Some(Box::from(&b"a"[..])) },
            max: Page { rows: 1, bytes: 10 },
        },
        &mut above,
        &mut below,
    );
    assert_eq!(above.pop(), Some(Event::Refused { owner: Token::new(3), refusal: crate::Refusal::TooLarge }));
}

#[test]
fn commit_is_invisible_until_sync() {
    let env = env();
    let mut store = Store::new(&env.limits);
    let mut above = Queue::with_capacity(20);
    let mut below = Queue::with_capacity(20);
    down(&mut store, &env, Request::Open { owner: Token::new(1), root: Fd::new(5) }, &mut above, &mut below);
    let Some(file::Request::List { .. }) = below.pop() else {
        panic!("list requested");
    };
    up(
        &mut store,
        &env,
        file::Event::Listed { owner: Token::new(0x4b56), entries: Box::new([]) },
        &mut above,
        &mut below,
    );
    let Some(file::Request::Create { .. }) = below.pop() else {
        panic!("segment create requested");
    };
    up(
        &mut store,
        &env,
        file::Event::Opened { owner: Token::new(0x4b56), file: Token::new(9), len: 0 },
        &mut above,
        &mut below,
    );
    let Some(file::Request::SyncDirectory { .. }) = below.pop() else {
        panic!("directory sync requested");
    };
    up(&mut store, &env, file::Event::Synced { owner: Token::new(0x4b56) }, &mut above, &mut below);
    assert_eq!(above.pop(), Some(Event::Opened { owner: Token::new(1), last: 0 }));
    down(
        &mut store,
        &env,
        Request::Commit { owner: Token::new(2), ops: vec![put(b"k", b"v")].into_boxed_slice() },
        &mut above,
        &mut below,
    );
    let Some(file::Request::WriteAt { .. }) = below.pop() else {
        panic!("frame write requested");
    };
    down(&mut store, &env, Request::Get { owner: Token::new(3), key: Box::from(&b"k"[..]) }, &mut above, &mut below);
    assert_eq!(above.pop(), Some(Event::Got { owner: Token::new(3), value: None }));
    up(&mut store, &env, file::Event::Written { owner: Token::new(0x4b56) }, &mut above, &mut below);
    let Some(file::Request::Sync { .. }) = below.pop() else {
        panic!("log sync requested");
    };
    up(&mut store, &env, file::Event::Synced { owner: Token::new(0x4b56) }, &mut above, &mut below);
    assert_eq!(above.pop(), Some(Event::Committed { owner: Token::new(2), number: 1 }));
    down(&mut store, &env, Request::Get { owner: Token::new(4), key: Box::from(&b"k"[..]) }, &mut above, &mut below);
    assert_eq!(above.pop(), Some(Event::Got { owner: Token::new(4), value: Some(Box::from(&b"v"[..])) }));
}
