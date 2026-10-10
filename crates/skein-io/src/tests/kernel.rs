//! The records' rules (kernel.md, 3 to 5): each operation's one success
//! shape, what a completion may count, which errors answer which operation,
//! and the buffer handed back whatever the result.

use alloc::boxed::Box;
use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use skein_lib::Token;

use crate::kernel::{
    Addr, Complete, Done, Entry, Error, Family, Fd, Kind, LONGEST_NAME, Op, OpenHow, Shape, Stat, is_name,
};

const FD: Fd = Fd::new(3);
const NEW: Fd = Fd::new(4);
const STAT: Stat = Stat { kind: Kind::File, size: 5, mode: 0o644, owner: 1000, links: 1 };

fn v4() -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 8080))
}

fn bytes(len: usize) -> Box<[u8]> {
    Box::from(&[7; 8][..len])
}

fn name(text: &[u8]) -> Box<[u8]> {
    Box::from(text)
}

fn list(entries: usize) -> Op {
    Op::List { fd: FD, entries: Box::from(&[Entry::BLANK; 4][..entries]), names: Box::from(&[0; 256][..]) }
}

fn complete(kind: Op, result: Result<Done, Error>) -> Complete {
    Complete { op: Token::new(1), kind, result }
}

/// One of every operation, valid. A new operation makes the match in
/// `documented` fail to build until it is added there, and here.
fn every_op() -> [Op; 26] {
    [
        Op::Socket { family: Family::Ipv4 },
        Op::Bind { fd: FD, addr: v4() },
        Op::Listen { fd: FD, backlog: 16 },
        Op::Accept { fd: FD },
        Op::Connect { fd: FD, addr: v4() },
        Op::Recv { fd: FD, buf: bytes(4) },
        Op::Send { fd: FD, bytes: bytes(4), from: 1 },
        Op::Shutdown { fd: FD },
        Op::Close { fd: FD },
        Op::Open { root: FD, path: name(b"a/b"), how: OpenHow::Read },
        Op::Read { fd: FD, buf: bytes(4), at: 9 },
        Op::Write { fd: FD, bytes: bytes(4), from: 1, at: 9 },
        Op::Append { fd: FD, bytes: bytes(4), from: 1 },
        Op::Sync { fd: FD },
        Op::Stat { fd: FD },
        Op::Rename { from_dir: FD, from: name(b"a"), to_dir: NEW, to: name(b"b") },
        Op::Remove { dir: FD, name: name(b"a"), directory: false },
        Op::MakeDirectory { dir: FD, name: name(b"a"), mode: 0o777 },
        list(2),
        Op::Wait { pidfd: FD, reap: false },
        Op::Wait { pidfd: FD, reap: true },
        Op::Signal { pidfd: FD, signal: crate::kernel::Signal::Kill, to: crate::kernel::Target::Child },
        Op::Signal { pidfd: FD, signal: crate::kernel::Signal::Kill, to: crate::kernel::Target::Group },
        Op::Usage,
        Op::ReadSignal { fd: FD },
        Op::Cancel { target: Token::new(2) },
    ]
}

/// The success each operation documents on `Op`, written out again by an
/// exhaustive match.
fn documented(op: &Op) -> Done {
    match op {
        Op::Socket { .. } | Op::Open { .. } => Done::Fd(NEW),
        Op::Accept { .. } => Done::Accepted { fd: NEW, peer: v4() },
        Op::Recv { .. }
        | Op::Send { .. }
        | Op::Read { .. }
        | Op::Write { .. }
        | Op::Append { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. } => Done::Count(1),
        Op::List { .. } => Done::Count(0),
        Op::Bind { .. } => Done::Bound(v4()),
        Op::Stat { .. } => Done::Stat(STAT),
        Op::Spawn { .. } => Done::Spawned { pidfd: NEW },
        Op::Wait { .. } => Done::Exit(crate::kernel::Exit::Code(0)),
        Op::Usage => Done::Usage(crate::kernel::Usage::ZERO),
        Op::ReadSignal { .. } => Done::ServiceSignal(crate::kernel::ServiceSignal::Terminate),
        Op::Listen { .. }
        | Op::Connect { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Sync { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::Signal { .. }
        | Op::Cancel { .. } => Done::Nothing,
    }
}

#[test]
fn every_operation_succeeds_with_its_documented_shape_and_no_other() {
    let shapes = [
        Done::Nothing,
        Done::Count(0),
        Done::Fd(NEW),
        Done::Accepted { fd: NEW, peer: v4() },
        Done::Bound(v4()),
        Done::Stat(STAT),
        Done::Usage(crate::kernel::Usage::ZERO),
        Done::Exit(crate::kernel::Exit::Code(0)),
        Done::ServiceSignal(crate::kernel::ServiceSignal::Terminate),
    ];
    for op in every_op() {
        assert!(op.is_valid(), "the examples are valid: {op:?}");
        let expected = documented(&op);
        assert_eq!(op.shape(), expected.shape());
        let answer = complete(op, Ok(expected.clone()));
        assert!(answer.is_valid(), "the documented success fits: {answer:?}");
        let mut kind = answer.kind;
        for done in &shapes {
            if done.shape() != expected.shape() {
                let answer = complete(kind, Ok(done.clone()));
                assert!(!answer.is_valid(), "only the documented shape fits: {answer:?}");
                kind = answer.kind;
            }
        }
    }
}

#[test]
fn the_shapes_are_as_the_table_on_op_says() {
    let [
        socket,
        bind,
        listen,
        accept,
        connect,
        recv,
        send,
        shutdown,
        close,
        open,
        read,
        write,
        append,
        sync,
        stat,
        rename,
        remove,
        make_directory,
        list,
        observing,
        reaping,
        child_signal,
        group_signal,
        usage,
        read_signal,
        cancel,
    ] = every_op();
    assert_eq!((socket.shape(), open.shape()), (Shape::Fd, Shape::Fd));
    assert_eq!(accept.shape(), Shape::Accepted);
    assert_eq!(bind.shape(), Shape::Bound);
    assert_eq!(stat.shape(), Shape::Stat);
    assert_eq!(read_signal.shape(), Shape::ServiceSignal);
    assert_eq!(observing.shape(), Shape::Exit);
    assert_eq!(reaping.shape(), Shape::Exit);
    assert_eq!(usage.shape(), Shape::Usage);
    assert_eq!(child_signal.shape(), Shape::Nothing);
    assert_eq!(group_signal.shape(), Shape::Nothing);
    for op in [recv, send, read, write, append, list] {
        assert_eq!(op.shape(), Shape::Count);
    }
    for op in [listen, connect, shutdown, close, sync, rename, remove, make_directory, cancel] {
        assert_eq!(op.shape(), Shape::Nothing);
    }
}

#[test]
fn a_bind_answers_with_the_address_it_asked_for_and_port_zero_resolved() {
    let ip = Ipv4Addr::LOCALHOST;
    let bind = |port: u16| Op::Bind { fd: FD, addr: SocketAddr::from((ip, port)) };
    let cases = [
        (8080, SocketAddr::from((ip, 8080)), true),
        (8080, SocketAddr::from((ip, 8081)), false),
        (0, SocketAddr::from((ip, 40000)), true),
        (0, SocketAddr::from((ip, 0)), false),
        (8080, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 8080)), false),
        (8080, SocketAddr::from((Ipv6Addr::LOCALHOST, 8080)), false),
    ];
    for (port, bound, valid) in cases {
        assert_eq!(complete(bind(port), Ok(Done::Bound(bound))).is_valid(), valid, "port {port}");
    }
}

#[test]
fn a_cancel_may_fail_to_be_submitted() {
    for error in [Error::InvalidArgument, Error::Other(5)] {
        assert!(complete(Op::Cancel { target: Token::new(2) }, Err(error)).is_valid(), "a backend failure");
    }
}

#[test]
fn a_recv_buffer_is_never_empty() {
    assert_eq!(Op::recv(FD, bytes(0)), Err(bytes(0)));
    assert!(!Op::Recv { fd: FD, buf: bytes(0) }.is_valid());
    assert_eq!(Op::recv(FD, bytes(3)), Ok(Op::Recv { fd: FD, buf: bytes(3) }));
}

#[test]
fn a_send_has_something_left_to_send_from_its_offset() {
    assert_eq!(Op::send(FD, bytes(3), 0), Ok(Op::Send { fd: FD, bytes: bytes(3), from: 0 }));
    assert_eq!(Op::send(FD, bytes(3), 2), Ok(Op::Send { fd: FD, bytes: bytes(3), from: 2 }));
    assert_eq!(Op::send(FD, bytes(3), 3), Err(bytes(3)));
    assert_eq!(Op::send(FD, bytes(3), 4), Err(bytes(3)));
    assert_eq!(Op::send(FD, bytes(0), 0), Err(bytes(0)));
    assert!(!Op::Send { fd: FD, bytes: bytes(3), from: 3 }.is_valid());
}

#[test]
fn a_recv_count_is_at_most_its_buffer_and_zero_is_the_end() {
    for (n, valid) in [(0, true), (1, true), (4, true), (5, false)] {
        let answer = complete(Op::Recv { fd: FD, buf: bytes(4) }, Ok(Done::Count(n)));
        assert_eq!(answer.is_valid(), valid, "count {n}");
    }
}

#[test]
fn a_send_count_is_at_least_one_and_at_most_what_was_left() {
    for (n, valid) in [(0, false), (1, true), (3, true), (4, false)] {
        let answer = complete(Op::Send { fd: FD, bytes: bytes(4), from: 1 }, Ok(Done::Count(n)));
        assert_eq!(answer.is_valid(), valid, "count {n}");
    }
}

#[test]
fn too_late_answers_a_cancel_only() {
    assert!(complete(Op::Cancel { target: Token::new(2) }, Err(Error::TooLate)).is_valid());
    assert!(complete(Op::Cancel { target: Token::new(2) }, Ok(Done::Nothing)).is_valid());
    assert!(!complete(Op::Accept { fd: FD }, Err(Error::TooLate)).is_valid());
    assert!(complete(Op::Accept { fd: FD }, Err(Error::Cancelled)).is_valid());
}

#[test]
fn a_cancel_fails_only_as_too_late_or_unsubmitted() {
    let cancel = || Op::Cancel { target: Token::new(2) };
    for error in [Error::Cancelled, Error::Reset, Error::NoBufferSpace, Error::NotConnected] {
        assert!(!complete(cancel(), Err(error)).is_valid(), "not an answer to a cancel");
    }
    for error in [Error::TooLate, Error::InvalidArgument, Error::Other(5)] {
        assert!(complete(cancel(), Err(error)).is_valid(), "an answer to a cancel");
    }
}

#[test]
fn an_error_hands_the_buffer_back_too() {
    let answer = complete(Op::Recv { fd: FD, buf: bytes(4) }, Err(Error::Reset));
    assert!(answer.is_valid(), "any operation may fail");
    assert_eq!(answer.kind, Op::Recv { fd: FD, buf: bytes(4) });
}

#[test]
fn the_family_follows_the_address() {
    assert_eq!(Family::of(&v4()), Family::Ipv4);
    assert_eq!(Family::of(&SocketAddr::from((Ipv6Addr::LOCALHOST, 80))), Family::Ipv6);
}

#[test]
fn a_descriptor_is_its_raw_number() {
    assert_eq!(Fd::new(-1).raw(), -1_i32);
    assert_eq!(Fd::new(7), Fd::new(7));
}

/// The errors each operation on files names, beyond `NoBufferSpace`,
/// `InvalidArgument` and `Other`: the module documentation's table, written
/// out again.
fn named(op: &Op) -> &'static [Error] {
    match op {
        Op::Open { .. } => &[
            Error::NotFound,
            Error::Exists,
            Error::NotADirectory,
            Error::IsADirectory,
            Error::Permission,
            Error::NoSpace,
            Error::ReadOnly,
            Error::TooManyLinks,
            Error::NameTooLong,
            Error::Escape,
            Error::NotAFile,
            Error::TooManyOpenFiles,
        ],
        Op::Read { .. } => &[Error::IsADirectory],
        Op::Write { .. } | Op::Append { .. } => &[Error::NoSpace, Error::ReadOnly],
        Op::Sync { .. } => &[Error::NoSpace],
        Op::Rename { .. } => &[
            Error::NotFound,
            Error::NotADirectory,
            Error::IsADirectory,
            Error::NotEmpty,
            Error::Permission,
            Error::NoSpace,
            Error::ReadOnly,
            Error::TooManyLinks,
            Error::NameTooLong,
        ],
        Op::Remove { .. } => &[
            Error::NotFound,
            Error::NotADirectory,
            Error::IsADirectory,
            Error::NotEmpty,
            Error::Permission,
            Error::ReadOnly,
            Error::NameTooLong,
        ],
        Op::MakeDirectory { .. } => &[
            Error::NotFound,
            Error::Exists,
            Error::NotADirectory,
            Error::Permission,
            Error::NoSpace,
            Error::ReadOnly,
            Error::TooManyLinks,
            Error::NameTooLong,
        ],
        Op::List { .. } => &[Error::NotFound, Error::NotADirectory, Error::NameTooLong],
        Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Recv { .. }
        | Op::Send { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Stat { .. }
        | Op::Spawn { .. }
        | Op::Wait { .. }
        | Op::Signal { .. }
        | Op::Usage
        | Op::ReadSignal { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. }
        | Op::Cancel { .. } => &[],
    }
}

const SOCKETS_ERRORS: [Error; 8] = [
    Error::Refused,
    Error::Reset,
    Error::BrokenPipe,
    Error::NotConnected,
    Error::AddressInUse,
    Error::AddressNotAvailable,
    Error::Unreachable,
    Error::TimedOut,
];

/// Whether a `Cancel` may stop `op`, so that it may answer `Cancelled`.
fn cancellable(op: &Op) -> bool {
    match op {
        Op::Stat { .. }
        | Op::Rename { .. }
        | Op::Remove { .. }
        | Op::MakeDirectory { .. }
        | Op::List { .. }
        | Op::Spawn { .. }
        | Op::Signal { .. }
        | Op::Usage
        | Op::ReadSignal { .. }
        | Op::Cancel { .. } => false,
        Op::Open { .. }
        | Op::Read { .. }
        | Op::Write { .. }
        | Op::Append { .. }
        | Op::Sync { .. }
        | Op::Socket { .. }
        | Op::Bind { .. }
        | Op::Listen { .. }
        | Op::Accept { .. }
        | Op::Connect { .. }
        | Op::Recv { .. }
        | Op::Send { .. }
        | Op::Shutdown { .. }
        | Op::Close { .. }
        | Op::Wait { .. }
        | Op::PipeRead { .. }
        | Op::PipeWrite { .. } => true,
    }
}

const FILES_ERRORS: [Error; 12] = [
    Error::NotFound,
    Error::Exists,
    Error::NotADirectory,
    Error::IsADirectory,
    Error::NotEmpty,
    Error::Permission,
    Error::NoSpace,
    Error::ReadOnly,
    Error::TooManyLinks,
    Error::NameTooLong,
    Error::Escape,
    Error::NotAFile,
];

#[test]
fn each_operation_on_files_answers_the_errors_its_table_names_and_no_other() {
    for op in every_op() {
        if !op.is_file() {
            continue;
        }
        let mut kind = op;
        for error in [Error::NoBufferSpace, Error::InvalidArgument, Error::Other(5)] {
            let answer = complete(kind, Err(error));
            assert!(answer.is_valid(), "any operation may answer {error:?}: {answer:?}");
            kind = answer.kind;
        }
        for error in FILES_ERRORS.into_iter().chain([Error::TooManyOpenFiles]) {
            let answer = complete(kind, Err(error));
            assert_eq!(answer.is_valid(), named(&answer.kind).contains(&error), "{answer:?}");
            kind = answer.kind;
        }
        let answer = complete(kind, Err(Error::Cancelled));
        assert_eq!(answer.is_valid(), cancellable(&answer.kind), "cancelled only if a Cancel may stop it: {answer:?}");
        kind = answer.kind;
        for error in SOCKETS_ERRORS.into_iter().chain([Error::TooLate]) {
            let answer = complete(kind, Err(error));
            assert!(!answer.is_valid(), "never a socket's error: {answer:?}");
            kind = answer.kind;
        }
    }
}

#[test]
fn an_operation_on_sockets_never_answers_a_files_error() {
    for op in every_op() {
        if op.is_file() {
            continue;
        }
        let mut kind = op;
        for error in FILES_ERRORS {
            let answer = complete(kind, Err(error));
            assert!(!answer.is_valid(), "{answer:?}");
            kind = answer.kind;
        }
    }
    assert!(complete(Op::Socket { family: Family::Ipv4 }, Err(Error::TooManyOpenFiles)).is_valid());
}

#[test]
fn the_operations_on_files_are_those_on_names_beneath_a_root() {
    let files = [
        false, false, false, false, false, false, false, false, false, true, true, true, true, true, true, true, true,
        true, true, false, false, false, false, false, false, false,
    ];
    assert_eq!(every_op().len(), files.len(), "the classification table covers every fixture");
    for (op, file) in every_op().into_iter().zip(files) {
        assert_eq!(op.is_file(), file, "{op:?}");
    }
}

#[test]
fn a_name_is_one_entry_of_a_directory_and_nothing_more() {
    for good in [&b"a"[..], b"a.txt", b"...", b".hidden", b"with space", &[b'x'; 300]] {
        assert!(is_name(good), "{good:?}");
    }
    for bad in [&b""[..], b".", b"..", b"a/b", b"/", b"a/", b"nul\0"] {
        assert!(!is_name(bad), "{bad:?}");
    }
    let op = |name: &[u8]| Op::MakeDirectory { dir: FD, name: Box::from(name), mode: 0o777 };
    assert!(op(b"fine").is_valid());
    assert!(!op(b"../out").is_valid(), "no lookup beyond the entry");
    assert!(!Op::Remove { dir: FD, name: name(b".."), directory: true }.is_valid());
    let rename =
        |from: &[u8], to: &[u8]| Op::Rename { from_dir: FD, from: Box::from(from), to_dir: FD, to: Box::from(to) };
    assert!(rename(b"a", b"b").is_valid());
    assert!(!rename(b"a", b"x/b").is_valid());
    assert!(!rename(b".", b"b").is_valid());
}

#[test]
fn an_open_path_holds_no_nul_byte_and_may_be_anything_else() {
    let open = |path: &[u8]| Op::Open { root: FD, path: Box::from(path), how: OpenHow::Create { mode: None } };
    for path in [&b""[..], b"..", b"/etc/passwd", b"a//b/", &[b'x'; 5000]] {
        assert!(open(path).is_valid(), "the kernel answers it: {path:?}");
    }
    assert!(!open(b"a\0b").is_valid());
}

#[test]
fn a_read_buffer_is_never_empty_nor_past_the_largest_offset() {
    let largest = u64::try_from(i64::MAX).unwrap();
    assert_eq!(Op::read(FD, bytes(0), 0), Err(bytes(0)));
    assert_eq!(Op::read(FD, bytes(3), 7), Ok(Op::Read { fd: FD, buf: bytes(3), at: 7 }));
    assert_eq!(Op::read(FD, bytes(3), largest - 3), Ok(Op::Read { fd: FD, buf: bytes(3), at: largest - 3 }));
    assert_eq!(Op::read(FD, bytes(3), largest - 2), Err(bytes(3)));
    assert_eq!(Op::read(FD, bytes(3), u64::MAX), Err(bytes(3)), "the descriptor's position, on the ring");
    assert!(!Op::Read { fd: FD, buf: bytes(0), at: 0 }.is_valid());
}

#[test]
fn a_write_has_something_left_to_write_within_the_largest_offset() {
    let largest = u64::try_from(i64::MAX).unwrap();
    assert_eq!(Op::write(FD, bytes(3), 1, 4), Ok(Op::Write { fd: FD, bytes: bytes(3), from: 1, at: 4 }));
    assert_eq!(Op::write(FD, bytes(3), 3, 4), Err(bytes(3)));
    assert_eq!(Op::write(FD, bytes(0), 0, 0), Err(bytes(0)));
    assert!(Op::write(FD, bytes(3), 1, largest - 2).is_ok(), "two bytes left end at the largest offset");
    assert_eq!(Op::write(FD, bytes(3), 1, largest - 1), Err(bytes(3)));
    assert!(!Op::Write { fd: FD, bytes: bytes(3), from: 0, at: u64::MAX }.is_valid());
}

#[test]
fn a_read_count_is_at_most_its_buffer_and_zero_is_the_end_of_the_file() {
    for (n, valid) in [(0, true), (1, true), (4, true), (5, false)] {
        let answer = complete(Op::Read { fd: FD, buf: bytes(4), at: 100 }, Ok(Done::Count(n)));
        assert_eq!(answer.is_valid(), valid, "count {n}");
    }
}

#[test]
fn a_write_count_is_at_least_one_and_at_most_what_was_left() {
    for (n, valid) in [(0, false), (1, true), (3, true), (4, false)] {
        let answer = complete(Op::Write { fd: FD, bytes: bytes(4), from: 1, at: 0 }, Ok(Done::Count(n)));
        assert_eq!(answer.is_valid(), valid, "count {n}");
    }
}

#[test]
fn a_list_has_room_for_an_entry_and_the_longest_name() {
    assert!(list(1).is_valid());
    assert!(!list(0).is_valid(), "no room for an entry");
    let names = [0; 256];
    let short = Op::List { fd: FD, entries: Box::new([Entry::BLANK]), names: Box::from(&names[..LONGEST_NAME - 1]) };
    assert!(!short.is_valid(), "room for fewer bytes than the longest name");
    let enough = Op::List { fd: FD, entries: Box::new([Entry::BLANK]), names: Box::from(&names[..LONGEST_NAME]) };
    assert!(enough.is_valid());
}

/// A `List` that counts `n` of `entries`, with `names`.
fn listed(entries: &[Entry], names: &[u8], n: u32) -> Complete {
    complete(Op::List { fd: FD, entries: Box::from(entries), names: Box::from(names) }, Ok(Done::Count(n)))
}

#[test]
fn a_list_counts_entries_whose_names_lie_in_order_within_its_names() {
    let mut names = [0_u8; 256];
    for (slot, byte) in names.iter_mut().zip(b"onetwo...") {
        *slot = *byte;
    }
    let one = Entry { kind: Kind::File, start: 0, len: 3 };
    let two = Entry { kind: Kind::Directory, start: 3, len: 3 };
    let dots = Entry { kind: Kind::Symlink, start: 6, len: 3 };
    assert!(listed(&[one, two, Entry::BLANK], &names, 2).is_valid());
    assert!(listed(&[one, two, dots], &names, 3).is_valid(), "`...` is a name");
    assert!(listed(&[one, two], &names, 0).is_valid(), "the end");
    assert!(!listed(&[one, two], &names, 3).is_valid(), "more than the entries");
    assert!(!listed(&[two, one], &names, 2).is_valid(), "names out of order");
    assert!(!listed(&[one, one], &names, 2).is_valid(), "a name shared");
    assert!(!listed(&[Entry::BLANK], &names, 1).is_valid(), "an empty name");
    let dot = Entry { kind: Kind::Directory, start: 6, len: 1 };
    assert!(!listed(&[dot], &names, 1).is_valid(), "`.` is left out");
    let past = Entry { kind: Kind::File, start: 250, len: 7 };
    assert!(!listed(&[past], &names, 1).is_valid(), "a name past the end of names");
    let wide = Entry { kind: Kind::File, start: u32::MAX, len: u32::MAX };
    assert!(!listed(&[wide], &names, 1).is_valid(), "a range that overflows");
}

#[test]
fn an_entry_names_its_bytes_of_names() {
    let names = b"alphabeta";
    assert_eq!(Entry { kind: Kind::File, start: 5, len: 4 }.name(names), Some(&b"beta"[..]));
    assert_eq!(Entry { kind: Kind::File, start: 5, len: 5 }.name(names), None);
    assert_eq!(Entry::BLANK.name(names), Some(&b""[..]));
}

#[test]
fn a_stat_answers_any_kind_and_size() {
    for kind in [Kind::File, Kind::Directory, Kind::Symlink, Kind::Other] {
        let answer = complete(
            Op::Stat { fd: FD },
            Ok(Done::Stat(Stat { kind, size: u64::MAX, mode: 0o777, owner: u32::MAX, links: u32::MAX })),
        );
        assert!(answer.is_valid(), "{answer:?}");
        let answer =
            complete(Op::Stat { fd: FD }, Ok(Done::Stat(Stat { kind, size: 0, mode: 0o4755, owner: 1000, links: 1 })));
        assert!(!answer.is_valid(), "permission bits only: {answer:?}");
    }
}

#[test]
fn an_error_hands_every_buffer_and_path_back_too() {
    let rename = || Op::Rename { from_dir: FD, from: name(b"a"), to_dir: NEW, to: name(b"b") };
    let answer = complete(rename(), Err(Error::NotFound));
    assert!(answer.is_valid());
    assert_eq!(answer.kind, rename());
    let answer = complete(list(3), Err(Error::NotADirectory));
    assert!(answer.is_valid());
    assert_eq!(answer.kind, list(3));
}

#[test]
fn a_cancel_never_answers_a_files_error() {
    for error in FILES_ERRORS {
        assert!(!complete(Op::Cancel { target: Token::new(2) }, Err(error)).is_valid(), "{error:?}");
    }
}

#[test]
fn the_examples_of_each_kind_are_distinct() {
    let ip = Ipv6Addr::LOCALHOST;
    assert_ne!(Family::of(&SocketAddr::from((ip, 1))), Family::of(&v4()));
    assert_ne!(OpenHow::Read, OpenHow::Directory);
}

#[test]
fn a_create_asks_for_permission_bits_only() {
    let create = |mode| Op::Open { root: FD, path: name(b"f"), how: OpenHow::Create { mode } };
    assert!(create(None).is_valid());
    assert!(create(Some(0o640)).is_valid());
    assert!(create(Some(0o777)).is_valid());
    assert!(!create(Some(0o4644)).is_valid(), "no set-user-ID bit");
    assert!(!create(Some(0o10644)).is_valid(), "no file type");
}

#[test]
fn spawn_parent_slots_are_empty_until_a_successful_completion() {
    use crate::kernel::{Pipe, Spawn, Way};
    let mut kind = Op::Spawn {
        spawn: Box::new(Spawn {
            program: name(b"child"),
            args: Box::default(),
            env: Box::default(),
            root: FD,
            dir: Box::default(),
            pipes: Box::new([Pipe { child: 1, way: Way::Out, parent: None }]),
        }),
    };
    assert!(kind.is_valid(), "kernel.md, section 4: parent slots start empty");
    let success = Done::Spawned { pidfd: NEW };
    let mut complete = Complete { op: Token::new(1), kind, result: Ok(success) };
    assert!(!complete.is_valid(), "success must fill each requested parent end");
    let Op::Spawn { spawn } = &mut complete.kind else { panic!("spawn record") };
    spawn.pipes[0].parent = Some(FD);
    assert!(complete.is_valid(), "the original pipe table carries the parent end");
    assert!(!complete.kind.is_valid(), "a filled parent slot is not a submission");
    complete.result = Err(Error::NotFound);
    assert!(!complete.is_valid(), "failure cannot leave a usable pipe end");
    kind = complete.kind;
    let Op::Spawn { spawn } = &mut kind else { panic!("spawn record") };
    spawn.pipes[0].parent = None;
    complete.kind = kind;
    assert!(complete.is_valid(), "a failed spawn returns empty parent slots");
}

#[test]
fn an_append_validates_remaining_bytes_and_counts() {
    assert_eq!(Op::append(FD, bytes(3), 1), Ok(Op::Append { fd: FD, bytes: bytes(3), from: 1 }));
    for from in [3, 4, u32::MAX] {
        assert_eq!(Op::append(FD, bytes(3), from), Err(bytes(3)));
        assert!(!Op::Append { fd: FD, bytes: bytes(3), from }.is_valid());
    }
    assert_eq!(Op::append(FD, bytes(0), 0), Err(bytes(0)));
    for (n, valid) in [(0, false), (1, true), (3, true), (4, false)] {
        let answer = complete(Op::Append { fd: FD, bytes: bytes(4), from: 1 }, Ok(Done::Count(n)));
        assert_eq!(answer.is_valid(), valid, "append count {n}");
    }
}
