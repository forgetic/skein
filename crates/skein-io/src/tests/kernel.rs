//! The records' rules (kernel.md, 3 to 5): each operation's one success
//! shape, what a completion may count, which errors answer a cancel, and the
//! buffer handed back whatever the result.

use alloc::boxed::Box;
use core::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use skein_lib::Token;

use crate::kernel::{Addr, Complete, Done, Error, Family, Fd, Op, Shape};

const FD: Fd = Fd::new(3);
const NEW: Fd = Fd::new(4);

fn v4() -> Addr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 8080))
}

fn bytes(len: usize) -> Box<[u8]> {
    Box::from(&[7; 8][..len])
}

fn complete(kind: Op, result: Result<Done, Error>) -> Complete {
    Complete { op: Token::new(1), kind, result }
}

/// One of every operation, valid. A new operation makes the match in
/// `documented` fail to build until it is added there, and here.
fn every_op() -> [Op; 10] {
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
        Op::Cancel { target: Token::new(2) },
    ]
}

/// The success each operation documents on `Op`, written out again by an
/// exhaustive match.
fn documented(op: &Op) -> Done {
    match op {
        Op::Socket { .. } => Done::Fd(NEW),
        Op::Accept { .. } => Done::Accepted { fd: NEW, peer: v4() },
        Op::Recv { .. } | Op::Send { .. } => Done::Count(1),
        Op::Bind { .. } => Done::Bound(v4()),
        Op::Listen { .. } | Op::Connect { .. } | Op::Shutdown { .. } | Op::Close { .. } | Op::Cancel { .. } => {
            Done::Nothing
        }
    }
}

#[test]
fn every_operation_succeeds_with_its_documented_shape_and_no_other() {
    let candidates =
        [Done::Nothing, Done::Count(1), Done::Fd(NEW), Done::Accepted { fd: NEW, peer: v4() }, Done::Bound(v4())];
    for op in every_op() {
        assert!(op.is_valid(), "the examples are valid");
        let expected = documented(&op);
        assert_eq!(op.shape(), expected.shape());
        let mut kind = op;
        for done in candidates {
            let answer = complete(kind, Ok(done));
            assert_eq!(answer.is_valid(), done == expected, "only the documented shape fits");
            kind = answer.kind;
        }
    }
}

#[test]
fn the_shapes_are_as_the_table_on_op_says() {
    let [socket, bind, listen, accept, connect, recv, send, shutdown, close, cancel] = every_op();
    assert_eq!(socket.shape(), Shape::Fd);
    assert_eq!(accept.shape(), Shape::Accepted);
    assert_eq!(bind.shape(), Shape::Bound);
    assert_eq!((recv.shape(), send.shape()), (Shape::Count, Shape::Count));
    for op in [listen, connect, shutdown, close, cancel] {
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
