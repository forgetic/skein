//! Actual native TLS/IO ciphertext and independently owed read/output rights.
//! Contract: tls.md, 3.6; lib.md, 7.1; testing-strategy.md, 6.

use skein_lib::stream::OutputOutcome;
use skein_sim::Config;
use skein_tls::client::Version;
use skein_tls_world::native::{Case, conversation};

#[test]
fn both_real_versions_send_late_plaintext_while_upper_and_ciphertext_reads_wait() {
    for version in [Version::Tls12, Version::Tls13] {
        let actual = conversation(801, Config { buffer: 7, ..Config::calm() }, Case::Late, version);
        assert_eq!(actual.server_plaintext, b"ping");
        assert_eq!(actual.reply, b"ok");
        assert!(actual.behind_both_reads);
        assert_eq!(actual.terminals.len(), 1);
        assert_eq!(actual.terminals[0].1, OutputOutcome::Granted);
        assert_eq!((actual.closed, actual.transport_closing, actual.transport_closed), (1, 0, 0));
    }
}

#[test]
fn actual_ciphertext_flight_proves_exact_byte_credit_and_one_short_waiting() {
    for version in [Version::Tls12, Version::Tls13] {
        for case in [Case::ByteOneShort, Case::ByteExact] {
            let actual = conversation(802, Config { buffer: 7, ..Config::calm() }, case, version);
            assert_eq!(actual.server_plaintext, b"ping");
            assert_eq!(actual.reply, b"ok");
            assert!(actual.behind_both_reads);
            assert_eq!(actual.blocked, case == Case::ByteOneShort);
            assert_eq!(actual.terminals.len(), 2);
            assert!(actual.terminals.iter().all(|(_, terminal)| *terminal == OutputOutcome::Granted));
            assert_eq!((actual.closed, actual.transport_closing, actual.transport_closed), (1, 0, 0));
        }
    }
}

#[test]
fn actual_flight_plus_one_queued_send_blocks_slots_with_full_bytes_available() {
    for version in [Version::Tls12, Version::Tls13] {
        let actual = conversation(803, Config { buffer: 7, ..Config::calm() }, Case::SlotsFull, version);
        assert_eq!(actual.server_plaintext, b"ping");
        assert_eq!(actual.reply, b"ok");
        assert!(actual.behind_both_reads && actual.blocked);
        assert_eq!(actual.terminals.len(), 3);
        assert!(actual.terminals.iter().all(|(_, terminal)| *terminal == OutputOutcome::Granted));
        assert_eq!((actual.closed, actual.transport_closing, actual.transport_closed), (1, 0, 0));
    }
}

#[test]
fn actual_io_closed_before_ready_cancels_upper_without_lower_output_cancelled() {
    for version in [Version::Tls12, Version::Tls13] {
        let actual = conversation(804, Config { buffer: 7, ..Config::calm() }, Case::DirectClosed, version);
        assert!(actual.server_plaintext.is_empty() && actual.reply.is_empty());
        assert_eq!(actual.terminals.len(), 1);
        assert_eq!(actual.terminals[0].1, OutputOutcome::Cancelled);
        assert_eq!((actual.closed, actual.transport_closing, actual.transport_closed), (0, 0, 1));
    }
}

#[test]
fn genuine_io_cancel_and_closed_both_orders_preserve_one_native_close_terminal() {
    for version in [Version::Tls12, Version::Tls13] {
        for case in [Case::LocalCloseFirst, Case::PhysicalCloseFirst] {
            let actual = conversation(805, Config { buffer: 7, ..Config::calm() }, case, version);
            assert!(actual.reply.is_empty());
            assert!(actual.blocked && actual.behind_both_reads);
            assert_eq!(actual.terminals.len(), 2);
            assert_eq!(actual.terminals[0].1, OutputOutcome::Granted);
            assert_eq!(actual.terminals[1].1, OutputOutcome::Cancelled);
            assert_eq!(actual.transport_closing, 1);
            assert_eq!(
                (actual.closed, actual.transport_closed),
                if case == Case::LocalCloseFirst { (1, 0) } else { (0, 1) }
            );
        }
    }
}

fn ready_pair() -> skein_tls_world::native_pair::Pair {
    use skein_tls::client::{self, native};
    use skein_tls_world::{pki, server::Server};
    let limits = client::Limits { read: 16, send: 16, records: client::MAX_RECORD };
    let mut pair = skein_tls_world::native_pair::Pair::new(
        &pki::client(&[]),
        pki::name(),
        limits,
        Server::new(pki::Server::plain().config()),
    );
    let mut events = pair.down(native::Request::Client(client::Request::Handshake));
    events.extend(pair.settle());
    assert!(matches!(&events[..], [native::Event::Client(client::Event::Ready(_))]));
    assert!(!pair.server.as_ref().expect("actual peer").handshaking());
    pair
}

#[test]
fn actual_lower_grant_queued_across_cancel_releases_before_a_new_upper_right() {
    use skein_lib::{
        Token,
        stream::{OutputDown, OutputUp},
    };
    use skein_tls::client::native;
    let mut pair = ready_pair();
    let first = Token::new(50);
    let next = Token::new(51);
    assert!(pair.down(native::Request::Output(OutputDown::Room { right: first, bytes: 16 })).is_empty());
    let queued = pair.answer().expect("actual emitted lower winner");
    assert!(matches!(queued, native::LowerEvent::Output(OutputUp::Settled { outcome: OutputOutcome::Granted, .. })));
    assert_eq!(
        pair.down(native::Request::Output(OutputDown::Cancel { right: first })),
        [native::Event::Output(OutputUp::Settled { right: first, outcome: OutputOutcome::Cancelled })]
    );
    assert!(pair.down(native::Request::Output(OutputDown::Room { right: next, bytes: 16 })).is_empty());
    assert!(pair.up(queued).is_empty(), "queued lower grant releases; no second upper terminal");
    assert_eq!(
        pair.settle(),
        [native::Event::Output(OutputUp::Settled { right: next, outcome: OutputOutcome::Granted })]
    );
    assert!(
        pair.down(native::Request::Output(OutputDown::Cancel { right: next })).is_empty(),
        "emitted upper grant wins"
    );
    assert!(
        pair.down(native::Request::Output(OutputDown::Send { right: next, bytes: b"ping".as_slice().into() }))
            .is_empty()
    );
    assert_eq!(pair.server.as_ref().expect("actual peer").received, b"ping");
}

#[test]
fn native_finish_spends_a_real_held_grant_and_emits_actual_close_notify_and_one_finish() {
    use skein_lib::{
        Token,
        stream::{Down, OutputDown, OutputUp},
    };
    use skein_tls::client::{self, native};
    let mut pair = ready_pair();
    let right = Token::new(60);
    assert!(pair.down(native::Request::Output(OutputDown::Room { right, bytes: 16 })).is_empty());
    assert_eq!(pair.settle(), [native::Event::Output(OutputUp::Settled { right, outcome: OutputOutcome::Granted })]);
    assert!(pair.down(native::Request::Client(client::Request::Stream(Down::Finish))).is_empty());
    assert!(pair.settle().is_empty());
    assert!(pair.server.as_ref().expect("actual peer").closed, "real rustls peer authenticated close_notify");
    assert_eq!(pair.finished, 1);
}

#[test]
#[should_panic(expected = "no native read after Finish withdrawal")]
fn native_finish_forbids_later_positive_reads_without_changing_classic_read_after_finish() {
    use skein_lib::stream::{Down, Read};
    use skein_tls::client::{self, native};
    let mut pair = ready_pair();
    pair.down(native::Request::Client(client::Request::Stream(Down::Finish)));
    pair.settle();
    pair.down(native::Request::Client(client::Request::Stream(Down::Demand { read: Read::Fill(1), room: 0 })));
}

#[test]
#[should_panic(expected = "native Finish with no live upper read")]
fn native_finish_refuses_a_live_upper_read_before_withdrawal_effects() {
    use skein_lib::stream::{Down, Read};
    use skein_tls::client::{self, native};
    let mut pair = ready_pair();
    assert!(
        pair.down(native::Request::Client(client::Request::Stream(Down::Demand { read: Read::Fill(1), room: 0 })))
            .is_empty()
    );
    pair.down(native::Request::Client(client::Request::Stream(Down::Finish)));
}

#[test]
fn real_peer_key_updates_held_behind_upper_grant_precede_exact_maximum_plaintext() {
    use skein_lib::{
        Token,
        stream::{Down, OutputDown, OutputUp, Read},
    };
    use skein_tls::client::{self, native};
    let mut pair = ready_pair();
    let right = Token::new(70);
    assert!(
        pair.down(native::Request::Client(client::Request::Stream(Down::Demand { read: Read::Fill(16), room: 0 })))
            .is_empty()
    );
    assert!(pair.down(native::Request::Output(OutputDown::Room { right, bytes: 16 })).is_empty());
    assert_eq!(pair.settle(), [native::Event::Output(OutputUp::Settled { right, outcome: OutputOutcome::Granted })]);
    for _ in 0..3 {
        pair.server.as_mut().expect("actual peer").key_update();
        pair.pull();
        assert!(
            pair.settle().is_empty(),
            "control records never satisfy the held plaintext read or consume upper grant"
        );
    }
    let payload = vec![b'x'; 16];
    assert!(pair.down(native::Request::Output(OutputDown::Send { right, bytes: payload.clone().into() })).is_empty());
    assert_eq!(
        pair.server.as_ref().expect("actual peer").received,
        payload,
        "every actual owed key-update response precedes authenticated data"
    );
    assert_eq!(
        pair.last_send().expect("actual deferred reply plus maximum16 Send").1,
        27 + 16 + 22,
        "pinned rustls coalesces the three valid requests to one authenticated deferred reply"
    );
}

#[test]
fn empty_native_send_and_explicit_release_each_spend_one_real_backed_grant() {
    use skein_lib::{
        Token,
        stream::{OutputDown, OutputUp},
    };
    use skein_tls::client::native;
    let mut pair = ready_pair();
    for (right, empty) in [(Token::new(80), true), (Token::new(81), false)] {
        assert!(pair.down(native::Request::Output(OutputDown::Room { right, bytes: 16 })).is_empty());
        assert_eq!(
            pair.settle(),
            [native::Event::Output(OutputUp::Settled { right, outcome: OutputOutcome::Granted })]
        );
        let spent =
            if empty { OutputDown::Send { right, bytes: Box::from([]) } } else { OutputDown::Release { right } };
        assert!(pair.down(native::Request::Output(spent)).is_empty());
        assert!(pair.settle().is_empty());
        assert!(
            pair.server.as_ref().expect("actual peer").received.is_empty(),
            "no invented application bytes from empty/release"
        );
    }
    let right = Token::new(82);
    assert!(pair.down(native::Request::Output(OutputDown::Room { right, bytes: 4 })).is_empty());
    assert_eq!(pair.settle(), [native::Event::Output(OutputUp::Settled { right, outcome: OutputOutcome::Granted })]);
    assert!(pair.down(native::Request::Output(OutputDown::Send { right, bytes: Box::from(*b"ping") })).is_empty());
    assert_eq!(
        pair.server.as_ref().expect("actual peer").received,
        b"ping",
        "both spent real slots admit the next actual Send"
    );
}

#[test]
#[should_panic(expected = "native Finish with no pending output")]
fn native_finish_refuses_a_real_pending_output_before_consuming_lower_reservation() {
    use skein_lib::{
        Token,
        stream::{Down, OutputDown},
    };
    use skein_tls::client::{self, native};
    let mut pair = ready_pair();
    assert!(pair.down(native::Request::Output(OutputDown::Room { right: Token::new(90), bytes: 16 })).is_empty());
    pair.down(native::Request::Client(client::Request::Stream(Down::Finish)));
}

/// An actual completed extractable peer, with the original authenticated
/// `TLS1.2` fixture suite (tls.md, 3.6; testing-strategy.md, 2.4).
fn ready_tls12_pair() -> skein_tls_world::native_pair::Pair {
    use skein_tls::client::{self, native};
    use skein_tls_world::{pki, server::Server};
    let server = pki::Server { versions: pki::Versions::Tls12, extractable: true, ..pki::Server::plain() };
    let mut pair = skein_tls_world::native_pair::Pair::new(
        &pki::client(&[]),
        pki::name(),
        client::Limits { read: 16, send: 16, records: client::MAX_RECORD },
        Server::new(server.config()),
    );
    let mut events = pair.down(native::Request::Client(client::Request::Handshake));
    events.extend(pair.settle());
    assert!(
        matches!(&events[..], [native::Event::Client(client::Event::Ready(agreed))] if agreed.version == Version::Tls12)
    );
    pair.extract_peer();
    pair
}

#[test]
fn actual_owed_growth_across_an_older_queued_grant_preserves_fifo_prefix_and_suffix() {
    use rustls::ContentType;
    use skein_lib::stream::{Down, OutputUp, Read};
    use skein_tls::client::{self, native};
    let mut pair = ready_tls12_pair();
    assert!(
        pair.down(native::Request::Client(client::Request::Stream(Down::Demand { read: Read::Fill(16), room: 0 })))
            .is_empty()
    );
    pair.extracted.as_mut().expect("actual extracted peer").hello_request();
    pair.pull();
    // An authentic HelloRequest produces one actual warning refusal in Held.
    // Rustls permits one such request, not a fictional repeated-control flood.
    for _ in 0..2 {
        let actual = pair.answer().expect("actual HelloRequest header/body");
        assert!(matches!(actual, native::LowerEvent::Stream(_)));
        assert!(pair.up(actual).is_empty());
    }
    let original = pair.output_bytes().expect("actual TLS-owned refusal reservation");
    assert_eq!(original, 31, "two-byte authenticated warning plus TLS1.2 record overhead");
    let winner = pair.answer().expect("actual older full output grant emitted by proxy");
    let native::LowerEvent::Output(OutputUp::Settled { right: first, outcome: OutputOutcome::Granted }) = &winner
    else {
        panic!("actual queued winner")
    };
    let first = *first;
    // Real native Close appends close_notify while the older grant is queued.
    // This actual growth is 31+31, preserving the unchanged 2048 allocation.
    assert!(pair.down(native::Request::Client(client::Request::Close)).is_empty());
    assert!(pair.up(winner).is_empty());
    assert_eq!(pair.last_send(), Some((first, original)), "fitting ordered prefix spends original full grant");
    assert_eq!(pair.extracted.as_ref().expect("actual peer").alerts, [[1, 100]], "authenticated warning refusal first");
    assert_eq!(pair.output_bytes(), Some(original), "exact retained close suffix needs a distinct right");
    assert!(pair.settle().is_empty(), "draining ciphertext alone cannot impersonate physical Closed");
    let (second, suffix) = pair.last_send().expect("actual close suffix Send");
    assert_ne!(second, first, "checked distinct lower generation");
    assert_eq!(suffix, original, "whole ordered suffix, no lost control bytes");
    let peer = pair.extracted.as_ref().expect("actual authenticated peer");
    assert_eq!(peer.alerts, [[1, 100], [1, 0]], "actual warning then actual close_notify authenticate in order");
    assert_eq!(peer.records, [ContentType::Alert, ContentType::Alert]);
    assert!(peer.received.is_empty(), "no invented data from two actual control records");
    assert_eq!(
        pair.up(native::LowerEvent::Closed),
        [native::Event::Client(client::Event::Closed)],
        "bounded proxy lifetime terminates after both settled Sends; actual IO closure tested separately"
    );
}

#[test]
fn authentic_tls12_refusal_held_with_upper_grant_precedes_exact_maximum_plaintext() {
    use rustls::ContentType;
    use skein_lib::{
        Token,
        stream::{Down, OutputDown, OutputUp, Read},
    };
    use skein_tls::client::{self, native};
    let mut pair = ready_tls12_pair();
    let upper = Token::new(100);
    assert!(
        pair.down(native::Request::Client(client::Request::Stream(Down::Demand { read: Read::Fill(16), room: 0 })))
            .is_empty()
    );
    assert!(pair.down(native::Request::Output(OutputDown::Room { right: upper, bytes: 16 })).is_empty());
    assert_eq!(
        pair.settle(),
        [native::Event::Output(OutputUp::Settled { right: upper, outcome: OutputOutcome::Granted })]
    );
    let before = pair.last_send();
    pair.extracted.as_mut().expect("actual peer").hello_request();
    pair.pull();
    assert!(pair.settle().is_empty(), "refusal neither answers the read nor consumes backed upper grant");
    assert_eq!(pair.last_send(), before, "owed warning stays held behind the actual upper grant");
    assert!(
        pair.down(native::Request::Output(OutputDown::Send { right: upper, bytes: Box::from([b'x'; 16]) })).is_empty()
    );
    assert_eq!(pair.last_send().expect("actual complete owed+data Send").1, 31 + 16 + 29);
    let peer = pair.extracted.as_ref().expect("actual authenticated peer");
    assert_eq!(peer.alerts, [[1, 100]]);
    assert_eq!(peer.records, [ContentType::Alert, ContentType::ApplicationData], "complete prefix precedes data");
    assert_eq!(peer.received, [b'x'; 16], "full plaintext authenticates after actual owed warning");
}

/// Real negotiated peer of either version for queued-read controls (tls.md, 3.6).
fn queued_pair(version: Version) -> skein_tls_world::native_pair::Pair {
    use skein_tls::client::{self, native};
    use skein_tls_world::{pki, server::Server};
    let versions = if version == Version::Tls12 { pki::Versions::Tls12 } else { pki::Versions::Tls13 };
    let server = pki::Server { versions, ..pki::Server::plain() };
    let mut pair = skein_tls_world::native_pair::Pair::new(
        &pki::client(&[]),
        pki::name(),
        client::Limits { read: 16, send: 16, records: client::MAX_RECORD },
        Server::new(server.config()),
    );
    let mut events = pair.down(native::Request::Client(client::Request::Handshake));
    events.extend(pair.settle());
    assert!(matches!(&events[..], [native::Event::Client(client::Event::Ready(agreed))] if agreed.version == version));
    pair
}

/// Emit one actual exact Header or Body Fill winner, retaining its owning box
/// above TLS until the chosen withdrawal/Finish ordering (tls.md, 3.6).
fn queued_ciphertext(
    pair: &mut skein_tls_world::native_pair::Pair,
    body: bool,
) -> skein_tls::client::native::LowerEvent {
    use skein_lib::stream::{Read, Up};
    use skein_tls::client::{self, native};
    pair.server.as_mut().expect("real completed peer").write(&[b'x'; 16]);
    pair.pull();
    assert_eq!(pair.read, Some(Read::Fill(client::HEADER)));
    let header = pair.answer().expect("real emitted Header winner");
    let native::LowerEvent::Stream(Up::Bytes(bytes)) = &header else { panic!("actual Header box") };
    assert_eq!(bytes.len(), usize::try_from(client::HEADER).expect("Header"));
    let length = u32::from(u16::from_be_bytes([bytes[3], bytes[4]]));
    if body {
        assert!(pair.up(header).is_empty(), "Header alone invents no upper answer");
        assert_eq!(pair.read, Some(Read::Fill(length)));
        let winner = pair.answer().expect("real emitted Body winner");
        let native::LowerEvent::Stream(Up::Bytes(bytes)) = &winner else { panic!("actual Body box") };
        assert_eq!(bytes.len(), usize::try_from(length).expect("bounded real Body"));
        assert!(pair.read.is_none(), "Fill ownership moved to queued box");
        winner
    } else {
        assert!(pair.read.is_none(), "Fill ownership moved to queued box");
        header
    }
}

#[test]
fn genuine_queued_header_and_body_after_ready_withdrawal_preserve_pending_and_held_output() {
    use skein_lib::{
        Token,
        stream::{Down, OutputDown, OutputUp, Read},
    };
    use skein_tls::client::{self, native};
    for version in [Version::Tls12, Version::Tls13] {
        for body in [false, true] {
            for held in [false, true] {
                let mut pair = queued_pair(version);
                let right = Token::new(110);
                assert!(
                    pair.down(native::Request::Client(client::Request::Stream(Down::Demand {
                        read: Read::Fill(16),
                        room: 0
                    })))
                    .is_empty()
                );
                assert!(pair.down(native::Request::Output(OutputDown::Room { right, bytes: 16 })).is_empty());
                if held {
                    assert_eq!(
                        pair.settle(),
                        [native::Event::Output(OutputUp::Settled { right, outcome: OutputOutcome::Granted })]
                    );
                }
                let winner = queued_ciphertext(&mut pair, body);
                let before = pair.last_send();
                assert!(
                    pair.down(native::Request::Client(client::Request::Stream(Down::Demand {
                        read: Read::Nothing,
                        room: 0
                    })))
                    .is_empty()
                );
                assert!(pair.up(winner).is_empty(), "actual withdrawn winner creates no upper observation");
                assert!(pair.read.is_none(), "no new ciphertext Fill after ready withdrawal");
                assert_eq!(pair.last_send(), before, "no fabricated output spend from dropped read");
                if !held {
                    assert_eq!(
                        pair.settle(),
                        [native::Event::Output(OutputUp::Settled { right, outcome: OutputOutcome::Granted })]
                    );
                }
                assert!(
                    pair.down(native::Request::Output(OutputDown::Send { right, bytes: Box::from(*b"ping") }))
                        .is_empty()
                );
                assert_eq!(pair.server.as_ref().expect("real peer").received, b"ping");
                assert!(pair.read.is_none(), "independent output cannot restart withdrawn read");
            }
        }
    }
}

/// Exact read-winner ordering relative to permitted native Finish (tls.md, 3.6).
#[derive(Clone, Copy)]
enum QueuedOrder {
    BeforeFinish,
    Flushing,
    Finished,
}

#[test]
fn genuine_queued_read_winners_cross_finish_before_during_and_after_close_notify_settlement() {
    use skein_lib::stream::{Down, Read};
    use skein_tls::client::{self, native};
    for version in [Version::Tls12, Version::Tls13] {
        for body in [false, true] {
            for order in [QueuedOrder::BeforeFinish, QueuedOrder::Flushing, QueuedOrder::Finished] {
                let mut pair = queued_pair(version);
                assert!(
                    pair.down(native::Request::Client(client::Request::Stream(Down::Demand {
                        read: Read::Fill(16),
                        room: 0
                    })))
                    .is_empty()
                );
                let winner = queued_ciphertext(&mut pair, body);
                assert!(
                    pair.down(native::Request::Client(client::Request::Stream(Down::Demand {
                        read: Read::Nothing,
                        room: 0
                    })))
                    .is_empty()
                );
                if matches!(order, QueuedOrder::BeforeFinish) {
                    assert!(pair.up(winner).is_empty());
                    assert!(pair.down(native::Request::Client(client::Request::Stream(Down::Finish))).is_empty());
                } else {
                    assert!(pair.down(native::Request::Client(client::Request::Stream(Down::Finish))).is_empty());
                    assert_eq!(pair.finished, 0, "real close_notify output is still pending");
                    if matches!(order, QueuedOrder::Finished) {
                        assert!(pair.settle().is_empty());
                        assert_eq!(pair.finished, 1, "actual output grant and Send settled before winner delivery");
                    }
                    assert!(pair.up(winner).is_empty(), "Flushing/Finished drops only the actual withdrawn Fill");
                }
                assert!(pair.settle().is_empty());
                assert_eq!(pair.finished, 1, "one genuine lower Finish");
                let peer = pair.server.as_ref().expect("real peer");
                assert!(peer.closed && peer.failed.is_none(), "real peer authenticates close_notify");
                assert!(peer.received.is_empty(), "no invented plaintext from read disposal");
                assert!(pair.read.is_none() && pair.output_bytes().is_none());
                assert!(pair.down(native::Request::Client(client::Request::Close)).is_empty());
                assert_eq!(pair.up(native::LowerEvent::Closed), [native::Event::Client(client::Event::Closed)]);
                assert!(pair.settle().is_empty(), "one supplied physical Closed has no second local terminal");
            }
        }
    }
}

#[test]
fn handshake_running_withdrawal_keeps_real_engine_reads_through_ready() {
    use skein_lib::stream::{Down, Read};
    use skein_tls::client::{self, native};
    use skein_tls_world::{pki, server::Server};
    for versions in [pki::Versions::Tls12, pki::Versions::Tls13] {
        let server = pki::Server { versions, ..pki::Server::plain() };
        let mut pair = skein_tls_world::native_pair::Pair::new(
            &pki::client(&[]),
            pki::name(),
            client::Limits { read: 16, send: 16, records: client::MAX_RECORD },
            Server::new(server.config()),
        );
        assert!(pair.down(native::Request::Client(client::Request::Handshake)).is_empty());
        assert_eq!(pair.read, Some(Read::Fill(client::HEADER)), "actual handshake engine read");
        assert!(
            pair.down(native::Request::Client(client::Request::Stream(Down::Demand { read: Read::Fill(16), room: 0 })))
                .is_empty()
        );
        assert!(
            pair.down(native::Request::Client(client::Request::Stream(Down::Demand { read: Read::Nothing, room: 0 })))
                .is_empty()
        );
        assert_eq!(
            pair.read,
            Some(Read::Fill(client::HEADER)),
            "upper withdrawal cannot withdraw engine handshake read"
        );
        assert!(matches!(&pair.settle()[..], [native::Event::Client(client::Event::Ready(_))]));
        assert!(!pair.server.as_ref().expect("real peer").handshaking());
        assert!(pair.read.is_none(), "Ready does not create an ordinary read after withdrawal");
    }
}

#[test]
fn authenticated_peer_end_leaves_partial_fill_unanswered_and_permits_finish() {
    use skein_lib::stream::{Down, Read, Up};
    use skein_tls::client::{self, native};
    for version in [Version::Tls12, Version::Tls13] {
        for body in [false, true] {
            for withdraw in [false, true] {
                let mut pair = queued_pair(version);
                assert!(
                    pair.down(native::Request::Client(client::Request::Stream(Down::Demand {
                        read: Read::Fill(16),
                        room: 0
                    })))
                    .is_empty()
                );
                let peer = pair.server.as_mut().expect("real peer");
                peer.write(b"x");
                peer.close_notify();
                pair.pull();
                let mut winner = pair.answer().expect("genuine queued Header before peer End");
                if body {
                    assert!(pair.up(winner).is_empty());
                    winner = pair.answer().expect("genuine queued Body before peer End");
                }
                assert!(matches!(&winner, native::LowerEvent::Stream(Up::Bytes(_))));
                assert!(
                    pair.up(winner).is_empty(),
                    "actual earlier Fill arrives before authentic End; short plaintext cannot answer Fill16"
                );
                assert_eq!(
                    pair.settle(),
                    [native::Event::Client(client::Event::Stream(Up::End))],
                    "authenticated close_notify is End, never an invented short Fill answer"
                );
                assert!(pair.read.is_none(), "peer close forbids a new ciphertext Fill");
                if withdraw {
                    assert!(
                        pair.down(native::Request::Client(client::Request::Stream(Down::Demand {
                            read: Read::Nothing,
                            room: 0
                        })))
                        .is_empty(),
                        "crossed End leaves the actual upper Fill outstanding for withdrawal"
                    );
                }
                assert!(pair.down(native::Request::Client(client::Request::Stream(Down::Finish))).is_empty());
                assert!(pair.settle().is_empty());
                assert_eq!(pair.finished, 1);
                assert!(pair.server.as_ref().expect("real peer").closed, "opposite actual close_notify authenticates");
            }
        }
    }
}
