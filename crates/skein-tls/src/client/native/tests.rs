//! Native lowest-tier ownership controls (tls.md, 3.6; testing-strategy.md, 2.1).

use super::{Client, Event, LowerEvent, LowerOutput, LowerRequest, Request, State, UpperOutput};
use crate::Name;
use crate::client::{self, Limits};
use crate::tests::{LIMITS, config};
use alloc::boxed::Box;
use skein_lib::stream::{Down, OutputDown, OutputOutcome, OutputUp, Read, Up};
use skein_lib::{Env, Queue, Time, Token, Wall};

fn lower(output: u32) -> super::LowerLimits {
    super::LowerLimits { read: client::LARGEST_READ, output, sends: 1 }
}

struct Machine {
    client: Client,
    env: Env<Limits>,
    above: Queue<Event>,
    below: Queue<LowerRequest>,
}

impl Machine {
    fn new() -> Self {
        Self {
            client: Client::new(
                &config(),
                Name::new("skein.test").expect("name"),
                &LIMITS,
                &lower(super::largest_room(&LIMITS).expect("cap")),
            )
            .expect("compatible"),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: LIMITS },
            above: Queue::with_capacity(super::UP_MAX_OUT.above),
            below: Queue::with_capacity(super::UP_MAX_OUT.below),
        }
    }

    fn down(&mut self, request: Request) {
        assert!(self.above.is_empty() && self.below.is_empty(), "previous step fully observed");
        super::down(&mut self.client, &self.env, request, &mut self.above, &mut self.below);
        assert!(self.above.len() <= super::DOWN_MAX_OUT.above, "down upper maximum");
        assert!(self.below.len() <= super::DOWN_MAX_OUT.below, "down lower maximum");
    }

    fn up(&mut self, event: LowerEvent) {
        assert!(self.above.is_empty() && self.below.is_empty(), "previous step fully observed");
        super::up(&mut self.client, &self.env, event, &mut self.above, &mut self.below);
        assert!(self.above.len() <= super::UP_MAX_OUT.above, "up upper maximum");
        assert!(self.below.len() <= super::UP_MAX_OUT.below, "up lower maximum");
    }

    fn started(&mut self) -> Token {
        self.down(Request::Client(client::Request::Handshake));
        assert!(self.above.is_empty(), "real ClientHello does not complete handshake");
        let right = match self.below.pop().expect("ClientHello output right") {
            LowerRequest::Output(OutputDown::Room { right, bytes }) => {
                assert!(bytes > 0 && bytes <= client::FLIGHT, "real bounded ClientHello");
                right
            }
            LowerRequest::Output(OutputDown::Cancel { .. } | OutputDown::Send { .. } | OutputDown::Release { .. })
            | LowerRequest::Stream(_) => unreachable!("first request reserves real flight"),
        };
        assert_eq!(
            self.below.pop(),
            Some(LowerRequest::Stream(Down::Demand { read: Read::Fill(client::HEADER), room: 0 })),
            "separate real ciphertext read"
        );
        assert!(self.below.is_empty(), "exact initial two lower cells");
        right
    }

    fn sent_hello(&mut self, right: Token) {
        self.up(LowerEvent::Output(OutputUp::Settled { right, outcome: OutputOutcome::Granted }));
        assert!(self.above.is_empty(), "handshake still unanswered");
        match self.below.pop().expect("actual hello Send") {
            LowerRequest::Output(OutputDown::Send { right: sent, bytes }) => {
                assert_eq!(sent, right, "exact actual grant consumed");
                assert!(!bytes.is_empty(), "real rustls ClientHello");
            }
            LowerRequest::Output(OutputDown::Room { .. } | OutputDown::Cancel { .. } | OutputDown::Release { .. })
            | LowerRequest::Stream(_) => unreachable!("granted flight sends exactly once"),
        }
        assert!(self.below.is_empty(), "read remains outstanding independently");
    }
}

fn wanted(machine: &mut Machine, right: Token) {
    machine.down(Request::Output(OutputDown::Room { right, bytes: 1 }));
    assert!(machine.above.is_empty() && machine.below.is_empty(), "upper Wanted waits for real Ready");
}

#[test]
fn native_envelope_and_value_bound_are_checked_before_startup_allocation() {
    for plaintext in [1, 16, 16_384, 16_385, 40_000] {
        assert_eq!(
            super::room_for(plaintext),
            match client::room_for(plaintext) {
                Some(bytes) => bytes.checked_add(client::FLIGHT),
                None => None,
            },
            "whole owed prefix beside original envelope"
        );
    }
    assert_eq!(super::room_for(u32::MAX), None, "checked overflow");
    let cap = super::largest_room(&LIMITS).expect("checked configured cap");
    assert!(
        Client::new(
            &config(),
            Name::new("skein.test").expect("name"),
            &LIMITS,
            &lower(cap.checked_sub(1).expect("positive cap"))
        )
        .is_none(),
        "one-short configured cap refuses"
    );
    assert!(
        Client::new(&config(), Name::new("skein.test").expect("name"), &LIMITS, &lower(cap)).is_some(),
        "exact cap admits"
    );
    for incompatible in [
        super::LowerLimits { read: client::LARGEST_READ - 1, output: cap, sends: 1 },
        super::LowerLimits { read: client::LARGEST_READ, output: cap, sends: 0 },
    ] {
        assert!(Client::new(&config(), Name::new("skein.test").expect("name"), &LIMITS, &incompatible).is_none());
    }
    assert!(
        super::worst_case(&LIMITS).expect("native bound") >= client::worst_case(&LIMITS).expect("classic bound"),
        "actual native value/work counted"
    );
}

#[test]
fn upper_cancel_before_ready_settles_only_its_named_right_and_never_reads() {
    let mut machine = Machine::new();
    let lower = machine.started();
    wanted(&mut machine, Token::new(70));
    machine.down(Request::Output(OutputDown::Cancel { right: Token::new(71) }));
    assert!(machine.above.is_empty() && machine.below.is_empty(), "stale cancel is inert");
    machine.down(Request::Output(OutputDown::Cancel { right: Token::new(70) }));
    assert_eq!(
        machine.above.pop(),
        Some(Event::Output(OutputUp::Settled { right: Token::new(70), outcome: OutputOutcome::Cancelled })),
        "upper owns its cancellation"
    );
    assert!(machine.above.is_empty() && machine.below.is_empty(), "TLS flight/read are still unanswered");
    machine.sent_hello(lower);
    machine.down(Request::Client(client::Request::Close));
    assert!(machine.above.is_empty(), "local close awaits actual physical Closed");
    assert_eq!(
        machine.below.pop(),
        Some(LowerRequest::Stream(Down::Demand { read: Read::Nothing, room: 0 })),
        "actual read withdrawn only on close"
    );
    machine.up(LowerEvent::Closed);
    assert_eq!(machine.above.pop(), Some(Event::Client(client::Event::Closed)), "local terminal after actual closure");
}

#[test]
fn direct_actual_closed_cancels_pre_ready_upper_without_any_lower_output_notice() {
    let mut machine = Machine::new();
    let lower = machine.started();
    machine.sent_hello(lower);
    wanted(&mut machine, Token::new(8));
    machine.up(LowerEvent::Closed);
    assert_eq!(
        machine.above.pop(),
        Some(Event::Output(OutputUp::Settled { right: Token::new(8), outcome: OutputOutcome::Cancelled })),
        "own upper debt settles before lifecycle"
    );
    assert_eq!(machine.above.pop(), Some(Event::TransportClosed), "actual physical close ends Handshake/read");
    assert!(machine.below.is_empty(), "no fabricated lower Failed or cancellation");
    machine.down(Request::Client(client::Request::Close));
    assert!(
        machine.above.is_empty() && machine.below.is_empty(),
        "crossed native Close after TransportClosed is inert"
    );
}

#[test]
fn unsolicited_lower_cancel_and_both_coordinated_close_orders_keep_one_lifecycle() {
    for close_first in [false, true] {
        let mut machine = Machine::new();
        let lower = machine.started();
        wanted(&mut machine, Token::new(8));
        machine.up(LowerEvent::Output(OutputUp::Settled { right: lower, outcome: OutputOutcome::Cancelled }));
        assert_eq!(
            machine.above.pop(),
            Some(Event::Output(OutputUp::Settled { right: Token::new(8), outcome: OutputOutcome::Cancelled })),
            "upper debt cancellation"
        );
        assert_eq!(machine.above.pop(), Some(Event::TransportClosing), "real unsolicited cancellation lifecycle");
        assert_eq!(
            machine.below.pop(),
            Some(LowerRequest::Stream(Down::Demand { read: Read::Nothing, room: 0 })),
            "closing read withdrawal"
        );
        if close_first {
            machine.down(Request::Client(client::Request::Close));
            assert!(machine.above.is_empty(), "local Close awaits real physical closure");
            machine.up(LowerEvent::Closed);
            assert_eq!(
                machine.above.pop(),
                Some(Event::Client(client::Event::Closed)),
                "local Close answers once after actual Closed"
            );
        } else {
            machine.up(LowerEvent::Closed);
            assert_eq!(
                machine.above.pop(),
                Some(Event::TransportClosed),
                "physical lifecycle wins before queued Close"
            );
            machine.down(Request::Client(client::Request::Close));
        }
        assert!(machine.above.is_empty() && machine.below.is_empty(), "no invented second Closed");
    }
}

#[test]
fn actual_grant_queued_across_local_close_is_released_without_fabricated_cancelled() {
    let mut machine = Machine::new();
    let lower = machine.started();
    machine.down(Request::Client(client::Request::Close));
    assert!(machine.above.is_empty(), "Close waits for actual admitted lower terminal");
    assert_eq!(
        machine.below.pop(),
        Some(LowerRequest::Stream(Down::Demand { read: Read::Nothing, room: 0 })),
        "close withdraws real read"
    );
    assert_eq!(
        machine.below.pop(),
        Some(LowerRequest::Output(OutputDown::Cancel { right: lower })),
        "self-cancel retains lower identity"
    );
    machine.up(LowerEvent::Output(OutputUp::Settled { right: lower, outcome: OutputOutcome::Granted }));
    assert_eq!(
        machine.below.pop(),
        Some(LowerRequest::Output(OutputDown::Release { right: lower })),
        "queued grant remains real winner"
    );
    assert!(
        machine.above.is_empty() && machine.below.is_empty(),
        "lower grant settles without inventing physical closure"
    );
    machine.up(LowerEvent::Closed);
    assert_eq!(
        machine.above.pop(),
        Some(Event::Client(client::Event::Closed)),
        "no abandoned lower debt at actual closure"
    );
    assert!(machine.above.is_empty() && machine.below.is_empty(), "no cancellation terminal fabricated");
}

#[test]
fn actual_self_cancel_drain_does_not_cancel_a_new_upper_right_until_physical_closed() {
    let mut machine = Machine::new();
    let lower = machine.started();
    // An actual cancellation of this TLS-owned flight is injected at the
    // lower cell tier, retaining precisely its issued identity and witness.
    super::cancel_lower(&mut machine.client, &mut machine.below);
    assert_eq!(
        machine.below.pop(),
        Some(LowerRequest::Output(OutputDown::Cancel { right: lower })),
        "real pending lower self-cancel"
    );
    wanted(&mut machine, Token::new(88));
    machine.up(LowerEvent::Output(OutputUp::Settled { right: lower, outcome: OutputOutcome::Granted }));
    // Grant wins across the cancel: the actual owed ClientHello is sent,
    // rather than falsely claiming cancellation. No new upper grant pre-Ready.
    match machine.below.pop().expect("flight consumes winning grant") {
        LowerRequest::Output(OutputDown::Send { right, .. }) => assert_eq!(right, lower, "same lower right"),
        LowerRequest::Output(OutputDown::Room { .. } | OutputDown::Cancel { .. } | OutputDown::Release { .. })
        | LowerRequest::Stream(_) => unreachable!("owed flight sent in queued grant"),
    }
    assert!(machine.above.is_empty() && machine.below.is_empty(), "new upper right remains Wanted");
    machine.up(LowerEvent::Closed);
    assert_eq!(
        machine.above.pop(),
        Some(Event::Output(OutputUp::Settled { right: Token::new(88), outcome: OutputOutcome::Cancelled })),
        "new debt cancelled by actual Closed"
    );
    assert_eq!(machine.above.pop(), Some(Event::TransportClosed), "one physical lifecycle");
}

#[test]
fn checked_lower_generator_emits_maximum_once_then_fails_before_new_room() {
    let mut machine = Machine::new();
    machine.client.next_lower = Some(u64::MAX);
    let lower = machine.started();
    assert_eq!(lower, Token::new(u64::MAX), "last distinct lower token may be used once");
    assert_eq!(machine.client.next_lower, None, "never wrap or reuse");
    machine.sent_hello(lower);
    wanted(&mut machine, Token::new(4));
    super::request_room(
        &mut machine.client,
        1,
        super::Purpose::Upper { right: Token::new(4), bytes: 1 },
        &mut machine.above,
        &mut machine.below,
    );
    assert_eq!(
        machine.above.pop(),
        Some(Event::Output(OutputUp::Settled {
            right: Token::new(4),
            outcome: OutputOutcome::Failed(skein_lib::stream::Fault::Other)
        })),
        "pending upper settles first"
    );
    assert_eq!(
        machine.above.pop(),
        Some(Event::Client(client::Event::Stream(Up::Failed(skein_lib::stream::Fault::Other)))),
        "stream failure next"
    );
    assert_eq!(
        machine.above.pop(),
        Some(Event::Client(client::Event::Failed(client::Error::Other))),
        "local checked exhaustion cause"
    );
    assert_eq!(
        machine.below.pop(),
        Some(LowerRequest::Stream(Down::Demand { read: Read::Nothing, room: 0 })),
        "actual independent ciphertext read is withdrawn on local failure"
    );
    assert!(machine.below.is_empty(), "no Room, encryption or transfer after exhaustion");
}

#[test]
fn stale_oversized_send_and_release_cannot_touch_another_upper_grant() {
    let mut machine = Machine::new();
    let lower = machine.started();
    machine.sent_hello(lower);
    machine.client.upper_output = UpperOutput::Granted { right: Token::new(5), bytes: 1 };
    machine.client.lower_output =
        LowerOutput::Granted { right: Token::new(6), bytes: super::room_for(1).expect("cap") };
    machine.down(Request::Output(OutputDown::Send { right: Token::new(4), bytes: Box::from(&b"oversized"[..]) }));
    assert!(machine.above.is_empty() && machine.below.is_empty(), "stale identity precedes payload size");
    machine.down(Request::Output(OutputDown::Release { right: Token::new(4) }));
    assert!(machine.above.is_empty() && machine.below.is_empty(), "another grant remains affine");
    assert_eq!(
        machine.client.upper_output,
        UpperOutput::Granted { right: Token::new(5), bytes: 1 },
        "actual upper right unchanged"
    );
}

#[test]
#[should_panic(expected = "matching native Send within grant")]
fn matching_oversized_send_asserts_before_encryption_or_lower_transfer() {
    let mut machine = Machine::new();
    let lower = machine.started();
    machine.sent_hello(lower);
    machine.client.upper_output = UpperOutput::Granted { right: Token::new(5), bytes: 1 };
    machine.down(Request::Output(OutputDown::Send { right: Token::new(5), bytes: Box::from(&b"large"[..]) }));
}

#[test]
#[should_panic(expected = "native Finish after Ready once")]
fn native_finish_before_ready_is_a_receiving_violation() {
    let mut machine = Machine::new();
    machine.started();
    machine.down(Request::Client(client::Request::Stream(Down::Finish)));
}

#[test]
#[should_panic(expected = "native plaintext Demand is read-only")]
fn native_classic_room_is_rejected_before_effects() {
    let mut machine = Machine::new();
    machine.started();
    machine.down(Request::Client(client::Request::Stream(Down::Demand { read: Read::Nothing, room: 1 })));
}

#[test]
#[should_panic(expected = "actual lower terminal precedes physical Closed")]
fn physical_closed_cannot_invent_a_missing_lower_output_terminal() {
    let mut machine = Machine::new();
    machine.started();
    machine.up(LowerEvent::Closed);
}

#[test]
fn duplicate_physical_closed_preserves_the_already_emitted_winner() {
    let mut machine = Machine::new();
    let lower = machine.started();
    machine.sent_hello(lower);
    machine.up(LowerEvent::Closed);
    assert_eq!(machine.above.pop(), Some(Event::TransportClosed), "actual lifecycle once");
    machine.up(LowerEvent::Closed);
    assert!(machine.above.is_empty() && machine.below.is_empty(), "duplicate lifecycle inert");
    match machine.client.state {
        State::TransportClosed => {}
        State::Fresh(_) | State::Open(_) | State::Failed | State::Closing | State::TransportClosing | State::Closed => {
            unreachable!("physical terminal persists")
        }
    }
}

/// Actual unbuffered rustls peer; no hand-written Ready or protocol records.
struct Peer {
    tls: rustls::server::UnbufferedServerConnection,
    input: crate::held::Held,
    output: crate::held::Held,
}

impl Peer {
    fn process(&mut self) {
        use rustls::unbuffered::{ConnectionState, UnbufferedStatus};
        for _ in 0..64_u32 {
            let UnbufferedStatus { discard, state } = self.tls.process_tls_records(self.input.filled_mut());
            let state = state.expect("real valid client fixture handshake");
            // rustls's foreign state is non-exhaustive. Narrow positive fixture
            // variants explicitly, without a blanket lint waiver or own-enum
            // catch-all; every other actual state is a fixture failure.
            let resting = if let ConnectionState::EncodeTlsData(mut encode) = state {
                let written = encode.encode(self.output.spare_mut()).expect("actual bounded server flight");
                self.output.wrote(written);
                false
            } else if let ConnectionState::TransmitTlsData(transmit) = state {
                transmit.done();
                false
            } else if let ConnectionState::BlockedHandshake
            | ConnectionState::WriteTraffic(_)
            | ConnectionState::Closed
            | ConnectionState::PeerClosed = state
            {
                true
            } else {
                unreachable!("actual rustls 0.23 positive handshake states; no peer application data")
            };
            self.input.discard(discard);
            if resting {
                return;
            }
        }
        unreachable!("bounded actual server processing rests");
    }
}

/// Every actual emitted lower handshake request updates the independent proxy.
fn take_handshake(
    machine: &mut Machine,
    peer: &mut Peer,
    read: &mut Option<Read>,
    room: &mut Option<(Token, u32)>,
    grant: &mut Option<(Token, u32)>,
) {
    for _ in 0..super::UP_MAX_OUT.below {
        let Some(request) = machine.below.pop() else { break };
        match request {
            LowerRequest::Stream(Down::Demand { read: requested, room: 0 }) => *read = Some(requested),
            LowerRequest::Output(OutputDown::Room { right, bytes }) => {
                assert!(room.replace((right, bytes)).is_none(), "one actual lower fixture demand");
            }
            LowerRequest::Output(OutputDown::Send { right, bytes }) => {
                // This output consumes the actual grant previously observed.
                assert!(room.is_none(), "grant observed before its Send");
                let (expected, cap) = grant.take().expect("one actual lower fixture grant");
                assert_eq!(right, expected, "actual lower token");
                assert!(bytes.len() <= usize::try_from(cap).expect("finite capacity"), "within real fixture grant");
                peer.input.append(&bytes).expect("bounded actual peer input");
                peer.process();
            }
            LowerRequest::Stream(Down::Demand { .. } | Down::Send(_) | Down::Finish)
            | LowerRequest::Output(OutputDown::Cancel { .. } | OutputDown::Release { .. }) => {
                unreachable!("positive handshake fixture requests")
            }
        }
    }
    assert!(machine.below.is_empty(), "all actual lower output observed");
}

/// Genuine Ready remains first; all three actual exhaustion faults follow.
fn assert_four_events(machine: &mut Machine, upper: Token) {
    assert_eq!(machine.above.len(), 4, "actual maximum upper co-emission, no suppressed Ready");
    match machine.above.pop().expect("genuine Ready first") {
        Event::Client(client::Event::Ready(agreed)) => {
            assert_eq!(agreed.version, client::Version::Tls12, "real TLS1.2 peer");
        }
        Event::Client(client::Event::Stream(_) | client::Event::Failed(_) | client::Event::Closed)
        | Event::Output(_)
        | Event::TransportClosing
        | Event::TransportClosed => unreachable!("genuine Ready precedes actual local exhaustion"),
    }
    assert_eq!(
        machine.above.pop(),
        Some(Event::Output(OutputUp::Settled {
            right: upper,
            outcome: OutputOutcome::Failed(skein_lib::stream::Fault::Other)
        })),
        "actual upper failure second"
    );
    assert_eq!(
        machine.above.pop(),
        Some(Event::Client(client::Event::Stream(Up::Failed(skein_lib::stream::Fault::Other)))),
        "plaintext failure third"
    );
    assert_eq!(
        machine.above.pop(),
        Some(Event::Client(client::Event::Failed(client::Error::Other))),
        "TLS failure fourth"
    );
    assert!(machine.above.is_empty() && machine.below.is_empty(), "no new lower right after exhaustion");
}

/// Real TLS1.2 handshake for both genuine Ready and exhaustion controls.
fn actual_ready(exhaustion: bool) -> Machine {
    let (config, tls) = crate::config::native_test_peer();
    let mut machine = Machine::new();
    machine.env.wall = Wall::from_nanos(1_893_456_000_u64.checked_mul(1_000_000_000).expect("fixture valid time"));
    machine.client = Client::new(
        &config,
        Name::new("skein.test").expect("actual certificate name"),
        &LIMITS,
        &lower(super::largest_room(&LIMITS).expect("cap")),
    )
    .expect("compatible");
    let capacity = client::MAX_HANDSHAKE.checked_add(client::MAX_RECORD).expect("actual maximum peer flight");
    let mut peer = Peer {
        tls,
        input: crate::held::Held::with_capacity(capacity),
        output: crate::held::Held::with_capacity(capacity),
    };
    machine.down(Request::Client(client::Request::Handshake));
    let mut read = None;
    let mut room = None;
    let mut grant: Option<(Token, u32)> = None;
    let mut admitted = false;
    let upper = Token::new(900);
    for _ in 0..512_u32 {
        take_handshake(&mut machine, &mut peer, &mut read, &mut room, &mut grant);
        assert!(machine.below.is_empty(), "all actual lower output observed");
        if !admitted {
            if exhaustion {
                wanted(&mut machine, upper);
            }
            admitted = true;
        }
        if exhaustion && !peer.tls.is_handshaking() {
            // TLS1.2 server has accepted actual client Finished and queued its
            // own Finished; client still owes a genuine Ready from that input.
            machine.client.next_lower = None;
        }
        if let Some((right, bytes)) = room.take() {
            assert!(grant.replace((right, bytes)).is_none(), "one lower fixture grant");
            machine.up(LowerEvent::Output(OutputUp::Settled { right, outcome: OutputOutcome::Granted }));
        } else if let Some(requested) = read {
            let count = match requested {
                Read::Fill(count) => count,
                Read::Nothing | Read::Line { .. } | Read::Scan { .. } => unreachable!("exact native record Fill"),
            };
            if peer.output.len() >= count {
                read = None;
                machine.up(LowerEvent::Stream(Up::Bytes(peer.output.take(count))));
            } else {
                unreachable!("actual server handshake supplies pending record");
            }
        } else {
            unreachable!("actual handshake retains lower work");
        }
        if !machine.above.is_empty() {
            if exhaustion {
                assert_four_events(&mut machine, upper);
            } else {
                assert_eq!(machine.above.len(), 1, "one genuine Ready without exhaustion");
                match machine.above.pop().expect("actual Ready") {
                    Event::Client(client::Event::Ready(agreed)) => assert_eq!(agreed.version, client::Version::Tls12),
                    Event::Client(client::Event::Stream(_) | client::Event::Failed(_) | client::Event::Closed)
                    | Event::Output(_)
                    | Event::TransportClosing
                    | Event::TransportClosed => unreachable!("actual Ready"),
                }
                assert!(machine.above.is_empty() && machine.below.is_empty());
            }
            return machine;
        }
    }
    unreachable!("actual bounded handshake reaches Ready/exhaustion");
}

#[test]
fn actual_lower_failed_settles_upper_and_read_then_waits_for_genuine_physical_close() {
    let mut machine = Machine::new();
    let right = machine.started();
    let upper = Token::new(100);
    wanted(&mut machine, upper);
    let fault = skein_lib::stream::Fault::Reset;
    machine.up(LowerEvent::Output(OutputUp::Settled { right, outcome: OutputOutcome::Failed(fault) }));
    assert_eq!(
        machine.above.pop(),
        Some(Event::Output(OutputUp::Settled { right: upper, outcome: OutputOutcome::Failed(fault) }))
    );
    assert_eq!(machine.above.pop(), Some(Event::Client(client::Event::Stream(Up::Failed(fault)))));
    assert_eq!(machine.above.pop(), Some(Event::Client(client::Event::Failed(client::Error::Stream(fault)))));
    assert_eq!(machine.below.pop(), Some(LowerRequest::Stream(Down::Demand { read: Read::Nothing, room: 0 })));
    assert!(machine.above.is_empty() && machine.below.is_empty());
    machine.up(LowerEvent::Stream(Up::Failed(fault)));
    assert!(
        machine.above.is_empty() && machine.below.is_empty(),
        "the lower's following classic Failed cannot duplicate real native settlement"
    );
    machine.down(Request::Client(client::Request::Close));
    assert!(machine.above.is_empty() && machine.below.is_empty(), "Close alone is not physical closure");
    machine.up(LowerEvent::Closed);
    assert_eq!(machine.above.pop(), Some(Event::Client(client::Event::Closed)));
    assert!(machine.above.is_empty() && machine.below.is_empty());
}

#[test]
fn ready_wanted_and_checked_exhaustion_reach_three_down_upper_events() {
    let mut machine = actual_ready(false);
    let upper = Token::new(101);
    machine.client.next_lower = None;
    machine.down(Request::Output(OutputDown::Room { right: upper, bytes: 1 }));
    assert_eq!(machine.above.len(), 3, "actual maximum down upper co-emission");
    assert_eq!(
        machine.above.pop(),
        Some(Event::Output(OutputUp::Settled {
            right: upper,
            outcome: OutputOutcome::Failed(skein_lib::stream::Fault::Other)
        }))
    );
    assert_eq!(
        machine.above.pop(),
        Some(Event::Client(client::Event::Stream(Up::Failed(skein_lib::stream::Fault::Other))))
    );
    assert_eq!(machine.above.pop(), Some(Event::Client(client::Event::Failed(client::Error::Other))));
    assert!(machine.above.is_empty() && machine.below.is_empty(), "exhaustion before lower right or read effects");
}

#[test]
fn genuine_ready_and_checked_exhaustion_emit_all_four_upper_events_in_order() {
    actual_ready(true);
}

#[test]
#[should_panic(expected = "ciphertext Fill is answered exactly")]
fn active_ciphertext_fill_still_rejects_wrong_size_before_processing() {
    let mut machine = Machine::new();
    let right = machine.started();
    machine.sent_hello(right);
    machine.up(LowerEvent::Stream(Up::Bytes(Box::from([0; 4]))));
}
