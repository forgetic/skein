//! The server-sent events writer (http.md, 6): each field framed, data
//! split at every line ending, comments, every refusal, an event sent in
//! pieces within the room granted, the stream ending and failing, a close
//! in each state, and every event read back by the reader as it was
//! written.

#![expect(clippy::disallowed_types, reason = "a test collects what it writes in a Vec")]

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, Queue};

use super::{boxed, env, stream};
use crate::sse::writer::{self, Event, Limits, Outgoing, Refusal, Request, Waiting, Writer};
use crate::sse::{self, Message};

const LIMITS: Limits = Limits { event: 128, chunk: 16 };

/// A writer and its two queues, with room for what one call emits.
struct Machine {
    writer: Writer,
    env: Env<Limits>,
    above: Queue<Event>,
    below: Queue<Down>,
}

impl Machine {
    fn new(limits: Limits) -> Machine {
        Machine {
            writer: Writer::new(&limits),
            env: env(limits),
            above: Queue::with_capacity(writer::UP_MAX_OUT.above.max(writer::DOWN_MAX_OUT.above)),
            below: Queue::with_capacity(writer::UP_MAX_OUT.below.max(writer::DOWN_MAX_OUT.below)),
        }
    }

    fn down(&mut self, rq: Request) -> (Vec<Event>, Vec<Down>) {
        writer::down(&mut self.writer, &self.env, rq, &mut self.above, &mut self.below);
        assert!(self.above.len() <= writer::DOWN_MAX_OUT.above, "MAX_OUT above");
        assert!(self.below.len() <= writer::DOWN_MAX_OUT.below, "MAX_OUT below");
        self.take()
    }

    fn up(&mut self, ev: Up) -> (Vec<Event>, Vec<Down>) {
        writer::up(&mut self.writer, &self.env, ev, &mut self.above, &mut self.below);
        assert!(self.above.len() <= writer::UP_MAX_OUT.above, "MAX_OUT above");
        assert!(self.below.len() <= writer::UP_MAX_OUT.below, "MAX_OUT below");
        self.take()
    }

    fn take(&mut self) -> (Vec<Event>, Vec<Down>) {
        let mut events = Vec::new();
        while let Some(event) = self.above.pop() {
            events.push(event);
        }
        let mut requests = Vec::new();
        while let Some(request) = self.below.pop() {
            requests.push(request);
        }
        (events, requests)
    }

    /// Writes `rq`, an event or a comment, granting every room asked for:
    /// the pieces sent, which must end with `Sent`.
    fn written(&mut self, rq: Request) -> Vec<Box<[u8]>> {
        let (mut events, mut requests) = self.down(rq);
        let mut pieces = Vec::new();
        for _ in 0..1000_u32 {
            let mut room = None;
            for request in requests.drain(..) {
                match request {
                    Down::Demand { read: Read::Nothing, room: wanted } => {
                        assert!(wanted > 0 && wanted <= self.env.limits.chunk, "room within a chunk: {wanted}");
                        room = Some(wanted);
                    }
                    Down::Send(piece) => pieces.push(piece),
                    other @ (Down::Demand { .. } | Down::Finish) => panic!("the writer sent {other:?}"),
                }
            }
            match events.pop() {
                Some(Event::Sent) => {
                    assert!(room.is_none() && events.is_empty(), "nothing more once sent");
                    return pieces;
                }
                Some(other) => panic!("{other:?}"),
                None => {}
            }
            let Some(room) = room else { panic!("the writer waits for room it did not ask for") };
            (events, requests) = self.up(Up::Room);
            let mut sends = 0_u32;
            for request in &requests {
                if let Down::Send(piece) = request {
                    sends = sends.saturating_add(1);
                    assert!(piece.len() <= usize::try_from(room).unwrap(), "a Send within the room granted");
                }
            }
            assert_eq!(sends, 1_u32, "one Send a grant");
        }
        panic!("an event goes down in a few pieces");
    }

    /// Writes `rq` and joins its pieces.
    fn framed(&mut self, rq: Request) -> Vec<u8> {
        let mut framed = Vec::new();
        for piece in self.written(rq) {
            framed.extend_from_slice(&piece);
        }
        framed
    }
}

fn outgoing(name: &[u8], data: &[u8], id: Option<Box<[u8]>>, retry: Option<u64>) -> Outgoing {
    Outgoing { name: boxed(name), data: boxed(data), id, retry }
}

fn framed(name: &[u8], data: &[u8], id: Option<Box<[u8]>>, retry: Option<u64>) -> Vec<u8> {
    Machine::new(Limits { event: 1024, chunk: 7 }).framed(Request::Event(outgoing(name, data, id, retry)))
}

fn refused(rq: Request, limits: Limits) -> Refusal {
    let mut machine = Machine::new(limits);
    let (events, requests) = machine.down(rq);
    assert!(requests.is_empty(), "a refusal writes nothing");
    assert_eq!(machine.writer.waiting(), Waiting::Above, "the writer is as it was");
    match events.as_slice() {
        [Event::Refused(refusal)] => *refusal,
        other => panic!("refused, not {other:?}"),
    }
}

#[test]
fn an_event_is_its_fields_in_order_and_a_blank_line() {
    assert_eq!(
        framed(b"delta", b"{\"a\":1}", Some(boxed(b"7")), Some(1500)),
        b"event: delta\nid: 7\nretry: 1500\ndata: {\"a\":1}\n\n"
    );
    assert_eq!(framed(b"", b"x", None, None), b"data: x\n\n", "no type: the reader's `message`");
    assert_eq!(framed(b"", b"x", Some(boxed(b"")), None), b"id: \ndata: x\n\n", "an empty id resets the reader's");
    assert_eq!(framed(b"", b"", None, None), b"data: \n\n", "empty data is one empty line");
    assert_eq!(framed(b"", b" lead", None, None), b"data:  lead\n\n", "the reader drops one space, the writer's");
}

#[test]
fn data_is_split_at_every_line_ending() {
    assert_eq!(framed(b"", b"a\nb\r\nc\rd", None, None), b"data: a\ndata: b\ndata: c\ndata: d\n\n");
    assert_eq!(framed(b"", b"a\n", None, None), b"data: a\ndata: \n\n", "an ending last is an empty line after it");
    assert_eq!(framed(b"", b"\r\n\r\n", None, None), b"data: \ndata: \ndata: \n\n");
    assert_eq!(framed(b"", b"\n\r", None, None), b"data: \ndata: \ndata: \n\n", "an LF then a CR: two endings");
}

#[test]
fn a_comment_is_a_block_of_its_own() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.framed(Request::Comment(boxed(b"ping"))), b": ping\n\n");
    assert_eq!(machine.framed(Request::Comment(boxed(b""))), b":\n\n");
}

#[test]
fn what_the_side_above_gets_wrong_is_refused_in_order() {
    for name in [&b"a\nb"[..], b"a\r"] {
        assert_eq!(refused(Request::Event(outgoing(name, b"x", Some(boxed(b"\0")), None)), LIMITS), Refusal::Name);
    }
    for id in [&b"a\nb"[..], b"\r", b"a\0"] {
        assert_eq!(refused(Request::Event(outgoing(b"t", b"x", Some(boxed(id)), None)), LIMITS), Refusal::Id);
    }
    for text in [&b"a\nb"[..], b"\r"] {
        assert_eq!(refused(Request::Comment(boxed(text)), LIMITS), Refusal::Comment);
    }
    // At the limit, and one past it: `data: ` and the data and an LF, and
    // the blank line.
    let limits = Limits { event: 32, chunk: 8 };
    let at = [b'x'; 24];
    assert_eq!(Machine::new(limits).framed(Request::Event(outgoing(b"", &at, None, None))).len(), 32);
    assert_eq!(refused(Request::Event(outgoing(b"", &[b'x'; 25], None, None)), limits), Refusal::TooLong);
    assert_eq!(refused(Request::Comment(boxed(&[b'c'; 30])), limits), Refusal::TooLong);
}

#[test]
fn an_event_goes_down_in_pieces_within_the_room_granted() {
    let mut machine = Machine::new(Limits { event: 1024, chunk: 7 });
    let pieces = machine.written(Request::Event(outgoing(b"", b"0123456789", None, None)));
    let mut lengths = Vec::new();
    for piece in &pieces {
        lengths.push(piece.len());
    }
    assert_eq!(lengths, [7, 7, 4], "18 bytes, a chunk at a time");
    assert_eq!(machine.writer.waiting(), Waiting::Above);
    let pieces = machine.written(Request::Comment(boxed(b"hi")));
    assert_eq!(pieces, [boxed(b": hi\n\n")], "one piece, the frame itself");
}

#[test]
fn finish_ends_the_body_and_a_failure_before_it_leaves_nothing_to_end() {
    let mut machine = Machine::new(LIMITS);
    machine.framed(Request::Comment(boxed(b"")));
    let (events, requests) = machine.down(Request::Finish);
    assert!(events.is_empty());
    assert_eq!(requests, [Down::Finish]);
    assert_eq!(machine.writer.waiting(), Waiting::Close);
    let mut machine = Machine::new(LIMITS);
    machine.up(Up::Failed(Fault::Reset));
    let (events, requests) = machine.down(Request::Finish);
    assert!(events.is_empty() && requests.is_empty());
}

#[test]
fn the_stream_failing_fails_the_event_being_written_or_the_next() {
    let fault = Fault::Reset;
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Event(outgoing(b"", b"x", None, None)));
    let (events, requests) = machine.up(Up::Failed(fault));
    assert_eq!(events, [Event::Failed(fault)]);
    assert!(requests.is_empty(), "nothing follows a failure: nothing to withdraw");
    assert_eq!(machine.writer.waiting(), Waiting::Close);
    let mut machine = Machine::new(LIMITS);
    let (events, _) = machine.up(Up::Failed(fault));
    assert!(events.is_empty(), "with nothing being written, the next event hears it");
    let (events, requests) = machine.down(Request::Event(outgoing(b"", b"x", None, None)));
    assert_eq!(events, [Event::Failed(fault)]);
    assert!(requests.is_empty());
    // A refusal still comes first: it concerns what the side above wrote.
    let mut machine = Machine::new(LIMITS);
    machine.up(Up::Failed(fault));
    assert_eq!(machine.down(Request::Comment(boxed(b"\n"))).0, [Event::Refused(Refusal::Comment)]);
}

#[test]
fn the_stream_s_end_changes_nothing_for_a_writer() {
    let mut machine = Machine::new(LIMITS);
    let (_, requests) = machine.down(Request::Event(outgoing(b"", b"x", None, None)));
    assert_eq!(requests.len(), 1);
    let (events, requests) = machine.up(Up::End);
    assert!(events.is_empty() && requests.is_empty(), "room may still come");
    let (events, requests) = machine.up(Up::Room);
    assert_eq!(events, [Event::Sent]);
    assert_eq!(requests, [Down::Send(boxed(b"data: x\n\n"))]);
}

#[test]
fn a_close_in_each_state_withdraws_what_was_demanded_and_answers_closed() {
    let closed = |machine: &mut Machine, withdraws: bool| {
        let (events, requests) = machine.down(Request::Close);
        assert_eq!(events, [Event::Closed]);
        let withdrawal = [Down::Demand { read: Read::Nothing, room: 0 }];
        assert_eq!(requests.as_slice(), if withdraws { &withdrawal[..] } else { &[][..] });
        assert_eq!(machine.writer.waiting(), Waiting::Nothing);
        let (events, requests) = machine.up(Up::Room);
        assert!(events.is_empty() && requests.is_empty(), "room on its way is dropped");
    };
    closed(&mut Machine::new(LIMITS), false);
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Event(outgoing(b"", b"x", None, None)));
    closed(&mut machine, true);
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Finish);
    closed(&mut machine, false);
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Event(outgoing(b"", b"x", None, None)));
    machine.up(Up::Failed(Fault::Other));
    closed(&mut machine, false);
}

#[test]
fn what_the_writer_waits_for_follows_its_state() {
    let mut machine = Machine::new(LIMITS);
    assert_eq!(machine.writer.waiting(), Waiting::Above);
    machine.down(Request::Event(outgoing(b"", b"x", None, None)));
    assert_eq!(machine.writer.waiting(), Waiting::Room);
    machine.up(Up::Room);
    assert_eq!(machine.writer.waiting(), Waiting::Above);
    machine.down(Request::Finish);
    assert_eq!(machine.writer.waiting(), Waiting::Close);
}

#[test]
fn every_event_written_is_read_back_as_it_was_written() {
    let reader = sse::Limits { line: 256, event: 1024, field: 32, chunk: 8 };
    let events = [
        outgoing(b"message_start", b"{\"type\":\"message_start\"}", Some(boxed(b"1")), Some(3000)),
        outgoing(b"", b"line one\nline two", None, None),
        outgoing(b"delta", b"", None, None),
        outgoing(b"x", b"a\r\nb\rc\n", Some(boxed(b"")), None),
        outgoing(b"", b"[DONE]", None, None),
    ];
    let mut machine = Machine::new(Limits { event: 1024, chunk: 5 });
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&machine.framed(Request::Comment(boxed(b"open"))));
    for event in &events {
        bytes.extend_from_slice(&machine.framed(Request::Event(event.clone())));
    }
    bytes.extend_from_slice(&machine.framed(Request::Comment(boxed(b""))));
    let read = stream(&bytes, reader);
    let expected = [
        sse::Event::Message(Message {
            name: boxed(b"message_start"),
            data: boxed(b"{\"type\":\"message_start\"}"),
            id: boxed(b"1"),
        }),
        sse::Event::Message(Message { name: boxed(b"message"), data: boxed(b"line one\nline two"), id: boxed(b"1") }),
        sse::Event::Message(Message { name: boxed(b"delta"), data: boxed(b""), id: boxed(b"1") }),
        sse::Event::Message(Message { name: boxed(b"x"), data: boxed(b"a\nb\nc\n"), id: boxed(b"") }),
        sse::Event::Message(Message { name: boxed(b"message"), data: boxed(b"[DONE]"), id: boxed(b"") }),
        sse::Event::Ended,
    ];
    assert_eq!(read, expected);
}

#[test]
#[should_panic(expected = "an event while another is being written")]
fn one_event_at_a_time() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Event(outgoing(b"", b"x", None, None)));
    machine.down(Request::Event(outgoing(b"", b"y", None, None)));
}

#[test]
#[should_panic(expected = "an event after Finish")]
fn no_event_after_finish() {
    let mut machine = Machine::new(LIMITS);
    machine.down(Request::Finish);
    machine.down(Request::Event(outgoing(b"", b"x", None, None)));
}
