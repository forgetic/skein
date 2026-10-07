//! Heap bound while a full input body and a queued output frame coexist.
use skein_channel::{
    Control, Direction, Event, Kind, Limits, Lower, LowerEvent, MAX_DOWN, MAX_UP, Machine, Request, Role, Schema, Term,
    Version, control_frame, frame_writer, worst_case,
};
use skein_heap::{Counting, Meter};
use skein_lib::{List, Queue, Token, stream};

#[global_allocator]
static HEAP: Counting = Counting;

fn schema() -> Schema {
    let mut kinds = List::with_capacity(2);
    kinds.push(Kind { kind: 0x0100, direction: Direction::FromInitiator, largest: 120 }).expect("kind");
    kinds.push(Kind { kind: 0x0101, direction: Direction::FromResponder, largest: 120 }).expect("kind");
    let mut versions = List::with_capacity(1);
    versions.push(Version { version: 1, kinds }).expect("version");
    Schema { magic: *b"heap", versions }
}

fn limits() -> Limits {
    Limits { chunk: 8, credential: 16, skip: 32, output_bytes: 128, output_frames: 2, kinds: 2 }
}

#[expect(clippy::wildcard_enum_match_arm, reason = "the memory world rejects unexpected boundary records")]
#[expect(clippy::too_many_arguments, reason = "the small memory world keeps the machine's three queues explicit")]
fn feed(
    machine: &mut Machine,
    above: &mut Queue<Event>,
    below: &mut Queue<Lower>,
    pending: &mut Option<usize>,
    bytes: &[u8],
    drain_output: bool,
    ready: &mut bool,
    body_seen: &mut bool,
) {
    let mut offset = 0;
    for _ in 0..200 {
        machine.poll(above, below);
        while let Some(lower) = below.pop() {
            match lower {
                Lower::Read(stream::Down::Demand { read: stream::Read::Fill(count), room: 0 }) => {
                    *pending = Some(usize::try_from(count).expect("bounded demand"));
                }
                Lower::Write(stream::OutputDown::Room { right, .. }) => {
                    if drain_output {
                        machine.up(
                            LowerEvent::Write(stream::OutputUp::Settled {
                                right,
                                outcome: stream::OutputOutcome::Granted,
                            }),
                            above,
                            below,
                        );
                    }
                }
                Lower::Write(stream::OutputDown::Send { .. }) => {}
                lower => panic!("unexpected lower record: {lower:?}"),
            }
        }
        if let Some(count) = *pending
            && bytes.len().saturating_sub(offset) >= count
        {
            let end = offset.checked_add(count).expect("input length");
            machine.up(LowerEvent::Read(stream::Up::Bytes(Box::from(&bytes[offset..end]))), above, below);
            *pending = None;
            offset = end;
        }
        while let Some(event) = above.pop() {
            match event {
                Event::Opening { .. } => machine.down(Request::Accept { version: 1 }, above, below),
                Event::Ready { .. } => *ready = true,
                Event::Body { kind: 0x0100, body } => {
                    assert_eq!(body.len(), 120);
                    *body_seen = true;
                }
                Event::Drained | Event::Sent { .. } => {}
                event => panic!("unexpected owner event: {event:?}"),
            }
        }
        if offset == bytes.len()
            && (!drain_output || machine.waiting().write == skein_channel::WriteWait::Nothing)
            && (pending.is_some() || *ready || *body_seen)
        {
            return;
        }
    }
    panic!("frame did not settle");
}

#[test]
fn largest_input_body_with_full_output_frame_stays_within_worst_case() {
    let source = schema();
    let limits = limits();
    let bound = worst_case(&source, &limits).expect("checked bound");
    let open = control_frame(
        &Control::Open { magic: *b"heap", lowest: 1, highest: 1, features: 0, credential: Box::from([]) },
        &limits,
    )
    .expect("open");
    let mut entries = List::with_capacity(1);
    entries.push(Term { kind: 0x0101, largest: 120 }).expect("term");
    let terms = control_frame(&Control::Terms { entries }, &limits).expect("terms");
    let mut input = frame_writer(0x0100, 120).expect("input");
    input.put(&[7; 120]).expect("body");
    let input = input.finish().expect("frame");

    let meter = Meter::new();
    meter.start();
    let mut machine = Machine::new(source.clone(), Role::Responder, limits).expect("machine");
    let mut above = Queue::<Event>::with_capacity(MAX_UP);
    let mut below = Queue::<Lower>::with_capacity(MAX_DOWN);
    let mut pending = None;
    let mut ready = false;
    let mut body_seen = false;
    feed(&mut machine, &mut above, &mut below, &mut pending, open.bytes(), true, &mut ready, &mut body_seen);
    feed(&mut machine, &mut above, &mut below, &mut pending, terms.bytes(), true, &mut ready, &mut body_seen);
    assert!(ready);
    let mut output = frame_writer(0x0101, 120).expect("output");
    output.put(&[9; 120]).expect("body");
    machine.down(
        Request::Send { token: Token::new(1), frame: output.finish().expect("frame") },
        &mut above,
        &mut below,
    );
    machine.down(Request::Read, &mut above, &mut below);
    feed(&mut machine, &mut above, &mut below, &mut pending, input.bytes(), false, &mut ready, &mut body_seen);
    assert!(body_seen);
    let measured = meter.end();
    assert!(measured.peak() >= 240, "both largest bodies were held");
    assert!(measured.peak() <= bound, "peak {} exceeds bound {bound}", measured.peak());
    drop(machine);
    drop(above);
    drop(below);
    assert_eq!(meter.held(), 0);
}
