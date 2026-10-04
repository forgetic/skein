//! A message body coming up (http.md, 3.3 and 5.3): read below by its
//! framing, and handed to the side above as the stream it demands. The
//! client reads a response's body this way, and the server a request's.
//!
//! The machine is the side below of the body's stream, so it keeps the
//! stream's contract (lib.md, 7) and holds its carry-over: the bytes read
//! below that no demand above has taken yet, in an intake of the machine's
//! `Limits::read`. It reads below only what a demand above still needs, by
//! the demand's own shape (a fill, or a scan to the same delimiter) and
//! never past the body's framing, so a scan for a line passes through as a
//! scan, and a slow event stream is read as it comes. A demand met by one
//! delivery whole, with nothing held, goes up as it came; others are met
//! from the intake, across the chunks of a chunked body too, a delimiter
//! split between two chunks completed a byte at a time. The framing itself
//! (a chunk's size line, the line ending after its data, the trailer
//! section) is read only when a demand needs what lies past it.

use alloc::boxed::Box;

use skein_lib::stream::{Delimiter, Read, Up};
use skein_lib::{Intake, bytes};

use crate::header::{content, is_field_byte, trim};

/// A body coming up: what is left of it below, and the side above's side
/// of its stream.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct Incoming {
    pub(crate) rest: Rest,
    pub(crate) face: Face,
}

/// What is left of a body below, by its framing.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Rest {
    /// By length: this many bytes, at least one.
    Length(u64),
    /// Chunked: this many bytes of a chunk's data, at least one.
    Chunk(u64),
    /// Chunked: the line ending after a chunk's data.
    ChunkEnd,
    /// Chunked: the next chunk's size line.
    ChunkSize,
    /// Chunked: the trailer section after the last chunk, this many bytes
    /// of it at most, its blank line included.
    Trailer(u32),
    /// To the end of the stream: a response's only.
    UntilEnd,
    /// Nothing: the body is all read below.
    Over,
}

impl Rest {
    pub(crate) fn is_over(self) -> bool {
        match self {
            Rest::Over => true,
            Rest::Length(_) | Rest::Chunk(_) | Rest::ChunkEnd | Rest::ChunkSize | Rest::Trailer(_) | Rest::UntilEnd => {
                false
            }
        }
    }
}

/// The side above's side of the body's stream.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Face {
    /// No demand outstanding.
    Idle,
    /// A demand outstanding, which the intake does not meet yet.
    Demand(Read),
    /// The side above withdrew its demand: it reads no more (lib.md, 7),
    /// and discards the rest or closes the machine next.
    Withdrawn,
    /// The side above gave up the rest: read and dropped.
    Discarding,
}

/// Whether the body is over for the side above, once its demand was met
/// as far as it can be.
pub(crate) enum Pumped {
    /// The side above reads on, or will.
    Open,
    /// Its end went up, or the rest was discarded.
    Over,
}

/// What is wrong with a chunked body's framing.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) enum Bad {
    /// A chunk's size line is not one: not hexadecimal, past a `u64`, or
    /// longer than the head's limit.
    ChunkSize,
    /// A chunk's data is not followed by a line ending.
    Chunk,
    /// The trailer section is longer than the head's limit.
    Trailer,
}

/// Meets the side above's demand from the intake, if it can, with what goes
/// up: the bytes, or the end, if nothing below can meet it; drops what a
/// discard leaves.
pub(crate) fn pump(incoming: &mut Incoming, intake: &mut Intake) -> (Option<Up>, Pumped) {
    match incoming.face {
        // Until the side above discards the rest, or closes the machine.
        Face::Idle | Face::Withdrawn => (None, Pumped::Open),
        Face::Demand(read) => {
            if let Some(bytes) = intake.meet(read) {
                incoming.face = Face::Idle;
                return (Some(Up::Bytes(bytes)), Pumped::Open);
            }
            if !incoming.rest.is_over() {
                return (None, Pumped::Open);
            }
            // What is held cannot meet it, and nothing more comes: the end,
            // which leaves the rest of the intake unseen (lib.md, 7).
            (Some(Up::End), Pumped::Over)
        }
        Face::Discarding => {
            drain(intake);
            if incoming.rest.is_over() { (None, Pumped::Over) } else { (None, Pumped::Open) }
        }
    }
}

/// Empties the intake: what the side above left unread when the body ends.
pub(crate) fn drain(intake: &mut Intake) {
    if !intake.is_empty() {
        drop(intake.meet(Read::Fill(intake.len())));
    }
}

/// What to read below for the body, if anything: only for a demand above
/// that the intake does not meet, or to discard. `head` is the longest a
/// chunk's size line or the trailer section may be; `most`, the intake's
/// cap.
pub(crate) fn read(incoming: &Incoming, intake: &Intake, head: u32, most: u32) -> Option<Read> {
    match incoming.face {
        Face::Idle | Face::Withdrawn => return None,
        Face::Demand(_) | Face::Discarding => {}
    }
    match incoming.rest {
        Rest::Over => None,
        // The line ending after a chunk's data: an LF, or a CR and an LF.
        Rest::ChunkEnd => Some(Read::Scan { until: Delimiter::LF, max: 2 }),
        Rest::ChunkSize => Some(Read::Scan { until: Delimiter::LF, max: head }),
        Rest::Trailer(left) => Some(Read::Scan { until: Delimiter::LF, max: left }),
        Rest::Length(left) | Rest::Chunk(left) => Some(piece(incoming.face, intake, most, left)),
        Rest::UntilEnd => Some(piece(incoming.face, intake, most, u64::MAX)),
    }
}

/// A read of at most `left` of the body's own bytes: what the demand above
/// still needs beyond what the intake holds, in its shape, or a fill to
/// drop while discarding.
fn piece(face: Face, intake: &Intake, most: u32, left: u64) -> Read {
    let held = intake.len();
    match face {
        Face::Discarding => Read::Fill(at_most(most, left)),
        Face::Demand(Read::Fill(n)) => {
            Read::Fill(at_most(n.checked_sub(held).expect("a fill the intake has not met"), left))
        }
        Face::Demand(Read::Scan { until, max }) => {
            let wanted = at_most(max.checked_sub(held).expect("a scan the intake has not met"), left);
            // What is held ends partway through the delimiter, which the next
            // bytes may complete: a scan below would not see it, and would
            // read past it. A byte at a time until it is whole, or is not.
            if intake.ends_partway(until) {
                return Read::Fill(1);
            }
            // A scan holds its delimiter (lib.md, 7); a shorter read is a
            // fill, which cannot hold one.
            if index(wanted) >= until.as_bytes().len() { Read::Scan { until, max: wanted } } else { Read::Fill(wanted) }
        }
        // A line end is one byte: what is held has none, or the intake would
        // have met the scan, so the rest of the scan is a scan again.
        Face::Demand(Read::Line { max }) => {
            Read::Line { max: at_most(max.checked_sub(held).expect("a scan the intake has not met"), left) }
        }
        Face::Demand(Read::Nothing) | Face::Idle | Face::Withdrawn => {
            unreachable!("the body is read for a demand outstanding, or to discard")
        }
    }
}

/// Bytes delivered for the body's read below, with what they mean by the
/// framing: the body's own, handed up (returned, to go up as they came) or
/// held; or a line of its framing. `head` is the longest a chunk's size
/// line or the trailer section may be.
pub(crate) fn delivered(
    incoming: &mut Incoming,
    intake: &mut Intake,
    head: u32,
    bytes: Box<[u8]>,
) -> Result<Option<Box<[u8]>>, Bad> {
    let mut up = None;
    incoming.rest = match incoming.rest {
        Rest::Length(left) => match took(&mut incoming.face, intake, bytes, left, &mut up) {
            0 => Rest::Over,
            left => Rest::Length(left),
        },
        Rest::Chunk(left) => match took(&mut incoming.face, intake, bytes, left, &mut up) {
            0 => Rest::ChunkEnd,
            left => Rest::Chunk(left),
        },
        Rest::UntilEnd => {
            let _: u64 = took(&mut incoming.face, intake, bytes, u64::MAX, &mut up);
            Rest::UntilEnd
        }
        Rest::ChunkEnd => match &*bytes {
            b"\n" | b"\r\n" => Rest::ChunkSize,
            _ => return Err(Bad::Chunk),
        },
        Rest::ChunkSize => match chunk_size(&bytes) {
            Some(0) => Rest::Trailer(head),
            Some(size) => Rest::Chunk(size),
            None => return Err(Bad::ChunkSize),
        },
        Rest::Trailer(left) => {
            let left = left.checked_sub(len(&bytes)).expect("a scan within what is left of the trailer section");
            match content(&bytes) {
                // No LF within what was left of the section.
                None => return Err(Bad::Trailer),
                Some([]) => Rest::Over,
                // A field the machine does not keep, and the blank line
                // still due.
                Some(_) if left == 0 => return Err(Bad::Trailer),
                Some(_) => Rest::Trailer(left),
            }
        }
        Rest::Over => unreachable!("nothing is read below once the body is over"),
    };
    Ok(up)
}

/// The body's own bytes, read for `face`: handed up as they came (into
/// `up`) when they meet its demand whole and nothing is held, held in the
/// intake otherwise, or dropped while discarding. What is left of the body
/// after them.
fn took(face: &mut Face, intake: &mut Intake, bytes: Box<[u8]>, left: u64, up: &mut Option<Box<[u8]>>) -> u64 {
    let left = left.checked_sub(u64::from(len(&bytes))).expect("a read within what is left of the body");
    match *face {
        Face::Demand(read) if intake.is_empty() && answers(read, &bytes) => {
            *up = Some(bytes);
            *face = Face::Idle;
        }
        Face::Demand(_) => intake.append(&bytes).expect("a read within the intake's room"),
        // Read for a demand withdrawn, or to discard: the side above reads
        // no more.
        Face::Withdrawn | Face::Discarding => drop(bytes),
        Face::Idle => unreachable!("the body is read for a demand outstanding, or to discard"),
    }
    left
}

/// Whether `bytes` are exactly what `read` asks for, as lib's intake would
/// meet it from them (lib.md, 7).
fn answers(read: Read, bytes: &[u8]) -> bool {
    match read {
        Read::Nothing => false,
        Read::Fill(n) => index(n) == bytes.len(),
        Read::Scan { until, max } => match bytes::find(bytes, until.as_bytes()) {
            Some(at) => at.saturating_add(until.as_bytes().len()) == bytes.len(),
            None => bytes.len() == index(max),
        },
        Read::Line { max } => match bytes::line_end(bytes) {
            Some(at) => at.saturating_add(1) == bytes.len(),
            None => bytes.len() == index(max),
        },
    }
}

/// A chunk's size line (RFC 9112, 7.1): hexadecimal digits, then, past
/// whitespace, nothing or an extension, which is ignored. `None` for a
/// line that is not one, has no LF within the head's limit, or whose size
/// does not fit a `u64`.
fn chunk_size(bytes: &[u8]) -> Option<u64> {
    let line = content(bytes)?;
    let mut size = 0_u64;
    let mut digits = 0_usize;
    for &byte in line {
        let Some(value) = char::from(byte).to_digit(16) else { break };
        size = size.checked_mul(16)?.checked_add(u64::from(value))?;
        digits = digits.saturating_add(1);
    }
    if digits == 0 {
        return None;
    }
    let rest = trim(line.get(digits..)?);
    match rest.first() {
        None => Some(size),
        Some(b';') => {
            for &byte in rest {
                if !is_field_byte(byte) {
                    return None;
                }
            }
            Some(size)
        }
        Some(_) => None,
    }
}

/// The smaller of `n` and `left`.
fn at_most(n: u32, left: u64) -> u32 {
    match u32::try_from(left) {
        Ok(left) => n.min(left),
        Err(_) => n,
    }
}

fn len(bytes: &[u8]) -> u32 {
    u32::try_from(bytes.len()).expect("a delivery's length fits a u32")
}

fn index(n: u32) -> usize {
    usize::try_from(n).expect("a u32 fits a usize")
}
