//! The response body (http.md, 3.3): read below by its framing, and
//! handed to the side above as the stream it demands.
//!
//! The client is the side below of the body's stream, so it keeps the
//! stream's contract (lib.md, 7) and holds its carry-over: the bytes read
//! below that no demand above has taken yet, in an intake of
//! [`Limits::read`]. It reads below only what a demand above still needs,
//! by the demand's own shape (a fill, or a scan to the same delimiter)
//! and never past the body's framing, so a scan for a line passes through
//! as a scan, and a slow event stream is read as it comes. A demand met by
//! one delivery whole, with nothing held, goes up as it came; others are
//! met from the intake, across the chunks of a chunked body too. The
//! framing itself (a chunk's size line, the line ending after its data,
//! the trailer section) is read only when a demand needs what lies past
//! it.

use alloc::boxed::Box;

use skein_lib::stream::{Delimiter, Read, Up};
use skein_lib::{Intake, Queue, bytes};

use super::head::content;
use super::{Download, Error, Event, Face, Limits, Rest};
use crate::header::{is_field_byte, trim};

/// Whether the body is over for the exchange, once the side above's demand
/// was met as far as it can be.
pub(super) enum Pumped {
    /// The side above reads on, or will.
    Open,
    /// Its end went up, or the rest was discarded: the exchange is done.
    Over,
}

/// Meets the side above's demand from the intake, if it can; tells it the
/// end, if nothing below can meet it; drops what a discard leaves.
pub(super) fn pump(download: &mut Download, intake: &mut Intake, above: &mut Queue<Event>) -> Pumped {
    match download.face {
        Face::Idle => Pumped::Open,
        Face::Demand(read) => {
            if let Some(bytes) = intake.meet(read) {
                above.push(Event::Body(Up::Bytes(bytes)));
                download.face = Face::Idle;
                return Pumped::Open;
            }
            if !is_over(download.rest) {
                return Pumped::Open;
            }
            // What is held cannot meet it, and nothing more comes: the end,
            // which leaves the rest of the intake unseen (lib.md, 7).
            above.push(Event::Body(Up::End));
            Pumped::Over
        }
        Face::Discarding => {
            drain(intake);
            if is_over(download.rest) { Pumped::Over } else { Pumped::Open }
        }
    }
}

/// Empties the intake: what the side above left unread when the body ends.
pub(super) fn drain(intake: &mut Intake) {
    if !intake.is_empty() {
        drop(intake.meet(Read::Fill(intake.len())));
    }
}

fn is_over(rest: Rest) -> bool {
    match rest {
        Rest::Over => true,
        Rest::Length(_) | Rest::Chunk(_) | Rest::ChunkEnd | Rest::ChunkSize | Rest::Trailer(_) | Rest::UntilEnd => {
            false
        }
    }
}

/// What to read below for the body, if anything: only for a demand above
/// that the intake does not meet, or to discard.
pub(super) fn read(download: &Download, intake: &Intake, limits: &Limits) -> Option<Read> {
    match download.face {
        Face::Idle => return None,
        Face::Demand(_) | Face::Discarding => {}
    }
    match download.rest {
        Rest::Over => None,
        // The line ending after a chunk's data: an LF, or a CR and an LF.
        Rest::ChunkEnd => Some(Read::Scan { until: Delimiter::LF, max: 2 }),
        Rest::ChunkSize => Some(Read::Scan { until: Delimiter::LF, max: limits.head }),
        Rest::Trailer(left) => Some(Read::Scan { until: Delimiter::LF, max: left }),
        Rest::Length(left) | Rest::Chunk(left) => Some(piece(download.face, intake, limits, left)),
        Rest::UntilEnd => Some(piece(download.face, intake, limits, u64::MAX)),
    }
}

/// A read of at most `left` of the body's own bytes: what the demand above
/// still needs beyond what the intake holds, in its shape, or a fill to
/// drop while discarding.
fn piece(face: Face, intake: &Intake, limits: &Limits, left: u64) -> Read {
    let held = intake.len();
    match face {
        Face::Discarding => Read::Fill(at_most(limits.read, left)),
        Face::Demand(Read::Fill(n)) => {
            Read::Fill(at_most(n.checked_sub(held).expect("a fill the intake has not met"), left))
        }
        Face::Demand(Read::Scan { until, max }) => {
            let wanted = at_most(max.checked_sub(held).expect("a scan the intake has not met"), left);
            // A scan holds its delimiter (lib.md, 7); a shorter read is a
            // fill, and the intake finds a delimiter split across the two.
            if index(wanted) >= until.as_bytes().len() { Read::Scan { until, max: wanted } } else { Read::Fill(wanted) }
        }
        Face::Demand(Read::Nothing) | Face::Idle => unreachable!("a withdrawn demand leaves the body idle"),
    }
}

/// Bytes delivered for the body's read below, with what they mean by the
/// framing: the body's own, handed up or held; or a line of its framing.
pub(super) fn delivered(
    download: &mut Download,
    intake: &mut Intake,
    limits: &Limits,
    bytes: Box<[u8]>,
    above: &mut Queue<Event>,
) -> Result<(), Error> {
    download.rest = match download.rest {
        Rest::Length(left) => match took(&mut download.face, intake, bytes, left, above) {
            0 => Rest::Over,
            left => Rest::Length(left),
        },
        Rest::Chunk(left) => match took(&mut download.face, intake, bytes, left, above) {
            0 => Rest::ChunkEnd,
            left => Rest::Chunk(left),
        },
        Rest::UntilEnd => {
            let _: u64 = took(&mut download.face, intake, bytes, u64::MAX, above);
            Rest::UntilEnd
        }
        Rest::ChunkEnd => match &*bytes {
            b"\n" | b"\r\n" => Rest::ChunkSize,
            _ => return Err(Error::Chunk),
        },
        Rest::ChunkSize => match chunk_size(&bytes) {
            Some(0) => Rest::Trailer(limits.head),
            Some(size) => Rest::Chunk(size),
            None => return Err(Error::ChunkSize),
        },
        Rest::Trailer(left) => {
            let left = left.checked_sub(len(&bytes)).expect("a scan within what is left of the trailer section");
            match content(&bytes) {
                // No LF within what was left of the section.
                None => return Err(Error::Trailer),
                Some([]) => Rest::Over,
                // A field the client does not keep, and the blank line still
                // due.
                Some(_) if left == 0 => return Err(Error::Trailer),
                Some(_) => Rest::Trailer(left),
            }
        }
        Rest::Over => unreachable!("nothing is read below once the body is over"),
    };
    Ok(())
}

/// The body's own bytes, read for `face`: handed up as they came when they
/// meet its demand whole and nothing is held, held in the intake
/// otherwise, or dropped while discarding. What is left of the body after
/// them.
fn took(face: &mut Face, intake: &mut Intake, bytes: Box<[u8]>, left: u64, above: &mut Queue<Event>) -> u64 {
    let left = left.checked_sub(u64::from(len(&bytes))).expect("a read within what is left of the body");
    match *face {
        Face::Demand(read) if intake.is_empty() && answers(read, &bytes) => {
            above.push(Event::Body(Up::Bytes(bytes)));
            *face = Face::Idle;
        }
        // Held for the demand, or for the next if the side above withdrew
        // the one they were read for.
        Face::Demand(_) | Face::Idle => intake.append(&bytes).expect("a read within the intake's room"),
        Face::Discarding => drop(bytes),
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
    }
}

/// A chunk's size line (RFC 9112, 7.1): hexadecimal digits, then, past
/// whitespace, nothing or an extension, which the client ignores. `None`
/// for a line that is not one, has no LF within [`Limits::head`], or whose
/// size does not fit a `u64`.
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
