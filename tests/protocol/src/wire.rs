//! The bytes between the two ends (testing-strategy.md, 2.5): an in-memory
//! stream in each direction, its bytes carried in pieces cut at random and
//! joined in the receiving end's intake, and the side below each end's
//! stack of machines, which keeps the contract of a stream (lib.md, 7) as
//! io keeps it for a socket (io.md, 3.3).
//!
//! - **Reads** are met from the intake exactly, as soon as they can be;
//!   `End` comes once the other end will send nothing more and nothing
//!   held can meet the read, or, with none outstanding, nothing is held.
//! - **Room** is granted, late, while the output has it: one `Send` a
//!   grant, within it, made before room is demanded again, and a demand for
//!   no room leaves a grant as it is.
//! - **A read that crosses the end** stays outstanding until it is
//!   withdrawn, or room answers it if it asked for some, as TLS keeps it:
//!   a stack that demands again over it breaks the contract.
//! - **A close by the end's owner** sends what it queued, then ends the
//!   other end's reading, and drops what comes from it for a while, its
//!   linger; what comes after that resets the other end's stream, as on a
//!   socket whose peer closed.

use std::collections::VecDeque;

use skein_lib::Intake;
use skein_lib::stream::{Down, Fault, Read, Up};

/// The side below one end's stack.
#[derive(Debug)]
#[expect(clippy::struct_excessive_bools, reason = "what the side below knows of the stream, a flag each")]
pub struct Bottom {
    /// What arrived from the other end and the stack has not demanded.
    intake: Intake,
    /// What the stack sent, not yet carried to the other end.
    output: VecDeque<u8>,
    output_cap: u32,
    /// The stack's demand outstanding: its read, and its room.
    demand: Option<(Read, u32)>,
    /// The room the stack holds: what the last `Room` granted, less what it
    /// sent since.
    granted: u32,
    /// The `Send`s since the last `Room`.
    sends: u32,
    /// Whether the stack holds a grant it has not sent within.
    held: bool,
    /// Whether the read outstanding crossed the end.
    crossed: bool,
    /// When the end's owner closed it, if it did.
    closed: Option<u64>,
    /// The other end will send nothing more, and all it sent has arrived.
    peer_over: bool,
    told_end: bool,
    failed: Option<Fault>,
    told_failed: bool,
    /// A demand the stack withdrew, which an answer on its way may still
    /// meet.
    withdrawn: Option<(Read, u32)>,
    /// What the stack sent, in all.
    pub sent: u64,
    /// What went up to the stack, in all.
    pub delivered: u64,
}

impl Bottom {
    #[must_use]
    pub fn new(intake: u32, output: u32) -> Bottom {
        Bottom {
            intake: Intake::with_capacity(intake),
            output: VecDeque::new(),
            output_cap: output,
            demand: None,
            granted: 0,
            sends: 0,
            held: false,
            crossed: false,
            closed: None,
            peer_over: false,
            told_end: false,
            failed: None,
            told_failed: false,
            withdrawn: None,
            sent: 0,
            delivered: 0,
        }
    }

    /// Whether the end's owner closed it.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.is_some()
    }

    /// Whether the stream failed.
    #[must_use]
    pub fn failed(&self) -> Option<Fault> {
        self.failed
    }

    /// Whether the stack has a demand outstanding.
    #[must_use]
    pub fn demanding(&self) -> bool {
        self.demand.is_some()
    }

    /// Whether everything the stack sent was carried to the other end.
    #[must_use]
    pub fn flushed(&self) -> bool {
        self.output.is_empty()
    }

    /// A request from the stack, checked against the contract of a stream
    /// as io checks it; `stopping` says whether the call that made it may
    /// stop the stack's reading.
    pub fn receive(&mut self, request: Down, stopping: bool) {
        assert!(self.closed.is_none(), "nothing goes below once its owner closed the stream: {request:?}");
        match request {
            Down::Demand { read: Read::Nothing, room: 0 } => {
                assert!(
                    stopping || self.crossed,
                    "a withdrawal only as the stack stops reading, or of a read past the end"
                );
                self.withdrawn = Some(self.demand.take().expect("only a demand outstanding is withdrawn"));
                self.crossed = false;
            }
            Down::Demand { read, room } => {
                assert!(self.demand.is_none(), "one demand at a time: {read:?} over {:?}", self.demand);
                let wanted = match read {
                    Read::Nothing => 0,
                    Read::Fill(n) => n,
                    Read::Scan { max, .. } | Read::Line { max } => max,
                };
                assert!(wanted <= self.intake.capacity(), "no read past the intake's cap: {wanted}");
                assert!(room <= self.output_cap, "no room past the output's cap: {room}");
                assert!(room == 0 || !self.held, "room grants one Send, made before room is demanded again");
                if read != Read::Nothing {
                    assert!(!self.told_end, "nothing read once the end was told");
                }
                assert!(!self.told_failed, "nothing demanded once the failure was told");
                self.demand = Some((read, room));
            }
            Down::Send(bytes) => {
                assert!(!self.told_failed, "nothing sent once the failure was told");
                let len = u32::try_from(bytes.len()).expect("fits a u32");
                self.granted = self.granted.checked_sub(len).expect("a Send within the room granted");
                self.sends += 1;
                self.held = false;
                assert!(self.sends <= 1, "one Send a grant");
                self.sent += u64::from(len);
                self.output.extend(bytes.iter());
                assert!(self.output.len() <= self.output_cap as usize, "no more queued than the output's cap");
            }
            Down::Finish => panic!("an HTTP machine never finishes the stream below: its owner closes it"),
        }
    }

    /// What the side below tells the stack now, if anything: its failure;
    /// the answer to its demand, bytes as soon as the intake meets the
    /// read, or room, if `grant` draws it and the output has it; or the
    /// end.
    pub fn answer(&mut self, grant: bool) -> Option<Up> {
        if self.closed.is_some() {
            return None;
        }
        if let Some(fault) = self.failed {
            if self.told_failed {
                return None;
            }
            self.told_failed = true;
            self.demand = None;
            self.crossed = false;
            return Some(Up::Failed(fault));
        }
        if let Some((read, room)) = self.demand {
            if !self.told_end
                && let Some(bytes) = self.intake.meet(read)
            {
                self.demand = None;
                self.delivered += bytes.len() as u64;
                return Some(Up::Bytes(bytes));
            }
            let free = self.output_cap as usize - self.output.len();
            if room > 0 && grant && free >= room as usize {
                self.demand = None;
                self.crossed = false;
                self.granted = room;
                self.sends = 0;
                self.held = true;
                return Some(Up::Room);
            }
            if read != Read::Nothing && self.peer_over && !self.told_end {
                // Nothing more comes, and nothing held meets the read: the
                // read crosses the end and stays outstanding, until it is
                // withdrawn or room comes for it.
                self.told_end = true;
                self.crossed = true;
                return Some(Up::End);
            }
            return None;
        }
        if self.peer_over && !self.told_end && self.intake.is_empty() {
            self.told_end = true;
            return Some(Up::End);
        }
        None
    }

    /// An answer on its way for the demand the stack withdrew: bytes the
    /// intake meets it with, or room, which the stack drops.
    pub fn late(&mut self) -> Option<Up> {
        let (read, room) = self.withdrawn.take()?;
        if let Some(bytes) = self.intake.meet(read) {
            return Some(Up::Bytes(bytes));
        }
        if room > 0 {
            self.held = true;
            Some(Up::Room)
        } else {
            None
        }
    }

    /// The end's owner closes the stream at `now`: what it queued still
    /// goes; what comes is dropped.
    pub fn close(&mut self, now: u64) {
        assert!(
            self.demand.is_none() || self.told_failed || self.crossed,
            "a stack's close withdraws what it demanded below, but a read past the end, before its owner closes the \
             stream: {:?}",
            self.demand
        );
        self.closed = Some(now);
        let held = self.intake.len();
        if held > 0 {
            drop(self.intake.meet(Read::Fill(held)));
        }
    }

    /// The stream breaks, as a reset does.
    pub fn fail(&mut self, fault: Fault) {
        if self.failed.is_none() && self.closed.is_none() {
            self.failed = Some(fault);
            self.output.clear();
        }
    }
}

/// Carries at most `piece` bytes of what `from` sent to `to` at `now`, as
/// far as `to`'s intake has room; or drops them if `to`'s owner closed it,
/// and, past its `linger`, resets `from`'s stream with them; and tells `to`
/// once `from` will send nothing more. Whether it reset.
pub fn carry(from: &mut Bottom, to: &mut Bottom, piece: usize, now: u64, linger: u64) -> bool {
    let room = if to.closed.is_some() { usize::MAX } else { to.intake.room() as usize };
    let len = piece.min(room).min(from.output.len());
    let mut reset = false;
    if len > 0 && to.failed.is_none() {
        let bytes: Vec<u8> = from.output.drain(..len).collect();
        match to.closed {
            None => to.intake.append(&bytes).expect("within the room"),
            Some(at) if now >= at + linger && from.closed.is_none() && from.failed.is_none() => {
                from.fail(Fault::Reset);
                reset = true;
            }
            Some(_) => {}
        }
    }
    if (from.closed.is_some() || from.failed.is_some()) && from.output.is_empty() {
        to.peer_over = true;
    }
    reset
}
