//! A scripted peer for application protocol worlds (channel.md, section 11).
//! It performs the generic opening, then sends scripted frames and records
//! frames received from a channel. A caller supplies byte cuts and checks the
//! final observation list.

use std::collections::VecDeque;

use skein_channel::{
    Control, Direction, Frame, Header, Limits, Role, Schema, Term, control_frame, decode_control, parse_header,
};
use skein_lib::List;

/// One frame received from the channel after the opening.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observed {
    pub kind: u16,
    pub body: Box<[u8]>,
}

/// Why a scripted peer could not proceed or its expectations did not hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// The schema, version, or configured bound is unusable.
    Limits,
    /// The incoming frame is malformed or exceeds its bound.
    Framing,
    /// An opening message arrived out of order.
    Sequence,
    /// A received frame differs from the script's expectation.
    Mismatch,
    /// The script or expectation list has not finished.
    Incomplete,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    WaitOpen,
    WaitAccept,
    WaitTerms,
    Ready,
    Refused,
}

/// A peer that supplies a generic opening and a deterministic frame script.
pub struct ScriptedPeer {
    schema: Schema,
    role: Role,
    limits: Limits,
    chosen: u16,
    state: State,
    credential: Box<[u8]>,
    bytes: Vec<u8>,
    header: Option<Header>,
    opening_output: VecDeque<Box<[u8]>>,
    script: VecDeque<Frame>,
    expected: VecDeque<Observed>,
    observed: Vec<Observed>,
}

impl ScriptedPeer {
    /// Builds a peer; the initiator's credential is sent in Open.
    ///
    /// # Errors
    /// Returns `Limits` if the schema, selected version, or credential bound is invalid.
    pub fn new(schema: Schema, role: Role, limits: Limits, chosen: u16, credential: Box<[u8]>) -> Result<Self, Error> {
        schema.check(&limits).map_err(|_| Error::Limits)?;
        if schema.version(chosen).is_none()
            || credential.len() > usize::try_from(limits.credential).map_err(|_| Error::Limits)?
        {
            return Err(Error::Limits);
        }
        let state = match role {
            Role::Initiator => State::WaitAccept,
            Role::Responder => State::WaitOpen,
        };
        let mut peer = Self {
            schema,
            role,
            limits,
            chosen,
            state,
            credential,
            bytes: Vec::new(),
            header: None,
            opening_output: VecDeque::new(),
            script: VecDeque::new(),
            expected: VecDeque::new(),
            observed: Vec::new(),
        };
        if role == Role::Initiator {
            let (lowest, highest) = peer.schema.range().ok_or(Error::Limits)?;
            let frame = control_frame(
                &Control::Open {
                    magic: peer.schema.magic,
                    lowest,
                    highest,
                    features: 0,
                    credential: peer.credential.clone(),
                },
                &peer.limits,
            )
            .map_err(|_| Error::Limits)?;
            peer.opening_output.push_back(Box::from(frame.bytes()));
        }
        Ok(peer)
    }

    /// Adds one application or control frame to the outgoing script.
    pub fn play(&mut self, frame: Frame) {
        self.script.push_back(frame);
    }

    /// Adds one expected incoming frame in wire order.
    pub fn expect(&mut self, kind: u16, body: Box<[u8]>) {
        self.expected.push_back(Observed { kind, body });
    }

    /// Takes the next wire frame, allowing the caller to cut its bytes.
    #[must_use]
    pub fn pop_output(&mut self) -> Option<Box<[u8]>> {
        if let Some(frame) = self.opening_output.pop_front() {
            return Some(frame);
        }
        if self.state != State::Ready {
            return None;
        }
        self.script.pop_front().map(|frame| Box::from(frame.bytes()))
    }

    /// Feeds any byte cut, checking complete frames as they arrive.
    ///
    /// # Errors
    /// Returns a framing, sequence, or expectation error for an invalid peer frame.
    pub fn feed(&mut self, cut: &[u8]) -> Result<(), Error> {
        for byte in cut {
            self.bytes.push(*byte);
            if self.bytes.len() == 8 {
                let header = parse_header(&self.bytes).map_err(|_| Error::Framing)?;
                self.check_header(header)?;
                self.header = Some(header);
            }
            if let Some(header) = self.header {
                let body_len = usize::try_from(header.body_len).map_err(|_| Error::Framing)?;
                let frame_len = body_len.checked_add(8).ok_or(Error::Framing)?;
                if self.bytes.len() == frame_len {
                    let body = self.bytes[8..].to_vec().into_boxed_slice();
                    self.bytes.clear();
                    self.header = None;
                    self.handle(header.kind, body)?;
                }
            }
        }
        Ok(())
    }

    /// Whether both peers have exchanged terms.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.state == State::Ready
    }

    /// The opaque credential received in Open, or supplied by the caller.
    #[must_use]
    pub fn credential(&self) -> &[u8] {
        &self.credential
    }

    /// The frames observed after the opening, in wire order.
    #[must_use]
    pub fn observed(&self) -> &[Observed] {
        &self.observed
    }

    /// Checks that the opening, script and expected observations completed.
    ///
    /// # Errors
    /// Returns `Incomplete` while bytes, outgoing frames, or expectations remain.
    pub fn verify(&self) -> Result<(), Error> {
        if self.state != State::Ready || !self.bytes.is_empty() || !self.script.is_empty() || !self.expected.is_empty()
        {
            return Err(Error::Incomplete);
        }
        Ok(())
    }

    fn check_header(&self, header: Header) -> Result<(), Error> {
        let largest = match header.kind {
            1 => self.limits.credential.checked_add(14),
            2 => Some(4),
            3 => Some(262),
            4 => self.limits.kinds.checked_mul(6).and_then(|n| n.checked_add(4)),
            5 => Some(0),
            6 => Some(2),
            0 | 7..=255 => return Err(Error::Framing),
            kind => Some(
                self.schema
                    .version(self.chosen)
                    .and_then(|version| version.kind(kind))
                    .map_or(self.limits.skip, |known| known.largest),
            ),
        }
        .ok_or(Error::Framing)?;
        if header.body_len > largest {
            return Err(Error::Framing);
        }
        Ok(())
    }

    fn handle(&mut self, kind: u16, body: Box<[u8]>) -> Result<(), Error> {
        if kind < 0x0100 {
            let control = decode_control(kind, &body, &self.limits).map_err(|_| Error::Framing)?;
            match (self.state, control) {
                (State::WaitOpen, Control::Open { magic, lowest, highest, features: 0, credential }) => {
                    if magic != self.schema.magic || lowest > self.chosen || highest < self.chosen {
                        return Err(Error::Mismatch);
                    }
                    self.credential = credential;
                    self.queue_control(&Control::Accept { version: self.chosen, features: 0 })?;
                    self.send_terms()?;
                    self.state = State::WaitTerms;
                }
                (State::WaitAccept, Control::Accept { version, features: 0 }) if version == self.chosen => {
                    self.state = State::WaitTerms;
                }
                (State::WaitAccept | State::WaitTerms, Control::Refuse { .. }) => {
                    self.state = State::Refused;
                }
                (State::WaitTerms, Control::Terms { entries }) => {
                    self.check_terms(&entries)?;
                    if self.role == Role::Initiator {
                        self.send_terms()?;
                    }
                    self.state = State::Ready;
                }
                (State::Ready, Control::Open { .. } | Control::Accept { .. } | Control::Terms { .. }) => {
                    return Err(Error::Sequence);
                }
                (State::Ready, _) => self.record(kind, body)?,
                _ => return Err(Error::Sequence),
            }
        } else if self.state == State::Ready {
            self.record(kind, body)?;
        } else {
            return Err(Error::Sequence);
        }
        Ok(())
    }

    fn queue_control(&mut self, control: &Control) -> Result<(), Error> {
        let frame = control_frame(control, &self.limits).map_err(|_| Error::Limits)?;
        self.opening_output.push_back(Box::from(frame.bytes()));
        Ok(())
    }

    fn send_terms(&mut self) -> Result<(), Error> {
        let version = self.schema.version(self.chosen).ok_or(Error::Limits)?;
        let mut entries = List::with_capacity(self.limits.kinds);
        for kind in &version.kinds {
            let receives = matches!(
                (self.role, kind.direction),
                (Role::Initiator, Direction::FromResponder) | (Role::Responder, Direction::FromInitiator)
            );
            if receives {
                entries.push(Term { kind: kind.kind, largest: kind.largest }).map_err(|_| Error::Limits)?;
            }
        }
        self.queue_control(&Control::Terms { entries })
    }

    fn check_terms(&self, entries: &List<Term>) -> Result<(), Error> {
        let version = self.schema.version(self.chosen).ok_or(Error::Limits)?;
        for (index, entry) in entries.iter().enumerate() {
            let kind = version.kind(entry.kind).ok_or(Error::Mismatch)?;
            let sends = matches!(
                (self.role, kind.direction),
                (Role::Initiator, Direction::FromInitiator) | (Role::Responder, Direction::FromResponder)
            );
            if !sends || entry.largest > kind.largest || entries.iter().take(index).any(|old| old.kind == entry.kind) {
                return Err(Error::Mismatch);
            }
        }
        Ok(())
    }

    fn record(&mut self, kind: u16, body: Box<[u8]>) -> Result<(), Error> {
        let observed = Observed { kind, body };
        if let Some(expected) = self.expected.pop_front()
            && observed != expected
        {
            return Err(Error::Mismatch);
        }
        self.observed.push(observed);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
