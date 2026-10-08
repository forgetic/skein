//! A seeded in-memory plaintext stream for connection worlds
//! (llm-connection.md, section 8). It keeps demands, granted room, buffered
//! peer bytes and the bytes the client sent; it knows no connection state.

use std::collections::VecDeque;

use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Intake, Rng};

/// The stream below a plaintext connection, driven by the world's peer.
#[derive(Debug)]
pub struct Wire {
    pub received: Vec<u8>,
    pub eof: bool,
    pub ended: bool,
    pub finished: bool,
    pub trace: Vec<String>,
    input: Intake,
    pending: VecDeque<u8>,
    demand: Option<(Read, u32)>,
    granted: Option<u32>,
    rng: Rng,
}

impl Wire {
    /// A bounded receive intake and seeded byte delivery schedule.
    #[must_use]
    pub fn new(seed: u64) -> Wire {
        Wire {
            received: Vec::new(),
            eof: false,
            ended: false,
            finished: false,
            trace: Vec::new(),
            input: Intake::with_capacity(32768),
            pending: VecDeque::new(),
            demand: None,
            granted: None,
            rng: Rng::new(seed),
        }
    }

    /// Put the peer's bytes on the wire for delivery in seeded fragments.
    pub fn write(&mut self, bytes: &[u8]) {
        self.pending.extend(bytes);
    }

    /// Check one client demand, send, withdrawal or finish.
    ///
    /// # Panics
    /// On a broken stream contract.
    pub fn take(&mut self, down: Down) {
        self.trace.push(format!("down {down:?}"));
        match down {
            Down::Demand { read: Read::Nothing, room: 0 } => {
                assert!(self.demand.take().is_some(), "only an outstanding demand is withdrawn");
            }
            Down::Demand { read, room } => {
                assert!(self.demand.is_none(), "one demand at a time");
                if room > 0 {
                    assert!(!self.finished, "no room after Finish");
                    self.granted = None;
                }
                self.demand = Some((read, room));
            }
            Down::Send(bytes) => {
                let granted = self.granted.take().expect("Send has granted room");
                assert!(bytes.len() <= usize::try_from(granted).expect("u32 fits"));
                self.received.extend_from_slice(&bytes);
            }
            Down::Finish => {
                assert!(!self.finished, "one Finish");
                self.finished = true;
            }
        }
    }

    /// Deliver bytes and answer a demand, with seeded room and byte delays.
    ///
    /// # Panics
    /// If the scripted peer exceeds the bounded response intake.
    pub fn answer(&mut self) -> Option<Up> {
        if !self.pending.is_empty() && self.rng.chance(700) {
            let count = usize::try_from(self.rng.between(1, 31)).expect("small fragment").min(self.pending.len());
            let bytes: Vec<u8> = self.pending.drain(..count).collect();
            self.trace.push(format!("fragment {bytes:?}"));
            self.input.append(&bytes).expect("bounded scripted response intake");
        }
        let (read, room) = self.demand?;
        let answer = match self.input.meet(read) {
            Some(bytes) => {
                self.demand = None;
                Some(Up::Bytes(bytes))
            }
            None if room > 0 && self.rng.chance(700) => {
                self.demand = None;
                self.granted = Some(room);
                Some(Up::Room)
            }
            None if read != Read::Nothing && self.eof && self.pending.is_empty() && !self.ended => {
                self.ended = true;
                Some(Up::End)
            }
            None => {
                if room > 0 {
                    self.trace.push("room delayed".to_owned());
                }
                None
            }
        };
        if let Some(answer) = &answer {
            self.trace.push(format!("up {answer:?}"));
        }
        answer
    }
}
