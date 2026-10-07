//! A plain queue model of the journal's promises (lib.md, section 11).
//!
//! It stores writes and outputs in standard library queues, computes
//! admission from their lengths, and answers commits in send order. Each
//! operation is compared with the public journal interface.

use std::collections::VecDeque;

use skein_lib::{Journal, JournalLimits, JournalRoom, Queue, Released, Rng};

struct Model {
    limits: JournalLimits,
    outstanding: VecDeque<u64>,
    unsent: VecDeque<(u64, Vec<u32>)>,
    sent: VecDeque<u64>,
    held: VecDeque<(u64, u32)>,
    now: VecDeque<u32>,
    last: u64,
    durable: u64,
    stopped: bool,
}

impl Model {
    fn new(limits: JournalLimits) -> Model {
        Model {
            limits,
            outstanding: VecDeque::new(),
            unsent: VecDeque::new(),
            sent: VecDeque::new(),
            held: VecDeque::new(),
            now: VecDeque::new(),
            last: 0,
            durable: 0,
            stopped: false,
        }
    }

    fn takes(&self, room: JournalRoom) -> bool {
        !self.stopped
            && room.writes <= self.limits.writes
            && usize::try_from(room.held).expect("u32 fits") + self.held.len()
                <= usize::try_from(self.limits.held).expect("u32 fits")
            && (room.writes == 0 || self.outstanding.len() < usize::try_from(self.limits.commits).expect("u32 fits"))
            && (room.writes == 0 || self.last < u64::MAX)
    }

    fn accept(&mut self, writes: Vec<u32>, held: Vec<u32>) {
        if !writes.is_empty() {
            self.last += 1;
            self.outstanding.push_back(self.last);
            self.unsent.push_back((self.last, writes));
        }
        for output in held {
            self.held.push_back((self.last, output));
        }
    }

    fn now(&mut self, output: u32) -> Result<(), u32> {
        if self.stopped || self.now.len() >= usize::try_from(self.limits.now).expect("u32 fits") {
            return Err(output);
        }
        self.now.push_back(output);
        Ok(())
    }

    fn committed(&mut self, number: u64) {
        while self.sent.front().is_some_and(|&front| front <= number) {
            self.sent.pop_front();
        }
        while self.outstanding.front().is_some_and(|&front| front <= number) {
            self.outstanding.pop_front();
        }
        self.durable = number;
    }

    fn release(&mut self, capacity: u32) -> (Released, Vec<u32>) {
        if self.stopped {
            return (Released::Stopped, Vec::new());
        }
        let mut outputs = Vec::new();
        let n = self.limits.release.min(capacity);
        for _ in 0..n {
            if let Some(output) = self.now.pop_front() {
                outputs.push(output);
            } else if self.held.front().is_some_and(|&(after, _)| after <= self.durable) {
                let (_, output) = self.held.pop_front().expect("ready front exists");
                outputs.push(output);
            } else {
                break;
            }
        }
        let status = if outputs.is_empty() { Released::None } else { Released::Some };
        (status, outputs)
    }
}

fn below(rng: &mut Rng, n: u32) -> u32 {
    u32::try_from(rng.below(u64::from(n))).expect("below u32")
}

/// Runs random decision, send, answer, failure, door, and release sequences.
pub fn check(seed: u64, rounds: u32) {
    let mut rng = Rng::new(seed);
    let limits = JournalLimits { commits: 3, writes: 2, held: 5, now: 3, release: 2 };
    for round in 0..rounds {
        let mut journal = Journal::<u32, u32>::new(&limits);
        let mut model = Model::new(limits);
        for step in 0..60_u32 {
            let room = JournalRoom { writes: below(&mut rng, 4), held: below(&mut rng, 4) };
            assert_eq!(journal.takes(&room), model.takes(room), "round {round}, step {step}: admission");
            match below(&mut rng, 10) {
                0..=4 => {
                    let Some(mut decision) = journal.decision(&room) else {
                        assert!(!model.takes(room), "round {round}, step {step}: refusal");
                        continue;
                    };
                    let mut writes = Vec::new();
                    let mut held = Vec::new();
                    for _ in 0..below(&mut rng, room.writes + 1) {
                        let write = rng.next_u64().to_le_bytes();
                        let value = u32::from_le_bytes([write[0], write[1], write[2], write[3]]);
                        assert_eq!(decision.write(value), Ok(()));
                        writes.push(value);
                    }
                    for _ in 0..below(&mut rng, room.held + 1) {
                        let value = rng.next_u64().to_le_bytes();
                        let output = u32::from_le_bytes([value[0], value[1], value[2], value[3]]);
                        assert_eq!(decision.hold(output), Ok(()));
                        held.push(output);
                    }
                    journal.accept(decision);
                    model.accept(writes, held);
                }
                5 => {
                    let output = below(&mut rng, 1000);
                    assert_eq!(journal.now(output), model.now(output), "round {round}, step {step}: door");
                }
                6 => {
                    let actual = journal.commit();
                    let expected = model.unsent.pop_front();
                    match (actual, expected) {
                        (Some(mut commit), Some((number, writes))) => {
                            assert_eq!(commit.number, number, "round {round}, step {step}: number");
                            for write in writes {
                                assert_eq!(commit.writes.pop(), Some(write));
                            }
                            assert_eq!(commit.writes.pop(), None);
                            model.sent.push_back(number);
                        }
                        (None, None) => {}
                        _ => panic!("round {round}, step {step}: commit mismatch"),
                    }
                }
                7 => {
                    if !model.sent.is_empty() {
                        let number = if rng.chance(500) {
                            *model.sent.back().expect("sent is nonempty")
                        } else {
                            *model.sent.front().expect("sent is nonempty")
                        };
                        journal.committed(number);
                        model.committed(number);
                    }
                }
                8 => {
                    if !model.sent.is_empty() && rng.chance(30) {
                        let number = *model.sent.front().expect("sent is nonempty");
                        journal.failed(number);
                        model.stopped = true;
                    }
                }
                9 => {
                    let capacity = below(&mut rng, 4);
                    let mut out = Queue::with_capacity(capacity);
                    let actual = journal.release(&mut out);
                    let (expected, values) = model.release(capacity);
                    assert_eq!(actual, expected, "round {round}, step {step}: release status");
                    for value in values {
                        assert_eq!(out.pop(), Some(value), "round {round}, step {step}: release order");
                    }
                    assert_eq!(out.pop(), None);
                }
                _ => unreachable!("draw below ten"),
            }
            assert_eq!(journal.stopped(), model.stopped, "round {round}, step {step}: stopped");
            if model.stopped {
                let mut out = Queue::with_capacity(3);
                assert_eq!(journal.release(&mut out), Released::Stopped, "round {round}, step {step}: after failure");
                assert_eq!(out.pop(), None, "round {round}, step {step}: nothing after failure");
                break;
            }
        }
    }
}
