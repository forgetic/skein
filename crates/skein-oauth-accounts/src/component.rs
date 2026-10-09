//! Account records, holds and owner lifecycle (oauth.md, sections 6.3 and 6.6).

use crate::{Account, Asked, Ends, Event, Failure, Limits, Refusal, Request, worst_case};
use skein_lib::{Duration, Env, List, Queue, Rng, Time};
use skein_oauth::SavedToken;

/// The most outputs one entrance emits; the owner reserves this room first.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MaxOut {
    pub above: u32,
}

/// Room reserved before one owner request.
pub const MAX_OUT_DOWN: MaxOut = MaxOut { above: 2 };

/// Room reserved before firing one due account.
pub const MAX_OUT_FIRE: MaxOut = MaxOut { above: 2 };

/// Why the component cannot run its owner's configured accounts.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Unusable {
    /// More configured accounts than the account bound.
    Accounts { bound: u32 },
    /// A required bound is zero, or the worst case overflows.
    Limits,
}

#[derive(PartialEq, Eq, Hash)]
pub(crate) enum State {
    Empty,
    Record { record: SavedToken, expiry: Time, lead: Time, announced: bool, held: bool, rejected: bool },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Lifecycle {
    Live,
    Closed,
}

/// The owner's bounded accounts and holds, ending with one Closed.
#[derive(PartialEq, Eq, Hash)]
#[expect(missing_debug_implementations, reason = "credential values must never occur in traces")]
pub struct Component {
    accounts: List<State>,
    lifecycle: Lifecycle,
    random: Rng,
}

impl Component {
    /// Validates configuration before the loop; seed belongs to this component.
    pub fn new(accounts: List<Account>, limits: &Limits, seed: u64) -> Result<Component, Unusable> {
        if accounts.len() > limits.accounts {
            return Err(Unusable::Accounts { bound: limits.accounts });
        }
        if limits.accounts == 0
            || limits.client.document.token_bytes == 0
            || limits.client.document.record_bytes == 0
            || worst_case(limits).is_none()
        {
            return Err(Unusable::Limits);
        }
        let mut states = List::with_capacity(limits.accounts);
        for account in &accounts {
            match account {
                Account::HandedIn => match states.push(State::Empty) {
                    Ok(()) => {}
                    Err(State::Empty | State::Record { .. }) => unreachable!("validated account bound"),
                },
            }
        }
        Ok(Component { accounts: states, lifecycle: Lifecycle::Live, random: Rng::new(seed) })
    }

    /// Handles one owner request with `MAX_OUT_DOWN` room reserved in up.
    pub fn down(&mut self, env: &Env<Limits>, request: Request, up: &mut Queue<Event>) {
        match request {
            Request::HandIn { account, record } => {
                if self.refused(account, Asked::HandIn, up) {
                    return;
                }
                if record.refresh_token.is_some() {
                    up.push(Event::Refused { account, asked: Asked::HandIn, why: Refusal::NotAccessOnly });
                    return;
                }
                match skein_oauth::encode_record(&record, &env.limits.client.document) {
                    Ok(encoded) => drop(encoded),
                    Err(why) => {
                        up.push(Event::Refused { account, asked: Asked::HandIn, why: Refusal::Record(why) });
                        return;
                    }
                }
                let slot = self.accounts.get_mut(account).expect("validated account");
                let held = match slot {
                    State::Empty => false,
                    State::Record { record: prior, held, .. } => {
                        if record.generation <= prior.generation {
                            return;
                        }
                        *held
                    }
                };
                let remaining = record.remaining(env.wall);
                let expiry = env.now.saturating_add(remaining);
                let until_lead =
                    Duration::from_nanos(remaining.as_nanos().saturating_sub(env.limits.refresh_lead.as_nanos()));
                let lead = env.now.saturating_add(until_lead);
                *slot = State::Record { record, expiry, lead, announced: false, held, rejected: false };
                if held {
                    grant(slot, env.now, account, up);
                }
            }
            Request::Grant { account } => {
                if self.refused(account, Asked::Grant, up) {
                    return;
                }
                let slot = self.accounts.get_mut(account).expect("validated account");
                match slot {
                    State::Record { held: true, .. } => {
                        up.push(Event::Refused { account, asked: Asked::Grant, why: Refusal::Held });
                    }
                    State::Empty | State::Record { held: false, .. } => grant(slot, env.now, account, up),
                }
            }
            Request::Rejected { account, generation } => match self.accounts.get_mut(account) {
                Some(State::Record { record, rejected, held, .. }) if record.generation == generation => {
                    *rejected = true;
                    if *held {
                        *held = false;
                        failed(account, up);
                    }
                }
                Some(State::Empty | State::Record { .. }) | None => {}
            },
            Request::Release { account } => match self.accounts.get_mut(account) {
                Some(State::Record { held, .. }) => *held = false,
                Some(State::Empty) | None => {}
            },
            Request::Close | Request::Abort => match self.lifecycle {
                Lifecycle::Live => {
                    self.accounts.clear();
                    self.lifecycle = Lifecycle::Closed;
                    up.push(Event::Closed);
                }
                Lifecycle::Closed => {}
            },
        }
    }

    fn refused(&self, account: u32, asked: Asked, up: &mut Queue<Event>) -> bool {
        let why = match self.lifecycle {
            Lifecycle::Closed => Some(Refusal::Closed),
            Lifecycle::Live => {
                if account >= self.accounts.len() {
                    Some(Refusal::Account)
                } else {
                    None
                }
            }
        };
        match why {
            Some(why) => {
                up.push(Event::Refused { account, asked, why });
                true
            }
            None => false,
        }
    }

    /// Fires one due account with `MAX_OUT_FIRE` room reserved in up.
    pub fn fire(&mut self, env: &Env<Limits>, up: &mut Queue<Event>) {
        for account in 0..self.accounts.len() {
            match self.accounts.get_mut(account).expect("bounded account index") {
                State::Record { record, expiry, lead, announced, held, rejected } => {
                    if env.now >= *expiry {
                        if *held {
                            *held = false;
                            failed(account, up);
                            return;
                        }
                        if !*announced {
                            *announced = true;
                        }
                    } else if env.now >= *lead && !*announced && !*rejected {
                        *announced = true;
                        up.push(Event::Expiring { account, generation: record.generation });
                        return;
                    }
                }
                State::Empty => {}
            }
        }
    }

    /// No child work is buffered; the owner schedules `next_deadline` instead.
    #[must_use]
    pub const fn has_work(&self) -> bool {
        false
    }

    /// The earliest unannounced lead or held grant's expiry.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let mut earliest: Option<Time> = None;
        for slot in &self.accounts {
            let next = match slot {
                State::Record { lead, announced: false, rejected: false, .. } => Some(*lead),
                State::Record { expiry, held: true, .. } => Some(*expiry),
                State::Empty | State::Record { .. } => None,
            };
            if let Some(next) = next {
                earliest = Some(match earliest {
                    Some(prior) => prior.min(next),
                    None => next,
                });
            }
        }
        earliest
    }

    /// No dynamic entities need retirement in handed-in accounts.
    pub const fn reclaim(&mut self) {}
}

fn grant(slot: &mut State, now: Time, account: u32, up: &mut Queue<Event>) {
    match slot {
        State::Record { record, expiry, held, rejected, .. } if now < *expiry && !*rejected => {
            *held = true;
            up.push(Event::Granted {
                account,
                token: record.access_token.clone(),
                generation: record.generation,
                valid: expiry.saturating_since(now),
            });
        }
        State::Empty => failed(account, up),
        State::Record { held, .. } => {
            *held = false;
            failed(account, up);
        }
    }
}

fn failed(account: u32, up: &mut Queue<Event>) {
    up.push(Event::Failed { account, ends: Ends::Grant, failure: Failure::Expired });
}
