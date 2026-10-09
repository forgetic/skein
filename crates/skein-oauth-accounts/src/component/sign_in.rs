//! The owner's sign-in admission and confidential redirect boundary
//! (oauth.md, sections 6.3 and 6.5). Listener heads use the same decoder;
//! the OAuth client owns state authentication and the exchange's deadline.

use super::{Component, State};
use crate::accounts::{Source, copy_registration};
use crate::exchange::{Exchange, Stage};
use crate::keeper::Store;
use crate::listener::Listener;
use crate::redirect::{self, Redirect};
use crate::route::{self, Socket};
use crate::{Asked, Event, Limits, Lower, Refusal};
use alloc::boxed::Box;
use skein_lib::{Env, Queue, Rng, Time, Writer};
use skein_oauth as oauth;

impl Component {
    pub(super) fn sign_in(&mut self, env: &Env<Limits>, account: u32, up: &mut Queue<Event>, io: &mut Queue<Lower>) {
        if self.refused(account, Asked::SignIn, up) {
            return;
        }
        let registration = match self.sources.get(account).expect("configured source") {
            Source::SignIn { registration, .. } => copy_registration(registration),
            Source::HandedIn => {
                up.push(Event::Refused { account, asked: Asked::SignIn, why: Refusal::Source });
                return;
            }
        };
        if self.bindings.get(account) != Some(&None) {
            up.push(Event::Refused { account, asked: Asked::SignIn, why: Refusal::Busy });
            return;
        }
        if self.exchanges.is_full() {
            up.push(Event::Refused {
                account,
                asked: Asked::SignIn,
                why: Refusal::Full { bound: env.limits.exchanges },
            });
            return;
        }
        let public = registration.client_secret.is_none();
        if public {
            let mut listeners = 0_u32;
            for binding in &self.bindings {
                match binding {
                    Some(id) => {
                        let exchange = self.exchanges.get(*id).expect("bound exchange");
                        let reserved = match exchange.stage {
                            Stage::Loading => match self.sources.get(exchange.account).expect("account source") {
                                Source::SignIn { registration, .. } => registration.client_secret.is_none(),
                                Source::HandedIn => false,
                            },
                            Stage::Running | Stage::Keeping { .. } | Stage::Finished => false,
                        };
                        let listening = match &exchange.listener {
                            Some(listener) => !listener.settled(),
                            None => false,
                        };
                        if reserved || listening {
                            listeners = listeners.checked_add(1).expect("bounded listeners");
                        }
                    }
                    None => {}
                }
            }
            if listeners >= env.limits.listeners {
                up.push(Event::Refused {
                    account,
                    asked: Asked::SignIn,
                    why: Refusal::Full { bound: env.limits.listeners },
                });
                return;
            }
        }
        let client = oauth::Client::new(env.limits.client).expect("validated client");
        let mut exchange = Exchange::new(account, client, false);
        exchange.stage = Stage::Loading;
        exchange.purpose = crate::exchange::Purpose::SignIn { waiting: false };
        let id = match self.exchanges.insert(exchange) {
            Ok(id) => id,
            Err(_) => unreachable!("admitted exchange slot"),
        };
        *self.bindings.get_mut(account).expect("account binding") = Some(id);
        let must_load = match self.accounts.get(account).expect("account state") {
            State::Empty => match self.keepers.get_mut(account).expect("account keeper") {
                Store::Private(keeper) => {
                    keeper.reload();
                    true
                }
                Store::Owner => false,
            },
            State::Record { .. } | State::Loaded { .. } => false,
        };
        if !must_load {
            self.begin_sign_in(env, account, io);
        }
    }

    pub(super) fn begin_sign_in(&mut self, env: &Env<Limits>, account: u32, io: &mut Queue<Lower>) {
        let registration = match self.sources.get(account).expect("configured source") {
            Source::SignIn { registration, .. } => copy_registration(registration),
            Source::HandedIn => unreachable!("admitted sign-in source"),
        };
        let public = registration.client_secret.is_none();
        let generation = match self.accounts.get(account).expect("account") {
            State::Record { record, .. } | State::Loaded { record } => record.generation,
            State::Empty => 0,
        };
        let state = draw(&mut self.random, env.limits.client.state_bytes.min(32));
        let verifier =
            if public || registration.pkce_for_confidential { Some(draw(&mut self.random, 43)) } else { None };
        let id = self.bindings.get(account).expect("account binding").expect("admitted exchange");
        let exchange = self.exchanges.get_mut(id).expect("bound exchange");
        exchange.stage = Stage::Running;
        exchange.purpose = crate::exchange::Purpose::SignIn { waiting: true };
        if public {
            exchange.listener = Some(Listener::new());
            io.push(Lower::Listen {
                owner: route::owner(id, Socket::Listener),
                addr: oauth::redirect_address(&registration.redirect_uri).expect("validated public redirect"),
            });
        }
        exchange.client.step(
            oauth::Event::SignIn { registration, key: account, generation, state, verifier, now: env.now },
            &mut exchange.above,
        );
    }

    pub(super) fn redirected(&mut self, env: &Env<Limits>, account: u32, uri: &[u8], up: &mut Queue<Event>) {
        // A redirect completes the already admitted sign-in through a close.
        let draining_reply = match self.lifecycle {
            super::Lifecycle::Closing => match self.bindings.get(account) {
                Some(Some(id)) => self.exchanges.get(*id).expect("bound exchange").waiting(),
                Some(None) | None => false,
            },
            super::Lifecycle::Live | super::Lifecycle::Aborting | super::Lifecycle::Closed => false,
        };
        if !draining_reply && self.refused(account, Asked::Redirected, up) {
            return;
        }
        if account >= self.accounts.len() {
            up.push(Event::Refused { account, asked: Asked::Redirected, why: Refusal::Account });
            return;
        }
        let registered = match self.sources.get(account).expect("configured source") {
            Source::SignIn { registration, .. } if registration.client_secret.is_some() => &registration.redirect_uri,
            Source::SignIn { .. } | Source::HandedIn => {
                up.push(Event::Refused { account, asked: Asked::Redirected, why: Refusal::Source });
                return;
            }
        };
        let id = match self.bindings.get(account).expect("account binding") {
            Some(id) => *id,
            None => {
                up.push(Event::Refused { account, asked: Asked::Redirected, why: Refusal::NotWaiting });
                return;
            }
        };
        let exchange = self.exchanges.get_mut(id).expect("bound exchange");
        if !exchange.waiting() || exchange.visit.is_some() || !exchange.above.is_empty() {
            up.push(Event::Refused { account, asked: Asked::Redirected, why: Refusal::NotWaiting });
            return;
        }
        match redirect::parse(uri, registered, &env.limits.client) {
            Some(redirect) => {
                exchange.received();
                deliver(&mut exchange.client, &mut exchange.above, redirect, env.now);
            }
            None => up.push(Event::Refused { account, asked: Asked::Redirected, why: Refusal::Redirect }),
        }
    }
}

pub(super) fn deliver(client: &mut oauth::Client, above: &mut Queue<oauth::Request>, redirect: Redirect, now: Time) {
    client.step(
        oauth::Event::Redirected {
            uri: redirect.uri,
            state: redirect.state,
            code: redirect.code,
            error: redirect.error,
            now,
        },
        above,
    );
}

fn draw(random: &mut Rng, length: u32) -> Box<[u8]> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut writer = Writer::new(usize::try_from(length).expect("u32 fits"));
    for _ in 0..length {
        let index = usize::try_from(random.below(64)).expect("alphabet index");
        writer.put(&[*ALPHABET.get(index).expect("alphabet index")]).expect("fixed random string cap");
    }
    writer.finish()
}
