//! Account records, holds and owner lifecycle (oauth.md, sections 6.3 and 6.6).
//! Keeps one exchange binding per account; no provider identity or store policy.

#![expect(
    clippy::single_match,
    clippy::manual_let_else,
    reason = "explicit optional entity and socket presence cases at the owner boundary"
)]

use crate::accounts::{Source, copy_registration};
use crate::exchange::{Exchange, Stage, Web};
use crate::route::{self, Socket};
use crate::{Account, Asked, Ends, Event, Failure, Keeper, Keeping, Limits, Refusal, Request, Transport, worst_case};
use skein_lib::{Duration, Env, Id, List, Queue, Rng, Slab, Time};
use skein_oauth::{self as oauth, SavedToken};
mod sign_in;

/// An io request the owner sends on, translating component tokens to io tokens.
pub type Lower = skein_io::Request;

/// An io event the owner routes by the component token echoed as owner.
pub type LowerEvent = skein_io::Event;

/// The most outputs one entrance emits; the owner reserves this room first.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct MaxOut {
    pub above: u32,
    pub io: u32,
}

/// Room reserved before one owner request.
pub const MAX_OUT_DOWN: MaxOut = MaxOut { above: 3, io: 3 };

/// Room reserved before firing one due account or progressing one exchange.
pub const MAX_OUT_FIRE: MaxOut = MaxOut { above: 3, io: 3 };

/// Room reserved before one routed io event.
pub const MAX_OUT_UP: MaxOut = MaxOut { above: 1, io: 1 };

/// Why the component cannot run its owner's configured accounts.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Unusable {
    /// More configured accounts than the account bound.
    Accounts { bound: u32 },
    /// A required bound is zero, inconsistent or the worst case overflows.
    Limits,
    /// A public client's registered redirect is not a supported loopback URI.
    Redirect { account: u32 },
    /// Plaintext configured outside loopback.
    Plaintext { account: u32 },
    /// A configured registration violates the existing client's contract.
    Registration { account: u32, failure: oauth::Failure },
    /// A keeper's startup record violates the client's record bounds.
    Record { account: u32, why: oauth::DecodeError },
}

pub(crate) enum State {
    Empty,
    Record {
        record: SavedToken,
        expiry: Time,
        lead: Time,
        announced: bool,
        held: bool,
        rejected: bool,
        attempted: bool,
        due: bool,
        rejection: Option<u64>,
    },
    // Startup wall metadata is turned into this process's monotonic deadline
    // at its first entrance, which is the first clock the owner supplies.
    Loaded {
        record: SavedToken,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Live,
    Closing,
    Aborting,
    Closed,
}

/// The owner's bounded accounts, exchanges and holds, ending with one Closed.
#[expect(missing_debug_implementations, reason = "credential values must never occur in traces")]
pub struct Component {
    accounts: List<State>,
    sources: List<Source>,
    bindings: List<Option<Id<Exchange>>>,
    exchanges: Slab<Exchange>,
    lifecycle: Lifecycle,
    random: Rng,
}

impl Component {
    /// Validates configuration before the loop; seed belongs to this component.
    pub fn new(accounts: List<Account>, limits: &Limits, seed: u64) -> Result<Component, Unusable> {
        if accounts.len() > limits.accounts {
            return Err(Unusable::Accounts { bound: limits.accounts });
        }
        if !crate::limits::usable(limits) || worst_case(limits).is_none() {
            return Err(Unusable::Limits);
        }
        let mut states = List::with_capacity(limits.accounts);
        let mut sources = List::with_capacity(limits.accounts);
        let mut bindings = List::with_capacity(limits.accounts);
        for (index, account) in accounts.into_boxed().into_iter().enumerate() {
            let index = u32::try_from(index).expect("bounded accounts");
            let (source, state) = match account {
                Account::HandedIn => (Source::HandedIn, State::Empty),
                Account::SignIn { registration, endpoint, keeper } => {
                    match endpoint.transport {
                        Transport::Plaintext if !endpoint.address.ip().is_loopback() => {
                            return Err(Unusable::Plaintext { account: index });
                        }
                        Transport::Plaintext | Transport::Tls { .. } => {}
                    }
                    if registration.client_secret.is_none()
                        && oauth::redirect_address(&registration.redirect_uri).is_err()
                    {
                        return Err(Unusable::Redirect { account: index });
                    }
                    match oauth::validate_registration(&registration, &limits.client, true) {
                        Ok(()) => {}
                        Err(failure) => return Err(Unusable::Registration { account: index, failure }),
                    }
                    let state = match keeper {
                        Keeper::Owner { kept } => match kept {
                            Some(record) => {
                                match oauth::encode_record(&record, &limits.client.document) {
                                    Ok(encoded) => drop(encoded),
                                    Err(why) => return Err(Unusable::Record { account: index, why }),
                                }
                                State::Loaded { record }
                            }
                            None => State::Empty,
                        },
                    };
                    (Source::SignIn { registration, endpoint }, state)
                }
            };
            match states.push(state) {
                Ok(()) => {}
                Err(State::Empty | State::Record { .. } | State::Loaded { .. }) => unreachable!("validated accounts"),
            }
            match sources.push(source) {
                Ok(()) => {}
                Err(Source::HandedIn | Source::SignIn { .. }) => unreachable!("validated accounts"),
            }
            bindings.push(None).expect("validated accounts");
        }
        Ok(Component {
            accounts: states,
            sources,
            bindings,
            exchanges: Slab::with_capacity(limits.exchanges),
            lifecycle: Lifecycle::Live,
            random: Rng::new(seed),
        })
    }

    fn initialize(&mut self, env: &Env<Limits>) {
        for index in 0..self.accounts.len() {
            let slot = self.accounts.get_mut(index).expect("bounded account index");
            let loaded = match slot {
                State::Loaded { .. } => true,
                State::Empty | State::Record { .. } => false,
            };
            if loaded {
                let prior = core::mem::replace(slot, State::Empty);
                match prior {
                    State::Loaded { record } => *slot = recorded(record, env, false),
                    State::Empty | State::Record { .. } => unreachable!("loaded state was checked"),
                }
            }
        }
    }

    /// Handles one owner request with `MAX_OUT_DOWN` room reserved in both queues.
    pub fn down(&mut self, env: &Env<Limits>, request: Request, up: &mut Queue<Event>, io: &mut Queue<Lower>) {
        self.initialize(env);
        match request {
            Request::SignIn { account } => self.sign_in(env, account, up, io),
            Request::Redirected { account, uri } => self.redirected(env, account, &uri, up),
            Request::Cancel { account } => match self.bindings.get(account) {
                Some(Some(id)) => {
                    let id = *id;
                    self.fail_exchange(env, id, Failure::Exchange(oauth::Failure::Cancelled), up, io);
                    self.collect(id, up);
                }
                Some(None) | None => {}
            },
            Request::HandIn { account, record } => self.hand_in(env, account, record, up),
            Request::Grant { account } => self.grant_request(env, account, up),
            Request::Rejected { account, generation } => {
                let mut refresh = false;
                let mut repeated = false;
                match self.accounts.get_mut(account) {
                    Some(State::Record { record, held, rejected, rejection, due, .. })
                        if record.generation == generation =>
                    {
                        repeated = *rejection == Some(generation);
                        *rejection = Some(generation);
                        *rejected = true;
                        if *held {
                            match self.sources.get(account).expect("account source") {
                                Source::HandedIn => {
                                    *held = false;
                                    failed(account, Failure::Expired, up);
                                }
                                Source::SignIn { .. } => {
                                    if repeated {
                                        *held = false;
                                        failed(account, Failure::Expired, up);
                                    } else {
                                        *due = true;
                                        refresh = true;
                                    }
                                }
                            }
                        }
                    }
                    Some(State::Empty | State::Record { .. } | State::Loaded { .. }) | None => {}
                }
                if repeated {
                    self.end_exchange(env, account, io);
                } else if refresh
                    && self.lifecycle == Lifecycle::Live
                    && self.bindings.get(account) == Some(&None)
                    && !self.exchanges.is_full()
                    && self.refreshable(account)
                {
                    self.start_refresh(env, account, false);
                }
            }
            Request::Release { account } => match self.accounts.get_mut(account) {
                Some(slot) => set_held(slot, false),
                None => {}
            },
            Request::Kept { account, generation, keeping } => self.kept(env, account, generation, keeping, up, io),
            Request::Close => match self.lifecycle {
                Lifecycle::Live => {
                    self.lifecycle = Lifecycle::Closing;
                    self.settled(up);
                }
                Lifecycle::Closing | Lifecycle::Aborting | Lifecycle::Closed => {}
            },
            Request::Abort => match self.lifecycle {
                Lifecycle::Live | Lifecycle::Closing => {
                    self.lifecycle = Lifecycle::Aborting;
                    self.settled(up);
                }
                Lifecycle::Aborting | Lifecycle::Closed => {}
            },
        }
    }

    fn grant_request(&mut self, env: &Env<Limits>, account: u32, up: &mut Queue<Event>) {
        if self.refused(account, Asked::Grant, up) {
            return;
        }
        let refreshable = self.refreshable(account);
        let slot = self.accounts.get_mut(account).expect("validated account");
        match slot {
            State::Record { held: true, .. } => {
                up.push(Event::Refused { account, asked: Asked::Grant, why: Refusal::Held });
                return;
            }
            State::Empty | State::Record { held: false, .. } => {}
            State::Loaded { .. } => unreachable!("initialized account"),
        }
        match *self.bindings.get(account).expect("account binding") {
            Some(id) => {
                let exchange = self.exchanges.get_mut(id).expect("bound exchange");
                match exchange.stage {
                    Stage::Finished => grant(slot, env.now, account, up),
                    Stage::Running | Stage::Keeping { .. } => {
                        set_held(slot, true);
                        exchange.pending_grant = true;
                    }
                }
            }
            None => {
                let needs_refresh = match self.sources.get(account).expect("account source") {
                    Source::HandedIn => false,
                    Source::SignIn { .. } => match slot {
                        State::Record { lead, attempted: false, .. } => env.now >= *lead,
                        State::Empty | State::Record { attempted: true, .. } => false,
                        State::Loaded { .. } => unreachable!("initialized account"),
                    },
                };
                if needs_refresh && refreshable {
                    if self.exchanges.is_full() {
                        up.push(Event::Refused {
                            account,
                            asked: Asked::Grant,
                            why: Refusal::Full { bound: env.limits.exchanges },
                        });
                    } else {
                        set_held(slot, true);
                        self.start_refresh(env, account, true);
                    }
                } else {
                    grant(slot, env.now, account, up);
                }
            }
        }
    }

    fn hand_in(&mut self, env: &Env<Limits>, account: u32, record: SavedToken, up: &mut Queue<Event>) {
        if self.refused(account, Asked::HandIn, up) {
            return;
        }
        match self.sources.get(account).expect("validated account") {
            Source::HandedIn => {}
            Source::SignIn { .. } => {
                up.push(Event::Refused { account, asked: Asked::HandIn, why: Refusal::Source });
                return;
            }
        }
        if record.refresh_token.is_some() {
            up.push(Event::Refused { account, asked: Asked::HandIn, why: Refusal::NotAccessOnly });
            return;
        }
        match oauth::encode_record(&record, &env.limits.client.document) {
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
            State::Loaded { .. } => unreachable!("initialized account"),
        };
        *slot = recorded(record, env, held);
        if held {
            grant(slot, env.now, account, up);
        }
    }

    fn refused(&self, account: u32, asked: Asked, up: &mut Queue<Event>) -> bool {
        let why = match self.lifecycle {
            Lifecycle::Closed | Lifecycle::Closing | Lifecycle::Aborting => Some(Refusal::Closed),
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

    fn refreshable(&self, account: u32) -> bool {
        match self.accounts.get(account) {
            Some(State::Record { record, .. }) => record.refresh_token.is_some(),
            Some(State::Empty | State::Loaded { .. }) | None => false,
        }
    }

    fn start_refresh(&mut self, env: &Env<Limits>, account: u32, pending_grant: bool) {
        let prior = match self.accounts.get_mut(account).expect("account") {
            State::Record { record, due, attempted, .. } => {
                *due = false;
                *attempted = true;
                record.refresh_state().expect("refreshable account")
            }
            State::Empty | State::Loaded { .. } => unreachable!("refreshable account"),
        };
        let registration = match self.sources.get(account).expect("account source") {
            Source::SignIn { registration, .. } => copy_registration(registration),
            Source::HandedIn => unreachable!("sign-in source"),
        };
        let client = oauth::Client::new(env.limits.client).expect("validated client limits");
        let mut exchange = Exchange::new(account, client, pending_grant);
        exchange.client.step(oauth::Event::Refresh { registration, prior, now: env.now }, &mut exchange.above);
        let id = match self.exchanges.insert(exchange) {
            Ok(id) => id,
            Err(_) => unreachable!("exchange slot admitted"),
        };
        *self.bindings.get_mut(account).expect("account binding") = Some(id);
    }

    /// Routes one socket event by its component owner token, with `MAX_OUT_UP` room.
    pub fn up(&mut self, env: &Env<Limits>, event: LowerEvent, up: &mut Queue<Event>, io: &mut Queue<Lower>) {
        self.initialize(env);
        let owner = match &event {
            LowerEvent::Connecting { owner, .. }
            | LowerEvent::Connected { owner }
            | LowerEvent::Stream { owner, .. }
            | LowerEvent::Failed { owner, .. }
            | LowerEvent::Closed { owner }
            | LowerEvent::Listening { owner, .. }
            | LowerEvent::Accepted { owner, .. }
            | LowerEvent::Output { owner, .. }
            | LowerEvent::Spawned { owner, .. }
            | LowerEvent::Exited { owner, .. }
            | LowerEvent::Usage { owner, .. } => *owner,
            LowerEvent::Shutdown { .. } => return,
        };
        let (id, socket) = route::exchange(owner);
        match self.exchanges.get_mut(id) {
            Some(exchange) => {
                match socket {
                    Socket::Web => match &mut exchange.web {
                        Some(web) => web.up(env, event, io),
                        None => {}
                    },
                    Socket::Listener | Socket::Callback => match &mut exchange.listener {
                        Some(listener) => listener.up(env, id, socket, event, io),
                        None => {}
                    },
                }
                self.collect(id, up);
            }
            None => {}
        }
    }

    /// Advances one due account or one child transition with reserved output room.
    pub fn fire(&mut self, env: &Env<Limits>, up: &mut Queue<Event>, io: &mut Queue<Lower>) {
        self.initialize(env);
        for account in 0..self.bindings.len() {
            match *self.bindings.get(account).expect("account binding") {
                Some(id) => {
                    let exchange = self.exchanges.get_mut(id).expect("bound exchange");
                    if self.lifecycle == Lifecycle::Aborting && !exchange.aborted {
                        exchange.aborted = true;
                        self.fail_exchange(env, id, Failure::Exchange(oauth::Failure::Cancelled), up, io);
                        self.collect(id, up);
                        return;
                    }
                    if exchange.has_work()
                        || (!web_closing(exchange.web.as_ref()) && due(exchange.client.next_deadline(), env.now))
                    {
                        self.progress(env, id, up, io);
                        self.collect(id, up);
                        return;
                    }
                }
                None => {}
            }
        }
        if self.lifecycle != Lifecycle::Live {
            self.settled(up);
            return;
        }
        for account in 0..self.accounts.len() {
            let source = self.sources.get(account).expect("account source");
            match self.accounts.get_mut(account).expect("account state") {
                State::Record { record, expiry, lead, announced, held, rejected, attempted, due, .. } => {
                    if env.now >= *expiry {
                        if *held && self.bindings.get(account) == Some(&None) {
                            *held = false;
                            failed(account, Failure::Expired, up);
                            return;
                        }
                        *announced = true;
                    } else {
                        match source {
                            Source::HandedIn => {
                                if env.now >= *lead && !*announced && !*rejected {
                                    *announced = true;
                                    up.push(Event::Expiring { account, generation: record.generation });
                                    return;
                                }
                            }
                            Source::SignIn { .. } => {
                                if *held && !*attempted && env.now >= *lead {
                                    *attempted = record.refresh_token.is_none();
                                    *due = !*attempted;
                                }
                            }
                        }
                    }
                    if *due
                        && self.bindings.get(account) == Some(&None)
                        && !self.exchanges.is_full()
                        && record.refresh_token.is_some()
                    {
                        self.start_refresh(env, account, false);
                        return;
                    }
                }
                State::Empty => {}
                State::Loaded { .. } => unreachable!("initialized account"),
            }
        }
    }

    fn progress(&mut self, env: &Env<Limits>, id: Id<Exchange>, up: &mut Queue<Event>, io: &mut Queue<Lower>) {
        let exchange = self.exchanges.get_mut(id).expect("bound exchange");
        let account = exchange.account;
        if web_closing(exchange.web.as_ref()) && !exchange.above.is_empty() {
            if let Some(web) = &mut exchange.web {
                web.close_progress(env, io);
            }
            return;
        }
        if exchange.waiting() && exchange.above.is_empty() && due(exchange.client.next_deadline(), env.now) {
            exchange.client.step(oauth::Event::Tick { now: env.now }, &mut exchange.above);
            return;
        }
        let visit_ready = match &exchange.listener {
            Some(listener) => listener.ready(),
            None => true,
        };
        if visit_ready && let Some(url) = exchange.visit.take() {
            up.push(Event::Visit { account, url });
            return;
        }
        if let Some(request) = exchange.above.pop() {
            self.client_request(env, id, request, up, io);
            return;
        }
        let waiting = exchange.waiting();
        match &mut exchange.listener {
            Some(listener) => {
                if let Some(failure) = listener.failure.take() {
                    self.fail_exchange(env, id, Failure::Exchange(failure), up, io);
                    return;
                }
                if let Some(redirect) = listener.redirect.take() {
                    if waiting {
                        match &mut exchange.purpose {
                            crate::exchange::Purpose::SignIn { waiting } => *waiting = false,
                            crate::exchange::Purpose::Refresh => {}
                        }
                        sign_in::deliver(&mut exchange.client, &mut exchange.above, redirect, env.now);
                        listener.stop_waiting();
                    }
                    return;
                }
                if listener.has_work() {
                    let registered = match self.sources.get(account).expect("account source") {
                        Source::SignIn { registration, .. } => registration.redirect_uri.as_ref(),
                        Source::HandedIn => unreachable!("listener source"),
                    };
                    listener.progress(env, registered, io);
                    return;
                }
            }
            None => {}
        }
        match &mut exchange.web {
            Some(web) => {
                if let Some(failure) = web.failure.take() {
                    self.fail_exchange(env, id, Failure::Exchange(failure), up, io);
                    return;
                }
                if let Some(answer) = web.answer.take() {
                    exchange.client.step(oauth::Event::Http(answer), &mut exchange.above);
                    // A retry must settle this connection before starting another.
                    if !exchange.client.is_done() {
                        web.close(env, false, io);
                    }
                } else if web.closing() {
                    web.close_progress(env, io);
                } else {
                    web.progress(env, io);
                }
                if web.closed() && web.answer.is_none() {
                    exchange.web = None;
                }
            }
            None => {}
        }
        if exchange.above.is_empty()
            && !web_closing(exchange.web.as_ref())
            && due(exchange.client.next_deadline(), env.now)
        {
            exchange.client.step(oauth::Event::Tick { now: env.now }, &mut exchange.above);
            if exchange.client.is_done() {
                match &mut exchange.web {
                    Some(web) => web.close(env, true, io),
                    None => {}
                }
            }
        }
    }

    fn client_request(
        &mut self,
        env: &Env<Limits>,
        id: Id<Exchange>,
        request: oauth::Request,
        up: &mut Queue<Event>,
        io: &mut Queue<Lower>,
    ) {
        let exchange = self.exchanges.get_mut(id).expect("bound exchange");
        let account = exchange.account;
        match request {
            oauth::Request::Http(request) => {
                let endpoint = match self.sources.get(account).expect("account source") {
                    Source::SignIn { endpoint, .. } => endpoint,
                    Source::HandedIn => unreachable!("exchange source"),
                };
                let web = match Web::new(request, endpoint, &env.limits) {
                    Some(web) => web,
                    None => {
                        self.fail_exchange(env, id, Failure::Exchange(oauth::Failure::Malformed), up, io);
                        return;
                    }
                };
                assert!(exchange.web.is_none(), "previous socket settled before retry");
                exchange.web = Some(web);
                io.push(Lower::Connect { owner: route::owner(id, Socket::Web), addr: endpoint.address });
            }
            oauth::Request::Tokens { record } => {
                let kept = record.clone();
                exchange.stage = Stage::Keeping { candidate: record };
                up.push(Event::Keep { account, record: kept });
            }
            oauth::Request::Failed { failure } => self.fail_exchange(env, id, Failure::Exchange(failure), up, io),
            oauth::Request::Visit { url } => match &exchange.listener {
                Some(listener) if !listener.ready() => exchange.visit = Some(url),
                Some(_) | None => up.push(Event::Visit { account, url }),
            },
        }
    }

    fn kept(
        &mut self,
        env: &Env<Limits>,
        account: u32,
        generation: u64,
        keeping: Keeping,
        up: &mut Queue<Event>,
        io: &mut Queue<Lower>,
    ) {
        let id = match self.bindings.get(account) {
            Some(Some(id)) => *id,
            Some(None) | None => return,
        };
        let exchange = self.exchanges.get_mut(id).expect("bound exchange");
        match self.lifecycle {
            Lifecycle::Aborting | Lifecycle::Closed => return,
            Lifecycle::Live | Lifecycle::Closing => {}
        }
        match &exchange.stage {
            Stage::Keeping { candidate } if candidate.generation == generation => {}
            Stage::Keeping { .. } | Stage::Running | Stage::Finished => return,
        }
        let stage = core::mem::replace(&mut exchange.stage, Stage::Finished);
        exchange.received();
        exchange.visit = None;
        match stage {
            Stage::Keeping { candidate } => {
                let slot = self.accounts.get_mut(account).expect("account state");
                let held = held(slot) || exchange.pending_grant;
                match keeping {
                    Keeping::Kept => {
                        let generation = candidate.generation;
                        *slot = recorded(candidate, env, held);
                        if exchange.signing_in() {
                            up.push(Event::SignedIn { account, generation });
                        }
                        if held {
                            grant(slot, env.now, account, up);
                        }
                    }
                    Keeping::NotKept => {
                        if exchange.signing_in() {
                            up.push(Event::Failed { account, ends: Ends::SignIn, failure: Failure::NotKept });
                        }
                        if exchange.pending_grant {
                            grant(slot, env.now, account, up);
                        }
                    }
                }
            }
            Stage::Running | Stage::Finished => unreachable!("matching pending keep"),
        }
        match &mut exchange.web {
            Some(web) => web.close(env, false, io),
            None => {}
        }
        match &mut exchange.listener {
            Some(listener) => listener.close(env, false, io),
            None => {}
        }
        self.collect(id, up);
    }

    fn end_exchange(&mut self, env: &Env<Limits>, account: u32, io: &mut Queue<Lower>) {
        match self.bindings.get(account) {
            Some(Some(id)) => {
                let exchange = self.exchanges.get_mut(*id).expect("bound exchange");
                if exchange.signing_in() {
                    return;
                }
                exchange.aborted = true;
                exchange.stage = Stage::Finished;
                for _ in 0..oauth::MAX_OUT {
                    drop(exchange.above.pop());
                }
                exchange.client.step(oauth::Event::Cancel, &mut exchange.above);
                for _ in 0..oauth::MAX_OUT {
                    drop(exchange.above.pop());
                }
                match &mut exchange.web {
                    Some(web) => web.close(env, true, io),
                    None => {}
                }
            }
            Some(None) | None => {}
        }
    }

    fn fail_exchange(
        &mut self,
        env: &Env<Limits>,
        id: Id<Exchange>,
        failure: Failure,
        up: &mut Queue<Event>,
        io: &mut Queue<Lower>,
    ) {
        let exchange = self.exchanges.get_mut(id).expect("bound exchange");
        let account = exchange.account;
        let slot = self.accounts.get_mut(account).expect("account state");
        let unfinished = match exchange.stage {
            Stage::Finished => false,
            Stage::Running | Stage::Keeping { .. } => true,
        };
        if unfinished {
            if exchange.signing_in() {
                up.push(Event::Failed { account, ends: Ends::SignIn, failure });
            }
            if exchange.pending_grant || (!exchange.signing_in() && held(slot)) {
                set_held(slot, false);
                failed(account, failure, up);
            }
        }
        exchange.stage = Stage::Finished;
        exchange.received();
        exchange.visit = None;
        for _ in 0..oauth::MAX_OUT {
            drop(exchange.above.pop());
        }
        exchange.client.step(oauth::Event::Cancel, &mut exchange.above);
        for _ in 0..oauth::MAX_OUT {
            drop(exchange.above.pop());
        }
        match &mut exchange.web {
            Some(web) => web.close(env, true, io),
            None => {}
        }
        match &mut exchange.listener {
            Some(listener) => listener.close(env, true, io),
            None => {}
        }
    }

    fn collect(&mut self, id: Id<Exchange>, up: &mut Queue<Event>) {
        let exchange = self.exchanges.get_mut(id).expect("bound exchange");
        match &exchange.web {
            Some(web) if web.closed() && web.answer.is_none() => exchange.web = None,
            Some(_) | None => {}
        }
        if exchange.settled() {
            *self.bindings.get_mut(exchange.account).expect("account binding") = None;
            self.exchanges.retire(id);
        }
        self.settled(up);
    }

    fn settled(&mut self, up: &mut Queue<Event>) {
        match self.lifecycle {
            Lifecycle::Live | Lifecycle::Closed => return,
            Lifecycle::Closing | Lifecycle::Aborting => {}
        }
        for binding in &self.bindings {
            if binding.is_some() {
                return;
            }
        }
        self.accounts.clear();
        self.sources.clear();
        self.lifecycle = Lifecycle::Closed;
        up.push(Event::Closed);
    }

    /// Child work buffered for the owner to schedule before sleeping.
    #[must_use]
    pub fn has_work(&self) -> bool {
        for binding in &self.bindings {
            match binding {
                Some(id) => match self.exchanges.get(*id) {
                    Some(exchange)
                        if exchange.has_work() || (self.lifecycle == Lifecycle::Aborting && !exchange.aborted) =>
                    {
                        return true;
                    }
                    Some(_) | None => {}
                },
                None => {}
            }
        }
        if self.lifecycle == Lifecycle::Live && !self.exchanges.is_full() {
            for state in &self.accounts {
                match state {
                    State::Record { due: true, .. } => return true,
                    State::Empty | State::Loaded { .. } | State::Record { due: false, .. } => {}
                }
            }
        }
        false
    }

    /// The earliest client or live account deadline; closing adds no policy timers.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        let mut earliest = None;
        for account in 0..self.accounts.len() {
            if let Some(id) = *self.bindings.get(account).expect("account binding") {
                let exchange = self.exchanges.get(id).expect("bound exchange");
                if !web_closing(exchange.web.as_ref())
                    && let Some(next) = exchange.client.next_deadline()
                {
                    earlier(&mut earliest, next);
                }
                continue;
            }
            if self.lifecycle != Lifecycle::Live {
                continue;
            }
            let next = match self.accounts.get(account).expect("account state") {
                State::Record { lead, expiry, held, announced, rejected, attempted, due, .. } => {
                    match self.sources.get(account).expect("account source") {
                        Source::HandedIn => {
                            if !*announced && !*rejected {
                                Some(*lead)
                            } else if *held {
                                Some(*expiry)
                            } else {
                                None
                            }
                        }
                        Source::SignIn { .. } => {
                            if *held {
                                if !*attempted && !*due { Some(*lead) } else { Some(*expiry) }
                            } else {
                                None
                            }
                        }
                    }
                }
                State::Empty | State::Loaded { .. } => None,
            };
            if let Some(next) = next {
                earlier(&mut earliest, next);
            }
        }
        earliest
    }

    /// Frees retired exchange slots at the owner's reclaim point.
    pub fn reclaim(&mut self) {
        self.exchanges.reclaim();
    }
}

fn earlier(earliest: &mut Option<Time>, next: Time) {
    *earliest = Some(match earliest {
        Some(prior) => (*prior).min(next),
        None => next,
    });
}

fn recorded(record: SavedToken, env: &Env<Limits>, held: bool) -> State {
    let remaining = record.remaining(env.wall);
    let expiry = env.now.saturating_add(remaining);
    let lead = env
        .now
        .saturating_add(Duration::from_nanos(remaining.as_nanos().saturating_sub(env.limits.refresh_lead.as_nanos())));
    State::Record {
        record,
        expiry,
        lead,
        announced: false,
        held,
        rejected: false,
        attempted: false,
        due: false,
        rejection: None,
    }
}

fn held(slot: &State) -> bool {
    match slot {
        State::Record { held, .. } => *held,
        State::Empty | State::Loaded { .. } => false,
    }
}

fn set_held(slot: &mut State, value: bool) {
    match slot {
        State::Record { held, due, .. } => {
            *held = value;
            if !value {
                *due = false;
            }
        }
        State::Empty | State::Loaded { .. } => {}
    }
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
        State::Empty => failed(account, Failure::Expired, up),
        State::Record { held, .. } => {
            *held = false;
            failed(account, Failure::Expired, up);
        }
        State::Loaded { .. } => unreachable!("initialized account"),
    }
}

fn failed(account: u32, failure: Failure, up: &mut Queue<Event>) {
    up.push(Event::Failed { account, ends: Ends::Grant, failure });
}

fn due(deadline: Option<Time>, now: Time) -> bool {
    match deadline {
        Some(deadline) => deadline <= now,
        None => false,
    }
}

fn web_closing(web: Option<&Web>) -> bool {
    match web {
        Some(web) => web.closing(),
        None => false,
    }
}
