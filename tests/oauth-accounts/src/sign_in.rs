//! Actual accounts sign-in with an independent issuer and scripted browser.
//! The shared world owns schedules, replay and process heaps (oauth.md, 6.8).
use skein_fake_oauth as fake;
use skein_fake_peers::{Transport, oauth as peer};
use skein_io::{self as io, kernel};
use skein_lib::{Duration, Env, List, Queue, Time, Wall, bytes};
use skein_oauth as oauth;
use skein_oauth_accounts as accounts;
use skein_world::domain::Ledger;
use skein_world::{Host, Memory, Outcome, Referee, World};
use std::net::{Ipv4Addr, SocketAddr};
mod browser;
mod protocol;
pub use browser::Browser;
pub use protocol::protocol;
const ROOM: u32 = 64;
fn io_limits() -> io::Limits {
    super::world::limits().io
}

/// The outside owner's actions during one sign-in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Story {
    /// Public sign-in followed by a rotating refresh.
    Public,
    /// The owner's web server hands in a confidential redirect.
    Confidential,
    /// A wrong path is refused before the real callback.
    WrongPath,
    /// Another authority is refused before the real callback.
    WrongHost,
    /// An oversized request head is refused before the real callback.
    LongHead,
    /// Close keeps waiting for the person's redirect, then drains.
    CloseWaiting,
    /// A confidential redirect completes the admitted sign-in through a close.
    CloseConfidential,
    /// Owner cancel ends its sign-in and listener.
    Cancel,
    /// Abort after close cancels the sign-in.
    Abort,
    /// A keeper failure is the sign-in's failure terminal.
    NotKept,
    /// The shipped person deadline ends a sign-in that receives no redirect.
    Timeout,
}
fn confidential(story: Story) -> bool {
    matches!(story, Story::Confidential | Story::CloseConfidential)
}
fn registration(story: Story) -> oauth::Registration {
    let mut registration = super::world::registration(Transport::Plaintext);
    if confidential(story) {
        registration.client_secret = Some(bytes::copy_of(b"fake-secret"));
        registration.redirect_uri = bytes::copy_of(b"https://service.example/callback");
    }
    registration
}
/// Content-free outside facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fact {
    /// Authorization URL was ready to show.
    Visit,
    /// A new record reached the durable keeper.
    Keep(u64),
    /// The kept sign-in completed.
    SignedIn(u64),
    /// The owner heard a usable grant.
    Granted(u64),
    /// An admitted request failed.
    Failed(accounts::Failure),
    /// A late owner redirect was refused.
    Late,
    /// The component settled all children.
    Closed,
}
/// The sign-in terminal referee uses the shared request ledger.
pub struct Contracts {
    sign_in: Ledger<u32, ()>,
    keeps: Ledger<u64, ()>,
    kept: Vec<u64>,
    closed: bool,
}
impl Contracts {
    /// Starts with one admitted sign-in.
    #[must_use]
    pub fn new() -> Self {
        let mut sign_in = Ledger::new("sign-in and keeper terminals");
        sign_in.open(0, ());
        Self { sign_in, keeps: Ledger::new("sign-in and keeper terminals"), kept: Vec::new(), closed: false }
    }
    /// Records a candidate that has no keeper answer yet.
    pub fn keep(&mut self, generation: u64) {
        self.keeps.open(generation, ());
    }
    /// Records the owner's durable answer.
    pub fn kept(&mut self, generation: u64, yes: bool) {
        self.keeps.end(generation);
        if yes {
            self.kept.push(generation);
        }
    }
    /// Success must follow keeping and end the admitted sign-in once.
    pub fn signed_in(&mut self, generation: u64) {
        assert!(self.kept.contains(&generation), "signed in before kept");
        self.sign_in.end(0);
    }
    /// A failed sign-in has the same one-terminal rule.
    pub fn failed(&mut self) {
        self.sign_in.end(0);
    }
    /// Lending requires durable keeping.
    pub fn granted(&mut self, generation: u64) {
        assert!(self.kept.contains(&generation), "grant before kept");
    }
    /// Closed follows all request and keeper terminals once.
    pub fn closed(&mut self) {
        assert!(!self.closed);
        self.sign_in.assert_settled();
        self.keeps.assert_settled();
        self.closed = true;
    }
}
impl Default for Contracts {
    fn default() -> Self {
        Self::new()
    }
}
/// The actual owner adapter routes io to the accounts component.
pub struct Client {
    io: io::Io,
    component: Option<accounts::Component>,
    limits: accounts::Limits,
    events: Queue<io::Event>,
    requests: Queue<io::Request>,
    above: Queue<accounts::Event>,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
    address: Option<SocketAddr>,
    story: Story,
    facts: Vec<Fact>,
    records: Vec<oauth::SavedToken>,
    contracts: Contracts,
    visit: Option<Box<[u8]>>,
    closing: bool,
    closed: bool,
    large: bool,
    redirected: [u8; 1024],
    redirected_len: usize,
}
impl Client {
    fn new(story: Story, large: bool) -> Self {
        let limits = super::world::limits();
        Self {
            io: io::Io::new(&limits.io),
            component: None,
            limits,
            events: Queue::with_capacity(ROOM),
            requests: Queue::with_capacity(ROOM),
            above: Queue::with_capacity(ROOM),
            completions: Queue::with_capacity(ROOM),
            submissions: Queue::with_capacity(ROOM),
            address: None,
            story,
            facts: Vec::with_capacity(16),
            records: Vec::with_capacity(2),
            contracts: Contracts::new(),
            visit: None,
            closing: false,
            closed: false,
            large,
            redirected: [0; 1024],
            redirected_len: 0,
        }
    }
    fn start(&mut self, env: &Env<accounts::Limits>) {
        let mut configured = List::with_capacity(1);
        assert!(
            configured
                .push(accounts::Account::SignIn {
                    registration: registration(self.story),
                    endpoint: accounts::Endpoint {
                        address: self.address.expect("issuer address"),
                        transport: accounts::Transport::Plaintext
                    },
                    keeper: accounts::Keeper::Owner { kept: None }
                })
                .is_ok()
        );
        self.component = Some(accounts::Component::new(configured, &self.limits, 17).expect("accounts"));
        self.ask(env, accounts::Request::SignIn { account: 0 });
    }
    fn ask(&mut self, env: &Env<accounts::Limits>, request: accounts::Request) {
        self.component.as_mut().expect("component").down(env, request, &mut self.above, &mut self.requests);
    }
    fn close(&mut self, env: &Env<accounts::Limits>) {
        if !self.closing {
            self.closing = true;
            self.ask(env, accounts::Request::Close);
        }
    }
    fn observe(&mut self, env: &Env<accounts::Limits>) {
        if let Some(event) = self.above.pop() {
            assert!(!self.closed, "Closed once and last");
            match event {
                accounts::Event::Visit { url, .. } => {
                    assert!(self.visit.is_none());
                    self.visit = Some(url);
                    self.facts.push(Fact::Visit);
                    match self.story {
                        Story::CloseWaiting | Story::CloseConfidential => {
                            self.close(env);
                            assert!(!self.closed);
                        }
                        Story::Cancel => {
                            self.ask(env, accounts::Request::Cancel { account: 0 });
                            self.close(env);
                        }
                        Story::Abort => {
                            self.close(env);
                            self.ask(env, accounts::Request::Abort);
                        }
                        Story::Public
                        | Story::Confidential
                        | Story::WrongPath
                        | Story::WrongHost
                        | Story::LongHead
                        | Story::NotKept
                        | Story::Timeout => {}
                    }
                }
                accounts::Event::Keep { account, record } => {
                    let generation = record.generation;
                    self.contracts.keep(generation);
                    self.facts.push(Fact::Keep(generation));
                    let yes = self.story != Story::NotKept;
                    let size = if self.large { 256 } else { 16 };
                    assert_eq!(record.access_token.as_ref(), vec![if generation == 1 { b'a' } else { b'b' }; size]);
                    self.contracts.kept(generation, yes);
                    self.ask(
                        env,
                        accounts::Request::Kept {
                            account,
                            generation,
                            keeping: if yes { accounts::Keeping::Kept } else { accounts::Keeping::NotKept },
                        },
                    );
                    self.records.push(record);
                }
                accounts::Event::SignedIn { account, generation } => {
                    self.contracts.signed_in(generation);
                    self.facts.push(Fact::SignedIn(generation));
                    assert_eq!(generation, 1);
                    if !self.closing {
                        self.ask(env, accounts::Request::Grant { account });
                    }
                    if self.story == Story::Confidential {
                        self.ask(
                            env,
                            accounts::Request::Redirected {
                                account,
                                uri: bytes::copy_of(b"https://service.example/callback?state=late&code=late"),
                            },
                        );
                    }
                }
                accounts::Event::Granted { account, generation, .. } => {
                    self.contracts.granted(generation);
                    self.facts.push(Fact::Granted(generation));
                    if generation == 1 && self.story == Story::Public {
                        self.ask(env, accounts::Request::Rejected { account, generation });
                    } else {
                        self.close(env);
                    }
                }
                accounts::Event::Failed { ends, failure, .. } => {
                    assert_eq!(ends, accounts::Ends::SignIn);
                    self.contracts.failed();
                    self.facts.push(Fact::Failed(failure));
                    self.close(env);
                }
                accounts::Event::Refused {
                    asked: accounts::Asked::Redirected,
                    why: accounts::Refusal::NotWaiting,
                    ..
                } => self.facts.push(Fact::Late),
                accounts::Event::Closed => {
                    self.contracts.closed();
                    self.facts.push(Fact::Closed);
                    self.closed = true;
                }
                accounts::Event::Refused { .. } | accounts::Event::Expiring { .. } => {
                    panic!("unexpected sign-in owner event")
                }
            }
        }
    }
}
impl Host for Client {
    fn iterate(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: self.limits };
        if self.component.is_none() && self.address.is_some() {
            self.start(&env);
        }
        if self.redirected_len > 0 {
            let uri = bytes::copy_of(&self.redirected[..self.redirected_len]);
            self.redirected_len = 0;
            self.ask(&env, accounts::Request::Redirected { account: 0, uri });
        }
        let io_env = Env { now, wall, limits: self.limits.io };
        for _ in 0..ROOM {
            if self.events.room() < 3 || self.submissions.room() < 2 || !self.io.is_ready() {
                break;
            }
            io::resume(&mut self.io, &io_env, &mut self.events, &mut self.submissions);
        }
        for _ in 0..ROOM {
            if self.events.room() < 3 || self.submissions.room() < 2 {
                break;
            }
            let Some(complete) = self.completions.pop() else { break };
            io::up(&mut self.io, &io_env, complete, &mut self.events, &mut self.submissions);
        }
        for _ in 0..ROOM {
            if self.events.room() < 3 || self.submissions.room() < 2 || !self.io.is_due(now) {
                break;
            }
            io::fire(&mut self.io, &io_env, &mut self.events, &mut self.submissions);
        }
        for _ in 0..ROOM {
            if self.above.room() < accounts::MAX_OUT_UP.above || self.requests.room() < accounts::MAX_OUT_UP.io {
                break;
            }
            let Some(event) = self.events.pop() else { break };
            self.component.as_mut().expect("io follows started component").up(
                &env,
                event,
                &mut self.above,
                &mut self.requests,
            );
        }
        if let Some(component) = &mut self.component
            && self.above.room() >= accounts::MAX_OUT_FIRE.above
            && self.requests.room() >= accounts::MAX_OUT_FIRE.io
            && (component.has_work() || component.next_deadline().is_some_and(|due| due <= now))
        {
            component.fire(&env, &mut self.above, &mut self.requests);
        }
        if self.requests.room() >= accounts::MAX_OUT_DOWN.io && self.above.room() >= accounts::MAX_OUT_DOWN.above {
            self.observe(&env);
        }
        for _ in 0..ROOM {
            if !self.io.takes() || self.submissions.room() < 2 || self.above.room() < accounts::MAX_OUT_DOWN.above {
                break;
            }
            let Some(request) = self.requests.pop() else { break };
            io::down(&mut self.io, &io_env, request, &mut self.submissions);
        }
        self.io.reclaim();
        if let Some(component) = &mut self.component {
            component.reclaim();
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        &mut self.completions
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        &mut self.submissions
    }
    fn work_pending(&self, now: Time) -> bool {
        self.io.is_ready()
            || !self.events.is_empty()
            || !self.requests.is_empty()
            || !self.above.is_empty()
            || !self.completions.is_empty()
            || self.redirected_len > 0
            || (self.component.is_none() && self.address.is_some())
            || self.component.as_ref().is_some_and(|component| {
                component.has_work() || component.next_deadline().is_some_and(|due| due <= now)
            })
    }
    fn next_deadline(&self) -> Option<Time> {
        [self.io.next_deadline(), self.component.as_ref().and_then(accounts::Component::next_deadline)]
            .into_iter()
            .flatten()
            .min()
    }
    fn is_empty(&self) -> bool {
        self.closed
            && self.io.is_empty()
            && self.events.is_empty()
            && self.requests.is_empty()
            && self.above.is_empty()
            && self.completions.is_empty()
            && self.submissions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        accounts::worst_case(&self.limits).expect("component bound")
            + io::worst_case(&self.limits.io).expect("io bound")
            + 512_000
    }
    fn operations(&self) -> u32 {
        io::operations(&self.limits.io).expect("io operations")
    }
}

/// The two actual processes of a component world.
pub enum Process {
    /// The owner-driven component with its own io loop.
    Client(Box<Client>),
    /// The independent scripted issuer, which stays live until the owner settles.
    Issuer(Box<peer::Peer>),
    /// The browser receives the authorization redirect and callback page.
    Browser(Box<Browser>),
}

impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        match self {
            Process::Client(client) => client.iterate(now, wall),
            Process::Issuer(peer) => peer.iterate(now, wall),
            Process::Browser(browser) => browser.iterate(now, wall),
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        match self {
            Process::Client(client) => client.completions(),
            Process::Issuer(peer) => peer.completions(),
            Process::Browser(browser) => browser.completions(),
        }
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        match self {
            Process::Client(client) => client.submissions(),
            Process::Issuer(peer) => peer.submissions(),
            Process::Browser(browser) => browser.submissions(),
        }
    }
    fn work_pending(&self, now: Time) -> bool {
        match self {
            Process::Client(client) => client.work_pending(now),
            Process::Issuer(peer) => peer.work_pending(now),
            Process::Browser(browser) => browser.work_pending(now),
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        match self {
            Process::Client(client) => client.next_deadline(),
            Process::Issuer(peer) => peer.next_deadline(),
            Process::Browser(browser) => browser.next_deadline(),
        }
    }
    fn is_empty(&self) -> bool {
        match self {
            Process::Client(client) => client.is_empty(),
            Process::Issuer(peer) => peer.is_empty(),
            Process::Browser(browser) => browser.is_empty(),
        }
    }
    fn worst_case(&self) -> u64 {
        match self {
            Process::Client(client) => client.worst_case(),
            Process::Issuer(peer) => peer.worst_case(),
            Process::Browser(browser) => browser.worst_case(),
        }
    }
    fn operations(&self) -> u32 {
        match self {
            Process::Client(client) => client.operations(),
            Process::Issuer(peer) => peer.operations(),
            Process::Browser(browser) => browser.operations(),
        }
    }
}

#[expect(clippy::struct_excessive_bools, reason = "independent outside delivery, shutdown and success observations")]
struct Judge {
    delivered: bool,
    redirected: bool,
    shutdown: bool,
    wake: Option<Time>,
    passed: bool,
    story: Story,
}
impl Referee<Process> for Judge {
    fn act(&mut self, _now: Time, processes: &mut [Process]) {
        let address = processes.iter().find_map(|process| match process {
            Process::Issuer(peer) => peer.address(),
            Process::Client(_) | Process::Browser(_) => None,
        });
        let mut visit = [0; 1024];
        let mut visit_len = 0;
        let mut redirect = [0; 1024];
        let mut redirect_len = 0;
        for process in processes.iter() {
            match process {
                Process::Client(client) => {
                    if let Some(url) = &client.visit {
                        visit_len = url.len();
                        visit[..visit_len].copy_from_slice(url);
                    }
                }
                Process::Browser(browser) => {
                    if let Some(uri) = &browser.redirect {
                        redirect_len = uri.len();
                        redirect[..redirect_len].copy_from_slice(uri);
                    }
                }
                Process::Issuer(_) => {}
            }
        }
        let settled = processes.iter().filter(|process| !matches!(process, Process::Issuer(_))).all(Host::is_empty);
        for process in processes {
            match process {
                Process::Client(client) => {
                    client.address = address;
                    if confidential(self.story) && !self.redirected && redirect_len > 0 {
                        client.redirected[..redirect_len].copy_from_slice(&redirect[..redirect_len]);
                        client.redirected_len = redirect_len;
                        self.redirected = true;
                    }
                }
                Process::Browser(browser) => {
                    if !self.delivered
                        && !matches!(self.story, Story::Cancel | Story::Abort | Story::Timeout)
                        && visit_len > 0
                    {
                        browser.inbox[..visit_len].copy_from_slice(&visit[..visit_len]);
                        browser.inbox_len = visit_len;
                        self.delivered = true;
                    }
                }
                Process::Issuer(peer) => {
                    if settled && !self.shutdown {
                        peer.shutdown();
                        self.shutdown = true;
                    }
                }
            }
        }
    }
    fn observe(&mut self, now: Time, processes: &[Process]) {
        let client = processes
            .iter()
            .find_map(|process| match process {
                Process::Client(client) => Some(client),
                Process::Browser(_) | Process::Issuer(_) => None,
            })
            .expect("owner");
        let browser = processes
            .iter()
            .find_map(|process| match process {
                Process::Browser(browser) => Some(browser),
                Process::Client(_) | Process::Issuer(_) => None,
            })
            .expect("browser");
        let issuer = processes
            .iter()
            .find_map(|process| match process {
                Process::Issuer(peer) => Some(peer),
                Process::Client(_) | Process::Browser(_) => None,
            })
            .expect("issuer");
        self.wake = ((client.component.is_none() && issuer.address().is_some())
            || (!self.delivered
                && client.visit.is_some()
                && !matches!(self.story, Story::Cancel | Story::Abort | Story::Timeout))
            || (confidential(self.story) && !self.redirected && browser.redirect.is_some())
            || (!self.shutdown && client.is_empty() && browser.is_empty()))
        .then_some(now);
        if client.closed {
            assert_eq!(client.facts.last(), Some(&Fact::Closed));
            match self.story {
                Story::Cancel | Story::Abort => assert!(
                    client.facts.contains(&Fact::Failed(accounts::Failure::Exchange(oauth::Failure::Cancelled)))
                ),
                Story::Timeout => {
                    assert!(
                        client.facts.contains(&Fact::Failed(accounts::Failure::Exchange(oauth::Failure::TimedOut)))
                    );
                }
                Story::NotKept => {
                    assert!(client.facts.contains(&Fact::Failed(accounts::Failure::NotKept)));
                    assert!(!client.facts.iter().any(|fact| matches!(fact, Fact::SignedIn(_) | Fact::Granted(_))));
                }
                Story::Public => {
                    assert!(client.facts.contains(&Fact::Granted(2)));
                    assert_eq!(client.records.len(), 2);
                }
                Story::Confidential => {
                    assert!(client.facts.contains(&Fact::SignedIn(1)));
                    assert!(client.facts.contains(&Fact::Late));
                }
                Story::WrongPath
                | Story::WrongHost
                | Story::LongHead
                | Story::CloseWaiting
                | Story::CloseConfidential => {
                    assert!(client.facts.contains(&Fact::SignedIn(1)));
                }
            }
        }
        let posts = issuer
            .observations()
            .iter()
            .filter(|observation| matches!(observation, peer::Observation::Post { .. }))
            .count();
        assert!(posts <= if self.story == Story::Public { 2 } else { 1 });
        if client.closed && browser.is_empty() && !matches!(self.story, Story::Cancel | Story::Abort | Story::Timeout) {
            assert!(browser.done);
            assert_eq!(posts, if self.story == Story::Public { 2 } else { 1 });
        }
        self.passed = self.shutdown && processes.iter().all(Host::is_empty);
    }
    fn next_deadline(&self) -> Option<Time> {
        self.wake.or(Some(Time::from_nanos(180_000_000_000)))
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (now >= Time::from_nanos(180_000_000_000) && !self.passed).then(|| "accounts sign-in did not settle".to_owned())
    }
    fn passed(&self) -> bool {
        self.passed
    }
}
fn issuer(story: Story, large: bool) -> Process {
    let registration = registration(story);
    let profile = super::world::limits();
    let mut issuer = peer::Peer::new(
        (Ipv4Addr::LOCALHOST, 31000).into(),
        Transport::Plaintext,
        skein_fake_peers::Limits {
            io: profile.io,
            connections: 2,
            queue: ROOM,
            plaintext: 32_768,
            ciphertext: 32_768,
            observations: 16,
            observation_bytes: 4096,
        },
        fake::Config {
            authorization_url: registration.authorization_url,
            token_endpoint: registration.token_endpoint,
            client_id: registration.client_id,
            client_secret: registration.client_secret,
            redirect_uri: registration.redirect_uri,
            refresh_token: if large { vec![b's'; 256].into_boxed_slice() } else { bytes::copy_of(b"seed") },
        },
        fake::Limits {
            document: profile.client.document,
            uri_bytes: 256,
            request_bytes: 1024,
            codes: 1,
            rotations: 2,
            plans: 2,
        },
        profile.server,
    )
    .expect("issuer");
    for (access, refresh) in [(b'a', b"r1".as_slice()), (b'b', b"r2".as_slice())] {
        issuer
            .queue(fake::Plan {
                status: 200,
                body: fake::Body::Token(oauth::TokenResponse {
                    access_token: vec![access; if large { 256 } else { 16 }].into_boxed_slice(),
                    refresh_token: Some(if large {
                        vec![if access == b'a' { b'r' } else { b't' }; 256].into_boxed_slice()
                    } else {
                        bytes::copy_of(refresh)
                    }),
                    expires_in: 30,
                }),
                delay: Duration::from_millis(5),
                retry_after: Duration::ZERO,
            })
            .expect("plan");
    }
    Process::Issuer(Box::new(issuer))
}
/// Runs sign-in with the actual component and independent socket processes.
#[must_use]
pub fn run(seed: u64, story: Story, faulted: bool, large: bool, memory: Memory) -> Outcome<Process> {
    let mut config = skein_sim::Config::calm();
    if faulted {
        config.buffer = 127;
        config.faults.short_recv = 300;
        config.faults.short_send = 300;
    }
    let mut world = World::new(
        seed,
        config,
        Judge { delivered: false, redirected: false, shutdown: false, wake: None, passed: false, story },
        memory,
    );
    world.spawn(|| issuer(story, large));
    world.spawn(|| Process::Client(Box::new(Client::new(story, large))));
    world.spawn(|| Process::Browser(Box::new(Browser::new(story))));
    world.run()
}
/// Compares the actual wire trace, owner terminals and issuer observations.
pub fn assert_replay(first: &Outcome<Process>, second: &Outcome<Process>) {
    assert_eq!(first.trace, second.trace);
    for (first, second) in first.procs.iter().zip(&second.procs) {
        match (first, second) {
            (Process::Client(first), Process::Client(second)) => {
                assert_eq!(first.facts, second.facts);
                assert!(first.records == second.records);
                assert!(first.visit == second.visit);
            }
            (Process::Issuer(first), Process::Issuer(second)) => assert!(first.observations() == second.observations()),
            (Process::Browser(first), Process::Browser(second)) => {
                assert_eq!(first.done, second.done);
                assert!(first.redirect == second.redirect);
            }
            _ => panic!("process order"),
        }
    }
}
