//! Accounts and independent fake issuer processes, joined through shared world
//! scheduling, replay, heap and io contracts (oauth.md, section 6.8).

use skein_fake_oauth as fake;
use skein_fake_peers::{Transport, oauth as peer};
use skein_io::{self as io, kernel};
use skein_lib::{Duration, Env, List, Queue, Time, Wall, bytes};
use skein_oauth as oauth;
use skein_oauth_accounts as accounts;
use skein_world::domain::Ledger;
use skein_world::{Host, Memory, Outcome, Referee, World};
use std::net::{Ipv4Addr, SocketAddr};

const ROOM: u32 = 64;
const AUTHORIZE: &[u8] = b"http://127.0.0.1:31000/authorize";
const TOKEN: &[u8] = b"http://127.0.0.1:31000/token";
const REDIRECT: &[u8] = b"http://localhost:31234/callback";

/// The owner behaviour the referee expects from this run.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Story {
    /// A held valid grant refreshes at its lead, then the owner closes.
    Lead,
    /// A grant within the lead waits for a rotating refresh and its keep.
    Rotate,
    /// A rate limit retries only after its prior connection settles.
    Retry,
    /// The issuer omits a replacement refresh token; the old one is kept.
    Omit,
    /// The keeper declines the candidate; the previous valid generation serves.
    NotKept,
    /// A provider rejection starts one refresh.
    Rejected,
    /// A second rejection of the still-held generation ends the grant.
    Repeated,
    /// A released account starts no refresh at its lead.
    Released,
    /// The owner closes before the exchange connects, then it drains.
    CloseConnecting,
    /// The owner closes once a request has begun uploading, then it drains.
    CloseSending,
    /// A close with an owner keep still pending waits for its terminal.
    CloseKeeping,
    /// An abort after a close cancels the admitted exchange.
    Abort,
}

/// The component's tiny capacities and shipped deadlines.
#[must_use]
pub fn limits() -> accounts::Limits {
    accounts::Limits {
        accounts: 2,
        file_stall: Duration::from_secs(1),
        exchanges: 2,
        listeners: 1,
        server: skein_http::server::Limits {
            head: 2048,
            headers: 16,
            body: 1024,
            read: 256,
            response: 2048,
            send: 256,
        },
        refresh_lead: Duration::from_secs(10),
        client: oauth::ClientLimits {
            document: oauth::Limits {
                document_bytes: 1024,
                string_bytes: 256,
                token_bytes: 256,
                client_bytes: 64,
                detail_bytes: 64,
                record_bytes: 1024,
                depth: 8,
                tokens: 64,
            },
            uri_bytes: 256,
            scope_bytes: 64,
            state_bytes: 64,
            code_bytes: 64,
            url_bytes: 1024,
            request_bytes: 1024,
            sign_in_time: Duration::from_secs(120),
            request_time: Duration::from_secs(10),
            backoff_base: Duration::from_secs(1),
            backoff_ceiling: Duration::from_secs(4),
            max_attempts: 2,
        },
        http: skein_http::client::Limits { request: 2048, head: 2048, headers: 16, read: 256, send: 256 },
        tls: skein_tls::client::Limits { read: 2048, send: 2048, records: 131_072 },
        io: io::Limits {
            sockets: 4,
            refusals: 1,
            intake: 32_768,
            receive: 1024,
            output: 32_768,
            sends: 4,
            accepts: 1,
            backlog: 2,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(1),
        },
    }
}

pub(crate) fn registration(transport: Transport) -> oauth::Registration {
    oauth::Registration {
        authorization_url: bytes::copy_of(if transport == Transport::Tls {
            b"https://127.0.0.1:31000/authorize"
        } else {
            AUTHORIZE
        }),
        token_endpoint: bytes::copy_of(if transport == Transport::Tls {
            b"https://127.0.0.1:31000/token"
        } else {
            TOKEN
        }),
        client_id: bytes::copy_of(b"client"),
        redirect_uri: bytes::copy_of(REDIRECT),
        scope: bytes::copy_of(b"read"),
        wire: oauth::WireFormat::Form,
        client_secret: None,
        pkce_for_confidential: false,
        metadata_claim: None,
    }
}

/// Content-free facts retained at the actual owner boundary.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Fact {
    /// The owner heard a usable generation.
    Granted(u64),
    /// The candidate reached the owner's keeper.
    Keep(u64),
    /// The owner's hold or pending request failed.
    Failed(accounts::Failure),
    /// The component settled its owner close.
    Closed,
}

/// Contract referee shared by every tier; uses the kit's request ledger.
pub struct Contracts {
    grant: Ledger<u32, ()>,
    keeps: Ledger<u64, ()>,
    close: Ledger<u32, ()>,
    kept: Vec<u64>,
    last_generation: Option<u64>,
    closed: bool,
}

impl Contracts {
    /// Opens an owner grant that must have one terminal.
    #[must_use]
    pub fn with_pending_grant() -> Self {
        let mut grant = Ledger::new("owner grants");
        grant.open(0, ());
        Self {
            grant,
            keeps: Ledger::new("candidate keeps"),
            close: Ledger::new("component closes"),
            kept: vec![0],
            last_generation: None,
            closed: false,
        }
    }
    /// Checks a generation before accepting its grant or held notice.
    pub fn granted(&mut self, generation: u64) {
        assert!(!self.closed, "Closed is the last owner event");
        assert!(self.kept.contains(&generation), "nothing lent before its keep terminal");
        if self.grant.contains(0) {
            self.grant.end(0);
        } else {
            assert!(self.last_generation.is_none_or(|prior| generation > prior), "held notices name a new generation");
        }
        self.last_generation = Some(generation);
    }
    /// Opens the candidate's required durability terminal.
    pub fn keep(&mut self, generation: u64) {
        assert!(!self.closed, "Closed is the last owner event");
        self.keeps.open(generation, ());
    }
    /// Delivers exactly the pending keeper terminal.
    pub fn kept(&mut self, generation: u64, kept: bool) {
        self.keeps.end(generation);
        if kept {
            self.kept.push(generation);
        }
    }
    /// Ends a pending grant or the owner's existing hold.
    pub fn failed(&mut self) {
        assert!(!self.closed, "Closed is the last owner event");
        if self.grant.contains(0) {
            self.grant.end(0);
        } else {
            assert!(self.last_generation.take().is_some(), "only a pending or held grant fails");
        }
    }
    /// Opens the component's close obligation once, even when upgraded to abort.
    pub fn closing(&mut self) {
        if !self.close.contains(0) {
            self.close.open(0, ());
        }
    }
    /// Settles the owner's close only once, after keeper obligations.
    pub fn closed(&mut self) {
        assert!(!self.closed, "one Closed");
        self.close.end(0);
        self.keeps.assert_settled();
        self.grant.assert_settled();
        self.closed = true;
    }
}

/// The service's owner adapter, running the same component at each tier.
pub struct Client {
    io: io::Io,
    component: Option<accounts::Component>,
    limits: accounts::Limits,
    events: Queue<io::Event>,
    requests: Queue<io::Request>,
    pub(crate) files: Queue<accounts::FileLower>,
    file_driver: Option<skein_world::files::Driver>,
    file_keep: Option<(skein_lib::Token, oauth::SavedToken)>,
    pub(crate) stop_at_store: Option<bool>,
    pub(crate) stop_at_load: Option<bool>,
    pub(crate) retry_load: Option<()>,
    above: Queue<accounts::Event>,
    completions: Queue<kernel::Complete>,
    submissions: Queue<kernel::Submit>,
    pub(crate) address: Option<SocketAddr>,
    transport: Transport,
    story: Story,
    pub(crate) facts: Vec<Fact>,
    records: Vec<oauth::SavedToken>,
    contracts: Contracts,
    closing: bool,
    pub(crate) closed: bool,
    pub(crate) recovered: Option<u64>,
    rejected: bool,
    started: Time,
}

impl Client {
    pub(crate) fn new(story: Story, transport: Transport) -> Client {
        let limits = limits();
        Client {
            io: io::Io::new(&limits.io),
            component: None,
            limits,
            events: Queue::with_capacity(ROOM),
            requests: Queue::with_capacity(ROOM),
            files: Queue::with_capacity(ROOM),
            file_driver: None,
            file_keep: None,
            stop_at_store: None,
            stop_at_load: None,
            retry_load: None,
            above: Queue::with_capacity(ROOM),
            completions: Queue::with_capacity(ROOM),
            submissions: Queue::with_capacity(ROOM),
            address: None,
            transport,
            story,
            facts: Vec::with_capacity(16),
            records: Vec::with_capacity(4),
            contracts: Contracts::with_pending_grant(),
            closing: false,
            closed: false,
            recovered: None,
            rejected: false,
            started: Time::ZERO,
        }
    }
    pub(crate) fn private(root: kernel::Fd, story: Story) -> Client {
        let mut client = Client::new(story, Transport::Plaintext);
        client.file_driver = Some(skein_world::files::Driver::new(root, 4, 1024, client.limits.file_stall, 1234));
        client
    }
    pub(crate) fn recovery(root: kernel::Fd) -> Client {
        let mut client = Client::private(root, Story::NotKept);
        client.limits.refresh_lead = Duration::ZERO;
        client
    }
    pub(crate) fn refresh_at_start(&mut self) {
        self.limits.refresh_lead = Duration::from_secs(10);
    }
    pub(crate) fn unstarted(&self) -> bool {
        self.component.is_none()
    }
    pub(crate) fn start(&mut self, env: &Env<accounts::Limits>) {
        let mut configured = List::with_capacity(1);
        let seconds = if matches!(self.story, Story::Lead | Story::Rejected | Story::Repeated | Story::Released) {
            30
        } else {
            5
        };
        let record = oauth::SavedToken {
            key: 0,
            generation: 0,
            access_token: bytes::copy_of(b"old"),
            refresh_token: Some(bytes::copy_of(b"seed")),
            metadata: None,
            expires_at: Wall::from_nanos(
                env.wall.as_nanos().checked_add(Duration::from_secs(seconds).as_nanos()).expect("wall lifetime"),
            ),
        };
        assert!(
            configured
                .push(accounts::Account::SignIn {
                    registration: registration(self.transport),
                    endpoint: accounts::Endpoint {
                        address: self.address.expect("referee supplied peer address"),
                        transport: match self.transport {
                            Transport::Plaintext => accounts::Transport::Plaintext,
                            Transport::Tls => accounts::Transport::Tls {
                                server_name: skein_tls_world::pki::name(),
                                trust: skein_tls_world::pki::client(&[])
                            },
                        }
                    },
                    keeper: match &self.file_driver {
                        Some(driver) => accounts::Keeper::Private {
                            root: driver.root(),
                            directory: bytes::copy_of(b"secret"),
                            file: bytes::copy_of(b"record")
                        },
                        None => accounts::Keeper::Owner { kept: Some(record) },
                    }
                })
                .is_ok()
        );
        let mut component = accounts::Component::new(configured, &self.limits, 17).expect("configured accounts");
        component.down(
            env,
            accounts::Request::Grant { account: 0 },
            &mut self.above,
            &mut self.requests,
            &mut self.files,
        );
        self.component = Some(component);
        self.started = env.now;
    }
    pub(crate) fn ask(&mut self, env: &Env<accounts::Limits>, request: accounts::Request) {
        self.component.as_mut().expect("started component").down(
            env,
            request,
            &mut self.above,
            &mut self.requests,
            &mut self.files,
        );
    }
    pub(crate) fn close(&mut self, env: &Env<accounts::Limits>) {
        if !self.closing {
            self.closing = true;
            self.contracts.closing();
            self.ask(env, accounts::Request::Close);
        }
    }
    fn observe(&mut self, env: &Env<accounts::Limits>) {
        if let Some(event) = self.above.pop() {
            assert!(!self.closed, "Closed is once and last");
            match event {
                accounts::Event::Granted { generation, token, valid, .. } => {
                    self.contracts.granted(generation);
                    assert!(valid > Duration::ZERO);
                    assert_eq!(token.as_ref(), if generation == 0 { b"old" } else { b"new" });
                    self.facts.push(Fact::Granted(generation));
                    if generation == 0 && matches!(self.story, Story::Rejected | Story::Repeated) && !self.rejected {
                        self.rejected = true;
                        self.ask(env, accounts::Request::Rejected { account: 0, generation });
                        if self.story == Story::Repeated {
                            self.ask(env, accounts::Request::Rejected { account: 0, generation });
                            self.close(env);
                        }
                    } else if self.story == Story::Released && generation == 0 {
                        self.ask(env, accounts::Request::Release { account: 0 });
                    } else if generation == 1 || self.story == Story::NotKept {
                        self.close(env);
                    }
                }
                accounts::Event::Keep { account, record } => {
                    self.contracts.keep(record.generation);
                    self.facts.push(Fact::Keep(record.generation));
                    assert_eq!(
                        record.refresh_token.as_deref(),
                        Some(if self.story == Story::Omit { b"seed".as_slice() } else { b"next".as_slice() })
                    );
                    let kept = self.story != Story::NotKept;
                    if self.story == Story::CloseKeeping {
                        self.close(env);
                        assert!(!self.closed);
                    }
                    self.contracts.kept(record.generation, kept);
                    self.ask(
                        env,
                        accounts::Request::Kept {
                            account,
                            generation: record.generation,
                            keeping: if kept { accounts::Keeping::Kept } else { accounts::Keeping::NotKept },
                        },
                    );
                    self.records.push(record);
                }
                accounts::Event::Failed { failure, .. } => {
                    self.contracts.failed();
                    self.facts.push(Fact::Failed(failure));
                    if matches!(failure, accounts::Failure::Unloaded { .. }) && self.retry_load.take().is_some() {
                        self.contracts.grant.open(0, ());
                        self.ask(env, accounts::Request::Grant { account: 0 });
                    } else {
                        self.close(env);
                    }
                }
                accounts::Event::Closed => {
                    self.contracts.closed();
                    self.facts.push(Fact::Closed);
                    self.closed = true;
                    if let Some(driver) = &mut self.file_driver {
                        driver.close_root(env.now);
                    }
                }
                accounts::Event::Expiring { .. }
                | accounts::Event::Refused { .. }
                | accounts::Event::Visit { .. }
                | accounts::Event::SignedIn { .. } => {
                    panic!("positive story has no such event")
                }
            }
        }
    }
    fn private_io(&mut self, env: &Env<accounts::Limits>) {
        if self.file_driver.is_none() {
            return;
        }
        while let Some(lower) = self.files.pop() {
            match lower {
                accounts::FileLower::Request { request, deadline } => {
                    if let io::file::Request::Store { owner, bytes, .. } = &request {
                        let record =
                            oauth::decode_record(bytes, &env.limits.client.document).expect("candidate record");
                        self.contracts.keep(record.generation);
                        self.facts.push(Fact::Keep(record.generation));
                        assert!(self.file_keep.is_none());
                        self.file_keep = Some((*owner, record));
                        if let Some(abort) = self.stop_at_store.take() {
                            self.close(env);
                            if abort {
                                self.ask(env, accounts::Request::Abort);
                            }
                        }
                    }
                    if matches!(request, io::file::Request::Load { .. })
                        && let Some(abort) = self.stop_at_load.take()
                    {
                        self.close(env);
                        if abort {
                            self.ask(env, accounts::Request::Abort);
                        }
                    }
                    self.file_driver.as_mut().expect("file driver").request(request, deadline);
                }
                accounts::FileLower::Cancel { owner } => self.file_driver.as_mut().expect("file driver").cancel(owner),
            }
        }
        self.file_driver.as_mut().expect("file driver").progress(env.now);
        while let Some(event) = self.file_driver.as_mut().expect("file driver").take_event() {
            if event.owner() == skein_lib::Token::new(u64::MAX) {
                continue;
            }
            if let io::file::Event::Loaded { bytes, .. } = &event
                && let Ok(record) = oauth::decode_record(bytes, &env.limits.client.document)
            {
                if !self.contracts.kept.contains(&record.generation) {
                    self.contracts.kept.push(record.generation);
                }
                self.recovered = Some(record.generation);
            }
            if self.file_keep.as_ref().is_some_and(|(owner, _)| *owner == event.owner()) {
                let (_, record) = self.file_keep.take().expect("matching keep");
                self.contracts.kept(record.generation, matches!(event, io::file::Event::Stored { .. }));
                self.records.push(record);
            }
            self.component.as_mut().expect("started component").filed(
                env,
                event,
                &mut self.above,
                &mut self.requests,
                &mut self.files,
            );
        }
        while let Some(submit) = self.file_driver.as_mut().expect("file driver").take_submit() {
            self.submissions.push(submit);
        }
    }
}

impl Host for Client {
    fn iterate(&mut self, now: Time, wall: Wall) {
        let env = Env { now, wall, limits: self.limits };
        if self.component.is_none() && self.address.is_some() {
            self.start(&env);
        }
        let io_env = Env { now, wall, limits: self.limits.io };
        self.private_io(&env);
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
            if skein_world::files::Driver::owns(complete.op) {
                self.file_driver.as_mut().expect("tagged file completion").up(complete);
            } else {
                io::up(&mut self.io, &io_env, complete, &mut self.events, &mut self.submissions);
            }
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
                &mut self.files,
            );
        }
        if let Some(component) = &mut self.component
            && self.above.room() >= accounts::MAX_OUT_FIRE.above
            && self.requests.room() >= accounts::MAX_OUT_FIRE.io
            && (component.has_work() || component.next_deadline().is_some_and(|due| due <= now))
        {
            component.fire(&env, &mut self.above, &mut self.requests, &mut self.files);
        }
        if self.requests.room() >= accounts::MAX_OUT_DOWN.io && self.above.room() >= accounts::MAX_OUT_DOWN.above {
            self.observe(&env);
            if self.story == Story::Released
                && !self.closing
                && now >= self.started.saturating_add(Duration::from_secs(40))
            {
                self.close(&env);
            }
        }
        for _ in 0..ROOM {
            if !self.io.takes() || self.submissions.room() < 2 || self.above.room() < accounts::MAX_OUT_DOWN.above {
                break;
            }
            let Some(request) = self.requests.pop() else { break };
            if !self.closing
                && ((matches!(request, io::Request::Connect { .. })
                    && matches!(self.story, Story::CloseConnecting | Story::Abort))
                    || (matches!(request, io::Request::Stream { down: skein_lib::stream::Down::Send(_), .. })
                        && self.story == Story::CloseSending))
            {
                self.close(&env);
                if self.story == Story::Abort {
                    self.ask(&env, accounts::Request::Abort);
                }
            }
            io::down(&mut self.io, &io_env, request, &mut self.submissions);
        }
        self.private_io(&env);
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
        self.file_driver.as_ref().is_some_and(skein_world::files::Driver::has_work)
            || !self.files.is_empty()
            || self.io.is_ready()
            || !self.events.is_empty()
            || !self.requests.is_empty()
            || !self.above.is_empty()
            || !self.completions.is_empty()
            || (self.component.is_none() && self.address.is_some())
            || self.component.as_ref().is_some_and(|component| {
                component.has_work() || component.next_deadline().is_some_and(|due| due <= now)
            })
    }
    fn next_deadline(&self) -> Option<Time> {
        [
            self.io.next_deadline(),
            self.file_driver.as_ref().and_then(skein_world::files::Driver::next_deadline),
            self.component.as_ref().and_then(accounts::Component::next_deadline),
            (self.story == Story::Released && self.component.is_some() && !self.closing)
                .then(|| self.started.saturating_add(Duration::from_secs(40))),
        ]
        .into_iter()
        .flatten()
        .min()
    }
    fn is_empty(&self) -> bool {
        self.closed
            && self.io.is_empty()
            && self.file_driver.as_ref().is_none_or(skein_world::files::Driver::is_empty)
            && self.events.is_empty()
            && self.requests.is_empty()
            && self.files.is_empty()
            && self.above.is_empty()
            && self.completions.is_empty()
            && self.submissions.is_empty()
    }
    fn worst_case(&self) -> u64 {
        accounts::worst_case(&self.limits).expect("component bound")
            + io::worst_case(&self.limits.io).expect("io bound")
            + self.file_driver.as_ref().map_or(0, |driver| driver.worst_case(4))
            + 512_000
    }
    fn operations(&self) -> u32 {
        io::operations(&self.limits.io).expect("io operations") + if self.file_driver.is_some() { 4 } else { 0 }
    }
}

/// The two actual processes of a component world.
pub enum Process {
    /// The owner-driven component with its own io loop.
    Client(Box<Client>),
    /// The independent scripted issuer, which stays live until the owner settles.
    Issuer(Box<peer::Peer>),
}

impl Host for Process {
    fn iterate(&mut self, now: Time, wall: Wall) {
        match self {
            Process::Client(client) => client.iterate(now, wall),
            Process::Issuer(peer) => peer.iterate(now, wall),
        }
    }
    fn completions(&mut self) -> &mut Queue<kernel::Complete> {
        match self {
            Process::Client(client) => client.completions(),
            Process::Issuer(peer) => peer.completions(),
        }
    }
    fn submissions(&mut self) -> &mut Queue<kernel::Submit> {
        match self {
            Process::Client(client) => client.submissions(),
            Process::Issuer(peer) => peer.submissions(),
        }
    }
    fn work_pending(&self, now: Time) -> bool {
        match self {
            Process::Client(client) => client.work_pending(now),
            Process::Issuer(peer) => peer.work_pending(now),
        }
    }
    fn next_deadline(&self) -> Option<Time> {
        match self {
            Process::Client(client) => client.next_deadline(),
            Process::Issuer(peer) => peer.next_deadline(),
        }
    }
    fn is_empty(&self) -> bool {
        match self {
            Process::Client(client) => client.is_empty(),
            Process::Issuer(peer) => peer.is_empty(),
        }
    }
    fn worst_case(&self) -> u64 {
        match self {
            Process::Client(client) => client.worst_case(),
            Process::Issuer(peer) => peer.worst_case(),
        }
    }
    fn operations(&self) -> u32 {
        match self {
            Process::Client(client) => client.operations(),
            Process::Issuer(peer) => peer.operations(),
        }
    }
}

struct Judge {
    wake: Option<Time>,
    deadline: Option<Time>,
    story: Story,
    shutdown: bool,
    passed: bool,
}
impl Referee<Process> for Judge {
    fn act(&mut self, now: Time, processes: &mut [Process]) {
        if self.deadline.is_none() {
            self.deadline = Some(now.saturating_add(Duration::from_secs(60)));
        }
        let address = processes.iter().find_map(|process| match process {
            Process::Issuer(peer) => peer.address(),
            Process::Client(_) => None,
        });
        let settled = processes.iter().any(|process| matches!(process, Process::Client(client) if client.is_empty()));
        for process in processes {
            match process {
                Process::Client(client) => client.address = address,
                Process::Issuer(peer) if settled && !self.shutdown => {
                    peer.shutdown();
                    self.shutdown = true;
                }
                Process::Issuer(_) => {}
            }
        }
    }
    fn observe(&mut self, now: Time, processes: &[Process]) {
        let client = processes
            .iter()
            .find_map(|process| match process {
                Process::Client(client) => Some(client),
                Process::Issuer(_) => None,
            })
            .expect("client");
        self.wake = ((client.component.is_none()
            && processes.iter().any(|process| match process {
                Process::Issuer(peer) => peer.address().is_some(),
                Process::Client(_) => false,
            }))
            || (client.is_empty() && !self.shutdown))
            .then_some(now);
        let posts = processes
            .iter()
            .filter_map(|process| match process {
                Process::Issuer(peer) => Some(peer),
                Process::Client(_) => None,
            })
            .flat_map(|peer| peer.observations())
            .filter(|event| matches!(event, peer::Observation::Post { .. }))
            .count();
        assert!(posts <= if self.story == Story::Retry { 2 } else { 1 }, "bounded refresh attempts per generation");
        if client.closed {
            assert_eq!(client.facts.last(), Some(&Fact::Closed));
            match self.story {
                Story::Repeated => {
                    assert!(client.facts.contains(&Fact::Failed(accounts::Failure::Expired)));
                    assert_eq!(posts, 0);
                }
                Story::Released => {
                    assert_eq!(posts, 0);
                    assert_eq!(client.facts, vec![Fact::Granted(0), Fact::Closed]);
                }
                Story::Abort => assert!(
                    client.facts.contains(&Fact::Failed(accounts::Failure::Exchange(oauth::Failure::Cancelled)))
                ),
                Story::NotKept => {
                    assert_eq!(posts, 1);
                    assert_eq!(client.facts, vec![Fact::Keep(1), Fact::Granted(0), Fact::Closed]);
                }
                Story::Retry => {
                    assert_eq!(posts, 2);
                    assert!(client.facts.contains(&Fact::Granted(1)));
                    assert!(client.facts.contains(&Fact::Keep(1)));
                }
                Story::Lead
                | Story::Rejected
                | Story::Rotate
                | Story::Omit
                | Story::CloseConnecting
                | Story::CloseSending
                | Story::CloseKeeping => {
                    assert_eq!(posts, 1);
                    assert!(client.facts.contains(&Fact::Granted(1)));
                    assert!(client.facts.contains(&Fact::Keep(1)));
                }
            }
        }
        self.passed = self.shutdown && processes.iter().all(Host::is_empty);
    }
    fn next_deadline(&self) -> Option<Time> {
        self.wake.or(self.deadline)
    }
    fn overdue(&self, now: Time) -> Option<String> {
        (self.deadline.is_some_and(|deadline| now >= deadline) && !self.passed)
            .then(|| "OAuth accounts world did not settle".to_owned())
    }
    fn passed(&self) -> bool {
        self.passed
    }
}

pub(crate) fn issuer(transport: Transport, story: Story, address: SocketAddr) -> Process {
    let profile = limits();
    let mut peer = peer::Peer::new(
        address,
        transport,
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
            authorization_url: registration(transport).authorization_url,
            token_endpoint: registration(transport).token_endpoint,
            client_id: bytes::copy_of(b"client"),
            client_secret: None,
            redirect_uri: bytes::copy_of(REDIRECT),
            refresh_token: bytes::copy_of(b"seed"),
        },
        fake::Limits {
            document: profile.client.document,
            uri_bytes: 256,
            request_bytes: 1024,
            codes: 1,
            rotations: 2,
            plans: 2,
        },
        skein_http::server::Limits { head: 2048, headers: 16, body: 1024, read: 256, response: 2048, send: 256 },
    )
    .expect("independent issuer");
    if story == Story::Retry {
        peer.queue(fake::Plan {
            status: 429,
            body: fake::Body::Raw(Box::new([])),
            delay: Duration::from_millis(5),
            retry_after: Duration::from_secs(1),
        })
        .expect("rate limit before rotating refresh");
    }
    peer.queue(fake::Plan {
        status: 200,
        body: fake::Body::Token(oauth::TokenResponse {
            access_token: bytes::copy_of(b"new"),
            refresh_token: (story != Story::Omit).then(|| bytes::copy_of(b"next")),
            expires_in: 30,
        }),
        delay: Duration::from_millis(5),
        retry_after: Duration::ZERO,
    })
    .expect("one rotation plan");
    Process::Issuer(Box::new(peer))
}

/// Runs the actual owner component over io and the simulator, with seeded stream cuts.
#[must_use]
pub fn run(seed: u64, story: Story, faulted: bool, memory: Memory) -> Outcome<Process> {
    let mut config = skein_sim::Config::calm();
    config.wall = skein_tls_world::pki::VALID;
    if faulted {
        config.buffer = 127;
        config.faults.short_recv = 300;
        config.faults.short_send = 300;
    }
    let mut world =
        World::new(seed, config, Judge { wake: None, deadline: None, story, shutdown: false, passed: false }, memory);
    world.spawn(|| issuer(Transport::Plaintext, story, (Ipv4Addr::LOCALHOST, 31000).into()));
    world.spawn(|| Process::Client(Box::new(Client::new(story, Transport::Plaintext))));
    world.run()
}

/// Runs a rotating refresh through TLS on the actual shared ring and loopback fake issuer.
pub fn real_refresh() {
    let mut world = skein_world::real::World::new(Judge {
        wake: None,
        deadline: None,
        story: Story::Rotate,
        shutdown: false,
        passed: false,
    });
    world.spawn(|| issuer(Transport::Tls, Story::Rotate, (Ipv4Addr::LOCALHOST, 0).into()));
    world.spawn(|| Process::Client(Box::new(Client::new(Story::Rotate, Transport::Tls))));
    let clock = skein_shell::Clock::new();
    drop(world.run(&clock, Duration::from_secs(5)));
}

/// Replay compares shared kernel trace and content-free owner facts.
pub fn assert_replay(first: &Outcome<Process>, second: &Outcome<Process>) {
    assert_eq!(first.trace, second.trace);
    for (first, second) in first.procs.iter().zip(&second.procs) {
        match (first, second) {
            (Process::Client(first), Process::Client(second)) => assert_eq!(first.facts, second.facts),
            (Process::Issuer(first), Process::Issuer(second)) => {
                assert!(first.observations() == second.observations(), "outside issuer facts replay");
            }
            (Process::Client(_), Process::Issuer(_)) | (Process::Issuer(_), Process::Client(_)) => {
                panic!("process order replays")
            }
        }
    }
}

/// Runs the same owner and independent issuer directly over shared seeded streams.
/// Returns the content-free owner facts and stream trace for protocol replay.
#[must_use]
#[expect(
    clippy::too_many_lines,
    reason = "one protocol world loop exposes both stream boundaries and the independent issuer"
)]
pub fn protocol(seed: u64, story: Story) -> (Vec<Fact>, Vec<String>) {
    use skein_http::server as http;
    use skein_lib::Token;
    use skein_lib::stream::{Down, Read, Up};
    use skein_world::stream::Wire;

    let profile = limits();
    let registration = registration(Transport::Plaintext);
    let mut issuer = fake::Issuer::new(
        fake::Config {
            authorization_url: registration.authorization_url.clone(),
            token_endpoint: registration.token_endpoint.clone(),
            client_id: registration.client_id.clone(),
            client_secret: None,
            redirect_uri: registration.redirect_uri.clone(),
            refresh_token: bytes::copy_of(b"seed"),
        },
        fake::Limits {
            document: profile.client.document,
            uri_bytes: 256,
            request_bytes: 1024,
            codes: 1,
            rotations: 2,
            plans: 2,
        },
    )
    .expect("issuer");
    issuer
        .queue(fake::Plan {
            status: 200,
            body: fake::Body::Token(oauth::TokenResponse {
                access_token: bytes::copy_of(b"new"),
                refresh_token: (story != Story::Omit).then(|| bytes::copy_of(b"next")),
                expires_in: 30,
            }),
            delay: Duration::from_millis(5),
            retry_after: Duration::ZERO,
        })
        .expect("one issuer response");
    let server_limits = http::Limits { head: 2048, headers: 16, body: 1024, read: 256, response: 2048, send: 256 };
    let mut server = http::Server::new(&server_limits);
    let mut server_events = Queue::with_capacity(ROOM);
    let mut server_requests = Queue::with_capacity(ROOM);
    let mut server_down = Queue::with_capacity(ROOM);
    let mut issuer_out = Queue::with_capacity(ROOM);
    let mut client_wire = Wire::new(seed);
    let mut server_wire = Wire::new(seed.wrapping_add(1));
    let mut client = Client::new(story, Transport::Plaintext);
    client.address = Some((Ipv4Addr::LOCALHOST, 31000).into());
    let mut now = Time::ZERO;
    client.start(&Env { now, wall: Wall::EPOCH, limits: profile });
    let mut owner = None;
    let mut body = Vec::new();
    let mut response: Option<(Box<[u8]>, usize)> = None;
    let mut server_closed = false;
    for _ in 0..100_000 {
        let wall = Wall::from_nanos(now.as_nanos());
        let env = Env { now, wall, limits: profile };
        let server_env = Env { now, wall, limits: server_limits };
        let component = client.component.as_mut().expect("component");
        if component.has_work() || component.next_deadline().is_some_and(|due| due <= now) {
            component.fire(&env, &mut client.above, &mut client.requests, &mut client.files);
        }
        client.observe(&env);
        if story == Story::Released && now >= Time::ZERO.saturating_add(Duration::from_secs(40)) {
            client.close(&env);
        }
        if let Some(request) = client.requests.pop() {
            if !client.closing
                && ((matches!(request, io::Request::Connect { .. })
                    && matches!(story, Story::CloseConnecting | Story::Abort))
                    || (matches!(request, io::Request::Stream { down: Down::Send(_), .. })
                        && story == Story::CloseSending))
            {
                client.close(&env);
                if story == Story::Abort {
                    client.ask(&env, accounts::Request::Abort);
                }
            }
            match request {
                io::Request::Connect { owner: identity, .. } => {
                    assert!(owner.replace(identity).is_none(), "one exchange socket");
                    let component = client.component.as_mut().expect("component");
                    component.up(
                        &env,
                        io::Event::Connecting { owner: identity, socket: Token::new(9) },
                        &mut client.above,
                        &mut client.requests,
                        &mut client.files,
                    );
                    component.up(
                        &env,
                        io::Event::Connected { owner: identity },
                        &mut client.above,
                        &mut client.requests,
                        &mut client.files,
                    );
                    server_requests.push(http::Request::Next);
                }
                io::Request::Stream { down, .. } => {
                    match &down {
                        Down::Send(bytes) => server_wire.write(bytes),
                        Down::Finish => server_wire.eof = true,
                        Down::Demand { .. } => {}
                    }
                    client_wire.take(down);
                }
                io::Request::Close { .. } | io::Request::Abort { .. } => {
                    client.component.as_mut().expect("component").up(
                        &env,
                        io::Event::Closed { owner: owner.take().expect("socket owner") },
                        &mut client.above,
                        &mut client.requests,
                        &mut client.files,
                    );
                    server_requests.push(http::Request::Close);
                }
                other @ (io::Request::Listen { .. }
                | io::Request::Bind { .. }
                | io::Request::Reject { .. }
                | io::Request::Output { .. }
                | io::Request::Spawn { .. }
                | io::Request::Signal { .. }
                | io::Request::Usage { .. }) => panic!("token exchange emits only socket requests: {other:?}"),
            }
        }
        if let Some(identity) = owner
            && let Some(up) = client_wire.answer()
        {
            client.component.as_mut().expect("component").up(
                &env,
                io::Event::Stream { owner: identity, up },
                &mut client.above,
                &mut client.requests,
                &mut client.files,
            );
        }
        if let Some(request) = server_requests.pop() {
            http::down(&mut server, &server_env, request, &mut server_events, &mut server_down);
        }
        if !server_closed && let Some(up) = server_wire.answer() {
            http::up(&mut server, &server_env, up, &mut server_events, &mut server_down);
        }
        if let Some(down) = server_down.pop() {
            match &down {
                Down::Send(bytes) => client_wire.write(bytes),
                Down::Finish => client_wire.eof = true,
                Down::Demand { .. } => {}
            }
            server_wire.take(down);
        }
        if let Some(event) = server_events.pop() {
            match event {
                http::Event::Call(call) => {
                    assert_eq!(call.method, skein_http::Method::Post);
                    assert_eq!(call.target.as_ref(), b"/token");
                    assert_eq!(call.header(b"content-type"), Some(b"application/x-www-form-urlencoded".as_slice()));
                    server_requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
                }
                http::Event::Body(Up::Bytes(bytes)) => {
                    body.extend_from_slice(&bytes);
                    server_requests.push(http::Request::Body(Down::Demand { read: Read::Fill(1), room: 0 }));
                }
                http::Event::Body(Up::End) => issuer.step(
                    fake::Event::Post {
                        request: oauth::HttpRequest {
                            id: 0,
                            endpoint: registration.token_endpoint.clone(),
                            content_type: b"application/x-www-form-urlencoded",
                            body: bytes::copy_of(&body),
                            deadline: now.saturating_add(Duration::from_secs(10)),
                        },
                        now,
                        wall,
                    },
                    &mut issuer_out,
                ),
                http::Event::Reply(Up::Room) => {
                    let (bytes, offset) = response.as_mut().expect("response body");
                    let end = offset.saturating_add(256).min(bytes.len());
                    server_requests.push(http::Request::Reply(Down::Send(bytes::copy_of(&bytes[*offset..end]))));
                    *offset = end;
                    if end == bytes.len() {
                        server_requests.push(http::Request::Reply(Down::Finish));
                    } else {
                        server_requests.push(http::Request::Reply(Down::Demand {
                            read: Read::Nothing,
                            room: u32::try_from(bytes.len() - end).expect("bounded body").min(256),
                        }));
                    }
                }
                http::Event::Closed => server_closed = true,
                http::Event::Done(_) => {}
                other @ (http::Event::Ended
                | http::Event::Body(_)
                | http::Event::Reply(_)
                | http::Event::Refused(_)
                | http::Event::Failed(_)) => panic!("positive independent issuer HTTP event: {other:?}"),
            }
        }
        issuer.step(fake::Event::Tick { now, wall }, &mut issuer_out);
        if let Some(output) = issuer_out.pop() {
            match output {
                fake::Request::Http(answer) => {
                    server_requests.push(http::Request::Respond(http::Response {
                        status: answer.status,
                        headers: Box::new([]),
                        body: http::Body::Length(u64::try_from(answer.body.len()).expect("bounded response")),
                        close: true,
                    }));
                    server_requests.push(http::Request::Reply(Down::Demand {
                        read: Read::Nothing,
                        room: u32::try_from(answer.body.len()).expect("bounded response").min(256),
                    }));
                    response = Some((answer.body, 0));
                }
                fake::Request::Redirect { .. } | fake::Request::Refused => panic!("valid rotating refresh"),
            }
        }
        if client.closed && client.requests.is_empty() && client.above.is_empty() && (owner.is_none()) {
            assert!(issuer.posts() <= 1);
            client_wire.trace.extend(server_wire.trace);
            return (client.facts, client_wire.trace);
        }
        now = now.saturating_add(Duration::from_millis(1));
    }
    panic!("protocol world settles");
}
