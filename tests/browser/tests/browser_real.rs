//! Real Chromium in one service loop with a skein-http page server.

#[path = "../src/serve.rs"]
mod serve;

use std::collections::VecDeque;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use skein_browser::boundary::{Below, Down, Event, Expect, Key, Query, Refusal, Request, Trouble};
use skein_browser::{self as browser, Browser};
use skein_io::kernel::{self, Complete, Done, Op, Pipe, Spawn, Submit, Way};
use skein_io::{self as io, Io};
use skein_lib::stream;
use skein_lib::{Duration, Env, Queue, Time, Token, Wall};
use skein_shell::{Clock, Config, Kernel, Wait, open_root};

const PROCESS: Token = Token::new(10);
const BROWSER: Token = Token::new(11);
const PERSON: Token = Token::new(12);
const PAGE: Token = Token::new(13);
const FIND: Token = Token::new(14);
const PRESS: Token = Token::new(15);
const EXPECT: Token = Token::new(16);
const KEY: Token = Token::new(17);
const IMAGE: Token = Token::new(18);
const SNAPSHOT: Token = Token::new(19);
const ROOT_CLOSE: Token = Token::new(u64::MAX);

#[derive(Debug)]
struct Profile(PathBuf);

impl Profile {
    fn fresh() -> Profile {
        let mut path = std::env::temp_dir();
        let nonce = Clock::new().now().now.as_nanos();
        path.push(format!("skein-browser-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path).expect("fresh Chromium profile directory");
        Profile(path)
    }
}

impl Drop for Profile {
    fn drop(&mut self) {
        let _removed = std::fs::remove_dir_all(&self.0);
    }
}

fn chromium() -> PathBuf {
    if let Some(configured) = std::env::var_os("SKEIN_TEST_CHROMIUM") {
        let path = PathBuf::from(configured);
        assert!(path.is_file(), "SKEIN_TEST_CHROMIUM does not name a file: {}", path.display());
        return path.canonicalize().expect("resolve configured Chromium path");
    }
    let path = std::env::var_os("PATH").expect("PATH locates chromium");
    for directory in std::env::split_paths(&path) {
        let candidate = directory.join("chromium");
        if candidate.is_file() {
            return candidate.canonicalize().expect("resolve Chromium on PATH");
        }
    }
    panic!("Chromium is required for the browser suite: set SKEIN_TEST_CHROMIUM or put chromium on PATH");
}

fn spawn(chromium: &Path, profile: &Path) -> (Spawn, kernel::Fd) {
    let root = open_root(Path::new("/")).expect("open the child working directory root");
    let command = browser::command::command(chromium.as_os_str().as_bytes(), profile.as_os_str().as_bytes());
    let mut env = Vec::new();
    for variable in &command.env {
        let mut entry = variable.name.to_vec();
        entry.push(b'=');
        entry.extend_from_slice(&variable.value);
        env.push(entry.into_boxed_slice());
    }
    let mut pipes = Vec::new();
    for spec in command.pipes {
        let way = match spec.direction {
            browser::command::Direction::Read => Way::In,
            browser::command::Direction::Write => Way::Out,
        };
        pipes.push(Pipe { child: spec.descriptor, way });
    }
    (
        Spawn {
            program: command.program,
            args: command.args.into_boxed(),
            env: env.into_boxed_slice(),
            root,
            dir: Box::from(&b"."[..]),
            pipes: pipes.into_boxed_slice(),
        },
        root,
    )
}

fn io_limits(browser: &browser::Limits) -> io::Limits {
    io::Limits {
        sockets: 8,
        refusals: 8,
        intake: browser.message.checked_add(1).expect("message with NUL fits u32"),
        receive: 65_536,
        output: browser.command.checked_add(1).expect("command with NUL fits u32"),
        sends: 16,
        accepts: 1,
        backlog: 1,
        close_timeout: Duration::from_secs(2),
        retry: Duration::from_millis(10),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Starting,
    Context,
    Opening,
    Finding,
    Pressing,
    Typing,
    Keying,
    Expecting,
    Capturing,
    Closing,
    Complete,
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Button,
    Form,
    Covered,
    Scroll,
    Throw,
    Csp,
}

impl Scenario {
    fn path(self) -> &'static str {
        match self {
            Scenario::Button => "/button",
            Scenario::Form => "/form",
            Scenario::Covered => "/covered",
            Scenario::Scroll => "/scroll",
            Scenario::Throw => "/throw",
            Scenario::Csp => "/csp",
        }
    }
}

struct Rig {
    browser: Browser,
    pages: serve::Pages,
    io: Io,
    kernel: Kernel,
    clock: Clock,
    browser_env: Env<browser::Limits>,
    io_env: Env<io::Limits>,
    complete: Queue<Complete>,
    submits: Queue<Submit>,
    io_events: Queue<io::Event>,
    io_requests: VecDeque<io::Request>,
    events: Queue<Event>,
    browser_down: Queue<Down>,
    pipes: Option<[Token; 3]>,
    child: Option<Token>,
    root: kernel::Fd,
    root_closed: bool,
    exited: bool,
    child_closed: bool,
    stage: Stage,
    scenario: Scenario,
    context_ready: bool,
    exception_seen: bool,
    csp_log_seen: bool,
    await_met: bool,
    version: Box<[u8]>,
    stderr: Vec<u8>,
}

impl Rig {
    fn new(chromium: &Path, profile: &Path, scenario: Scenario) -> Rig {
        let browser_limits = browser::Limits::default();
        let limits = io_limits(&browser_limits);
        assert!(browser::largest_read(&browser_limits).expect("read size") <= limits.largest_read());
        assert!(browser::largest_room(&browser_limits).expect("room size") <= limits.largest_room());
        let operations = io::operations(&limits).expect("ring size fits u32");
        let kernel = Kernel::open(Config { operations })
            .unwrap_or_else(|error| panic!("io_uring is required for the browser suite: {error}"));
        let (spawn, root) = spawn(chromium, profile);
        let mut io_requests = VecDeque::new();
        io_requests.push_back(io::Request::Spawn { owner: PROCESS, spawn });
        Rig {
            browser: Browser::new(BROWSER, &browser_limits),
            pages: serve::Pages::new(Time::ZERO, Wall::EPOCH),
            io: Io::new(&limits),
            kernel,
            clock: Clock::new(),
            browser_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits: browser_limits },
            io_env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            complete: Queue::with_capacity(256),
            submits: Queue::with_capacity(256),
            io_events: Queue::with_capacity(256),
            io_requests,
            events: Queue::with_capacity(256),
            browser_down: Queue::with_capacity(256),
            pipes: None,
            child: None,
            root,
            root_closed: false,
            exited: false,
            child_closed: false,
            stage: Stage::Starting,
            scenario,
            context_ready: false,
            exception_seen: false,
            csp_log_seen: false,
            await_met: false,
            version: Box::default(),
            stderr: Vec::new(),
        }
    }

    fn run(&mut self) {
        let deadline = self.clock.now().now.saturating_add(Duration::from_secs(30));
        for _ in 0..50_000_u32 {
            self.kernel.reap(&mut self.complete);
            let now = self.clock.now();
            assert!(
                now.now < deadline,
                "Chromium smoke test timed out at {:?}, stderr: {}",
                self.stage,
                String::from_utf8_lossy(&self.stderr)
            );
            self.browser_env.now = now.now;
            self.browser_env.wall = now.wall;
            self.io_env.now = now.now;
            self.io_env.wall = now.wall;
            self.pages.update_time(now.now, now.wall);
            while self.io.is_ready() {
                io::resume(&mut self.io, &self.io_env, &mut self.io_events, &mut self.submits);
            }
            while let Some(complete) = self.complete.pop() {
                if complete.op == ROOT_CLOSE {
                    assert_eq!(complete.result, Ok(Done::Nothing), "close the process root descriptor");
                } else {
                    io::up(&mut self.io, &self.io_env, complete, &mut self.io_events, &mut self.submits);
                }
            }
            while self.io.is_due(now.now) {
                io::fire(&mut self.io, &self.io_env, &mut self.io_events, &mut self.submits);
            }
            while let Some(event) = self.io_events.pop() {
                self.io_event(event);
            }
            self.browser_steps(now.now);
            while let Some(event) = self.events.pop() {
                self.browser_event(event);
            }
            self.drain_browser_down();
            while self.io.takes() {
                let Some(request) = self.io_requests.pop_front().or_else(|| self.pages.take()) else { break };
                io::down(&mut self.io, &self.io_env, request, &mut self.submits);
            }
            self.io.reclaim();
            if self.stage == Stage::Complete
                && self.child_closed
                && self.pages.is_closed()
                && self.io.is_empty()
                && self.kernel.in_flight() == 0
            {
                return;
            }
            let pending = self.io.is_ready()
                || !self.complete.is_empty()
                || !self.io_events.is_empty()
                || !self.events.is_empty()
                || !self.browser_down.is_empty()
                || !self.io_requests.is_empty()
                || self.pages.pending()
                || self.browser.work_pending();
            let wait = if pending {
                Wait::No
            } else {
                let mut until = deadline;
                if let Some(next) = self.io.next_deadline() {
                    until = until.min(next);
                }
                if let Some(next) = self.browser.next_deadline() {
                    until = until.min(next);
                }
                Wait::Until(until)
            };
            self.kernel.submit(&mut self.submits, wait);
        }
        panic!("Chromium smoke test did not settle at {:?}", self.stage);
    }

    fn browser_steps(&mut self, now: Time) {
        if self.pipes.is_none() {
            return;
        }
        for _ in 0..32_u32 {
            let due = self.browser.next_deadline().is_some_and(|at| at <= now);
            if !self.browser.work_pending() && !due {
                break;
            }
            browser::fire(&mut self.browser, &self.browser_env, now, &mut self.events, &mut self.browser_down);
        }
    }

    fn io_event(&mut self, event: io::Event) {
        if matches!(
            &event,
            io::Event::Listening { owner: serve::OWNER, .. } | io::Event::Accepted { owner: serve::OWNER, .. }
        ) || matches!(&event, io::Event::Stream { owner, .. } | io::Event::Closed { owner } | io::Event::Failed { owner, .. } if self.pages.owns(*owner))
        {
            self.pages.event(event);
            self.open_if_ready();
            return;
        }
        match event {
            io::Event::Spawned { owner: PROCESS, child, pipes } => {
                let [commands, replies, errors] = pipes.as_ref() else {
                    panic!("Chromium needs pipes 3, 4 and stderr")
                };
                self.pipes = Some([*commands, *replies, *errors]);
                self.child = Some(child);
                self.submits.push(Submit { op: ROOT_CLOSE, kind: Op::Close { fd: self.root } });
                self.root_closed = true;
            }
            io::Event::Spawned { owner, .. } => panic!("unexpected spawned owner {owner:?}"),
            io::Event::Output { .. } => panic!("Chromium uses only classic output"),
            io::Event::Stream { owner, up } => {
                let [commands, replies, errors] = self.pipes.expect("pipe event after spawn");
                if owner == errors
                    && let stream::Up::Bytes(bytes) = &up
                {
                    self.stderr.extend_from_slice(bytes);
                    if self.stderr.len() > 32_768 {
                        let excess = self.stderr.len() - 32_768;
                        self.stderr.drain(..excess);
                    }
                }
                let context_reply = owner == replies
                    && match &up {
                        stream::Up::Bytes(bytes) => {
                            bytes.windows(b"browserContextId".len()).any(|part| part == b"browserContextId")
                        }
                        stream::Up::Room | stream::Up::End | stream::Up::Failed(_) => false,
                    };
                let from = if owner == commands {
                    Below::Commands(up)
                } else if owner == replies {
                    Below::Replies(up)
                } else if owner == errors {
                    Below::Errors(up)
                } else {
                    panic!("unknown Chromium pipe {owner:?}")
                };
                browser::up(&mut self.browser, &self.browser_env, from, &mut self.events, &mut self.browser_down);
                if context_reply && self.stage == Stage::Context {
                    self.context_ready = true;
                    self.open_if_ready();
                }
            }
            io::Event::Exited { owner: PROCESS, exit } => {
                assert_eq!(
                    exit,
                    kernel::Exit::Code(0),
                    "Chromium exited with {exit:?}; stderr: {}",
                    String::from_utf8_lossy(&self.stderr)
                );
                self.exited = true;
                for pipe in self.pipes.expect("spawned pipes") {
                    self.io_requests.push_back(io::Request::Close { entity: pipe });
                }
            }
            io::Event::Closed { owner: PROCESS } => self.child_closed = true,
            io::Event::Failed { owner: PROCESS, error } => panic!("Chromium spawn failed: {error:?}"),
            io::Event::Closed { .. } => {}
            io::Event::Listening { .. }
            | io::Event::Accepted { .. }
            | io::Event::Connecting { .. }
            | io::Event::Connected { .. }
            | io::Event::Exited { .. }
            | io::Event::Shutdown { .. }
            | io::Event::Failed { .. } => panic!("unexpected io event: {event:?}"),
        }
    }

    fn open_if_ready(&mut self) {
        if self.stage != Stage::Context || !self.context_ready {
            return;
        }
        let Some(addr) = self.pages.addr else { return };
        self.stage = Stage::Opening;
        let url = format!("http://{addr}{}", self.scenario.path());
        self.ask(Request::Page { person: PERSON, page: PAGE, url: url.into_bytes().into_boxed_slice() });
    }

    fn ask(&mut self, request: Request) {
        browser::down(&mut self.browser, &self.browser_env, request, &mut self.events, &mut self.browser_down);
    }

    fn browser_event(&mut self, event: Event) {
        match event {
            Event::Ready { version } => {
                assert_eq!(self.stage, Stage::Starting);
                assert!(!version.is_empty(), "Chromium gives its version");
                self.version = version;
                self.stage = Stage::Context;
                self.ask(Request::Person { person: PERSON });
            }
            Event::Opened { page } => {
                assert_eq!(page, PAGE);
                assert_eq!(self.stage, Stage::Opening);
                self.stage = Stage::Finding;
                match self.scenario {
                    Scenario::Button => {
                        self.ask(Request::Find { page: PAGE, op: FIND, query: query(b"button", b"Press") })
                    }
                    Scenario::Form => {
                        self.ask(Request::Find { page: PAGE, op: FIND, query: query(b"textbox", b"Name") })
                    }
                    Scenario::Covered => {
                        self.ask(Request::Find { page: PAGE, op: FIND, query: query(b"button", b"Covered") })
                    }
                    Scenario::Scroll => {
                        self.ask(Request::Find { page: PAGE, op: FIND, query: query(b"button", b"Far away") })
                    }
                    Scenario::Throw => {
                        self.ask(Request::Find { page: PAGE, op: FIND, query: query(b"button", b"Throw") })
                    }
                    Scenario::Csp => {
                        self.ask(Request::Find { page: PAGE, op: FIND, query: query(b"heading", b"CSP page") })
                    }
                }
            }
            Event::Found { op, seen, more } => {
                assert_eq!(op, FIND);
                assert_eq!(self.stage, Stage::Finding);
                assert_eq!(more, 0);
                assert_eq!(seen.len(), 1, "one accessible target for {:?}", self.scenario);
                let node = seen.get(0).expect("target").node;
                match self.scenario {
                    Scenario::Form => {
                        self.stage = Stage::Typing;
                        self.ask(Request::Type { page: PAGE, op: PRESS, node, text: Box::from(&b"Ada"[..]) });
                    }
                    Scenario::Csp => {
                        self.stage = Stage::Expecting;
                        self.ask(Request::Await {
                            page: PAGE,
                            op: EXPECT,
                            query: query(b"heading", b"Wrong"),
                            expect: Expect::Absent,
                            within: Duration::from_millis(500),
                        });
                    }
                    Scenario::Button | Scenario::Covered | Scenario::Scroll | Scenario::Throw => {
                        self.stage = Stage::Pressing;
                        self.ask(Request::Press { page: PAGE, op: PRESS, node });
                    }
                }
            }
            Event::Done { op } => match self.stage {
                Stage::Typing => {
                    assert_eq!(op, PRESS);
                    self.stage = Stage::Keying;
                    self.ask(Request::Key { page: PAGE, op: KEY, key: Key::Enter });
                }
                Stage::Keying | Stage::Pressing => {
                    assert_eq!(op, if self.stage == Stage::Keying { KEY } else { PRESS });
                    self.stage = Stage::Expecting;
                    let heading = match self.scenario {
                        Scenario::Button => b"Done".as_slice(),
                        Scenario::Form => b"Hello Ada".as_slice(),
                        Scenario::Scroll => b"Scrolled".as_slice(),
                        Scenario::Throw => b"Before".as_slice(),
                        Scenario::Covered | Scenario::Csp => {
                            panic!("no action should complete for {:?}", self.scenario)
                        }
                    };
                    self.ask(Request::Await {
                        page: PAGE,
                        op: EXPECT,
                        query: query(b"heading", heading),
                        expect: Expect::Present,
                        within: Duration::from_secs(3),
                    });
                }
                _ => panic!("unexpected Done {op:?} at {:?}", self.stage),
            },
            Event::Met { op, seen } => {
                assert_eq!(op, EXPECT);
                assert_eq!(self.stage, Stage::Expecting);
                assert_eq!(seen.len(), if matches!(self.scenario, Scenario::Csp) { 0 } else { 1 });
                if matches!(self.scenario, Scenario::Throw | Scenario::Csp) {
                    self.await_met = true;
                    if (matches!(self.scenario, Scenario::Throw) && self.exception_seen)
                        || (matches!(self.scenario, Scenario::Csp) && self.csp_log_seen)
                    {
                        self.close_browser();
                    }
                } else if matches!(self.scenario, Scenario::Button | Scenario::Form) {
                    self.stage = Stage::Capturing;
                    self.ask(if matches!(self.scenario, Scenario::Button) {
                        Request::Screenshot { page: PAGE, op: IMAGE }
                    } else {
                        Request::Snapshot { page: PAGE, op: SNAPSHOT }
                    });
                } else {
                    self.close_browser();
                }
            }
            Event::Screenshot { op: IMAGE, png } => {
                assert_eq!(self.stage, Stage::Capturing);
                assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
                self.close_browser();
            }
            Event::Snapshot { op: SNAPSHOT, text } => {
                assert_eq!(self.stage, Stage::Capturing);
                assert!(text.windows(b"Hello Ada".len()).any(|part| part == b"Hello Ada"));
                self.close_browser();
            }
            Event::Closed { owner: BROWSER } => {
                assert_eq!(self.stage, Stage::Closing);
                self.stage = Stage::Complete;
                self.pages.stop();
            }
            Event::Closed { owner } if owner == PAGE || owner == PERSON => {}
            Event::Trouble { trouble: Trouble::Log, .. } if matches!(self.scenario, Scenario::Csp) => {
                self.csp_log_seen = true;
                if self.await_met && self.stage == Stage::Expecting {
                    self.close_browser();
                }
            }
            Event::Trouble { trouble: Trouble::Exception, text, .. } if matches!(self.scenario, Scenario::Throw) => {
                assert!(!text.is_empty(), "exception has a message");
                self.exception_seen = true;
                if self.await_met && self.stage == Stage::Expecting {
                    self.close_browser();
                }
            }
            Event::Trouble { trouble, text, .. } => {
                panic!("unexpected page trouble {trouble:?}: {}", String::from_utf8_lossy(&text))
            }
            Event::Refused { op, why } => {
                if matches!(self.scenario, Scenario::Covered)
                    && self.stage == Stage::Pressing
                    && op == PRESS
                    && why == Refusal::Covered
                {
                    self.close_browser();
                    return;
                }
                panic!("browser refused operation {op:?}: {why:?}; stderr: {}", String::from_utf8_lossy(&self.stderr))
            }
            Event::Missed { op, seen, more } => panic!("expectation {op:?} missed; saw {seen:?}, {more} more"),
            other => panic!("unexpected browser event: {other:?}"),
        }
    }

    fn close_browser(&mut self) {
        self.stage = Stage::Closing;
        self.ask(Request::Close { entity: BROWSER });
    }

    fn drain_browser_down(&mut self) {
        while let Some(down) = self.browser_down.pop() {
            let [commands, replies, errors] = self.pipes.expect("browser output follows spawn");
            let (pipe, down) = match down {
                Down::Commands(down) => (commands, down),
                Down::Replies(down) => (replies, down),
                Down::Errors(down) => (errors, down),
            };
            self.io_requests.push_back(io::Request::Stream { stream: pipe, down });
        }
    }
}

fn query(role: &[u8], name: &[u8]) -> Query {
    Query { role: Box::from(role), name: Box::from(name), within: None, boxes: false }
}

#[test]
fn chromium_opens_finds_and_presses_a_button() {
    let chromium = chromium();
    let profile = Profile::fresh();
    let mut rig = Rig::new(&chromium, &profile.0, Scenario::Button);
    rig.run();
    assert!(rig.root_closed && rig.exited && rig.child_closed);
    assert!(rig.version.starts_with(b"Chrome/"), "version: {}", String::from_utf8_lossy(&rig.version));
}

#[test]
fn chromium_types_submits_and_snapshots_a_form() {
    let chromium = chromium();
    let profile = Profile::fresh();
    Rig::new(&chromium, &profile.0, Scenario::Form).run();
}

#[test]
fn chromium_rejects_a_covered_button() {
    let chromium = chromium();
    let profile = Profile::fresh();
    Rig::new(&chromium, &profile.0, Scenario::Covered).run();
}

#[test]
fn chromium_scrolls_to_a_far_button() {
    let chromium = chromium();
    let profile = Profile::fresh();
    Rig::new(&chromium, &profile.0, Scenario::Scroll).run();
}

#[test]
fn chromium_obeys_content_security_policy() {
    let chromium = chromium();
    let profile = Profile::fresh();
    Rig::new(&chromium, &profile.0, Scenario::Csp).run();
}

#[test]
fn chromium_reports_a_script_exception() {
    let chromium = chromium();
    let profile = Profile::fresh();
    Rig::new(&chromium, &profile.0, Scenario::Throw).run();
}
