//! The browser and its operations, each advanced by one reply or event.

#![expect(
    clippy::disallowed_types,
    reason = "bounded entity and command tables reuse Vec slots; capacities are fixed at construction"
)]
#![expect(
    clippy::manual_find,
    clippy::manual_map,
    clippy::match_like_matches_macro,
    reason = "the step subset excludes closure-based combinators and the matches macro"
)]

use alloc::boxed::Box;
use alloc::collections::VecDeque;
use alloc::vec::Vec;

use skein_lib::stream::{self, Delimiter, Read};
use skein_lib::{Env, List, Queue, Time, Token};

use crate::boundary::{Below, Down, Event, Expect, Go, Key, Query, Rect, Refusal, Seen, States, Trouble};
use crate::limits::Limits;
use crate::params::Params;
use crate::wire::decode::{Document, Value};
use crate::wire::encode;
use crate::wire::frame::Framer;

/// Each step emits at most one event and two stream requests. Further
/// terminals are retained for the next call to `fire`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MaxOut {
    pub above: u32,
    pub below: u32,
}

pub const UP_MAX_OUT: MaxOut = MaxOut { above: 1, below: 3 };
pub const DOWN_MAX_OUT: MaxOut = MaxOut { above: 1, below: 2 };
pub const FIRE_MAX_OUT: MaxOut = MaxOut { above: 1, below: 3 };

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Starting,
    Ready,
    Closing,
    Closed,
}

#[derive(Debug)]
struct Person {
    token: Token,
    context: Option<Box<[u8]>>,
    closed: bool,
}

#[derive(Debug)]
struct Page {
    token: Token,
    person: Token,
    target: Option<Box<[u8]>>,
    session: Option<Box<[u8]>>,
    initial_url: Box<[u8]>,
    root: Option<u64>,
    opening: bool,
    load: Option<Load>,
    closed: bool,
}

#[derive(Clone, Copy, Debug)]
enum Load {
    Opening,
    Go(Token),
}

#[derive(Debug)]
struct Operation {
    token: Token,
    page: Token,
    kind: OpKind,
    due: Option<Time>,
}

#[derive(Debug)]
enum OpKind {
    Go(Go),
    Find { query: Query, expect: Option<(Expect, Time)>, seen: List<Seen>, more: u32, next: u32 },
    Press { node: u64, at: Option<(i64, i64)>, hit: Option<u64> },
    Type { text: Box<[u8]>, compose: bool },
    Key(Key),
    Snapshot,
    Screenshot,
}

fn matches_go(kind: &OpKind) -> bool {
    if let OpKind::Go(_) = kind { true } else { false }
}

#[derive(Debug)]
struct Pending {
    id: u64,
    due: Time,
    kind: PendingKind,
}

#[derive(Clone, Copy, Debug)]
enum PendingKind {
    Version,
    Context(Token),
    Target(Token),
    Attach(Token),
    Enable(Token, u8),
    Viewport(Token),
    Navigate(Token),
    Step(Token, Step),
    CloseBrowser,
    ClosePage(Token),
    ClosePerson(Token),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    History,
    Go,
    Query,
    Box,
    Scroll,
    Accessibility,
    Quads,
    Hit,
    Describe,
    MouseMove,
    MouseDown,
    MouseUp,
    Focus,
    Insert,
    KeyDown,
    KeyUp,
    Snapshot,
    Screenshot,
}

/// A Chromium pipe peer. The owner drives `fire` at `next_deadline` and
/// whenever `work_pending` is true, and settles the child after `Closed`.
#[expect(clippy::struct_excessive_bools, reason = "each pipe demand and room grant is independent")]
#[derive(Debug)]
pub struct Browser {
    owner: Token,
    phase: Phase,
    persons: Vec<Option<Person>>,
    pages: Vec<Option<Page>>,
    ops: Vec<Option<Operation>>,
    pending: Vec<Pending>,
    next_id: u64,
    framing: Framer,
    outgoing: VecDeque<Box<[u8]>>,
    events: VecDeque<Event>,
    room: bool,
    command_demanded: bool,
    replies_demanded: bool,
    errors_demanded: bool,
    stderr: Vec<u8>,
}

impl Browser {
    /// The browser token is also the owner of its terminal `Closed` event.
    #[must_use]
    pub fn new(owner: Token, limits: &Limits) -> Browser {
        assert!(limits.commands > 0 && limits.command > 0 && limits.message > 0, "the wire has positive limits");
        Browser {
            owner,
            phase: Phase::Starting,
            persons: Vec::with_capacity(usize::try_from(limits.persons).expect("u32 fits usize")),
            pages: Vec::with_capacity(usize::try_from(limits.pages).expect("u32 fits usize")),
            ops: Vec::with_capacity(usize::try_from(limits.ops).expect("u32 fits usize")),
            pending: Vec::with_capacity(usize::try_from(limits.commands).expect("u32 fits usize")),
            next_id: 1,
            framing: Framer::new(limits.message),
            outgoing: VecDeque::with_capacity(usize::try_from(limits.commands).expect("u32 fits usize")),
            events: VecDeque::with_capacity(
                usize::try_from(
                    limits.persons.saturating_add(limits.pages).saturating_add(limits.ops).saturating_add(4),
                )
                .expect("u32 fits usize"),
            ),
            room: false,
            command_demanded: false,
            replies_demanded: false,
            errors_demanded: false,
            stderr: Vec::with_capacity(usize::try_from(limits.stderr).expect("u32 fits usize")),
        }
    }

    #[must_use]
    pub fn work_pending(&self) -> bool {
        !self.events.is_empty()
            || (self.phase == Phase::Starting && self.pending.is_empty())
            || (self.room && !self.outgoing.is_empty())
    }

    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        if self.work_pending() {
            return Some(Time::ZERO);
        }
        let mut next: Option<Time> = None;
        for pending in &self.pending {
            next = Some(match next {
                Some(at) => at.min(pending.due),
                None => pending.due,
            });
        }
        for op in &self.ops {
            let at = match op {
                Some(op) => op.due,
                None => None,
            };
            if let Some(at) = at {
                next = Some(match next {
                    Some(old) => old.min(at),
                    None => at,
                });
            }
        }
        next
    }

    #[must_use]
    pub fn stderr_tail(&self) -> &[u8] {
        &self.stderr
    }

    pub fn reclaim(&mut self) {
        for op in &mut self.ops {
            let done_go = match op {
                Some(op) => matches_go(&op.kind) && op.due == Some(Time::ZERO),
                None => false,
            };
            if done_go {
                *op = None;
            }
        }
    }

    fn person(&self, token: Token) -> Option<&Person> {
        for person in self.persons.iter().flatten() {
            if person.token == token && !person.closed {
                return Some(person);
            }
        }
        None
    }
    fn person_mut(&mut self, token: Token) -> Option<&mut Person> {
        for person in self.persons.iter_mut().flatten() {
            if person.token == token && !person.closed {
                return Some(person);
            }
        }
        None
    }
    fn page(&self, token: Token) -> Option<&Page> {
        for page in self.pages.iter().flatten() {
            if page.token == token && !page.closed {
                return Some(page);
            }
        }
        None
    }
    fn page_mut(&mut self, token: Token) -> Option<&mut Page> {
        for page in self.pages.iter_mut().flatten() {
            if page.token == token && !page.closed {
                return Some(page);
            }
        }
        None
    }
    fn op(&self, token: Token) -> Option<&Operation> {
        for op in self.ops.iter().flatten() {
            if op.token == token {
                return Some(op);
            }
        }
        None
    }
    fn op_mut(&mut self, token: Token) -> Option<&mut Operation> {
        for op in self.ops.iter_mut().flatten() {
            if op.token == token {
                return Some(op);
            }
        }
        None
    }
    fn remove_op(&mut self, token: Token) -> Option<Operation> {
        for op in &mut self.ops {
            if let Some(candidate) = op
                && candidate.token == token
            {
                return op.take();
            }
        }
        None
    }
    fn insert_person(&mut self, person: Person, limit: u32) -> bool {
        for slot in &mut self.persons {
            if slot.is_none() {
                *slot = Some(person);
                return true;
            }
        }
        if self.persons.len() >= usize::try_from(limit).expect("u32 fits usize") {
            return false;
        }
        self.persons.push(Some(person));
        true
    }
    fn insert_page(&mut self, page: Page, limit: u32) -> bool {
        for slot in &mut self.pages {
            if slot.is_none() {
                *slot = Some(page);
                return true;
            }
        }
        if self.pages.len() >= usize::try_from(limit).expect("u32 fits usize") {
            return false;
        }
        self.pages.push(Some(page));
        true
    }
    fn insert_op(&mut self, op: Operation, limit: u32) -> bool {
        for slot in &mut self.ops {
            if slot.is_none() {
                *slot = Some(op);
                return true;
            }
        }
        if self.ops.len() >= usize::try_from(limit).expect("u32 fits usize") {
            return false;
        }
        self.ops.push(Some(op));
        true
    }

    fn queue_command(
        &mut self,
        env: &Env<Limits>,
        method: &[u8],
        session: Option<&[u8]>,
        args: &Params<'_>,
        kind: PendingKind,
    ) -> bool {
        if self.pending.len() >= usize::try_from(env.limits.commands).expect("u32 fits usize") {
            return false;
        }
        let Some(params) = args.encode(env.limits.command) else {
            return false;
        };
        let Some(id) = self.next_id.checked_add(1) else {
            return false;
        };
        let Ok(bytes) = encode::command(self.next_id, method, session, &params, env.limits.command) else {
            return false;
        };
        self.pending.push(Pending { id: self.next_id, due: env.now.saturating_add(env.limits.answer), kind });
        self.next_id = id;
        self.outgoing.push_back(bytes);
        true
    }

    fn queue_page(
        &mut self,
        env: &Env<Limits>,
        page: Token,
        method: &[u8],
        args: &Params<'_>,
        kind: PendingKind,
    ) -> bool {
        let session = match self.page(page) {
            Some(page) => page.session.clone(),
            None => None,
        };
        self.queue_command(env, method, session.as_deref(), args, kind)
    }

    fn flush(&mut self, below: &mut Queue<Down>, limit: u32) {
        if self.room
            && let Some(bytes) = self.outgoing.pop_front()
        {
            below.push(Down::Commands(stream::Down::Send(bytes)));
            self.room = false;
            below.push(Down::Commands(stream::Down::Demand { read: Read::Nothing, room: limit.saturating_add(1) }));
            self.command_demanded = true;
        }
    }

    fn emit(&mut self, above: &mut Queue<Event>) {
        if let Some(event) = self.events.pop_front() {
            above.push(event);
        }
    }

    fn start(&mut self, env: &Env<Limits>, below: &mut Queue<Down>) {
        if self.phase != Phase::Starting || !self.pending.is_empty() {
            return;
        }
        let sent = self.queue_command(env, b"Browser.getVersion", None, &Params::Empty, PendingKind::Version);
        assert!(sent, "version command fits startup limits");
        if !self.command_demanded {
            below.push(Down::Commands(stream::Down::Demand {
                read: Read::Nothing,
                room: env.limits.command.saturating_add(1),
            }));
            self.command_demanded = true;
        }
        if !self.replies_demanded {
            let nul = Delimiter::new(b"\0").expect("NUL is a delimiter");
            below.push(Down::Replies(stream::Down::Demand {
                read: Read::Scan { until: nul, max: env.limits.message.saturating_add(1) },
                room: 0,
            }));
            self.replies_demanded = true;
        }
        if !self.errors_demanded {
            below.push(Down::Errors(stream::Down::Demand {
                read: Read::Line { max: env.limits.stderr.max(1) },
                room: 0,
            }));
            self.errors_demanded = true;
        }
    }

    #[expect(clippy::too_many_lines, reason = "one match dispatches each public browser request")]
    fn request(&mut self, env: &Env<Limits>, request: crate::boundary::Request) {
        use crate::boundary::Request;
        match request {
            Request::Person { person } => {
                if self.phase != Phase::Ready || self.person(person).is_some() {
                    self.events.push_back(Event::Closed { owner: person });
                    return;
                }
                let ok = self.insert_person(Person { token: person, context: None, closed: false }, env.limits.persons);
                if !ok
                    || !self.queue_command(
                        env,
                        b"Target.createBrowserContext",
                        None,
                        &Params::Empty,
                        PendingKind::Context(person),
                    )
                {
                    self.close_person_local(person);
                }
            }
            Request::Page { person, page, url } => {
                let context = match self.person(person) {
                    Some(person) => person.context.clone(),
                    None => None,
                };
                let Some(context) = context else {
                    self.events.push_back(Event::Closed { owner: page });
                    return;
                };
                if self.page(page).is_some() {
                    self.events.push_back(Event::Closed { owner: page });
                    return;
                }
                let added = self.insert_page(
                    Page {
                        token: page,
                        person,
                        target: None,
                        session: None,
                        initial_url: url,
                        root: None,
                        opening: true,
                        load: None,
                        closed: false,
                    },
                    env.limits.pages,
                );
                if !added
                    || !self.queue_command(
                        env,
                        b"Target.createTarget",
                        None,
                        &Params::Target { url: b"about:blank", context: &context },
                        PendingKind::Target(page),
                    )
                {
                    self.close_page_local(page);
                }
            }
            Request::Go { page, op, to } => {
                if !self.admit_op(env, page, op, OpKind::Go(to)) {
                    return;
                }
                self.start_go(env, op);
            }
            Request::Find { page, op, query } => {
                let seen = List::with_capacity(env.limits.matches);
                if !self.admit_op(env, page, op, OpKind::Find { query, expect: None, seen, more: 0, next: 0 }) {
                    return;
                }
                self.start_find(env, op);
            }
            Request::Await { page, op, query, expect, within } => {
                let seen = List::with_capacity(env.limits.matches);
                let until = env.now.saturating_add(within);
                if !self.admit_op(
                    env,
                    page,
                    op,
                    OpKind::Find { query, expect: Some((expect, until)), seen, more: 0, next: 0 },
                ) {
                    return;
                }
                self.start_find(env, op);
            }
            Request::Press { page, op, node } => {
                if !self.admit_op(env, page, op, OpKind::Press { node, at: None, hit: None }) {
                    return;
                }
                self.issue_step(env, op, b"DOM.scrollIntoViewIfNeeded", &Params::Backend(node), Step::Scroll);
            }
            Request::Type { page, op, node, text } => {
                if !self.admit_op(env, page, op, OpKind::Type { text, compose: false }) {
                    return;
                }
                self.issue_step(env, op, b"DOM.focus", &Params::Backend(node), Step::Focus);
            }
            Request::Compose { page, op, node, text } => {
                if !self.admit_op(env, page, op, OpKind::Type { text, compose: true }) {
                    return;
                }
                self.issue_step(env, op, b"DOM.focus", &Params::Backend(node), Step::Focus);
            }
            Request::Key { page, op, key } => {
                if !self.admit_op(env, page, op, OpKind::Key(key)) {
                    return;
                }
                self.issue_key(env, op, key, true);
            }
            Request::Snapshot { page, op } => {
                if !self.admit_op(env, page, op, OpKind::Snapshot) {
                    return;
                }
                self.issue_step(env, op, b"Accessibility.getFullAXTree", &Params::Empty, Step::Snapshot);
            }
            Request::Screenshot { page, op } => {
                if !self.admit_op(env, page, op, OpKind::Screenshot) {
                    return;
                }
                self.issue_step(env, op, b"Page.captureScreenshot", &Params::Empty, Step::Screenshot);
            }
            Request::Close { entity } => self.close(env, entity),
        }
    }

    fn admit_op(&mut self, env: &Env<Limits>, page: Token, token: Token, kind: OpKind) -> bool {
        let unavailable = match self.page(page) {
            Some(page) => page.opening,
            None => true,
        };
        if self.phase != Phase::Ready || unavailable || self.op(token).is_some() {
            self.events.push_back(Event::Refused { op: token, why: Refusal::Gone });
            return false;
        }
        let accepted = self.insert_op(Operation { token, page, kind, due: None }, env.limits.ops);
        if !accepted {
            self.events.push_back(Event::Refused { op: token, why: Refusal::Limit });
        }
        accepted
    }

    fn start_go(&mut self, env: &Env<Limits>, op: Token) {
        let Some(op_state) = self.op(op) else {
            return;
        };
        let OpKind::Go(to) = &op_state.kind else {
            return;
        };
        let page = op_state.page;
        let to = to.clone();
        if let Some(page_state) = self.page_mut(page) {
            page_state.root = None;
            if let Go::Address(_) | Go::Reload = to {
                page_state.load = Some(Load::Go(op));
            }
        }
        match to {
            Go::Address(url) => self.issue_step(env, op, b"Page.navigate", &Params::Url(&url), Step::Go),
            Go::Reload => self.issue_step(env, op, b"Page.reload", &Params::Empty, Step::Go),
            Go::Back | Go::Forward => {
                self.issue_step(env, op, b"Page.getNavigationHistory", &Params::Empty, Step::History);
            }
        }
    }

    fn start_find(&mut self, env: &Env<Limits>, op: Token) {
        let Some(op_state) = self.op(op) else {
            return;
        };
        let page_token = op_state.page;
        let OpKind::Find { query, .. } = &op_state.kind else {
            return;
        };
        let query = query.clone();
        let root = match query.within {
            Some(root) => Some(root),
            None => match self.page(page_token) {
                Some(page) => page.root,
                None => None,
            },
        };
        match root {
            Some(root) => self.issue_step(
                env,
                op,
                b"Accessibility.queryAXTree",
                &Params::Query { root, role: &query.role, name: &query.name },
                Step::Query,
            ),
            None => self.issue_step(env, op, b"DOM.getDocument", &Params::Empty, Step::Query),
        }
    }

    fn issue_step(&mut self, env: &Env<Limits>, op: Token, method: &[u8], params: &Params<'_>, step: Step) {
        let Some(op_state) = self.op(op) else {
            return;
        };
        let page = op_state.page;
        if !self.queue_page(env, page, method, params, PendingKind::Step(op, step)) {
            self.finish_refused(op, Refusal::Limit);
        }
    }

    fn issue_key(&mut self, env: &Env<Limits>, op: Token, key: Key, down: bool) {
        let (name, code, windows, text) = key_info(key);
        let kind = if down { b"keyDown".as_slice() } else { b"keyUp".as_slice() };
        let step = if down { Step::KeyDown } else { Step::KeyUp };
        self.issue_step(
            env,
            op,
            b"Input.dispatchKeyEvent",
            &Params::Key { kind, key: name, code, windows, text },
            step,
        );
    }

    fn finish_refused(&mut self, op: Token, why: Refusal) {
        if self.remove_op(op).is_some() {
            self.events.push_back(Event::Refused { op, why });
        }
    }
    fn finish_done(&mut self, op: Token) {
        if self.remove_op(op).is_some() {
            self.events.push_back(Event::Done { op });
        }
    }

    fn close_page_local(&mut self, token: Token) {
        let mut closing = Vec::new();
        for op in self.ops.iter().flatten() {
            if op.page == token {
                closing.push(op.token);
            }
        }
        for op in closing {
            self.finish_refused(op, Refusal::Crashed);
        }
        if let Some(page) = self.page_mut(token) {
            page.closed = true;
            self.events.push_back(Event::Closed { owner: token });
        }
    }
    fn close_person_local(&mut self, token: Token) {
        let mut closing = Vec::new();
        for page in self.pages.iter().flatten() {
            if page.person == token && !page.closed {
                closing.push(page.token);
            }
        }
        for page in closing {
            self.close_page_local(page);
        }
        if let Some(person) = self.person_mut(token) {
            person.closed = true;
            self.events.push_back(Event::Closed { owner: token });
        }
    }
    fn close_browser_local(&mut self) {
        if self.phase == Phase::Closed {
            return;
        }
        let mut closing = Vec::new();
        for person in self.persons.iter().flatten() {
            if !person.closed {
                closing.push(person.token);
            }
        }
        for person in closing {
            self.close_person_local(person);
        }
        self.phase = Phase::Closed;
        self.events.push_back(Event::Closed { owner: self.owner });
    }
    fn close(&mut self, env: &Env<Limits>, entity: Token) {
        if entity == self.owner {
            if self.phase == Phase::Closed || self.phase == Phase::Closing {
                return;
            }
            self.phase = Phase::Closing;
            if !self.queue_command(env, b"Browser.close", None, &Params::Empty, PendingKind::CloseBrowser) {
                self.close_browser_local();
            }
            return;
        }
        let target = match self.page(entity) {
            Some(page) => page.target.clone(),
            None => None,
        };
        if let Some(target) = target {
            if !self.queue_command(
                env,
                b"Target.closeTarget",
                None,
                &Params::TargetId(&target),
                PendingKind::ClosePage(entity),
            ) {
                self.close_page_local(entity);
            }
            return;
        }
        let context = match self.person(entity) {
            Some(person) => person.context.clone(),
            None => None,
        };
        if let Some(context) = context
            && !self.queue_command(
                env,
                b"Target.disposeBrowserContext",
                None,
                &Params::Context(&context),
                PendingKind::ClosePerson(entity),
            )
        {
            self.close_person_local(entity);
        }
    }
}

fn field_text(value: Value<'_>, key: &[u8]) -> Option<Box<[u8]>> {
    value.get(key)?.text()
}

fn field_u64(value: Value<'_>, key: &[u8]) -> Option<u64> {
    value.get(key)?.u64()
}

fn field_i64(value: Value<'_>, key: &[u8]) -> Option<i64> {
    value.get(key)?.i64()
}

fn field_array<'a>(value: Value<'a>, key: &[u8]) -> Option<crate::wire::decode::Array<'a>> {
    value.get(key)?.array()
}

fn opt_field<'a>(value: Option<Value<'a>>, key: &[u8]) -> Option<Value<'a>> {
    value?.get(key)
}

fn opt_text(value: Option<Value<'_>>) -> Option<Box<[u8]>> {
    value?.text()
}

fn opt_bool(value: Option<Value<'_>>) -> Option<bool> {
    value?.bool()
}

fn opt_array(value: Option<Value<'_>>) -> Option<crate::wire::decode::Array<'_>> {
    value?.array()
}

fn parse_seen(node: Value<'_>, text_limit: u32) -> Option<Seen> {
    let backend = field_u64(node, b"backendDOMNodeId")?;
    let role = ax_text(node, b"role", text_limit);
    let name = ax_text(node, b"name", text_limit);
    let value = ax_text(node, b"value", text_limit);
    let mut states = States { focused: false, disabled: false, checked: false, expanded: false, selected: false };
    if let Some(properties) = field_array(node, b"properties") {
        for property in properties {
            let name = field_text(property, b"name");
            let value = ax_property_bool(property);
            match name.as_deref() {
                Some(b"focused") => states.focused = value,
                Some(b"disabled") => states.disabled = value,
                Some(b"checked") => states.checked = value,
                Some(b"expanded") => states.expanded = value,
                Some(b"selected") => states.selected = value,
                _ => {}
            }
        }
    }
    Some(Seen { node: backend, role, name, value, states, rect: None })
}

fn ax_text(node: Value<'_>, key: &[u8], limit: u32) -> Box<[u8]> {
    let mut text = opt_text(opt_field(node.get(key), b"value")).unwrap_or_default();
    let max = usize::try_from(limit).expect("u32 fits usize");
    if text.len() > max {
        text = Box::from(text.get(..max).expect("text is at least the limit"));
    }
    text
}

fn ax_disabled(node: Value<'_>) -> bool {
    if let Some(properties) = field_array(node, b"properties") {
        for property in properties {
            if field_text(property, b"name").as_deref() == Some(b"disabled") {
                return ax_property_bool(property);
            }
        }
    }
    false
}

fn ax_property_bool(property: Value<'_>) -> bool {
    opt_bool(opt_field(property.get(b"value"), b"value")).unwrap_or(false)
}

fn parse_rect(result: Value<'_>) -> Option<Rect> {
    let mut quads = result.get(b"quads")?.array()?;
    let quad = quads.next()?;
    let mut points = quad.array()?;
    let mut left = i64::MAX;
    let mut top = i64::MAX;
    let mut right = i64::MIN;
    let mut bottom = i64::MIN;
    for _ in 0_u8..4_u8 {
        let x = points.next()?.whole_pixels()?;
        let y = points.next()?.whole_pixels()?;
        left = left.min(x);
        right = right.max(x);
        top = top.min(y);
        bottom = bottom.max(y);
    }
    Some(Rect {
        x: left,
        y: top,
        width: right.checked_sub(left)?.try_into().ok()?,
        height: bottom.checked_sub(top)?.try_into().ok()?,
    })
}

fn subtree_has(root: Option<Value<'_>>, backend: u64) -> bool {
    let Some(root) = root else {
        return false;
    };
    let mut nodes = Vec::new();
    nodes.push(root);
    while let Some(node) = nodes.pop() {
        if field_u64(node, b"backendNodeId") == Some(backend) {
            return true;
        }
        for key in [b"children".as_slice(), b"shadowRoots".as_slice(), b"pseudoElements".as_slice()] {
            if let Some(children) = opt_array(node.get(key)) {
                for child in children {
                    nodes.push(child);
                }
            }
        }
    }
    false
}

fn snapshot(result: Value<'_>, limit: u32, text_limit: u32) -> Box<[u8]> {
    let mut out = Vec::new();
    let cap = usize::try_from(limit).expect("u32 fits usize");
    if let Some(nodes) = field_array(result, b"nodes") {
        for node in nodes {
            let role = ax_text(node, b"role", text_limit);
            let name = ax_text(node, b"name", text_limit);
            let value = ax_text(node, b"value", text_limit);
            for part in [role.as_ref(), b" ", name.as_ref(), b" ", value.as_ref(), b"\n"] {
                let room = cap.saturating_sub(out.len());
                if room == 0 {
                    break;
                }
                out.extend_from_slice(part.get(..room.min(part.len())).expect("within part"));
            }
            if out.len() == cap {
                break;
            }
        }
    }
    out.into_boxed_slice()
}

/// A request from the test.
pub fn down(
    browser: &mut Browser,
    env: &Env<Limits>,
    request: crate::boundary::Request,
    above: &mut Queue<Event>,
    below: &mut Queue<Down>,
) {
    if browser.phase == Phase::Closed {
        return;
    }
    browser.request(env, request);
    browser.flush(below, env.limits.command);
    browser.emit(above);
}

/// An event from a child pipe.
pub fn up(browser: &mut Browser, env: &Env<Limits>, event: Below, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    if browser.phase == Phase::Closed && browser.events.is_empty() {
        return;
    }
    match event {
        Below::Commands(stream::Up::Room) => {
            browser.room = true;
            browser.command_demanded = false;
        }
        Below::Commands(stream::Up::Failed(_) | stream::Up::End | stream::Up::Bytes(_))
        | Below::Replies(stream::Up::End | stream::Up::Failed(_) | stream::Up::Room) => browser.crash(),
        Below::Replies(stream::Up::Bytes(bytes)) => {
            browser.replies_demanded = false;
            match browser.framing.push(&bytes) {
                Ok(messages) => {
                    for message in messages {
                        browser.message(env, &message);
                    }
                }
                Err(_) => browser.crash(),
            }
            if browser.phase != Phase::Closed {
                let nul = Delimiter::new(b"\0").expect("NUL is a delimiter");
                below.push(Down::Replies(stream::Down::Demand {
                    read: Read::Scan { until: nul, max: env.limits.message.saturating_add(1) },
                    room: 0,
                }));
                browser.replies_demanded = true;
            }
        }
        Below::Errors(stream::Up::Bytes(bytes)) => {
            browser.errors_demanded = false;
            for byte in bytes {
                if browser.stderr.len() == usize::try_from(env.limits.stderr).expect("u32 fits usize")
                    && !browser.stderr.is_empty()
                {
                    browser.stderr.remove(0);
                }
                if env.limits.stderr > 0 {
                    browser.stderr.push(byte);
                }
            }
            if browser.phase != Phase::Closed {
                below.push(Down::Errors(stream::Down::Demand {
                    read: Read::Line { max: env.limits.stderr.max(1) },
                    room: 0,
                }));
                browser.errors_demanded = true;
            }
        }
        Below::Errors(stream::Up::End | stream::Up::Failed(_)) => browser.errors_demanded = false,
        Below::Errors(stream::Up::Room) => {}
    }
    if below.room() >= 2 {
        browser.flush(below, env.limits.command);
    }
    browser.emit(above);
}

/// One deadline or queued output, called while the loop reports work due.
pub fn fire(browser: &mut Browser, env: &Env<Limits>, now: Time, above: &mut Queue<Event>, below: &mut Queue<Down>) {
    if browser.phase == Phase::Starting && browser.pending.is_empty() {
        browser.start(env, below);
        browser.emit(above);
        return;
    }
    let mut expired = None;
    for (index, pending) in browser.pending.iter().enumerate() {
        if pending.due <= now {
            expired = Some(index);
            break;
        }
    }
    if let Some(index) = expired {
        let pending = browser.pending.swap_remove(index);
        match pending.kind {
            PendingKind::Step(op, _) => browser.finish_refused(op, Refusal::Timeout),
            PendingKind::Version | PendingKind::CloseBrowser => browser.crash(),
            PendingKind::Context(person) | PendingKind::ClosePerson(person) => browser.close_person_local(person),
            PendingKind::Target(page)
            | PendingKind::Attach(page)
            | PendingKind::Enable(page, _)
            | PendingKind::Viewport(page)
            | PendingKind::Navigate(page)
            | PendingKind::ClosePage(page) => browser.close_page_local(page),
        }
    } else {
        let mut due = None;
        for op in browser.ops.iter().flatten() {
            if let Some(at) = op.due
                && at <= now
            {
                due = Some(op.token);
                break;
            }
        }
        if let Some(op) = due {
            if let Some(op_state) = browser.op_mut(op) {
                op_state.due = None;
            }
            browser.start_find(env, op);
        }
    }
    if below.room() >= 2 {
        browser.flush(below, env.limits.command);
    }
    browser.emit(above);
}

fn key_info(key: Key) -> (&'static [u8], &'static [u8], u64, &'static [u8]) {
    match key {
        Key::Enter => (b"Enter", b"Enter", 13, b"\r"),
        Key::Escape => (b"Escape", b"Escape", 27, b""),
        Key::Tab => (b"Tab", b"Tab", 9, b"\t"),
        Key::Backspace => (b"Backspace", b"Backspace", 8, b""),
        Key::ArrowUp => (b"ArrowUp", b"ArrowUp", 38, b""),
        Key::ArrowDown => (b"ArrowDown", b"ArrowDown", 40, b""),
        Key::ArrowLeft => (b"ArrowLeft", b"ArrowLeft", 37, b""),
        Key::ArrowRight => (b"ArrowRight", b"ArrowRight", 39, b""),
    }
}

impl Browser {
    fn message(&mut self, env: &Env<Limits>, bytes: &[u8]) {
        let Ok(document) = Document::parse(bytes, env.limits.message) else {
            self.crash();
            return;
        };
        let root = document.root();
        if let Some(id) = field_u64(root, b"id") {
            let mut at = None;
            for (index, pending) in self.pending.iter().enumerate() {
                if pending.id == id {
                    at = Some(index);
                    break;
                }
            }
            let Some(at) = at else {
                self.crash();
                return;
            };
            let pending = self.pending.swap_remove(at);
            if let Some(error) = root.get(b"error") {
                self.command_error(pending.kind, error);
                return;
            }
            let Some(result) = root.get(b"result") else {
                self.command_error(pending.kind, root);
                return;
            };
            self.reply(env, pending.kind, result);
        } else if let Some(method) = field_text(root, b"method") {
            self.notification(env, &method, root);
        } else {
            self.crash();
        }
    }

    fn command_error(&mut self, kind: PendingKind, error: Value<'_>) {
        let message = field_text(error, b"message").unwrap_or_default();
        let why = if contains(&message, b"No node found at given location") {
            Refusal::Covered
        } else if contains(&message, b"No node found") {
            Refusal::Gone
        } else {
            Refusal::Protocol
        };
        match kind {
            PendingKind::Version | PendingKind::CloseBrowser => self.crash(),
            PendingKind::Context(person) | PendingKind::ClosePerson(person) => self.close_person_local(person),
            PendingKind::Target(page)
            | PendingKind::Attach(page)
            | PendingKind::Enable(page, _)
            | PendingKind::Viewport(page)
            | PendingKind::Navigate(page)
            | PendingKind::ClosePage(page) => self.close_page_local(page),
            PendingKind::Step(op, step) => {
                let reason = if step == Step::Hit && why == Refusal::Gone { Refusal::Covered } else { why };
                self.finish_refused(op, reason);
            }
        }
    }

    fn reply(&mut self, env: &Env<Limits>, kind: PendingKind, result: Value<'_>) {
        match kind {
            PendingKind::Version => {
                let version = field_text(result, b"product").unwrap_or_default();
                self.phase = Phase::Ready;
                self.events.push_back(Event::Ready { version });
            }
            PendingKind::Context(person) => {
                let context = field_text(result, b"browserContextId");
                if let (Some(person), Some(context)) = (self.person_mut(person), context) {
                    person.context = Some(context);
                } else {
                    self.close_person_local(person);
                }
            }
            PendingKind::Target(page) => {
                let target = field_text(result, b"targetId");
                if let Some(target) = target {
                    if let Some(page_state) = self.page_mut(page) {
                        page_state.target = Some(target.clone());
                    }
                    if !self.queue_command(
                        env,
                        b"Target.attachToTarget",
                        None,
                        &Params::TargetId(&target),
                        PendingKind::Attach(page),
                    ) {
                        self.close_page_local(page);
                    }
                } else {
                    self.close_page_local(page);
                }
            }
            PendingKind::Attach(page) => {
                let session = field_text(result, b"sessionId");
                if let Some(session) = session {
                    if let Some(page_state) = self.page_mut(page) {
                        page_state.session = Some(session);
                    }
                    self.enable(env, page, 0);
                } else {
                    self.close_page_local(page);
                }
            }
            PendingKind::Enable(page, index) => self.enable(env, page, index.saturating_add(1)),
            PendingKind::Viewport(page) => {
                let url = match self.page(page) {
                    Some(page) => Some(page.initial_url.clone()),
                    None => None,
                };
                if let Some(url) = url {
                    if let Some(page_state) = self.page_mut(page) {
                        page_state.load = Some(Load::Opening);
                    }
                    if !self.queue_page(env, page, b"Page.navigate", &Params::Url(&url), PendingKind::Navigate(page)) {
                        self.close_page_local(page);
                    }
                }
            }
            PendingKind::Navigate(page) => {
                if result.get(b"errorText").is_some() {
                    self.close_page_local(page);
                }
            }
            PendingKind::Step(op, step) => self.step_reply(env, op, step, result),
            PendingKind::CloseBrowser => self.close_browser_local(),
            PendingKind::ClosePage(page) => self.close_page_local(page),
            PendingKind::ClosePerson(person) => self.close_person_local(person),
        }
    }

    fn enable(&mut self, env: &Env<Limits>, page: Token, index: u8) {
        const DOMAINS: [&[u8]; 6] = [
            b"Page.enable",
            b"Runtime.enable",
            b"Log.enable",
            b"Accessibility.enable",
            b"DOM.enable",
            b"Inspector.enable",
        ];
        if let Some(method) = DOMAINS.get(usize::from(index)) {
            if !self.queue_page(env, page, method, &Params::Empty, PendingKind::Enable(page, index)) {
                self.close_page_local(page);
            }
        } else if !self.queue_page(
            env,
            page,
            b"Emulation.setDeviceMetricsOverride",
            &Params::Viewport { width: 1280, height: 800 },
            PendingKind::Viewport(page),
        ) {
            self.close_page_local(page);
        }
    }

    fn notification(&mut self, env: &Env<Limits>, method: &[u8], root: Value<'_>) {
        let session = field_text(root, b"sessionId");
        let mut page = None;
        for candidate in self.pages.iter().flatten() {
            if !candidate.closed && candidate.session.as_deref() == session.as_deref() {
                page = Some(candidate.token);
                break;
            }
        }
        let params = root.get(b"params");
        match method {
            b"Page.loadEventFired" | b"Page.navigatedWithinDocument" => {
                if let Some(page) = page {
                    self.loaded(page);
                }
            }
            b"Runtime.exceptionThrown" => {
                if let (Some(page), Some(params)) = (page, params) {
                    let text = opt_text(opt_field(params.get(b"exceptionDetails"), b"text")).unwrap_or_default();
                    self.trouble(page, Trouble::Exception, text, env.limits.text);
                }
            }
            b"Runtime.consoleAPICalled" => {
                if let (Some(page), Some(params)) = (page, params)
                    && field_text(params, b"type").as_deref() == Some(b"error")
                {
                    let first = match field_array(params, b"args") {
                        Some(mut args) => args.next(),
                        None => None,
                    };
                    let content = match first {
                        Some(arg) => match arg.get(b"value") {
                            Some(value) => Some(value),
                            None => arg.get(b"description"),
                        },
                        None => None,
                    };
                    let text = opt_text(content).unwrap_or_default();
                    self.trouble(page, Trouble::Console, text, env.limits.text);
                }
            }
            b"Log.entryAdded" => {
                if let (Some(page), Some(params)) = (page, params) {
                    let entry = params.get(b"entry");
                    if opt_text(opt_field(entry, b"level")).as_deref() == Some(b"error") {
                        let text = opt_text(opt_field(entry, b"text")).unwrap_or_default();
                        self.trouble(page, Trouble::Log, text, env.limits.text);
                    }
                }
            }
            b"Inspector.targetCrashed" => {
                if let Some(page) = page {
                    self.trouble(page, Trouble::Crash, Box::default(), env.limits.text);
                    self.close_page_local(page);
                }
            }
            b"Target.targetCrashed" => {
                if let Some(params) = params {
                    let target = field_text(params, b"targetId");
                    let mut page = None;
                    for candidate in self.pages.iter().flatten() {
                        if !candidate.closed && candidate.target.as_deref() == target.as_deref() {
                            page = Some(candidate.token);
                            break;
                        }
                    }
                    if let Some(page) = page {
                        self.trouble(page, Trouble::Crash, Box::default(), env.limits.text);
                        self.close_page_local(page);
                    }
                }
            }
            _ => {}
        }
    }

    fn trouble(&mut self, page: Token, trouble: Trouble, mut text: Box<[u8]>, limit: u32) {
        let limit = usize::try_from(limit).expect("u32 fits usize");
        if text.len() > limit {
            text = Box::from(text.get(..limit).expect("text is longer than limit"));
        }
        self.events.push_back(Event::Trouble { page, trouble, text });
    }

    fn loaded(&mut self, page: Token) {
        let load = match self.page_mut(page) {
            Some(page) => page.load.take(),
            None => None,
        };
        match load {
            Some(Load::Opening) => {
                if let Some(page_state) = self.page_mut(page) {
                    page_state.opening = false;
                }
                self.events.push_back(Event::Opened { page });
            }
            Some(Load::Go(op)) => self.finish_done(op),
            None => {}
        }
    }

    fn crash(&mut self) {
        self.pending.clear();
        self.outgoing.clear();
        self.close_browser_local();
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    for window in haystack.windows(needle.len()) {
        if window == needle {
            return true;
        }
    }
    false
}

impl Browser {
    fn op_page(&self, token: Token) -> Option<Token> {
        Some(self.op(token)?.page)
    }

    fn press_node(&self, token: Token) -> Option<u64> {
        if let OpKind::Press { node, .. } = &self.op(token)?.kind { Some(*node) } else { None }
    }

    fn press_hit(&self, token: Token) -> Option<u64> {
        if let OpKind::Press { hit, .. } = &self.op(token)?.kind { *hit } else { None }
    }

    fn type_detail(&self, token: Token) -> Option<(Box<[u8]>, bool)> {
        if let OpKind::Type { text, compose } = &self.op(token)?.kind { Some((text.clone(), *compose)) } else { None }
    }

    fn key_state(&self, token: Token) -> Option<Key> {
        if let OpKind::Key(key) = &self.op(token)?.kind { Some(*key) } else { None }
    }

    #[expect(clippy::too_many_lines, reason = "one match lists the reply transition for each operation step")]
    fn step_reply(&mut self, env: &Env<Limits>, token: Token, step: Step, result: Value<'_>) {
        match step {
            Step::History => {
                let current = field_i64(result, b"currentIndex");
                let entries = field_array(result, b"entries");
                let direction = if let Some(Operation { kind: OpKind::Go(Go::Back), .. }) = self.op(token) {
                    Some(-1_i64)
                } else if let Some(Operation { kind: OpKind::Go(Go::Forward), .. }) = self.op(token) {
                    Some(1_i64)
                } else {
                    None
                };
                let Some(current) = current else {
                    self.finish_refused(token, Refusal::Protocol);
                    return;
                };
                let Some(entries) = entries else {
                    self.finish_refused(token, Refusal::Protocol);
                    return;
                };
                let Some(direction) = direction else {
                    self.finish_refused(token, Refusal::Protocol);
                    return;
                };
                let Some(wanted) = current.checked_add(direction) else {
                    self.finish_refused(token, Refusal::Gone);
                    return;
                };
                let mut entry_id = None;
                for (index, entry) in entries.enumerate() {
                    if i64::try_from(index).ok() == Some(wanted) {
                        entry_id = field_u64(entry, b"id");
                        break;
                    }
                }
                if let Some(id) = entry_id {
                    if let Some(page) = self.op_page(token)
                        && let Some(page_state) = self.page_mut(page)
                    {
                        page_state.load = Some(Load::Go(token));
                    }
                    self.issue_step(env, token, b"Page.navigateToHistoryEntry", &Params::HistoryEntry(id), Step::Go);
                } else {
                    self.finish_refused(token, Refusal::Gone);
                }
            }
            Step::Go => {
                if result.get(b"errorText").is_some() {
                    self.finish_refused(token, Refusal::Protocol);
                    return;
                }
                let page = self.op_page(token);
                if let Some(page) = page
                    && let Some(page_state) = self.page_mut(page)
                {
                    page_state.load = Some(Load::Go(token));
                }
            }
            Step::Query => {
                if let Some(root) = result.get(b"root") {
                    let backend = field_u64(root, b"backendNodeId");
                    let page = self.op_page(token);
                    if let (Some(page), Some(backend)) = (page, backend) {
                        if let Some(page_state) = self.page_mut(page) {
                            page_state.root = Some(backend);
                        }
                        self.start_find(env, token);
                    } else {
                        self.finish_refused(token, Refusal::Protocol);
                    }
                    return;
                }
                let Some(nodes) = field_array(result, b"nodes") else {
                    self.finish_refused(token, Refusal::Protocol);
                    return;
                };
                if let Some(op) = self.op_mut(token)
                    && let OpKind::Find { seen, more, next, .. } = &mut op.kind
                {
                    seen.clear();
                    *more = 0;
                    *next = 0;
                    for node in nodes {
                        let Some(seen_node) = parse_seen(node, env.limits.text) else {
                            continue;
                        };
                        if seen.push(seen_node).is_err() {
                            *more = more.saturating_add(1);
                        }
                    }
                }
                let needs_box = if let Some(Operation { kind: OpKind::Find { query, seen, .. }, .. }) = self.op(token) {
                    query.boxes && !seen.is_empty()
                } else {
                    false
                };
                if needs_box {
                    self.next_find_box(env, token);
                } else {
                    self.finish_find(env, token);
                }
            }
            Step::Box => {
                let rect = parse_rect(result);
                if let Some(op) = self.op_mut(token)
                    && let OpKind::Find { seen, next, .. } = &mut op.kind
                {
                    if let Some(seen) = seen.get_mut(*next) {
                        seen.rect = rect;
                    }
                    *next = next.saturating_add(1);
                }
                let remaining = if let Some(Operation { kind: OpKind::Find { seen, next, .. }, .. }) = self.op(token) {
                    *next < seen.len()
                } else {
                    false
                };
                if remaining {
                    self.next_find_box(env, token);
                } else {
                    self.finish_find(env, token);
                }
            }
            Step::Scroll => {
                let node = self.press_node(token);
                if let Some(node) = node {
                    self.issue_step(
                        env,
                        token,
                        b"Accessibility.getPartialAXTree",
                        &Params::Backend(node),
                        Step::Accessibility,
                    );
                }
            }
            Step::Accessibility => {
                let first = match field_array(result, b"nodes") {
                    Some(mut nodes) => nodes.next(),
                    None => None,
                };
                let disabled = match first {
                    Some(node) => ax_disabled(node),
                    None => false,
                };
                if disabled {
                    self.finish_refused(token, Refusal::Disabled);
                    return;
                }
                let node = self.press_node(token);
                if let Some(node) = node {
                    self.issue_step(env, token, b"DOM.getContentQuads", &Params::Quads(node), Step::Quads);
                }
            }
            Step::Quads => {
                let Some(rect) = parse_rect(result) else {
                    self.finish_refused(token, Refusal::Hidden);
                    return;
                };
                if rect.width == 0 || rect.height == 0 {
                    self.finish_refused(token, Refusal::Hidden);
                    return;
                }
                let x = rect.x.saturating_add(i64::try_from(rect.width / 2).unwrap_or(i64::MAX));
                let y = rect.y.saturating_add(i64::try_from(rect.height / 2).unwrap_or(i64::MAX));
                if let Some(op) = self.op_mut(token)
                    && let OpKind::Press { at, .. } = &mut op.kind
                {
                    *at = Some((x, y));
                }
                self.issue_step(env, token, b"DOM.getNodeForLocation", &Params::Point { x, y }, Step::Hit);
            }
            Step::Hit => {
                let hit = field_u64(result, b"backendNodeId");
                let node = self.press_node(token);
                match (hit, node) {
                    (Some(hit), Some(node)) if hit == node => self.mouse(env, token, Step::MouseMove),
                    (Some(hit), Some(node)) => {
                        if let Some(op) = self.op_mut(token)
                            && let OpKind::Press { hit: slot, .. } = &mut op.kind
                        {
                            *slot = Some(hit);
                        }
                        self.issue_step(
                            env,
                            token,
                            b"DOM.describeNode",
                            &Params::Describe { backend: node, depth: -1 },
                            Step::Describe,
                        );
                    }
                    _ => self.finish_refused(token, Refusal::Covered),
                }
            }
            Step::Describe => {
                let hit = self.press_hit(token);
                if let Some(hit) = hit
                    && subtree_has(result.get(b"node"), hit)
                {
                    self.mouse(env, token, Step::MouseMove);
                    return;
                }
                self.finish_refused(token, Refusal::Covered);
            }
            Step::MouseMove => self.mouse(env, token, Step::MouseDown),
            Step::MouseDown => self.mouse(env, token, Step::MouseUp),
            Step::MouseUp | Step::Insert | Step::KeyUp => self.finish_done(token),
            Step::Focus => {
                let detail = self.type_detail(token);
                if let Some((text, compose)) = detail {
                    if compose {
                        self.issue_step(
                            env,
                            token,
                            b"Input.imeSetComposition",
                            &Params::Composition(&text),
                            Step::Insert,
                        );
                    } else {
                        self.issue_step(env, token, b"Input.insertText", &Params::Text(&text), Step::Insert);
                    }
                }
            }
            Step::KeyDown => {
                let key = self.key_state(token);
                if let Some(key) = key {
                    self.issue_key(env, token, key, false);
                }
            }
            Step::Snapshot => {
                let text = snapshot(result, env.limits.snapshot, env.limits.text);
                if self.remove_op(token).is_some() {
                    self.events.push_back(Event::Snapshot { op: token, text });
                }
            }
            Step::Screenshot => {
                let png = match field_text(result, b"data") {
                    Some(base64) => crate::wire::base64::decode(&base64, env.limits.screenshot).ok(),
                    None => None,
                };
                if let Some(png) = png {
                    if self.remove_op(token).is_some() {
                        self.events.push_back(Event::Screenshot { op: token, png });
                    }
                } else {
                    self.finish_refused(token, Refusal::Limit);
                }
            }
        }
    }

    fn next_find_box(&mut self, env: &Env<Limits>, token: Token) {
        let node = if let Some(Operation { kind: OpKind::Find { seen, next, .. }, .. }) = self.op(token) {
            match seen.get(*next) {
                Some(seen) => Some(seen.node),
                None => None,
            }
        } else {
            None
        };
        if let Some(node) = node {
            self.issue_step(env, token, b"DOM.getContentQuads", &Params::Quads(node), Step::Box);
        } else {
            self.finish_find(env, token);
        }
    }

    fn finish_find(&mut self, env: &Env<Limits>, token: Token) {
        let Some(mut op) = self.remove_op(token) else {
            return;
        };
        let OpKind::Find { query, expect, seen, more, .. } = op.kind else {
            return;
        };
        match expect {
            None => self.events.push_back(Event::Found { op: token, seen, more }),
            Some((expect, until)) => {
                let count = seen.len().saturating_add(more);
                let met = match expect {
                    Expect::Present => count > 0,
                    Expect::Count(wanted) => count == wanted,
                    Expect::Absent => count == 0,
                };
                if met {
                    self.events.push_back(Event::Met { op: token, seen });
                } else if env.now >= until {
                    self.events.push_back(Event::Missed { op: token, seen, more });
                } else {
                    op.kind = OpKind::Find {
                        query,
                        expect: Some((expect, until)),
                        seen: List::with_capacity(env.limits.matches),
                        more: 0,
                        next: 0,
                    };
                    op.due = Some(env.now.saturating_add(env.limits.poll).min(until));
                    let accepted = self.insert_op(op, env.limits.ops);
                    assert!(accepted, "the same operation's slot is reusable");
                }
            }
        }
    }

    fn mouse(&mut self, env: &Env<Limits>, token: Token, step: Step) {
        let at = if let Some(Operation { kind: OpKind::Press { at, .. }, .. }) = self.op(token) { *at } else { None };
        let Some((x, y)) = at else {
            self.finish_refused(token, Refusal::Protocol);
            return;
        };
        let (kind, pressed) = match step {
            Step::MouseMove => (b"mouseMoved".as_slice(), false),
            Step::MouseDown => (b"mousePressed".as_slice(), true),
            Step::MouseUp => (b"mouseReleased".as_slice(), false),
            Step::History
            | Step::Go
            | Step::Query
            | Step::Box
            | Step::Scroll
            | Step::Accessibility
            | Step::Quads
            | Step::Hit
            | Step::Describe
            | Step::Focus
            | Step::Insert
            | Step::KeyDown
            | Step::KeyUp
            | Step::Snapshot
            | Step::Screenshot => unreachable!("only mouse steps reach mouse"),
        };
        self.issue_step(env, token, b"Input.dispatchMouseEvent", &Params::Mouse { kind, x, y, pressed }, step);
    }
}
