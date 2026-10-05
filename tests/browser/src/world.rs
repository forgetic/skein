use std::collections::{BTreeMap, VecDeque};

use skein_browser::boundary::{Below, Down, Event, Request};
use skein_browser::wire::decode::Document;
use skein_browser::{Browser, Limits, down, fire, up};
use skein_lib::stream;
use skein_lib::{Env, Queue, Time, Token, Wall};

/// A fault applied to the next reply for a named CDP method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    Drop,
    Delay,
    Error,
    Oversize,
    Crash,
    PageCrash,
    Interleave,
}

/// An in-memory CDP peer and the browser step machine it drives.
#[derive(Debug)]
pub struct World {
    pub browser: Browser,
    pub env: Env<Limits>,
    pub above: Queue<Event>,
    pub below: Queue<Down>,
    pub sent: Vec<Vec<u8>>,
    pub ax_visible: bool,
    pub disabled: bool,
    pub covered: bool,
    pub hidden: bool,
    fault: Option<(Vec<u8>, Fault)>,
    delayed: VecDeque<Vec<u8>>,
    outstanding: BTreeMap<u64, Time>,
}

impl Default for World {
    fn default() -> Self {
        Self::new()
    }
}

impl World {
    pub fn new() -> World {
        Self::with_limits(Limits::default())
    }

    pub fn with_limits(limits: Limits) -> World {
        let above_room = limits.persons.saturating_add(limits.pages).saturating_add(limits.ops).saturating_add(4);
        let below_room = limits.commands.saturating_add(8);
        World {
            browser: Browser::new(Token::new(1), &limits),
            env: Env { now: Time::ZERO, wall: Wall::EPOCH, limits },
            above: Queue::with_capacity(above_room),
            below: Queue::with_capacity(below_room),
            sent: Vec::new(),
            ax_visible: true,
            disabled: false,
            covered: false,
            hidden: false,
            fault: None,
            delayed: VecDeque::new(),
            outstanding: BTreeMap::new(),
        }
    }

    pub fn fault(&mut self, method: &[u8], fault: Fault) {
        self.fault = Some((method.to_vec(), fault));
    }

    pub fn release_delayed(&mut self) {
        while let Some(reply) = self.delayed.pop_front() {
            self.mark_answered(&reply);
            up(
                &mut self.browser,
                &self.env,
                Below::Replies(stream::Up::Bytes(reply.into_boxed_slice())),
                &mut self.above,
                &mut self.below,
            );
        }
        self.drive();
    }

    pub fn tick(&mut self, now: Time) {
        self.env.now = now;
        fire(&mut self.browser, &self.env, now, &mut self.above, &mut self.below);
        self.outstanding.retain(|_, due| *due > now);
        self.drive();
    }

    /// Every command has received a reply or reached its answer deadline or
    /// the peer's failure. A delayed reply after the deadline stays harmless.
    pub fn assert_commands_settled(&self) {
        assert!(self.outstanding.is_empty(), "unanswered CDP commands: {:?}", self.outstanding);
    }

    fn mark_answered(&mut self, reply: &[u8]) {
        if let Some(document) =
            reply.strip_suffix(&[0]).and_then(|bytes| Document::parse(bytes, self.env.limits.message).ok())
            && let Some(id) = document.root().get(b"id").and_then(|id| id.u64())
        {
            self.outstanding.remove(&id);
        }
    }

    pub fn start(&mut self) {
        fire(&mut self.browser, &self.env, self.env.now, &mut self.above, &mut self.below);
        self.drive();
        assert!(matches!(self.above.pop(), Some(Event::Ready { version }) if version.as_ref() == b"Chrome/153"));
    }

    pub fn ask(&mut self, request: Request) {
        down(&mut self.browser, &self.env, request, &mut self.above, &mut self.below);
        self.drive();
    }

    pub fn drive(&mut self) {
        let mut replies = VecDeque::new();
        for _ in 0..1000 {
            if let Some(record) = self.below.pop() {
                match record {
                    Down::Commands(stream::Down::Demand { .. }) => {
                        up(
                            &mut self.browser,
                            &self.env,
                            Below::Commands(stream::Up::Room),
                            &mut self.above,
                            &mut self.below,
                        );
                    }
                    Down::Commands(stream::Down::Send(bytes)) => {
                        self.sent.push(bytes.to_vec());
                        let document =
                            Document::parse(&bytes[..bytes.len() - 1], self.env.limits.command).expect("valid command");
                        let root = document.root();
                        let id = root.get(b"id").and_then(|id| id.u64()).expect("command id");
                        self.outstanding.insert(id, self.env.now.saturating_add(self.env.limits.answer));
                        let method = root.get(b"method").and_then(|method| method.text()).expect("method");
                        let session = root.get(b"sessionId").and_then(|session| session.text());
                        let result = self.reply(&method);
                        let session_field = session
                            .as_deref()
                            .map(|session| format!(",\"sessionId\":\"{}\"", String::from_utf8_lossy(session)))
                            .unwrap_or_default();
                        let reply = format!("{{\"id\":{id}{session_field},\"result\":{result}}}\0").into_bytes();
                        let fault = if self.fault.as_ref().is_some_and(|(target, _)| target == method.as_ref()) {
                            self.fault.take().map(|(_, fault)| fault)
                        } else {
                            None
                        };
                        match fault {
                            Some(Fault::Drop) => {}
                            Some(Fault::Delay) => self.delayed.push_back(reply),
                            Some(Fault::Error) => replies.push_back(format!("{{\"id\":{id}{session_field},\"error\":{{\"code\":-32000,\"message\":\"No node found for given backend id\"}}}}\0").into_bytes()),
                            Some(Fault::Oversize) => replies.push_back(vec![b'x'; usize::try_from(self.env.limits.message).expect("message fits usize") + 2]),
                            Some(Fault::Crash) => {
                                up(&mut self.browser, &self.env, Below::Replies(stream::Up::End), &mut self.above, &mut self.below);
                                self.outstanding.clear();
                            }
                            Some(Fault::PageCrash) => {
                                replies.push_back(b"{\"method\":\"Inspector.targetCrashed\",\"sessionId\":\"s1\",\"params\":{}}\0".to_vec());
                            }
                            Some(Fault::Interleave) => {
                                replies.push_back(b"{\"method\":\"Runtime.exceptionThrown\",\"sessionId\":\"s1\",\"params\":{\"exceptionDetails\":{\"text\":\"injected\"}}}\0".to_vec());
                                replies.push_back(reply);
                            }
                            None => replies.push_back(reply),
                        }
                        if method.as_ref() == b"Page.navigate"
                            || method.as_ref() == b"Page.reload"
                            || method.as_ref() == b"Page.navigateToHistoryEntry"
                        {
                            replies.push_back(
                                b"{\"method\":\"Page.loadEventFired\",\"sessionId\":\"s1\",\"params\":{}}\0".to_vec(),
                            );
                        }
                    }
                    Down::Replies(stream::Down::Demand { .. }) | Down::Errors(stream::Down::Demand { .. }) => {}
                    Down::Commands(stream::Down::Finish)
                    | Down::Replies(stream::Down::Finish)
                    | Down::Errors(stream::Down::Finish) => {}
                    Down::Replies(stream::Down::Send(_)) | Down::Errors(stream::Down::Send(_)) => {
                        panic!("browser sends only commands")
                    }
                }
                continue;
            }
            if let Some(reply) = replies.pop_front() {
                self.mark_answered(&reply);
                if reply.len() > usize::try_from(self.env.limits.message).expect("message fits usize") + 1 {
                    self.outstanding.clear();
                }
                if reply.windows(b"Inspector.targetCrashed".len()).any(|part| part == b"Inspector.targetCrashed") {
                    self.outstanding.clear();
                }
                up(
                    &mut self.browser,
                    &self.env,
                    Below::Replies(stream::Up::Bytes(reply.into_boxed_slice())),
                    &mut self.above,
                    &mut self.below,
                );
                if !self.outstanding.is_empty() && self.browser.next_deadline().is_none() {
                    self.outstanding.clear();
                }
                continue;
            }
            if self.browser.work_pending() {
                fire(&mut self.browser, &self.env, self.env.now, &mut self.above, &mut self.below);
                continue;
            }
            return;
        }
        panic!("world did not settle");
    }

    fn reply(&self, method: &[u8]) -> &'static str {
        match method {
            b"Browser.getVersion" => r#"{"product":"Chrome/153"}"#,
            b"Target.createBrowserContext" => r#"{"browserContextId":"ctx1"}"#,
            b"Target.createTarget" => r#"{"targetId":"t1"}"#,
            b"Target.attachToTarget" => r#"{"sessionId":"s1"}"#,
            b"DOM.getDocument" => r#"{"root":{"backendNodeId":1}}"#,
            b"Accessibility.queryAXTree" if self.ax_visible => {
                r#"{"nodes":[{"backendDOMNodeId":7,"role":{"value":"button"},"name":{"value":"Save"},"value":{"value":""},"properties":[]}]}"#
            }
            b"Accessibility.queryAXTree" => r#"{"nodes":[]}"#,
            b"Accessibility.getPartialAXTree" if self.disabled => {
                r#"{"nodes":[{"backendDOMNodeId":7,"properties":[{"name":"disabled","value":{"value":true}}]}]}"#
            }
            b"Accessibility.getPartialAXTree" => {
                r#"{"nodes":[{"backendDOMNodeId":7,"properties":[{"name":"disabled","value":{"value":false}}]}]}"#
            }
            b"DOM.getContentQuads" if self.hidden => r#"{"quads":[]}"#,
            b"DOM.getContentQuads" => r#"{"quads":[[10.25,10.25,110.75,10.25,110.75,40.75,10.25,40.75]]}"#,
            b"DOM.getNodeForLocation" if self.covered => r#"{"backendNodeId":99}"#,
            b"DOM.getNodeForLocation" => r#"{"backendNodeId":7}"#,
            b"DOM.describeNode" => r#"{"node":{"backendNodeId":7,"children":[]}}"#,
            b"Page.captureScreenshot" => r#"{"data":"iVBORw0KGgo="}"#,
            _ => "{}",
        }
    }

    pub fn open(&mut self) {
        self.ask(Request::Person { person: Token::new(2) });
        self.ask(Request::Page {
            person: Token::new(2),
            page: Token::new(3),
            url: b"https://example.test/".to_vec().into_boxed_slice(),
        });
        assert!(matches!(self.above.pop(), Some(Event::Opened { page }) if page == Token::new(3)));
    }
}
