//! Fixed pages for the Chromium suite, served by skein-http over skein-io.

use std::collections::{BTreeMap, VecDeque};
use std::net::{Ipv4Addr, SocketAddr};

use skein_http::Header;
use skein_http::server::{self, Body, Response, Reuse, Server};
use skein_io::{self as io, Request};
use skein_lib::stream::{self, Read};
use skein_lib::{Env, Queue, Token};

pub const OWNER: Token = Token::new(100);

const BUTTON: &[u8] = br#"<!doctype html><html><body><button onclick="document.querySelector('h1').textContent='Done'">Press</button><h1>Before</h1></body></html>"#;
const FORM: &[u8] = br#"<!doctype html><html><body><form onsubmit="event.preventDefault();document.querySelector('h1').textContent='Hello '+document.querySelector('input').value"><label>Name <input aria-label="Name"></label><button type="submit">Submit</button></form><h1>Before</h1></body></html>"#;
const COVERED: &[u8] = br#"<!doctype html><html><body><button onclick="document.querySelector('h1').textContent='Wrong'">Covered</button><h1>Before</h1><div style="position:fixed;inset:0;background:#abc;opacity:.9;z-index:10"></div></body></html>"#;
const SCROLL: &[u8] = br#"<!doctype html><html><body><div style="height:2200px">Long page</div><button onclick="document.querySelector('h1').textContent='Scrolled'">Far away</button><h1>Before</h1></body></html>"#;
const THROW: &[u8] = br#"<!doctype html><html><body><button onclick="throw Error('test exception')">Throw</button><h1>Before</h1></body></html>"#;
const CSP: &[u8] = br#"<!doctype html><html><body><h1>CSP page</h1><script>document.querySelector('h1').textContent='Wrong'</script></body></html>"#;

fn page(target: &[u8]) -> (u16, &'static [u8], bool) {
    match target {
        b"/button" => (200, BUTTON, false),
        b"/form" => (200, FORM, false),
        b"/covered" => (200, COVERED, false),
        b"/scroll" => (200, SCROLL, false),
        b"/throw" => (200, THROW, false),
        b"/csp" => (200, CSP, true),
        _ => (404, b"not found", false),
    }
}

fn limits() -> server::Limits {
    server::Limits { head: 4096, headers: 32, body: 4096, read: 1024, response: 1024, send: 4096 }
}

struct Connection {
    server: Server,
    reply: &'static [u8],
    sent: usize,
}

pub struct Pages {
    pub addr: Option<SocketAddr>,
    listener: Option<Token>,
    next_owner: u64,
    connections: BTreeMap<Token, Connection>,
    env: Env<server::Limits>,
    above: Queue<server::Event>,
    below: Queue<stream::Down>,
    requests: VecDeque<Request>,
    stopping: bool,
}

impl Pages {
    pub fn new(now: skein_lib::Time, wall: skein_lib::Wall) -> Pages {
        let mut requests = VecDeque::new();
        requests.push_back(Request::Listen { owner: OWNER, addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)) });
        Pages {
            addr: None,
            listener: None,
            next_owner: 101,
            connections: BTreeMap::new(),
            env: Env { now, wall, limits: limits() },
            above: Queue::with_capacity(16),
            below: Queue::with_capacity(16),
            requests,
            stopping: false,
        }
    }

    pub fn update_time(&mut self, now: skein_lib::Time, wall: skein_lib::Wall) {
        self.env.now = now;
        self.env.wall = wall;
    }

    pub fn take(&mut self) -> Option<Request> {
        self.requests.pop_front()
    }

    pub fn pending(&self) -> bool {
        !self.requests.is_empty()
    }

    pub fn owns(&self, owner: Token) -> bool {
        owner == OWNER || self.connections.contains_key(&owner)
    }

    pub fn is_closed(&self) -> bool {
        self.stopping && self.listener.is_none() && self.connections.is_empty() && self.requests.is_empty()
    }

    pub fn stop(&mut self) {
        if self.stopping {
            return;
        }
        self.stopping = true;
        if let Some(listener) = self.listener {
            self.requests.push_back(Request::Close { entity: listener });
        }
        for &owner in self.connections.keys() {
            self.requests.push_back(Request::Close { entity: owner });
        }
    }

    pub fn event(&mut self, event: io::Event) {
        match event {
            io::Event::Listening { owner: OWNER, listener, addr } => {
                self.listener = Some(listener);
                self.addr = Some(addr);
            }
            io::Event::Accepted { owner: OWNER, socket, .. } => {
                if self.stopping {
                    self.requests.push_back(Request::Reject { socket });
                    return;
                }
                let owner = Token::new(self.next_owner);
                self.next_owner += 1;
                self.connections
                    .insert(owner, Connection { server: Server::new(&self.env.limits), reply: b"", sent: 0 });
                self.requests.push_back(Request::Bind { socket, owner });
                self.down(owner, server::Request::Next);
            }
            io::Event::Stream { owner, up } => self.up(owner, up),
            io::Event::Closed { owner: OWNER } => self.listener = None,
            io::Event::Closed { owner } => {
                self.connections.remove(&owner);
            }
            io::Event::Failed { owner, error } => panic!("page server {owner:?} failed: {error:?}"),
            other => panic!("unexpected page server event: {other:?}"),
        }
    }

    fn up(&mut self, owner: Token, up: stream::Up) {
        let conn = self.connections.get_mut(&owner).expect("page connection");
        server::up(&mut conn.server, &self.env, up, &mut self.above, &mut self.below);
        self.drain(owner);
    }

    fn down(&mut self, owner: Token, request: server::Request) {
        let conn = self.connections.get_mut(&owner).expect("page connection");
        server::down(&mut conn.server, &self.env, request, &mut self.above, &mut self.below);
        self.drain(owner);
    }

    fn drain(&mut self, owner: Token) {
        while let Some(down) = self.below.pop() {
            self.requests.push_back(Request::Stream { stream: owner, down });
        }
        while let Some(event) = self.above.pop() {
            match event {
                server::Event::Call(call) => {
                    let (status, body, csp) = page(&call.target);
                    let conn = self.connections.get_mut(&owner).expect("page connection");
                    conn.reply = body;
                    conn.sent = 0;
                    self.down(owner, server::Request::Discard);
                    let mut headers = vec![Header {
                        name: Box::from(&b"Content-Type"[..]),
                        value: Box::from(&b"text/html; charset=utf-8"[..]),
                    }];
                    if csp {
                        headers.push(Header {
                            name: Box::from(&b"Content-Security-Policy"[..]),
                            value: Box::from(&b"default-src 'none'"[..]),
                        });
                    }
                    self.down(
                        owner,
                        server::Request::Respond(Response {
                            status,
                            headers: headers.into_boxed_slice(),
                            body: Body::Length(body.len() as u64),
                            close: true,
                        }),
                    );
                    if !body.is_empty() {
                        self.down(
                            owner,
                            server::Request::Reply(stream::Down::Demand {
                                read: Read::Nothing,
                                room: body.len() as u32,
                            }),
                        );
                    }
                }
                server::Event::Reply(stream::Up::Room) => {
                    let conn = self.connections.get_mut(&owner).expect("page connection");
                    let end = (conn.sent + self.env.limits.send as usize).min(conn.reply.len());
                    let bytes = Box::from(&conn.reply[conn.sent..end]);
                    conn.sent = end;
                    self.down(owner, server::Request::Reply(stream::Down::Send(bytes)));
                    if end == self.connections.get(&owner).expect("page connection").reply.len() {
                        self.down(owner, server::Request::Reply(stream::Down::Finish));
                    } else {
                        self.down(
                            owner,
                            server::Request::Reply(stream::Down::Demand {
                                read: Read::Nothing,
                                room: self.env.limits.send,
                            }),
                        );
                    }
                }
                server::Event::Done(Reuse::Close) | server::Event::Ended | server::Event::Failed(_) => {
                    self.down(owner, server::Request::Close);
                    self.requests.push_back(Request::Close { entity: owner });
                }
                server::Event::Closed | server::Event::Body(_) => {}
                server::Event::Done(Reuse::Keep) => panic!("page responses close their socket"),
                server::Event::Reply(other) => panic!("unexpected page reply: {other:?}"),
                server::Event::Refused(reason) => panic!("page response refused: {reason:?}"),
            }
        }
    }
}
