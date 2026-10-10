//! One public sign-in's loopback listener and bounded request head (oauth.md,
//! section 6.5). It knows the registered URI, no state or verifier. It hands a
//! checked redirect to its exchange, answers a fixed page, and settles at io.

#![expect(
    clippy::single_match,
    clippy::manual_let_else,
    reason = "explicit optional socket and callback presence at the io boundary"
)]

use crate::Limits;
use crate::exchange::{Exchange, ROUTES};
use crate::redirect::{self, Redirect};
use crate::route::{self, Socket};
use alloc::boxed::Box;
use skein_http::{Header, server as http};
use skein_io::{Event, Request};
use skein_lib::stream::{Down, Read, Up};
use skein_lib::{Env, Id, Queue, Token, bytes};

const PAGE: &[u8] = b"Sign-in received. You may close this page.\n";
const REFUSAL: &[u8] = b"This is not the registered redirect.\n";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Listening,
    Received,
    Closing,
    Aborting,
    Closed,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Head {
    Reading,
    Responding,
    Finishing,
    Closing,
    Aborting,
}

pub(crate) struct Listener {
    socket: Option<Token>,
    close_sent: bool,
    phase: Phase,
    callback: Option<Callback>,
    pub(crate) redirect: Option<Redirect>,
    pub(crate) failure: Option<skein_oauth::Failure>,
}

struct Callback {
    socket: Token,
    http: http::Server,
    events: Queue<http::Event>,
    requests: Queue<http::Request>,
    below: Queue<Down>,
    page: &'static [u8],
    offset: usize,
    phase: Head,
}

impl Listener {
    pub(crate) fn new() -> Self {
        Listener {
            socket: None,
            close_sent: false,
            phase: Phase::Listening,
            callback: None,
            redirect: None,
            failure: None,
        }
    }

    pub(crate) fn ready(&self) -> bool {
        self.socket.is_some() && self.phase == Phase::Listening
    }

    pub(crate) fn settled(&self) -> bool {
        self.phase == Phase::Closed && self.callback.is_none()
    }

    pub(crate) fn up(
        &mut self,
        env: &Env<Limits>,
        id: Id<Exchange>,
        socket: Socket,
        event: Event,
        io: &mut Queue<Request>,
    ) {
        match socket {
            Socket::Listener => match event {
                Event::Listening { listener, .. } => {
                    self.socket = Some(listener);
                    self.end_listener(io);
                }
                Event::Accepted { socket, .. } => {
                    if self.phase != Phase::Listening || self.callback.is_some() {
                        io.push(Request::Reject { socket });
                    } else {
                        self.callback = Some(Callback::new(socket, &env.limits));
                        io.push(Request::Bind { socket, owner: route::owner(id, Socket::Callback) });
                    }
                }
                Event::Failed { .. } => {
                    self.failure = Some(skein_oauth::Failure::Unavailable);
                }
                Event::Closed { .. } => {
                    self.socket = None;
                    self.phase = Phase::Closed;
                }
                Event::Connecting { .. }
                | Event::Connected { .. }
                | Event::Stream { .. }
                | Event::Output { .. }
                | Event::Spawned { .. }
                | Event::Exited { .. }
                | Event::Usage { .. }
                | Event::Shutdown { .. } => {}
            },
            Socket::Callback => match event {
                Event::Stream { up, .. } => match &mut self.callback {
                    Some(callback) if callback.phase != Head::Closing && callback.phase != Head::Aborting => http::up(
                        &mut callback.http,
                        &Env { now: env.now, wall: env.wall, limits: env.limits.server },
                        up,
                        &mut callback.events,
                        &mut callback.below,
                    ),
                    Some(_) | None => {}
                },
                Event::Closed { .. } => self.callback = None,
                Event::Failed { .. } => match &mut self.callback {
                    Some(callback) => callback.stop(env, false, io),
                    None => {}
                },
                Event::Listening { .. }
                | Event::Accepted { .. }
                | Event::Connecting { .. }
                | Event::Connected { .. }
                | Event::Output { .. }
                | Event::Spawned { .. }
                | Event::Exited { .. }
                | Event::Usage { .. }
                | Event::Shutdown { .. } => {}
            },
            Socket::Web => {}
        }
    }

    pub(crate) fn stop_waiting(&mut self) {
        if self.phase == Phase::Listening {
            self.phase = Phase::Received;
        }
    }

    fn end_listener(&mut self, io: &mut Queue<Request>) {
        if (self.phase == Phase::Closing || self.phase == Phase::Aborting) && !self.close_sent {
            match self.socket {
                Some(socket) => {
                    self.close_sent = true;
                    io.push(if self.phase == Phase::Aborting {
                        Request::Abort { entity: socket }
                    } else {
                        Request::Close { entity: socket }
                    });
                }
                None => {}
            }
        }
    }

    pub(crate) fn close(&mut self, env: &Env<Limits>, abort: bool, io: &mut Queue<Request>) {
        match self.phase {
            Phase::Listening | Phase::Received => self.phase = if abort { Phase::Aborting } else { Phase::Closing },
            Phase::Closing if abort => {
                self.phase = Phase::Aborting;
                self.close_sent = false;
            }
            Phase::Closing | Phase::Aborting | Phase::Closed => {}
        }
        self.end_listener(io);
        match &mut self.callback {
            Some(callback) if abort || callback.phase == Head::Reading => callback.stop(env, abort, io),
            Some(_) | None => {}
        }
        self.redirect = None;
    }

    pub(crate) fn progress(&mut self, env: &Env<Limits>, registered: &[u8], io: &mut Queue<Request>) {
        let callback = match &mut self.callback {
            Some(callback) => callback,
            None => return,
        };
        if callback.phase == Head::Closing || callback.phase == Head::Aborting {
            return;
        }
        let http_env = Env { now: env.now, wall: env.wall, limits: env.limits.server };
        if let Some(down) = callback.below.pop() {
            io.push(Request::Stream { stream: callback.socket, down });
            return;
        }
        if let Some(request) = callback.requests.pop() {
            http::down(&mut callback.http, &http_env, request, &mut callback.events, &mut callback.below);
            return;
        }
        if let Some(event) = callback.events.pop() {
            match event {
                http::Event::Call(call) => {
                    let redirect = redirect::from_head(&call, registered, &env.limits.client);
                    let accepted = redirect.is_some() && self.phase == Phase::Listening;
                    if accepted {
                        self.redirect = redirect;
                    }
                    callback.phase = Head::Responding;
                    callback.page = if accepted { PAGE } else { REFUSAL };
                    callback.requests.push(http::Request::Discard);
                    callback.requests.push(http::Request::Respond(http::Response {
                        status: if accepted { 200 } else { 404 },
                        headers: Box::new([Header {
                            name: bytes::copy_of(b"content-type"),
                            value: bytes::copy_of(b"text/plain; charset=utf-8"),
                        }]),
                        body: http::Body::Length(u64::try_from(callback.page.len()).expect("fixed page")),
                        close: true,
                    }));
                    callback.requests.push(http::Request::Reply(Down::Demand {
                        read: Read::Nothing,
                        room: env.limits.server.send.min(u32::try_from(callback.page.len()).expect("fixed page")),
                    }));
                }
                http::Event::Reply(Up::Room) => {
                    let end = callback
                        .offset
                        .saturating_add(usize::try_from(env.limits.server.send).expect("u32 fits"))
                        .min(callback.page.len());
                    callback.requests.push(http::Request::Reply(Down::Send(bytes::copy_of(
                        callback.page.get(callback.offset..end).expect("fixed page offset"),
                    ))));
                    callback.offset = end;
                    if end == callback.page.len() {
                        callback.requests.push(http::Request::Reply(Down::Finish));
                    } else {
                        callback.requests.push(http::Request::Reply(Down::Demand {
                            read: Read::Nothing,
                            room: env.limits.server.send.min(
                                u32::try_from(callback.page.len().checked_sub(end).expect("page remainder"))
                                    .expect("fixed page"),
                            ),
                        }));
                    }
                }
                http::Event::Done(_)
                | http::Event::Ended
                | http::Event::Failed(_)
                | http::Event::Reply(Up::Failed(_) | Up::End | Up::Bytes(_))
                | http::Event::Refused(_) => callback.phase = Head::Finishing,
                http::Event::Body(_) | http::Event::Closed => {}
            }
            return;
        }
        if callback.phase == Head::Finishing {
            callback.stop(env, false, io);
        }
    }

    pub(crate) fn has_work(&self) -> bool {
        self.redirect.is_some()
            || self.failure.is_some()
            || match &self.callback {
                Some(callback) => {
                    callback.phase != Head::Closing
                        && callback.phase != Head::Aborting
                        && (callback.phase == Head::Finishing
                            || !callback.events.is_empty()
                            || !callback.requests.is_empty()
                            || !callback.below.is_empty())
                }
                None => false,
            }
    }
}

impl Callback {
    fn new(socket: Token, limits: &Limits) -> Self {
        let mut requests = Queue::with_capacity(ROUTES);
        requests.push(http::Request::Next);
        Callback {
            socket,
            http: http::Server::new(&limits.server),
            events: Queue::with_capacity(ROUTES),
            requests,
            below: Queue::with_capacity(ROUTES),
            page: PAGE,
            offset: 0,
            phase: Head::Reading,
        }
    }
    fn stop(&mut self, env: &Env<Limits>, abort: bool, io: &mut Queue<Request>) {
        if self.phase == Head::Aborting || (self.phase == Head::Closing && !abort) {
            return;
        }
        if self.phase != Head::Closing {
            for _ in 0..ROUTES {
                drop(self.events.pop());
                drop(self.requests.pop());
                drop(self.below.pop());
            }
            http::down(
                &mut self.http,
                &Env { now: env.now, wall: env.wall, limits: env.limits.server },
                http::Request::Close,
                &mut self.events,
                &mut self.below,
            );
            for _ in 0..ROUTES {
                drop(self.events.pop());
                drop(self.below.pop());
            }
        }
        self.phase = if abort { Head::Aborting } else { Head::Closing };
        io.push(if abort { Request::Abort { entity: self.socket } } else { Request::Close { entity: self.socket } });
    }
}

pub(crate) fn worst_case(limits: &Limits) -> Option<u64> {
    Queue::<http::Event>::worst_case(ROUTES)?
        .checked_add(Queue::<http::Request>::worst_case(ROUTES)?)?
        .checked_add(Queue::<Down>::worst_case(ROUTES)?)?
        .checked_add(
            u64::from(limits.server.head.max(limits.server.response).max(limits.server.send))
                .checked_mul(u64::from(ROUTES))?,
        )?
        .checked_add(u64::from(limits.client.url_bytes).checked_mul(2)?)?
        .checked_add(512)
}
