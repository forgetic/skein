//! The actual accounts owner and independent issuer over shared seeded wires.
use super::browser::url_parts;
use super::{Client, Fact, ROOM, Story, confidential, registration};
use skein_fake_oauth as fake;
use skein_io as io;
use skein_lib::{Duration, Env, Queue, Time, Wall, bytes};
use skein_oauth as oauth;
use skein_oauth_accounts as accounts;
use std::net::Ipv4Addr;
/// The same owner runs directly over seeded protocol streams and fake issuer.
#[must_use]
#[expect(clippy::too_many_lines, reason = "the protocol loop exposes every owner and stream boundary")]
pub fn protocol(seed: u64, story: Story) -> (Vec<Fact>, Vec<String>) {
    use skein_http::server as http;
    use skein_lib::Token;
    use skein_lib::stream::{Down, Read, Up};
    use skein_world::stream::Wire;

    let profile = crate::world::limits();
    let registration = registration(story);
    let mut issuer = fake::Issuer::new(
        fake::Config {
            authorization_url: registration.authorization_url.clone(),
            token_endpoint: registration.token_endpoint.clone(),
            client_id: registration.client_id.clone(),
            client_secret: registration.client_secret.clone(),
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
    for (access, refresh) in [(b'a', b"r1".as_slice()), (b'b', b"r2".as_slice())] {
        issuer
            .queue(fake::Plan {
                status: 200,
                body: fake::Body::Token(oauth::TokenResponse {
                    access_token: vec![access; 16].into_boxed_slice(),
                    refresh_token: Some(bytes::copy_of(refresh)),
                    expires_in: 30,
                }),
                delay: Duration::from_millis(5),
                retry_after: Duration::ZERO,
            })
            .expect("plan");
    }
    let server_limits = http::Limits { head: 2048, headers: 16, body: 1024, read: 256, response: 2048, send: 256 };
    let mut server = http::Server::new(&server_limits);
    let mut server_events = Queue::with_capacity(ROOM);
    let mut server_requests = Queue::with_capacity(ROOM);
    let mut server_down = Queue::with_capacity(ROOM);
    let mut issuer_out = Queue::with_capacity(ROOM);
    let mut client_wire = Wire::new(seed);
    let mut server_wire = Wire::new(seed.wrapping_add(1));
    let mut client = Client::new(story, false);
    client.address = Some((Ipv4Addr::LOCALHOST, 31000).into());
    let mut now = Time::ZERO;
    client.start(&Env { now, wall: Wall::EPOCH, limits: profile });
    let mut owner = None;
    let mut body = Vec::new();
    let mut response: Option<(Box<[u8]>, usize)> = None;
    let mut server_closed = false;
    let mut listener = None;
    let mut callback = None;
    let mut callback_wire = Wire::new(seed.wrapping_add(2));
    let mut browser_bytes = Vec::new();
    let mut redirect = None;
    let mut callback_count = 0;
    let mut scripted = false;
    let mut callback_pending = false;
    let mut trace = Vec::new();

    for _ in 0..200_000 {
        let wall = Wall::from_nanos(now.as_nanos());
        let env = Env { now, wall, limits: profile };
        let server_env = Env { now, wall, limits: server_limits };
        let component = client.component.as_mut().expect("component");
        if component.has_work() || component.next_deadline().is_some_and(|due| due <= now) {
            component.fire(&env, &mut client.above, &mut client.requests, &mut client.files);
        }
        client.observe(&env);
        if !scripted && let Some(url) = &client.visit {
            scripted = true;
            if !matches!(story, Story::Cancel | Story::Abort | Story::Timeout) {
                issuer.step(fake::Event::Authorize { url: url.clone(), now }, &mut issuer_out);
                let Some(fake::Request::Redirect { uri, state, code }) = issuer_out.pop() else {
                    panic!("authorized fake browser")
                };
                let mut location = uri.to_vec();
                location.extend_from_slice(b"?state=");
                location.extend_from_slice(&state);
                location.extend_from_slice(b"&code=");
                location.extend_from_slice(&code);
                redirect = Some(location.into_boxed_slice());
            }
        }
        if !callback_pending
            && callback.is_none()
            && callback_count
                < if matches!(story, Story::WrongPath | Story::WrongHost | Story::LongHead) { 2 } else { 1 }
            && let Some(uri) = &redirect
        {
            if confidential(story) {
                client.ask(&env, accounts::Request::Redirected { account: 0, uri: uri.clone() });
                redirect = None;
            } else if let Some(identity) = listener {
                client.component.as_mut().expect("component").up(
                    &env,
                    io::Event::Accepted {
                        owner: identity,
                        socket: Token::new(11),
                        peer: (Ipv4Addr::LOCALHOST, 31235).into(),
                    },
                    &mut client.above,
                    &mut client.requests,
                    &mut client.files,
                );
                let (_, authority, target) = url_parts(uri);
                let target = if callback_count == 0 && story == Story::WrongPath {
                    b"/wrong?state=fake&code=fake".as_slice()
                } else {
                    target
                };
                let authority = if callback_count == 0 && story == Story::WrongHost {
                    b"127.0.0.1:31234".as_slice()
                } else {
                    authority
                };
                let mut head = b"GET ".to_vec();
                head.extend_from_slice(target);
                if callback_count == 0 && story == Story::LongHead {
                    head.extend_from_slice(&vec![b'x'; 3000]);
                }
                head.extend_from_slice(b" HTTP/1.1\r\nHost: ");
                head.extend_from_slice(authority);
                head.extend_from_slice(b"\r\nConnection: close\r\n\r\n");
                callback_wire = Wire::new(seed.wrapping_add(2 + callback_count));
                callback_wire.write(&head);
                browser_bytes.clear();
                callback_count += 1;
                callback_pending = true;
            }
        }
        if let Some(request) = client.requests.pop() {
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
                    server = http::Server::new(&server_limits);
                    server_closed = false;
                    server_events = Queue::with_capacity(ROOM);
                    server_requests = Queue::with_capacity(ROOM);
                    server_down = Queue::with_capacity(ROOM);
                    client_wire = Wire::new(seed.wrapping_add(4 + issuer.posts()));
                    server_wire = Wire::new(seed.wrapping_add(5 + issuer.posts()));
                    body.clear();
                    response = None;
                    server_requests.push(http::Request::Next);
                }
                io::Request::Listen { owner: identity, addr } => {
                    assert_eq!(addr, (Ipv4Addr::LOCALHOST, 31234).into());
                    assert!(listener.replace(identity).is_none());
                    client.component.as_mut().expect("component").up(
                        &env,
                        io::Event::Listening { owner: identity, listener: Token::new(10), addr },
                        &mut client.above,
                        &mut client.requests,
                        &mut client.files,
                    );
                }
                io::Request::Bind { owner: identity, socket } => {
                    assert_eq!(socket, Token::new(11));
                    assert!(callback.replace(identity).is_none());
                }
                io::Request::Stream { stream, down } if stream == Token::new(11) => {
                    if let Down::Send(bytes) = &down {
                        browser_bytes.extend_from_slice(bytes);
                    }
                    callback_wire.take(down);
                }
                io::Request::Stream { down, .. } => {
                    match &down {
                        Down::Send(bytes) => server_wire.write(bytes),
                        Down::Finish => server_wire.eof = true,
                        Down::Demand { .. } => {}
                    }
                    client_wire.take(down);
                }
                io::Request::Close { entity } | io::Request::Abort { entity } => {
                    let identity = if entity == Token::new(9) {
                        trace.append(&mut client_wire.trace);
                        trace.append(&mut server_wire.trace);
                        server_requests.push(http::Request::Close);
                        owner.take().expect("web owner")
                    } else if entity == Token::new(10) {
                        listener.take().expect("listener owner")
                    } else {
                        assert_eq!(entity, Token::new(11));
                        if !matches!(story, Story::Cancel | Story::Abort | Story::Timeout) {
                            assert!(browser_bytes.starts_with(if callback_count == 1 && story == Story::LongHead {
                                b"HTTP/1.1 414"
                            } else if callback_count == 1 && matches!(story, Story::WrongPath | Story::WrongHost) {
                                b"HTTP/1.1 404"
                            } else {
                                b"HTTP/1.1 200"
                            }));
                        }
                        trace.append(&mut callback_wire.trace);
                        callback_pending = false;
                        callback.take().expect("callback owner")
                    };
                    client.component.as_mut().expect("component").up(
                        &env,
                        io::Event::Closed { owner: identity },
                        &mut client.above,
                        &mut client.requests,
                        &mut client.files,
                    );
                }
                io::Request::Reject { .. }
                | io::Request::Output { .. }
                | io::Request::Spawn { .. }
                | io::Request::Signal { .. }
                | io::Request::Usage { .. } => panic!("only admitted sockets"),
            }
        }
        if let Some(identity) = callback
            && let Some(up) = callback_wire.answer()
        {
            client.component.as_mut().expect("component").up(
                &env,
                io::Event::Stream { owner: identity, up },
                &mut client.above,
                &mut client.requests,
                &mut client.files,
            );
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
        if client.closed
            && client.requests.is_empty()
            && client.above.is_empty()
            && owner.is_none()
            && listener.is_none()
            && callback.is_none()
        {
            assert_eq!(
                issuer.posts(),
                if story == Story::Public {
                    2
                } else {
                    u64::from(!matches!(story, Story::Cancel | Story::Abort | Story::Timeout))
                }
            );
            trace.append(&mut client_wire.trace);
            trace.append(&mut server_wire.trace);
            trace.append(&mut callback_wire.trace);
            return (client.facts, trace);
        }
        now = now.saturating_add(Duration::from_millis(1));
    }
    panic!("protocol world settles");
}
