use skein_heap::{Counting, Meter};
use skein_lib::{Duration, Env, List, Queue, Time, Wall, bytes};
use skein_oauth::{ClientLimits, SavedToken};
use skein_oauth_accounts::{Account, Component, Limits, MAX_OUT_DOWN, Request, worst_case};

#[global_allocator]
static HEAP: Counting = Counting;

#[test]
fn every_account_holds_a_maximal_record_within_the_heap_bound() {
    let document = skein_oauth::Limits {
        document_bytes: 512,
        string_bytes: 64,
        token_bytes: 64,
        client_bytes: 64,
        detail_bytes: 64,
        record_bytes: 256,
        depth: 8,
        tokens: 64,
    };
    let limits = Limits {
        accounts: 4,
        file_stall: Duration::from_secs(1),
        exchanges: 4,
        listeners: 0,
        server: skein_http::server::Limits {
            head: 2048,
            headers: 16,
            body: 1024,
            read: 256,
            response: 2048,
            send: 256,
        },
        http: skein_http::client::Limits { request: 2048, head: 2048, headers: 16, read: 256, send: 256 },
        tls: skein_tls::client::Limits { read: 2048, send: 2048, records: 131_072 },
        io: skein_io::Limits {
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
        refresh_lead: Duration::from_secs(10),
        client: ClientLimits {
            document,
            uri_bytes: 256,
            scope_bytes: 64,
            state_bytes: 64,
            code_bytes: 64,
            url_bytes: 512,
            request_bytes: 512,
            sign_in_time: Duration::from_secs(120),
            request_time: Duration::from_secs(10),
            backoff_base: Duration::from_secs(2),
            backoff_ceiling: Duration::from_secs(8),
            max_attempts: 3,
        },
    };
    let bound = worst_case(&limits).expect("heap bound");
    let meter = Meter::new();
    meter.start();
    let mut configured = List::with_capacity(limits.accounts);
    for _ in 0..limits.accounts {
        assert!(configured.push(Account::HandedIn).is_ok(), "account slot");
    }
    let mut component = Component::new(configured, &limits, 17).expect("component");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut out = Queue::with_capacity(MAX_OUT_DOWN.above);
    for account in 0..limits.accounts {
        component.down(
            &env,
            Request::HandIn {
                account,
                record: SavedToken {
                    key: account,
                    generation: 1,
                    access_token: bytes::copy_of(&[b'x'; 64]),
                    refresh_token: None,
                    metadata: Some(bytes::copy_of(&[b'm'; 64])),
                    expires_at: Wall::from_nanos(30_000_000_000),
                },
            },
            &mut out,
            &mut Queue::with_capacity(MAX_OUT_DOWN.io),
            &mut Queue::with_capacity(1),
        );
        component.down(
            &env,
            Request::Grant { account },
            &mut out,
            &mut Queue::with_capacity(MAX_OUT_DOWN.io),
            &mut Queue::with_capacity(1),
        );
        drop(out.pop());
    }
    let measured = meter.end();
    meter.check(measured, bound, &"OAuth handed-in accounts");
}

#[test]
fn every_refresh_slot_holds_its_maximal_records_and_tls_connection() {
    use skein_oauth_accounts::{Endpoint, Keeper, MAX_OUT_FIRE, Transport};
    let mut limits = skein_oauth_accounts_world::world::limits();
    limits.accounts = 4;
    limits.exchanges = 4;
    limits.io.sockets = 6;
    let trust = skein_tls_world::pki::client(&[]);
    let bound = worst_case(&limits).expect("component heap bound");
    let meter = Meter::new();
    meter.start();
    let mut configured = List::with_capacity(limits.accounts);
    for account in 0..limits.accounts {
        let registration = skein_oauth::Registration {
            authorization_url: bytes::copy_of(b"https://localhost/authorize"),
            token_endpoint: bytes::copy_of(b"https://localhost/token"),
            client_id: bytes::copy_of(&[b'c'; 64]),
            redirect_uri: bytes::copy_of(b"http://localhost:31234/callback"),
            scope: bytes::copy_of(&[b's'; 64]),
            wire: skein_oauth::WireFormat::Form,
            client_secret: None,
            pkce_for_confidential: false,
            metadata_claim: None,
        };
        assert!(
            configured
                .push(Account::SignIn {
                    registration,
                    endpoint: Endpoint {
                        address: "127.0.0.1:31000".parse().expect("loopback"),
                        transport: Transport::Tls { server_name: skein_tls_world::pki::name(), trust: trust.clone() },
                    },
                    keeper: Keeper::Owner {
                        kept: Some(SavedToken {
                            key: account,
                            generation: 1,
                            access_token: bytes::copy_of(&[b'a'; 256]),
                            refresh_token: Some(bytes::copy_of(&[b'r'; 256])),
                            metadata: Some(bytes::copy_of(&[b'm'; 256])),
                            expires_at: Wall::from_nanos(5_000_000_000),
                        })
                    },
                })
                .is_ok()
        );
    }
    let mut component = Component::new(configured, &limits, 17).expect("configured slots");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut above = Queue::with_capacity(MAX_OUT_DOWN.above.max(MAX_OUT_FIRE.above));
    let mut io = Queue::with_capacity(MAX_OUT_DOWN.io.max(MAX_OUT_FIRE.io));
    for account in 0..limits.accounts {
        component.down(&env, Request::Grant { account }, &mut above, &mut io, &mut Queue::with_capacity(1));
        assert!(above.is_empty(), "grant waits for a kept refresh");
    }
    let mut connects = 0;
    for _ in 0..64 {
        if !component.has_work() {
            break;
        }
        component.fire(&env, &mut above, &mut io, &mut Queue::with_capacity(1));
        assert!(above.is_empty());
        while let Some(request) = io.pop() {
            assert!(matches!(request, skein_io::Request::Connect { .. }));
            connects += 1;
        }
    }
    assert_eq!(connects, limits.exchanges, "one TLS connection in every slot");
    let measured = meter.end();
    meter.check(measured, bound, &"all OAuth TLS exchange slots");
}

#[test]
fn every_listener_slot_keeps_a_bounded_partial_head_within_the_heap_bound() {
    use skein_io::{Event as IoEvent, Request as IoRequest};
    use skein_lib::Token;
    use skein_oauth_accounts::{Endpoint, Keeper, MAX_OUT_FIRE, MAX_OUT_UP, Transport};
    use skein_world::stream::Wire;
    let mut limits = skein_oauth_accounts_world::world::limits();
    limits.accounts = 4;
    limits.exchanges = 4;
    limits.listeners = 4;
    limits.io.sockets = 12;
    let bound = worst_case(&limits).expect("all listener bounds");
    let meter = Meter::new();
    meter.start();
    let mut configured = List::with_capacity(4);
    for account in 0..4 {
        let uri = format!("http://localhost:{}/callback", 31234 + account).into_bytes().into_boxed_slice();
        assert!(
            configured
                .push(Account::SignIn {
                    registration: registration(b"client", uri, None, b"read"),
                    endpoint: Endpoint {
                        address: "127.0.0.1:31000".parse().expect("loopback"),
                        transport: Transport::Plaintext
                    },
                    keeper: Keeper::Owner { kept: None }
                })
                .is_ok()
        );
    }
    let mut component = Component::new(configured, &limits, 17).expect("component");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut above = Queue::with_capacity(64);
    let mut below = Queue::with_capacity(64);
    let mut callbacks = Vec::with_capacity(4);
    let mut wires = Vec::with_capacity(4);
    for account in 0..4 {
        component.down(&env, Request::SignIn { account }, &mut above, &mut below, &mut Queue::with_capacity(1));
    }
    for _ in 0..512 {
        if component.has_work() {
            assert!(above.room() >= MAX_OUT_FIRE.above && below.room() >= MAX_OUT_FIRE.io);
            component.fire(&env, &mut above, &mut below, &mut Queue::with_capacity(1));
        }
        while let Some(event) = above.pop() {
            assert!(matches!(event, skein_oauth_accounts::Event::Visit { .. }));
        }
        while let Some(request) = below.pop() {
            match request {
                IoRequest::Listen { owner, addr } => {
                    let index = u64::try_from(wires.len()).expect("four listeners");
                    let listener = Token::new(20 + index);
                    let socket = Token::new(40 + index);
                    component.up(
                        &env,
                        IoEvent::Listening { owner, listener, addr },
                        &mut above,
                        &mut below,
                        &mut Queue::with_capacity(1),
                    );
                    component.up(
                        &env,
                        IoEvent::Accepted { owner, socket, peer: addr },
                        &mut above,
                        &mut below,
                        &mut Queue::with_capacity(1),
                    );
                    let mut wire = Wire::new(index);
                    let mut partial = b"GET /callback?state=".to_vec();
                    partial.resize(usize::try_from(limits.server.head - 64).expect("head size"), b'x');
                    wire.write(&partial);
                    wires.push((socket, wire));
                }
                IoRequest::Bind { socket, owner } => callbacks.push((socket, owner)),
                IoRequest::Stream { stream, down } => {
                    wires.iter_mut().find(|(socket, _)| *socket == stream).expect("callback wire").1.take(down);
                }
                other @ (IoRequest::Connect { .. }
                | IoRequest::Reject { .. }
                | IoRequest::Output { .. }
                | IoRequest::Spawn { .. }
                | IoRequest::Signal { .. }
                | IoRequest::Usage { .. }
                | IoRequest::Close { .. }
                | IoRequest::Abort { .. }) => panic!("partial head remains live: {other:?}"),
            }
        }
        for (socket, wire) in &mut wires {
            if let Some(up) = wire.answer() {
                let owner = callbacks.iter().find(|(candidate, _)| candidate == socket).expect("bound callback").1;
                assert!(above.room() >= MAX_OUT_UP.above && below.room() >= MAX_OUT_UP.io);
                component.up(&env, IoEvent::Stream { owner, up }, &mut above, &mut below, &mut Queue::with_capacity(1));
            }
        }
    }
    assert_eq!(callbacks.len(), 4);
    assert!(component.next_deadline().is_some());
    let measured = meter.end();
    meter.check(measured, bound, &"every OAuth listener and partial head");
}

#[test]
fn every_private_keeper_retains_its_maximal_paths_and_pending_load() {
    use skein_io::file;
    use skein_lib::Token;
    use skein_oauth_accounts::{Endpoint, FileLower, Keeper, Transport};
    let mut limits = skein_oauth_accounts_world::world::limits();
    limits.accounts = 4;
    limits.exchanges = 4;
    limits.listeners = 0;
    limits.io.sockets = 4;
    let bound = worst_case(&limits).expect("private account bound");
    let meter = Meter::new();
    meter.start();
    let mut configured = List::with_capacity(4);
    for _ in 0..4 {
        assert!(
            configured
                .push(Account::SignIn {
                    registration: registration(
                        &[b'c'; 64],
                        bytes::copy_of(b"https://service.example/callback"),
                        Some(bytes::copy_of(&[b's'; 64])),
                        &[b's'; 64],
                    ),
                    endpoint: Endpoint { address: ([127, 0, 0, 1], 31000).into(), transport: Transport::Plaintext },
                    keeper: Keeper::Private {
                        root: Token::new(99),
                        directory: bytes::copy_of(&[b'd'; 4095]),
                        file: bytes::copy_of(&[b'f'; 4095])
                    },
                })
                .is_ok()
        );
    }
    let mut component = Component::new(configured, &limits, 17).expect("maximal private keepers");
    let env = Env { now: Time::ZERO, wall: Wall::EPOCH, limits };
    let mut above = Queue::with_capacity(32);
    let mut io = Queue::with_capacity(32);
    let mut files = Queue::with_capacity(32);
    for account in 0..4 {
        component.down(&env, Request::SignIn { account }, &mut above, &mut io, &mut files);
        component.down(&env, Request::Grant { account }, &mut above, &mut io, &mut files);
    }
    for _ in 0..4 {
        component.fire(&env, &mut above, &mut io, &mut files);
    }
    assert!(above.is_empty() && io.is_empty(), "all exchanges wait for private loading");
    assert_eq!(files.len(), 4);

    let mut requests = Vec::with_capacity(4);
    while let Some(lower) = files.pop() {
        match lower {
            FileLower::Request { request: file::Request::OpenPrivate { owner, .. }, .. } => requests.push(owner),
            FileLower::Request { .. } | FileLower::Cancel { .. } => panic!("expected private opening"),
        }
    }
    for (account, owner) in requests.iter().enumerate() {
        component.filed(
            &env,
            file::Event::Opened {
                owner: *owner,
                file: Token::new(u64::try_from(account).expect("four roots") + 100),
                len: 0,
            },
            &mut above,
            &mut io,
            &mut files,
        );
    }
    for _ in 0..4 {
        component.fire(&env, &mut above, &mut io, &mut files);
    }
    assert_eq!(files.len(), 4, "one pending load per keeper at the account limit");
    let mut loads = Vec::with_capacity(4);
    while let Some(lower) = files.pop() {
        match lower {
            FileLower::Request { request: file::Request::Load { owner, .. }, .. } => loads.push(owner),
            FileLower::Request { .. } | FileLower::Cancel { .. } => panic!("expected private loading"),
        }
    }
    for (account, owner) in loads.iter().enumerate() {
        let record = maximal_private_record(u32::try_from(account).expect("four accounts"));
        component.filed(
            &env,
            file::Event::Loaded {
                owner: *owner,
                bytes: skein_oauth::encode_record(&record, &limits.client.document).expect("maximal record"),
            },
            &mut above,
            &mut io,
            &mut files,
        );
    }
    for _ in 0..4 {
        component.fire(&env, &mut above, &mut io, &mut files);
    }
    assert_eq!(above.len(), 4, "every sign-in slot starts only after its maximal record loaded");
    let measured = meter.end();
    meter.check(measured, bound, &"all private paths, pending roots, records and sign-in slots");
}

fn registration(
    client_id: &[u8],
    redirect_uri: Box<[u8]>,
    client_secret: Option<Box<[u8]>>,
    scope: &[u8],
) -> skein_oauth::Registration {
    skein_oauth::Registration {
        authorization_url: bytes::copy_of(b"http://127.0.0.1:31000/authorize"),
        token_endpoint: bytes::copy_of(b"http://127.0.0.1:31000/token"),
        client_id: bytes::copy_of(client_id),
        redirect_uri,
        scope: bytes::copy_of(scope),
        wire: skein_oauth::WireFormat::Form,
        client_secret,
        pkce_for_confidential: false,
        metadata_claim: None,
    }
}

fn maximal_private_record(account: u32) -> SavedToken {
    SavedToken {
        key: account,
        generation: 0,
        access_token: bytes::copy_of(&[b'a'; 256]),
        refresh_token: Some(bytes::copy_of(&[b'r'; 256])),
        metadata: Some(bytes::copy_of(&[b'm'; 256])),
        expires_at: Wall::from_nanos(30_000_000_000),
    }
}
