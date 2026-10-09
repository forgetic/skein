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
        exchanges: 4,
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
        );
        component.down(&env, Request::Grant { account }, &mut out, &mut Queue::with_capacity(MAX_OUT_DOWN.io));
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
        component.down(&env, Request::Grant { account }, &mut above, &mut io);
        assert!(above.is_empty(), "grant waits for a kept refresh");
    }
    let mut connects = 0;
    for _ in 0..64 {
        if !component.has_work() {
            break;
        }
        component.fire(&env, &mut above, &mut io);
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
