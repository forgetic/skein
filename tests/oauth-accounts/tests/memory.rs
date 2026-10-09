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
        configured.push(Account::HandedIn).expect("account slot");
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
        );
        component.down(&env, Request::Grant { account }, &mut out);
        drop(out.pop());
    }
    let measured = meter.end();
    meter.check(measured, bound, &"OAuth handed-in accounts");
}
