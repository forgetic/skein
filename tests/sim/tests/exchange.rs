//! A client and a server exchanging bytes (simulator.md, 6): every byte
//! both ways in a calm world, and a seed that replays. The sweep under
//! chaos is in `fuzzy_exchange.rs`.

use skein_sim::Config;
use skein_sim_tests::exchange::run;

#[test]
fn a_calm_exchange_delivers_every_byte_both_ways() {
    for seed in 0..20_u64 {
        let outcome = run(seed, Config::calm());
        assert!(!outcome.broken, "nothing breaks in a calm world");
    }
}

#[test]
fn the_same_seed_replays_to_the_same_trace() {
    for seed in [3_u64, 17, 4242] {
        let first = run(seed, Config::chaos());
        let second = run(seed, Config::chaos());
        assert!(first.trace == second.trace, "seed {seed} replays");
        assert_eq!((first.to_server, first.to_client), (second.to_server, second.to_client));
    }
    let a = run(1, Config::chaos()).trace;
    let b = run(2, Config::chaos()).trace;
    assert!(a != b, "another seed, another run");
}
