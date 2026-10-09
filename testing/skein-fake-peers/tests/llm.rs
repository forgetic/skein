//! Actual io listener admission and clean process settlement.
use std::net::{Ipv4Addr, SocketAddr};

use skein_fake_llm_domain::{self as domain, api};
use skein_fake_llm_protocol::{documents, provider};
use skein_fake_peers::{Limits, Transport, llm};
use skein_lib::{Duration, Time};
use skein_world::Host;

fn limits() -> Limits {
    Limits {
        io: skein_io::Limits {
            sockets: 3,
            refusals: 1,
            intake: 32_768,
            receive: 1024,
            output: 32_768,
            sends: 2,
            accepts: 1,
            backlog: 1,
            close_timeout: Duration::from_secs(1),
            retry: Duration::from_millis(1),
        },
        connections: 2,
        queue: 64,
        plaintext: 32_768,
        ciphertext: 32_768,
        observations: 16,
        observation_bytes: 32_768,
    }
}

fn make(address: SocketAddr, transport: Transport) -> Result<llm::Peer, skein_fake_peers::Error> {
    let machine_limits = skein_llm_world::fake::config();
    let domain = domain::Domain::configured(
        &machine_limits,
        23,
        Box::new([]),
        api::Menu { arguments: Box::new([]), invalid: Box::new([]) },
    )
    .expect("configured script domain");
    let call = skein_llm_world::call(1);
    llm::Peer::new(
        address,
        transport,
        limits(),
        provider::Config {
            echo: skein_llm::openai::Echo::NONE,
            provider: documents::Provider::OpenAi,
            path: call.endpoint.target,
            headers: Box::new([]),
        },
        call.credential,
        skein_llm_world::fake::limits(&skein_llm_world::limits()),
        domain,
        machine_limits,
    )
}

#[test]
fn fake_peer_binds_loopback_and_shutdown_settles_the_actual_listener() {
    for transport in [Transport::Plaintext, Transport::Tls] {
        let mut sim = skein_sim::Sim::new(23, skein_sim::Config::calm());
        let pid = sim.spawn_process();
        let mut peer = make(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), transport).unwrap();
        for _ in 0..100 {
            sim.reap(pid, peer.completions());
            peer.iterate(sim.now(), sim.wall());
            sim.submit(pid, peer.submissions());
            if peer.address().is_some() {
                break;
            }
            if !peer.work_pending(sim.now())
                && let Some(next) = sim.next_due()
            {
                sim.advance_to(next);
            }
        }
        assert!(peer.address().unwrap().ip().is_loopback());
        assert_ne!(peer.address().unwrap().port(), 0);
        peer.shutdown();
        for _ in 0..100 {
            sim.reap(pid, peer.completions());
            peer.iterate(sim.now(), sim.wall());
            sim.submit(pid, peer.submissions());
            if peer.is_empty() {
                break;
            }
            if !peer.work_pending(sim.now())
                && let Some(next) = sim.next_due()
            {
                sim.advance_to(next);
            }
        }
        assert!(peer.is_empty());
        sim.assert_quiescent(pid);
        sim.assert_no_open_fds(pid);
        assert!(!peer.work_pending(Time::ZERO));
    }
}

#[test]
fn fake_peer_refuses_non_loopback_configuration_before_effects() {
    assert!(matches!(
        make(SocketAddr::from((Ipv4Addr::new(192, 0, 2, 1), 80)), Transport::Plaintext),
        Err(skein_fake_peers::Error::Limits)
    ));
}
