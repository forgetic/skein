//! Actual issuer listener admission and settlement.
use skein_fake_oauth as issuer;
use skein_fake_peers::{Limits, Transport, oauth};
use skein_lib::{Duration, bytes};
use skein_world::Host;
use std::net::{Ipv4Addr, SocketAddr};

#[test]
fn issuer_process_binds_and_settles_for_both_transports() {
    for transport in [Transport::Plaintext, Transport::Tls] {
        let scheme = if transport == Transport::Plaintext { "http" } else { "https" };
        let document = skein_oauth::Limits {
            document_bytes: 1024,
            string_bytes: 256,
            token_bytes: 256,
            client_bytes: 64,
            detail_bytes: 64,
            record_bytes: 1024,
            depth: 8,
            tokens: 64,
        };
        let limits = Limits {
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
            observation_bytes: 4096,
        };
        let mut peer = oauth::Peer::new(
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            transport,
            limits,
            issuer::Config {
                authorization_url: format!("{scheme}://127.0.0.1/authorize").into_bytes().into_boxed_slice(),
                token_endpoint: format!("{scheme}://127.0.0.1/token").into_bytes().into_boxed_slice(),
                client_id: bytes::copy_of(b"client"),
                client_secret: None,
                redirect_uri: bytes::copy_of(b"http://127.0.0.1:1234/callback"),
                refresh_token: bytes::copy_of(b"seed"),
            },
            issuer::Limits { document, uri_bytes: 256, request_bytes: 1024, codes: 2, rotations: 2, plans: 2 },
            skein_http::server::Limits { head: 2048, headers: 16, body: 1024, read: 256, response: 2048, send: 512 },
        )
        .unwrap();
        let mut sim = skein_sim::Sim::new(31, skein_sim::Config::calm());
        let pid = sim.spawn_process();
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
    }
}
