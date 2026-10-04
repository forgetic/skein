//! Handshakes against a rustls server (tls.md, 6): each version, a retry,
//! ALPN, certificates checked against the wall time handed in, a chain
//! longer than the records held, peers that are not TLS or give up, and the
//! stream ending or failing before the handshake and during it.

use skein_lib::stream::{Down, Fault, Read, Up};
use skein_lib::{Env, Time, Wall};
use skein_tls::client::{self, Agreed, Certificate, Client, Error, Event, Limits, Request, Version, Waiting};
use skein_tls::{Config, Name};
use skein_tls_world::drive::Pair;
use skein_tls_world::pki::{self, Chain, Versions};
use skein_tls_world::server::Server;

const LIMITS: Limits = Limits { read: 4_096, send: 4_096, records: 2 * client::MAX_RECORD };

fn pair(server: &pki::Server, name: &str, wall: Wall, config: &Config, limits: Limits) -> Pair {
    let name = Name::new(name).expect("a name");
    let client = Client::new(config, name, &limits);
    Pair::new(client, Env { now: Time::ZERO, wall, limits }, Server::new(server.config()))
}

/// The handshake, as far as it goes.
fn handshake(pair: &mut Pair) -> Vec<Event> {
    let mut events = pair.down(Request::Handshake);
    events.extend(pair.settle());
    events
}

fn ready(version: Version, alpn: Option<&[u8]>) -> Vec<Event> {
    vec![Event::Ready(Agreed { version, alpn: alpn.map(Box::from) })]
}

fn failed(error: Error) -> Vec<Event> {
    vec![Event::Stream(Up::Failed(error.fault())), Event::Failed(error)]
}

#[test]
fn a_handshake_of_either_version_is_ready_with_what_was_agreed() {
    for (versions, version) in
        [(Versions::Both, Version::Tls13), (Versions::Tls13, Version::Tls13), (Versions::Tls12, Version::Tls12)]
    {
        let server = pki::Server { versions, ..pki::Server::plain() };
        let mut pair = pair(&server, "skein.test", pki::VALID, &pki::client(&[]), LIMITS);
        assert_eq!(pair.client.waiting(), Waiting::Handshake);
        assert_eq!(handshake(&mut pair), ready(version, None), "{versions:?}");
        assert_eq!(pair.client.waiting(), Waiting::Above, "{versions:?}: nothing demanded");
        assert!(!pair.wire.server.handshaking(), "{versions:?}: the server is done too");
    }
}

#[test]
fn alpn_agrees_the_servers_choice_of_what_the_client_offers() {
    let config = pki::client(&[b"h2", b"http/1.1"]);
    let server = pki::Server { alpn: vec![b"http/1.1".to_vec()], ..pki::Server::plain() };
    let mut accepted = pair(&server, "skein.test", pki::VALID, &config, LIMITS);
    assert_eq!(handshake(&mut accepted), ready(Version::Tls13, Some(b"http/1.1")));
    // A server that accepts none: no protocol agreed.
    let mut none = pair(&pki::Server::plain(), "skein.test", pki::VALID, &config, LIMITS);
    assert_eq!(handshake(&mut none), ready(Version::Tls13, None));
}

#[test]
fn a_server_that_offers_no_protocol_the_client_offered_gives_up_with_an_alert() {
    let server = pki::Server { alpn: vec![b"bar".to_vec()], ..pki::Server::plain() };
    let mut pair = pair(&server, "skein.test", pki::VALID, &pki::client(&[b"foo"]), LIMITS);
    // no_application_protocol (RFC 7301, 3.2): the peer reset the stream.
    assert_eq!(handshake(&mut pair), failed(Error::Alert(120)));
    assert_eq!(Error::Alert(120).fault(), Fault::Reset);
    assert_eq!(pair.client.waiting(), Waiting::Close);
}

#[test]
fn a_server_that_wants_another_key_share_retries_and_both_hellos_fit_a_flight() {
    let server = pki::Server { retry: true, ..pki::Server::plain() };
    let mut pair = pair(&server, "skein.test", pki::VALID, &pki::client(&[]), LIMITS);
    assert_eq!(handshake(&mut pair), ready(Version::Tls13, None));
    // Two ClientHellos, then the flight that ends the handshake.
    assert_eq!(pair.wire.sends.len(), 3, "{:?}", pair.wire.sends);
    for sent in &pair.wire.sends {
        assert!(*sent <= usize::try_from(client::FLIGHT).expect("fits"), "{:?}", pair.wire.sends);
    }
}

#[test]
fn the_longest_hello_fits_a_flight() {
    // A name of 253 bytes and protocols of ALPN bytes: the longest
    // ClientHello the configuration allows; and a second one after a retry.
    let label = "a".repeat(61);
    let name = format!("a{label}.{label}.{label}.{label}.test");
    assert_eq!(name.len(), 253);
    let protocols: Vec<Vec<u8>> = (0..64).map(|n| format!("p{n:02}").into_bytes()).collect();
    let protocols: Vec<&[u8]> = protocols.iter().map(Vec::as_slice).collect();
    let config = pki::client(&protocols);
    for retry in [false, true] {
        let server = pki::Server { retry, ..pki::Server::plain() };
        let mut pair = pair(&server, &name, pki::VALID, &config, LIMITS);
        let _ = handshake(&mut pair);
        let hello = pair.wire.sends[0];
        assert!(hello <= 1_024, "a third of FLIGHT, about: {hello}");
        assert!(pair.wire.sends.iter().all(|sent| *sent <= usize::try_from(client::FLIGHT).expect("fits")));
    }
}

#[test]
fn certificates_are_checked_against_the_wall_time_handed_in() {
    let config = pki::client(&[]);
    for (wall, outcome) in [
        (pki::VALID, ready(Version::Tls13, None)),
        (pki::EXPIRED, failed(Error::Certificate(Certificate::Expired))),
        (pki::EARLY, failed(Error::Certificate(Certificate::NotYetValid))),
    ] {
        let mut pair = pair(&pki::Server::plain(), "skein.test", wall, &config, LIMITS);
        assert_eq!(handshake(&mut pair), outcome, "{wall:?}");
    }
    // Either version.
    let server = pki::Server { versions: Versions::Tls12, ..pki::Server::plain() };
    let mut pair = pair(&server, "skein.test", pki::EXPIRED, &config, LIMITS);
    assert_eq!(handshake(&mut pair), failed(Error::Certificate(Certificate::Expired)));
}

#[test]
fn a_certificate_for_another_name_or_from_an_unknown_root_is_refused() {
    let config = pki::client(&[]);
    let mut other = pair(&pki::Server::plain(), "other.test", pki::VALID, &config, LIMITS);
    assert_eq!(handshake(&mut other), failed(Error::Certificate(Certificate::Name)));
    let untrusted = pki::Server { chain: Chain::Untrusted, ..pki::Server::plain() };
    let mut unknown = pair(&untrusted, "skein.test", pki::VALID, &config, LIMITS);
    assert_eq!(handshake(&mut unknown), failed(Error::Certificate(Certificate::Issuer)));
    // An IP address the certificate names.
    let mut address = pair(&pki::Server::plain(), "127.0.0.1", pki::VALID, &config, LIMITS);
    assert_eq!(handshake(&mut address), ready(Version::Tls13, None));
    assert_eq!(Error::Certificate(Certificate::Name).fault(), Fault::Invalid);
}

#[test]
fn a_chain_longer_than_the_records_held_is_too_long() {
    let config = pki::client(&[]);
    for versions in [Versions::Tls13, Versions::Tls12] {
        let server = pki::Server { chain: Chain::Big, versions, ..pki::Server::plain() };
        let short = Limits { records: client::MAX_RECORD, ..LIMITS };
        let mut pair_short = pair(&server, "big.skein.test", pki::VALID, &config, short);
        assert_eq!(handshake(&mut pair_short), failed(Error::TooLong), "{versions:?}");
        // Three records' room holds its 40 KB, joined in place.
        let long = Limits { records: 3 * client::MAX_RECORD, ..LIMITS };
        let mut pair_long = pair(&server, "big.skein.test", pki::VALID, &config, long);
        let version = if versions == Versions::Tls12 { Version::Tls12 } else { Version::Tls13 };
        assert_eq!(handshake(&mut pair_long), ready(version, None), "{versions:?}");
    }
}

#[test]
fn a_peer_that_is_not_tls_is_refused() {
    let mut pair = pair(&pki::Server::plain(), "skein.test", pki::VALID, &pki::client(&[]), LIMITS);
    let _ = pair.down(Request::Handshake);
    // The ClientHello goes, and an HTTP server answers it, in place of what
    // the TLS server answered.
    let room = pair.wire.answer().expect("room for the hello");
    assert_eq!(pair.up(room), vec![]);
    pair.wire.bytes.clear();
    pair.wire.bytes.extend(b"HTTP/1.1 400 Bad Request\r\n\r\n");
    assert_eq!(pair.settle(), failed(Error::Protocol));
}

#[test]
fn the_stream_ending_or_failing_before_the_handshake_fails_it_when_asked() {
    let config = pki::client(&[]);
    for (ev, error) in [(Up::End, Error::Truncated), (Up::Failed(Fault::Reset), Error::Stream(Fault::Reset))] {
        let mut pair = pair(&pki::Server::plain(), "skein.test", pki::VALID, &config, LIMITS);
        assert_eq!(pair.up(ev), vec![], "{error:?}: told when the handshake is asked for");
        assert_eq!(pair.client.waiting(), Waiting::Handshake);
        assert_eq!(pair.down(Request::Handshake), failed(error));
        assert_eq!(pair.wire.demand, None, "{error:?}: nothing demanded below");
        assert_eq!(pair.down(Request::Close), vec![Event::Closed]);
    }
}

#[test]
fn the_stream_ending_or_failing_during_the_handshake_fails_it() {
    let config = pki::client(&[]);
    for (ev, error) in [(Up::End, Error::Truncated), (Up::Failed(Fault::Other), Error::Stream(Fault::Other))] {
        let mut pair = pair(&pki::Server::plain(), "skein.test", pki::VALID, &config, LIMITS);
        let _ = pair.down(Request::Handshake);
        let room = pair.wire.answer().expect("room for the hello");
        assert_eq!(pair.up(room), vec![]);
        assert_eq!(pair.client.waiting(), Waiting::Handshaking);
        assert_eq!(pair.wire.demand, Some((Read::Fill(client::HEADER), 0)), "a record's header");
        if ev == Up::End {
            pair.wire.demand = None;
        }
        assert_eq!(pair.up(ev), failed(error));
        assert_eq!(pair.client.waiting(), Waiting::Close);
    }
}

#[test]
fn closes_before_and_during_the_handshake_send_nothing() {
    let config = pki::client(&[]);
    let mut fresh = pair(&pki::Server::plain(), "skein.test", pki::VALID, &config, LIMITS);
    assert_eq!(fresh.down(Request::Close), vec![Event::Closed]);
    assert_eq!(fresh.client.waiting(), Waiting::Nothing);
    let mut running = pair(&pki::Server::plain(), "skein.test", pki::VALID, &config, LIMITS);
    let _ = running.down(Request::Handshake);
    assert!(running.wire.demand.is_some());
    // The demand for the hello's room withdrawn, nothing sent.
    assert_eq!(running.down(Request::Close), vec![Event::Closed]);
    assert_eq!(running.wire.demand, None);
    assert!(running.wire.sends.is_empty());
}

#[test]
fn a_demand_made_during_the_handshake_waits_for_it() {
    let mut pair = pair(&pki::Server::plain(), "skein.test", pki::VALID, &pki::client(&[]), LIMITS);
    let _ = pair.down(Request::Handshake);
    assert_eq!(pair.down(Request::Stream(Down::Demand { read: Read::Fill(5), room: 16 })), vec![]);
    let events = pair.settle();
    // Ready, then room for the demand, once the flight that ends the
    // handshake went.
    let mut expected = ready(Version::Tls13, None);
    expected.push(Event::Stream(Up::Room));
    assert_eq!(events, expected);
}
