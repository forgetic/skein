//! The limits and what they price, the configuration and its refusals, a
//! server's name, and the fault each error tells the plaintext stream.

use rustls::RootCertStore;
use skein_lib::stream::Fault;

use super::{LIMITS, roots};
use crate::client::{self, Certificate, Error, Limits};
use crate::{ALPN, Config, Name, Refusal};

#[test]
fn room_for_a_send_counts_its_records_and_rustlss_slack() {
    // Each record adds at most 29 bytes; rustls may write 54 before them.
    assert_eq!(client::room_for(1), Some(1 + 29 + 54));
    assert_eq!(client::room_for(16_384), Some(16_384 + 29 + 54));
    assert_eq!(client::room_for(16_385), Some(16_385 + 2 * 29 + 54));
    assert_eq!(client::room_for(u32::MAX), None);
    // The largest room: a send's, or TLS's own output.
    assert_eq!(client::largest_room(&LIMITS), client::FLIGHT);
    let large = Limits { send: 40_000, ..LIMITS };
    assert_eq!(client::largest_room(&large), 40_000 + 3 * 29 + 54);
    assert_eq!(client::LARGEST_READ, 16_384 + 2_048);
    assert_eq!(client::MAX_RECORD, 5 + 16_384 + 2_048);
}

#[test]
fn the_worst_case_adds_the_buffers_and_rustlss_heap() {
    let worst = client::worst_case(&LIMITS).unwrap();
    // The intake (the read and a record's plaintext), the records, the
    // output owed, a delivery, rustls's own, and the decoded form of the
    // longest handshake message the records hold.
    let buffers = 16 + 16_384 + u64::from(client::MAX_RECORD) + u64::from(client::FLIGHT);
    let rustls = u64::from(client::LARGEST_READ) + 16 * 1_024 + 20 * u64::from(client::MAX_RECORD);
    assert_eq!(worst, buffers + rustls);
    // rustls reads no handshake message past 64 KB, whatever the records.
    let large = Limits { records: 8 * client::MAX_RECORD, ..LIMITS };
    let past = client::worst_case(&large).unwrap() - u64::from(7 * client::MAX_RECORD) - buffers;
    assert_eq!(past, u64::from(client::LARGEST_READ) + 16 * 1_024 + 20 * u64::from(client::MAX_HANDSHAKE));
    // Each limit that cannot be honoured.
    for limits in [
        Limits { read: 0, ..LIMITS },
        Limits { send: 0, ..LIMITS },
        Limits { records: client::MAX_RECORD - 1, ..LIMITS },
        Limits { send: u32::MAX, ..LIMITS },
        Limits { read: u32::MAX, ..LIMITS },
    ] {
        assert_eq!(client::worst_case(&limits), None, "{limits:?}");
    }
}

#[test]
fn a_configuration_trusts_some_root_and_offers_protocols_that_fit() {
    drop(Config::new(roots(), &[b"h2", b"http/1.1"]).unwrap());
    assert_eq!(Config::new(RootCertStore::empty(), &[]).unwrap_err(), Refusal::Roots);
    assert_eq!(Config::new(roots(), &[b""]).unwrap_err(), Refusal::Alpn);
    let long = [b'a'; 256];
    assert_eq!(Config::new(roots(), &[&long]).unwrap_err(), Refusal::Alpn);
    // Two names of 127 bytes, each with its length's byte, fill ALPN
    // exactly; a byte more is refused.
    let name = [b'a'; 127];
    drop(Config::new(roots(), &[&name, &name]).unwrap());
    assert_eq!(ALPN, 2 * (127 + 1));
    let longer = [b'a'; 128];
    assert_eq!(Config::new(roots(), &[&name, &longer]).unwrap_err(), Refusal::Alpn);
}

#[test]
fn a_name_is_a_dns_name_or_an_ip_address() {
    assert!(Name::new("skein.test").is_some());
    assert!(Name::new("127.0.0.1").is_some());
    assert!(Name::new("::1").is_some());
    assert_eq!(Name::new(""), None);
    assert_eq!(Name::new("not a name"), None);
    let name = Name::new("skein.test").unwrap();
    assert_eq!(name.clone(), name, "a name is data, each connection's copy");
}

#[test]
fn each_error_tells_the_stream_what_it_can_act_on() {
    for certificate in
        [Certificate::Expired, Certificate::NotYetValid, Certificate::Name, Certificate::Issuer, Certificate::Invalid]
    {
        assert_eq!(Error::Certificate(certificate).fault(), Fault::Invalid);
    }
    for error in [Error::Decrypt, Error::Protocol, Error::TooLong, Error::Truncated] {
        assert_eq!(error.fault(), Fault::Invalid, "{error:?}: the peer's data");
    }
    assert_eq!(Error::Alert(40).fault(), Fault::Reset, "the peer gave up");
    for fault in [Fault::Reset, Fault::Invalid, Fault::Other] {
        assert_eq!(Error::Stream(fault).fault(), fault);
    }
    assert_eq!(Error::Other.fault(), Fault::Other);
}
