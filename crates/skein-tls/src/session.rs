//! rustls's unbuffered client connection, driven in place (tls.md, 3.2): the
//! records received go in one slice, which rustls deciphers and joins in
//! place; what it deciphers goes to the intake; what it writes goes to the
//! output TLS owes the stream below.
//!
//! rustls's connection does no I/O and keeps no clock: it reads only the
//! slices it is handed, and the wall time its configuration carries. Its
//! states are `non_exhaustive`, so each match on them ends with an arm for
//! a state rustls may add, which is unreachable for the states of 0.23.

use core::fmt;

use alloc::boxed::Box;

use rustls::client::UnbufferedClientConnection;
use rustls::unbuffered::{ConnectionState, EncodeError, EncryptError, UnbufferedStatus};
use rustls::{CertificateError, ProtocolVersion};
use skein_lib::{Intake, Wall, bytes};

use crate::client::{Agreed, Certificate, Error, Version};
use crate::config::{Config, Name};
use crate::held::Held;

/// The most states rustls passes through before it comes to rest, for what
/// one call hands it: a record's plaintext, a flight's records, each to
/// encode, then its transmission, the peer's `close_notify`, and the rest. A
/// record holds at most one key update (RFC 8446, 4.6.3), and rustls
/// answers at most one renegotiation request; anything more is fatal.
const ROUNDS: u32 = 64;

/// A connection: rustls's, which has no `Debug` of its own.
pub(crate) struct Connection(Box<UnbufferedClientConnection>);

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").field("handshaking", &self.0.is_handshaking()).finish_non_exhaustive()
    }
}

/// A connection to `name`, which checks certificates against `wall`, with
/// its `ClientHello` written: rustls draws its randoms and its key share from
/// the kernel here (tls.md, 4).
pub(crate) fn connect(config: &Config, name: Name, wall: Wall) -> Result<Connection, Error> {
    match UnbufferedClientConnection::new(config.at(wall), name.into_server_name()) {
        Ok(connection) => Ok(Connection(Box::new(connection))),
        Err(error) => Err(failure(error)),
    }
}

/// Whether the handshake is still running.
pub(crate) fn handshaking(tls: &Connection) -> bool {
    tls.0.is_handshaking()
}

/// What the handshake agreed, once it is done.
pub(crate) fn agreed(tls: &Connection) -> Agreed {
    let version = tls.0.protocol_version();
    let version = if version == Some(ProtocolVersion::TLSv1_3) {
        Version::Tls13
    } else {
        assert!(version == Some(ProtocolVersion::TLSv1_2), "rustls agrees TLS 1.2 or 1.3");
        Version::Tls12
    };
    let mut alpn = None;
    if let Some(protocol) = tls.0.alpn_protocol() {
        alpn = Some(bytes::copy_of(protocol));
    }
    Agreed { version, alpn }
}

/// Hands rustls what `records` hold, until it comes to rest: plaintext into
/// `intake`, what it writes into `owed`, and the records it is done with
/// discarded. `Ok(true)` once it read the peer's `close_notify`.
pub(crate) fn process(
    tls: &mut Connection,
    records: &mut Held,
    intake: &mut Intake,
    owed: &mut Held,
) -> Result<bool, Error> {
    let mut closed = false;
    for _ in 0..ROUNDS {
        let UnbufferedStatus { mut discard, state } = tls.0.process_tls_records(records.filled_mut());
        let rest = match state {
            Err(error) => return Err(failure(error)),
            Ok(ConnectionState::ReadTraffic(mut traffic)) => {
                for _ in 0..ROUNDS {
                    match traffic.next_record() {
                        None => break,
                        Some(Err(error)) => return Err(failure(error)),
                        Some(Ok(record)) => {
                            discard = discard.checked_add(record.discard).expect("within the records");
                            intake.append(record.payload).expect("a record is read with room for its plaintext");
                        }
                    }
                }
                false
            }
            Ok(ConnectionState::EncodeTlsData(mut encode)) => {
                match encode.encode(owed.spare_mut()) {
                    Ok(written) => owed.wrote(written),
                    // The peer can make a flight longer than FLIGHT: a
                    // HelloRetryRequest's cookie, sent in the clear, is
                    // echoed in the second ClientHello.
                    Err(EncodeError::InsufficientSize(_)) => return Err(Error::FlightTooLong),
                    Err(EncodeError::AlreadyEncoded) => unreachable!("each record is encoded once"),
                }
                false
            }
            // What was encoded is owed below now, and goes when room comes.
            Ok(ConnectionState::TransmitTlsData(transmit)) => {
                transmit.done();
                false
            }
            Ok(ConnectionState::PeerClosed) => {
                closed = true;
                false
            }
            Ok(ConnectionState::BlockedHandshake | ConnectionState::WriteTraffic(_) | ConnectionState::Closed) => true,
            Ok(ConnectionState::ReadEarlyData(_)) => unreachable!("early data is a server's to read"),
            Ok(_) => unreachable!("rustls's states as of 0.23"),
        };
        records.discard(discard);
        if rest {
            return Ok(closed);
        }
    }
    // Past what 0.23 does: the connection fails, rather than the service.
    Err(Error::Other)
}

/// `plain` encrypted, after what TLS owes the stream below, in one box: what
/// one `Send` carries. rustls may write a key update before the data.
/// `None` if there is nothing to send.
pub(crate) fn encrypt(
    tls: &mut Connection,
    records: &mut Held,
    plain: &[u8],
    owed: &mut Held,
) -> Result<Option<Box<[u8]>>, Error> {
    let UnbufferedStatus { discard, state } = tls.0.process_tls_records(records.filled_mut());
    let sealed = match state {
        Ok(ConnectionState::WriteTraffic(mut traffic)) => {
            // Its length first: rustls says how much room it needs. Asked
            // until two answers agree: a query that finds the keys at their
            // limit schedules a key update, which the next call writes before
            // the data; and that update's own record, sealed at the limit,
            // schedules a second (RFC 8446, 4.6.3), which the call after
            // writes. Fresh keys schedule no third.
            let mut length = None;
            for _ in 0..4_u32 {
                let asked = match traffic.encrypt(plain, &mut []) {
                    Ok(written) => written,
                    Err(EncryptError::InsufficientSize(size)) => size.required_size,
                    Err(EncryptError::EncryptExhausted) => return Err(Error::Other),
                };
                if length == Some(asked) {
                    break;
                }
                length = Some(asked);
            }
            let length = length.expect("asked at least once");
            let owing = usize::try_from(owed.len()).expect("a u32 fits in a usize");
            let total = owing.checked_add(length).expect("within the room granted");
            if total == 0 {
                None
            } else {
                let mut out = bytes::zeroed(total);
                let (front, back) = out.split_at_mut_checked(owing).expect("the box holds what is owed");
                for (to, from) in front.iter_mut().zip(owed.filled()) {
                    *to = *from;
                }
                // Past what 0.23 does, a length that never settles fails the
                // connection, rather than the service.
                match traffic.encrypt(plain, back) {
                    Ok(written) if written == length => {}
                    Ok(_) | Err(EncryptError::InsufficientSize(_) | EncryptError::EncryptExhausted) => {
                        return Err(Error::Other);
                    }
                }
                owed.clear();
                Some(out)
            }
        }
        Err(error) => return Err(failure(error)),
        Ok(_) => unreachable!("at rest after the handshake, rustls writes"),
    };
    records.discard(discard);
    Ok(sealed)
}

/// Writes `close_notify` into what TLS owes the stream below (RFC 8446, 6.1).
pub(crate) fn close_notify(tls: &mut Connection, records: &mut Held, owed: &mut Held) -> Result<(), Error> {
    let UnbufferedStatus { discard, state } = tls.0.process_tls_records(records.filled_mut());
    match state {
        Ok(ConnectionState::WriteTraffic(mut traffic)) => match traffic.queue_close_notify(owed.spare_mut()) {
            Ok(written) => owed.wrote(written),
            Err(EncryptError::InsufficientSize(_)) => return Err(Error::FlightTooLong),
            Err(EncryptError::EncryptExhausted) => return Err(Error::Other),
        },
        Err(error) => return Err(failure(error)),
        Ok(_) => unreachable!("at rest after the handshake, rustls writes"),
    }
    records.discard(discard);
    Ok(())
}

/// What rustls's error is to the connection.
#[expect(clippy::wildcard_enum_match_arm, reason = "rustls's errors are non_exhaustive")]
fn failure(error: rustls::Error) -> Error {
    match error {
        rustls::Error::InvalidCertificate(certificate) => Error::Certificate(refused(certificate)),
        rustls::Error::DecryptError => Error::Decrypt,
        rustls::Error::AlertReceived(alert) => Error::Alert(u8::from(alert)),
        rustls::Error::InappropriateMessage { .. }
        | rustls::Error::InappropriateHandshakeMessage { .. }
        | rustls::Error::InvalidMessage(_)
        | rustls::Error::PeerMisbehaved(_)
        | rustls::Error::PeerIncompatible(_)
        | rustls::Error::PeerSentOversizedRecord
        | rustls::Error::NoCertificatesPresented
        | rustls::Error::NoApplicationProtocol => Error::Protocol,
        _ => Error::Other,
    }
}

/// Why rustls refused the server's certificate.
#[expect(clippy::wildcard_enum_match_arm, reason = "rustls's certificate errors are non_exhaustive")]
fn refused(error: CertificateError) -> Certificate {
    match error {
        CertificateError::Expired | CertificateError::ExpiredContext { .. } => Certificate::Expired,
        CertificateError::NotValidYet | CertificateError::NotValidYetContext { .. } => Certificate::NotYetValid,
        CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. } => Certificate::Name,
        CertificateError::UnknownIssuer => Certificate::Issuer,
        _ => Certificate::Invalid,
    }
}
