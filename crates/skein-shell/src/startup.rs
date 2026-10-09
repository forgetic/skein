//! Startup's bounded file and trust-root reads (shell.md, 6.1 and 6.2).
//! It keeps no environment or service state: main supplies `SSL_CERT_FILE`,
//! bounds and the trust choice. The result owns DER certificates and their
//! source path; TLS decides which parsed roots it supports.
//!
//! Without an explicit bundle, distribution paths are tried in this order:
//! Debian's /etc/ssl/certs/ca-certificates.crt, Red Hat's
//! /etc/pki/tls/certs/ca-bundle.crt, SUSE's /etc/ssl/ca-bundle.pem,
//! /etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem, then /etc/ssl/cert.pem.
//! Only an absent file moves to the next path; an existing unreadable or
//! oversized bundle refuses startup. PEM follows RFC 7468's lax form and
//! recovers at the next BEGIN line after a malformed certificate block.

use std::path::{Path, PathBuf};

use skein_io::kernel;

/// Why startup's regular-file read returned no bytes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Unread {
    /// The requested path was absent.
    Absent,
    /// The opened entry was not a regular file.
    NotAFile,
    /// The file exceeded startup's byte bound.
    TooLarge { max: u32 },
    /// The adapter's kernel call failed.
    Kernel(kernel::Error),
}

/// Reads a regular file whole within `max` bytes, without blocking on an
/// entry such as a FIFO or taking a controlling terminal (shell.md, 6.1).
pub fn read_file(path: &Path, max: u32) -> Result<Box<[u8]>, Unread> {
    crate::ring::read_file(path, max)
}

/// The trust source startup's configuration asks the shell to read.
#[derive(Clone, Debug)]
pub enum Trust {
    /// The explicit `SSL_CERT_FILE` bundle, or the documented distribution paths.
    Machine { ssl_cert_file: Option<PathBuf> },
    /// One DER certificate at the supplied path.
    Der(PathBuf),
}

/// Configuration's bounds for reading trust roots at startup (shell.md, 6.2).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RootBounds {
    pub bundle_bytes: u32,
    pub certificates: u32,
    pub certificate_bytes: u32,
}

/// Startup's DER certificates in file order, their source and skipped blocks.
#[derive(Debug)]
pub struct Roots {
    pub certificates: Box<[Box<[u8]>]>,
    pub path: PathBuf,
    pub other_blocks: u32,
    pub malformed: u32,
}

/// Why startup could not read trust roots within its configured bounds.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum RootsRefusal {
    /// No explicit or distribution bundle exists.
    NoBundle,
    /// The source file was not read.
    Unread { path: PathBuf, why: Unread },
    /// More DER certificates were taken than configuration permits.
    TooMany { bound: u32 },
    /// A certificate exceeded its byte bound while decoding.
    /// `index` counts certificate blocks from zero, including malformed blocks.
    TooLong { index: u32, bound: u32 },
}

/// Reads DER roots within `bounds`, decoding certificate PEM blocks and
/// counting malformed and other blocks (shell.md, 6.2). Empty results are
/// handed to TLS, whose `Config::from_der` refuses roots of none.
pub fn read_trust_roots(trust: &Trust, bounds: &RootBounds) -> Result<Roots, RootsRefusal> {
    match trust {
        Trust::Der(path) => {
            let certificate = source(path, bounds.bundle_bytes)?;
            if u64::try_from(certificate.len()).expect("length fits") > u64::from(bounds.certificate_bytes) {
                return Err(RootsRefusal::TooLong { index: 0, bound: bounds.certificate_bytes });
            }
            if bounds.certificates == 0 {
                return Err(RootsRefusal::TooMany { bound: 0 });
            }
            Ok(Roots { certificates: Box::from([certificate]), path: path.clone(), other_blocks: 0, malformed: 0 })
        }
        Trust::Machine { ssl_cert_file: Some(path) } => {
            let bytes = source(path, bounds.bundle_bytes)?;
            pem(&bytes, path, bounds)
        }
        Trust::Machine { ssl_cert_file: None } => {
            let paths = [
                "/etc/ssl/certs/ca-certificates.crt",
                "/etc/pki/tls/certs/ca-bundle.crt",
                "/etc/ssl/ca-bundle.pem",
                "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem",
                "/etc/ssl/cert.pem",
            ]
            .map(Path::new);
            machine(&paths, bounds)
        }
    }
}

fn machine(paths: &[&Path], bounds: &RootBounds) -> Result<Roots, RootsRefusal> {
    for path in paths {
        match read_file(path, bounds.bundle_bytes) {
            Ok(bytes) => return pem(&bytes, path, bounds),
            Err(Unread::Absent) => {}
            Err(why) => return Err(RootsRefusal::Unread { path: path.to_path_buf(), why }),
        }
    }
    Err(RootsRefusal::NoBundle)
}

fn source(path: &Path, max: u32) -> Result<Box<[u8]>, RootsRefusal> {
    read_file(path, max).map_err(|why| RootsRefusal::Unread { path: path.to_path_buf(), why })
}

fn pem(bundle: &[u8], path: &Path, bounds: &RootBounds) -> Result<Roots, RootsRefusal> {
    let mut certificates = Vec::new();
    let mut current: Option<Decoder> = None;
    let mut certificate_blocks: u32 = 0;
    let mut other_blocks: u32 = 0;
    let mut malformed: u32 = 0;
    for line in bundle.split(|byte| *byte == b'\n' || *byte == b'\r') {
        let line = trim_whitespace(line);
        if let Some(label) = boundary(line, b"-----BEGIN ") {
            finish(current.take(), false, bounds, &mut certificates, &mut malformed)?;
            if label == b"CERTIFICATE" {
                current = Some(Decoder::new(certificate_blocks));
                certificate_blocks = certificate_blocks.checked_add(1).expect("blocks fit bounded bundle bytes");
            } else {
                other_blocks = other_blocks.checked_add(1).expect("blocks fit bounded bundle bytes");
            }
        } else if let Some(label) = boundary(line, b"-----END ") {
            finish(current.take(), label == b"CERTIFICATE", bounds, &mut certificates, &mut malformed)?;
        } else if let Some(decoder) = &mut current {
            for byte in line {
                decoder.push(*byte, bounds.certificate_bytes)?;
            }
        }
    }
    finish(current, false, bounds, &mut certificates, &mut malformed)?;
    Ok(Roots { certificates: certificates.into_boxed_slice(), path: path.to_path_buf(), other_blocks, malformed })
}

fn boundary<'a>(line: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    line.strip_prefix(prefix)?.strip_suffix(b"-----")
}

fn whitespace(byte: u8) -> bool {
    byte.is_ascii_whitespace() || byte == 0x0b
}

fn trim_whitespace(mut bytes: &[u8]) -> &[u8] {
    while let Some(byte) = bytes.first() {
        if !whitespace(*byte) {
            break;
        }
        bytes = bytes.get(1..).expect("a leading byte exists");
    }
    while let Some(byte) = bytes.last() {
        if !whitespace(*byte) {
            break;
        }
        let end = bytes.len().checked_sub(1).expect("a trailing byte exists");
        bytes = bytes.get(..end).expect("within bytes");
    }
    bytes
}

fn finish(
    decoder: Option<Decoder>,
    ended: bool,
    bounds: &RootBounds,
    certificates: &mut Vec<Box<[u8]>>,
    malformed: &mut u32,
) -> Result<(), RootsRefusal> {
    if let Some(decoder) = decoder {
        if ended && decoder.valid() {
            if u64::try_from(certificates.len()).expect("count fits") >= u64::from(bounds.certificates) {
                return Err(RootsRefusal::TooMany { bound: bounds.certificates });
            }
            certificates.push(decoder.bytes.into_boxed_slice());
        } else {
            *malformed = malformed.checked_add(1).expect("blocks fit bounded bundle bytes");
        }
    }
    Ok(())
}

struct Decoder {
    index: u32,
    bytes: Vec<u8>,
    symbols: u32,
    carry: u32,
    bits: u32,
    padding: u32,
    invalid: bool,
}

impl Decoder {
    fn new(index: u32) -> Decoder {
        Decoder { index, bytes: Vec::new(), symbols: 0, carry: 0, bits: 0, padding: 0, invalid: false }
    }

    fn push(&mut self, byte: u8, bound: u32) -> Result<(), RootsRefusal> {
        if self.invalid || whitespace(byte) {
            return Ok(());
        }
        if byte == b'=' {
            self.padding = self.padding.checked_add(1).expect("padding stops at its first defect");
            let remainder = self.symbols % 4;
            if remainder < 2 || self.padding > 4_u32.checked_sub(remainder).expect("remainder below four") {
                self.invalid = true;
            }
            return Ok(());
        }
        let value = match byte {
            b'A'..=b'Z' => u32::from(byte.checked_sub(b'A').expect("uppercase range")),
            b'a'..=b'z' => u32::from(byte.checked_sub(b'a').expect("lowercase range")).checked_add(26).expect("sextet"),
            b'0'..=b'9' => u32::from(byte.checked_sub(b'0').expect("digit range")).checked_add(52).expect("sextet"),
            b'+' => 62,
            b'/' => 63,
            _ => {
                self.invalid = true;
                return Ok(());
            }
        };
        if self.padding != 0 {
            self.invalid = true;
            return Ok(());
        }
        self.symbols = self.symbols.checked_add(1).expect("symbols fit bounded bundle bytes");
        self.carry = (self.carry << 6) | value;
        self.bits = self.bits.checked_add(6).expect("at most twelve bits");
        if self.bits >= 8 {
            self.bits = self.bits.checked_sub(8).expect("one output byte");
            if u64::try_from(self.bytes.len()).expect("length fits") >= u64::from(bound) {
                return Err(RootsRefusal::TooLong { index: self.index, bound });
            }
            self.bytes.push(u8::try_from(self.carry >> self.bits).expect("one decoded byte"));
            self.carry &= (1_u32 << self.bits).checked_sub(1).expect("positive bit mask");
        }
        Ok(())
    }

    fn valid(&self) -> bool {
        let remainder = self.symbols % 4;
        !self.invalid
            && remainder != 1
            && self.carry == 0
            && (self.padding == 0 || self.padding == 4_u32.checked_sub(remainder).expect("remainder below four"))
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use skein_scratch::Scratch;

    use super::{RootBounds, RootsRefusal, Unread, machine};

    #[test]
    fn distribution_bundles_choose_the_first_existing_path_and_refuse_its_errors() {
        let scratch = Scratch::new("startup");
        let absent = scratch.path().join("absent");
        let first = scratch.path().join("first");
        let second = scratch.path().join("second");
        let bounds = RootBounds { bundle_bytes: 1_024, certificates: 1, certificate_bytes: 3 };
        fs::write(&first, b"-----BEGIN CERTIFICATE-----\nTWFu\n-----END CERTIFICATE-----\n").expect("first bundle");
        fs::write(&second, b"-----BEGIN CERTIFICATE-----\nTWE=\n-----END CERTIFICATE-----\n").expect("second bundle");
        let roots = machine(&[&absent, &first, &second], &bounds).expect("first existing distribution bundle");
        assert_eq!(roots.path, first, "documented path order");
        assert_eq!(machine(&[&absent], &bounds).expect_err("none exist"), RootsRefusal::NoBundle, "no fallback source");
        assert_eq!(
            machine(&[scratch.path(), &second], &bounds).expect_err("first source is a directory"),
            RootsRefusal::Unread { path: scratch.path().to_path_buf(), why: Unread::NotAFile },
            "an existing refused source never falls through"
        );
    }
}
