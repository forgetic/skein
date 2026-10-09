//! Startup reads against real filesystem entries and bounded PEM bundles.

use std::fs;
use std::path::Path;
use std::process::Command;

use skein_io::kernel::Error;
use skein_scratch::Scratch;
use skein_shell::{RootBounds, Roots, RootsRefusal, Trust, Unread, read_file, read_trust_roots};

const BOUNDS: RootBounds = RootBounds { bundle_bytes: 4_096, certificates: 8, certificate_bytes: 32 };

fn bundle(scratch: &Scratch, bytes: &[u8], bounds: RootBounds) -> Result<Roots, RootsRefusal> {
    let path = scratch.path().join("bundle");
    fs::write(&path, bytes).expect("plant the supplied bundle");
    read_trust_roots(&Trust::Machine { ssl_cert_file: Some(path) }, &bounds)
}

#[test]
fn startup_reads_whole_regular_files_at_their_bound_and_closes_every_entry() {
    let scratch = Scratch::new("startup");
    let path = scratch.path().join("file");
    let text = vec![0x31; 16_384];
    fs::write(&path, &text).expect("plant regular file");
    let fifo = scratch.path().join("fifo");
    assert!(Command::new("mkfifo").arg(&fifo).status().expect("mkfifo runs").success(), "plant a FIFO");
    let descriptors = fs::read_dir("/proc/self/fd").expect("descriptor observer").count();
    for _ in 0..8 {
        assert_eq!(read_file(&path, 16_384).expect("exact bound").as_ref(), text);
        assert_eq!(read_file(&path, 16_383), Err(Unread::TooLarge { max: 16_383 }));
        assert_eq!(read_file(&fifo, 32), Err(Unread::NotAFile));
        assert_eq!(read_file(scratch.path(), 32), Err(Unread::NotAFile));
        assert_eq!(read_file(&scratch.path().join("missing"), 32), Err(Unread::Absent));
        assert_eq!(read_file(Path::new("nul\0inside"), 32), Err(Unread::Kernel(Error::InvalidArgument)));
    }
    assert_eq!(fs::read_dir("/proc/self/fd").expect("descriptor observer").count(), descriptors, "all reads close");
    fs::write(&path, b"").expect("empty file");
    assert_eq!(read_file(&path, 0).expect("empty fits zero bytes").as_ref(), b"");
}

#[test]
fn pem_takes_der_in_order_and_skips_other_blocks_without_decoding_them() {
    let scratch = Scratch::new("startup");
    let bytes = b"explanatory text\r\n-----BEGIN PRIVATE KEY-----\ninvalid!\n-----END PRIVATE KEY-----\n\
        -----BEGIN CERTIFICATE-----\n T W\tF\x0bu\x0c \n-----END CERTIFICATE-----\n\
        -----BEGIN TRUSTED CERTIFICATE-----\nbroken!\n-----END TRUSTED CERTIFICATE-----\n\
        -----BEGIN CERTIFICATE-----\nAP8=\n-----END CERTIFICATE-----\n";
    let roots = bundle(&scratch, bytes, BOUNDS).expect("well-framed certificates");
    assert_eq!(roots.certificates.as_ref(), [Box::from(b"Man".as_slice()), Box::from([0, 255].as_slice())]);
    assert_eq!((roots.other_blocks, roots.malformed), (2, 0));
    assert_eq!(roots.path, scratch.path().join("bundle"));
}

#[test]
fn malformed_certificate_blocks_recover_at_the_next_begin_and_do_not_consume_the_count_bound() {
    let scratch = Scratch::new("startup");
    let valid = b"-----BEGIN CERTIFICATE-----\nTWFu\n-----END CERTIFICATE-----\n";
    for bad in [
        b"-----BEGIN CERTIFICATE-----\nTW!u\n-----END CERTIFICATE-----\n".as_slice(),
        b"-----BEGIN CERTIFICATE-----\nTWFu\n".as_slice(),
        b"-----BEGIN CERTIFICATE-----\nTWFu\n-----END KEY-----\n".as_slice(),
        b"-----BEGIN CERTIFICATE-----\nProc-Type: 4,ENCRYPTED\nTWFu\n-----END CERTIFICATE-----\n".as_slice(),
        b"-----BEGIN CERTIFICATE-----\nTW=Fu\n-----END CERTIFICATE-----\n".as_slice(),
        b"-----BEGIN CERTIFICATE-----\nT\n-----END CERTIFICATE-----\n".as_slice(),
    ] {
        let mut bytes = Vec::from(bad);
        bytes.extend_from_slice(valid);
        let roots = bundle(&scratch, &bytes, RootBounds { certificates: 1, ..BOUNDS }).expect("recover next root");
        assert_eq!(roots.certificates.as_ref(), [Box::from(b"Man".as_slice())]);
        assert_eq!(roots.malformed, 1, "one bad certificate block: {bad:?}");
    }
    let roots = bundle(&scratch, b"-----BEGIN CERTIFICATE-----\nTWFu", BOUNDS).expect("truncated block is skipped");
    assert!(roots.certificates.is_empty(), "none recovered");
    assert_eq!(roots.malformed, 1);
}

#[test]
fn root_bounds_refuse_and_first_decode_defect_or_byte_bound_wins() {
    let scratch = Scratch::new("startup");
    let valid = b"-----BEGIN CERTIFICATE-----\nTWFu\n-----END CERTIFICATE-----\n";
    assert_eq!(
        bundle(&scratch, valid, RootBounds { certificate_bytes: 2, ..BOUNDS }).expect_err("one byte past"),
        RootsRefusal::TooLong { index: 0, bound: 2 }
    );
    assert_eq!(
        bundle(&scratch, valid, RootBounds { certificates: 0, ..BOUNDS }).expect_err("no certificate allowed"),
        RootsRefusal::TooMany { bound: 0 }
    );
    let length = u32::try_from(valid.len()).expect("fixture fits");
    assert!(bundle(&scratch, valid, RootBounds { bundle_bytes: length, ..BOUNDS }).is_ok(), "exact bundle bytes");
    assert_eq!(
        bundle(&scratch, valid, RootBounds { bundle_bytes: length - 1, ..BOUNDS }).expect_err("bundle past bound"),
        RootsRefusal::Unread { path: scratch.path().join("bundle"), why: Unread::TooLarge { max: length - 1 } }
    );
    let bad_first = b"-----BEGIN CERTIFICATE-----\n!TWFu\n-----END CERTIFICATE-----\n";
    let roots = bundle(&scratch, bad_first, RootBounds { certificate_bytes: 0, ..BOUNDS }).expect("first defect wins");
    assert_eq!(roots.malformed, 1);
    let bound_first = b"-----BEGIN CERTIFICATE-----\nTWFu!\n-----END CERTIFICATE-----\n";
    assert_eq!(
        bundle(&scratch, bound_first, RootBounds { certificate_bytes: 0, ..BOUNDS }).expect_err("first bound wins"),
        RootsRefusal::TooLong { index: 0, bound: 0 }
    );
    let mut after_bad = Vec::from(bad_first);
    after_bad.extend_from_slice(valid);
    assert_eq!(
        bundle(&scratch, &after_bad, RootBounds { certificate_bytes: 2, ..BOUNDS })
            .expect_err("second certificate long"),
        RootsRefusal::TooLong { index: 1, bound: 2 }
    );
}

#[test]
fn startup_reads_one_der_certificate_and_honors_the_explicit_bundle_path() {
    let scratch = Scratch::new("startup");
    let path = scratch.path().join("root.der");
    fs::write(&path, b"DER bytes").expect("plant DER");
    let roots = read_trust_roots(&Trust::Der(path.clone()), &BOUNDS).expect("DER file");
    assert_eq!(roots.certificates.as_ref(), [Box::from(b"DER bytes".as_slice())]);
    assert_eq!((roots.other_blocks, roots.malformed), (0, 0));
    assert_eq!(roots.path, path);
    assert_eq!(
        read_trust_roots(&Trust::Der(path), &RootBounds { certificate_bytes: 8, ..BOUNDS })
            .expect_err("DER byte bound"),
        RootsRefusal::TooLong { index: 0, bound: 8 }
    );
    let missing = scratch.path().join("missing");
    assert_eq!(
        read_trust_roots(&Trust::Machine { ssl_cert_file: Some(missing.clone()) }, &BOUNDS)
            .expect_err("explicit absent"),
        RootsRefusal::Unread { path: missing, why: Unread::Absent }
    );
}
