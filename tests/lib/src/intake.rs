//! The intake's plain reference (lib.md, 7): what a demand takes from a
//! stream all of which is buffered, and a check of an intake fed in pieces
//! against it.

use skein_lib::Intake;
use skein_lib::Overflow;
use skein_lib::stream::{Delimiter, Read};

/// A scan for `until`, of at most `max` bytes.
#[must_use]
pub fn scan(until: Delimiter, max: u32) -> Read {
    Read::Scan { until, max }
}

/// What `read` takes from the front of `stream` when all of it is
/// buffered, found the plain way: how many bytes, or `None`.
#[must_use]
pub fn reference(stream: &[u8], read: Read) -> Option<usize> {
    match read {
        Read::Nothing => None,
        Read::Fill(n) => {
            let n = usize::try_from(n).expect("a u32 fits a usize");
            (stream.len() >= n).then_some(n)
        }
        Read::Scan { until, max } => {
            let max = usize::try_from(max).expect("a u32 fits a usize");
            let needle = until.as_bytes();
            let window = &stream[..stream.len().min(max)];
            for at in 0..window.len() {
                if window[at..].starts_with(needle) {
                    return Some(at.checked_add(needle.len()).expect("within the stream"));
                }
            }
            (stream.len() >= max).then_some(max)
        }
    }
}

/// Feeds `input` to an intake of `capacity` in pieces of the lengths in
/// `pieces` (cycled), cut further where a piece does not fit, meeting
/// `demands` in turn as soon as each can be met; checks every delivery
/// against the reference, and that the demands met are those the whole
/// input meets, returning how many that is.
#[expect(clippy::must_use_candidate, reason = "it asserts as it goes; the count is for a caller that knows it")]
pub fn check(input: &[u8], demands: &[Read], capacity: u32, pieces: &[usize]) -> usize {
    let mut expected = Vec::new();
    let mut rest = input;
    for &read in demands {
        if read == Read::Nothing {
            expected.push(None);
            continue;
        }
        let Some(n) = reference(rest, read) else { break };
        let (taken, after) = rest.split_at_checked(n).expect("the reference takes what the stream holds");
        expected.push(Some(taken));
        rest = after;
    }

    let mut intake = Intake::with_capacity(capacity);
    let mut unfed = input;
    let mut pieces = pieces.iter().cycle();
    for (index, (&read, &expected)) in demands.iter().zip(&expected).enumerate() {
        let delivered = loop {
            let delivered = intake.meet(read);
            if delivered.is_some() || read == Read::Nothing {
                break delivered;
            }
            assert!(!unfed.is_empty(), "demand {index} ({read:?}) is met by the whole input");
            assert!(intake.room() > 0, "demand {index} ({read:?}) fits the cap, so a full intake meets it");
            let want = (*pieces.next().expect("pieces cycle")).clamp(1, unfed.len());
            let fits = want.min(usize::try_from(intake.room()).expect("a u32 fits a usize"));
            if fits < want {
                let before = intake.len();
                assert_eq!(intake.append(&unfed[..want]), Err(Overflow));
                assert_eq!(intake.len(), before, "a refused append appends nothing");
            }
            let (piece, after) = unfed.split_at_checked(fits).expect("no more than is left");
            assert_eq!(intake.append(piece), Ok(()));
            unfed = after;
        };
        assert_eq!(delivered.as_deref(), expected, "demand {index}: {read:?}");
    }
    // Nothing more is met than the whole input meets.
    if let Some(&read) = demands.get(expected.len()) {
        intake.append(unfed).expect("the rest of the input fits the cap");
        assert_eq!(intake.meet(read), None, "demand {} is not met by the whole input", expected.len());
    }
    expected.len()
}

/// Takes `n` bytes from the front of the model, as a delivery does.
pub fn drain(model: &mut Vec<u8>, n: usize) {
    drop(model.drain(..n));
}
