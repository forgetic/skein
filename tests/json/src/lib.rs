//! The JSON tokenizer's machine worlds (testing-strategy.md, 2.4; json.md,
//! 6), and the writer against it.
//!
//! What the test binaries share:
//!
//! - [`world`]: one tokenizer from a seed, between a stream below that
//!   delivers exactly what was demanded, from the peer's bytes cut at
//!   random, and ends early or fails, and a user above that demands
//!   slowly, stops, and closes in every state. It checks the machine's
//!   contracts as it goes.
//! - [`reference`]: a simple parser of a whole document, held in memory,
//!   that the tokenizer is checked against.
//! - [`generate`]: documents from a seed, valid and mutated, and the
//!   token sequences the writer writes.
//! - [`transcript`]: the transcripts kept in `transcripts/`, each with
//!   what it must decode to.
//! - [`write`]: tokens through the writer's two passes.
//!
//! The focused tests are `tests/*.rs` (testing-strategy.md, 8).

pub mod generate;
pub mod reference;
pub mod transcript;
pub mod world;

use skein_json::Token;
use skein_json::tokenizer::Error;
use skein_json::writer::{self, Encoder, Refusal};

/// `tokens`, a document, through the writer's two passes (json.md, 4):
/// measured, then written into exactly that length; or why the measuring
/// pass refused it.
///
/// # Errors
///
/// The refusal of the measuring pass.
pub fn write(tokens: &[Token], limits: &writer::Limits) -> Result<Box<[u8]>, Refusal> {
    let mut measure = Encoder::measure(limits);
    for token in tokens {
        measure.token(token);
    }
    let len = measure.measured()?;
    let mut write = Encoder::write(len, limits);
    for token in tokens {
        write.token(token);
    }
    let document = write.finish();
    assert_eq!(document.len(), usize::try_from(len).expect("fits a usize"), "written at the length measured");
    Ok(document)
}

/// How a document ended, as the side above saw it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Outcome {
    Done,
    Failed(Error),
}

/// A token sequence and how it ended: what a document decodes to.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Decoded {
    pub tokens: Vec<Token>,
    pub outcome: Outcome,
}
