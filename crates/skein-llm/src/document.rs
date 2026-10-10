//! Neutral document admission bounds (llm.md, section 2). These describe a
//! value owned by the caller; a client's streamed receiving limits are separate.
use skein_json::{tokenizer, writer};

/// Byte, string, token and depth admission of one neutral JSON value.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct DocumentLimits {
    /// Complete compact JSON writing, including escapes and punctuation.
    pub bytes: u32,
    pub strings: u32,
    pub tokens: u32,
    pub depth: u32,
}

impl DocumentLimits {
    #[must_use]
    pub const fn writer_limits(&self) -> writer::Limits {
        writer::Limits { depth: self.depth, length: self.bytes }
    }

    #[must_use]
    pub const fn tokenizer_limits(&self) -> tokenizer::Limits {
        tokenizer::Limits { depth: self.depth, string: self.strings, number: 32, chunk: 256, length: self.bytes }
    }
}

/// Heap bound for neutral whole-value admission and writing.
pub(crate) fn worst_case(limits: &DocumentLimits) -> Option<u64> {
    use skein_json::{collector, document};
    use skein_lib::{Queue, stream};
    let collector = collector::worst_case(
        &collector::Limits {
            tokenizer: limits.tokenizer_limits(),
            tokens: limits.tokens,
            text: limits.bytes,
            skip: u64::from(limits.bytes),
        },
        &[],
        &collector::Filter { root: collector::Keep::Value },
    )?;
    collector
        .checked_add(document::worst_case(&document::Limits { tokens: limits.tokens, text: limits.bytes })?)?
        .checked_add(writer::worst_case(&limits.writer_limits())?)?
        .checked_add(Queue::<collector::Event>::worst_case(1)?)?
        .checked_add(Queue::<stream::Down>::worst_case(1)?)
}
