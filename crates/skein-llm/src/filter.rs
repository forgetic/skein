//! Named receive caps and the collector stack (llm.md, section 4.4).
//! The dialects own static paths; this module supplies their runtime bounds.
use crate::client;
use skein_json::{collector, tokenizer};

pub(crate) const INPUT: collector::Cap = collector::Cap::new(0);

pub(crate) const REASONING: collector::Cap = collector::Cap::new(1);

pub(crate) const STRINGS: collector::Cap = collector::Cap::new(2);

pub(crate) fn caps(limits: &client::Limits) -> [u32; 3] {
    [limits.input, limits.reasoning, limits.strings]
}

pub(crate) fn collector(limits: &client::Limits) -> collector::Limits {
    let bounds = caps(limits);
    collector::Limits {
        tokenizer: tokenizer::Limits {
            depth: limits.depth,
            string: bounds[0].max(bounds[1]).max(bounds[2]),
            number: 32,
            chunk: limits.sse.chunk,
            length: limits.skip,
        },
        tokens: limits.tokens,
        text: limits.retained,
        skip: u64::from(limits.skip),
    }
}

pub(crate) const fn field(name: &'static [u8], keep: collector::Keep) -> collector::Node {
    collector::Node { key: collector::Key::Field(name), keep }
}

pub(crate) const fn each(keep: collector::Keep) -> collector::Node {
    collector::Node { key: collector::Key::Each, keep }
}
