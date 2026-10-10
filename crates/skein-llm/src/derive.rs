//! Endpoint declarations and checked derivation (llm.md, sections 4.1–4.3).
//! Values describe the owner's models and deployment; this module knows no
//! model catalogue, transport pieces, credentials or per-call policy.
use crate::{Provider, client};

/// Largest model quantities and deployment counts, supplied by the owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Declared {
    /// Prompt tokens accepted by the largest model.
    pub window: u32,
    /// Completion tokens, including reasoning.
    pub output: u32,
    /// Bytes of one provider-owned reasoning item.
    pub reasoning_item: u32,
    /// Bytes of one tool call's unescaped arguments.
    pub tool_payload: u32,
    /// Tool calls in one completion.
    pub calls_per_response: u32,
    /// Calls outstanding across the owner; consumed by its connection pool.
    pub conversations: u32,
}

/// Largest JSON expansion of one byte, as `\u00XX`.
pub const ESCAPE: u32 = 6;

/// Text bytes allowed to stand for one declared token.
pub const TOKEN_BYTES: u32 = 8;

/// Client limits for both dialects; an endpoint selects its own value.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Limits {
    pub codex: client::Limits,
    pub anthropic: client::Limits,
}

impl Limits {
    /// Every rule of llm.md 4.2, checked before either endpoint can run.
    pub fn derive(declared: &Declared) -> Result<Limits, Violation> {
        Ok(Limits {
            codex: arithmetic::dialect(declared, &CODEX)?,
            anthropic: arithmetic::dialect(declared, &ANTHROPIC)?,
        })
    }

    /// The plain client bounds for an endpoint's provider.
    #[must_use]
    pub const fn dialect(&self, provider: Provider) -> client::Limits {
        match provider {
            Provider::OpenAiCodex => self.codex,
            Provider::Anthropic => self.anthropic,
        }
    }
}

/// Arithmetic or a relationship that refuses the owner's declaration.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Violation {
    /// A rule cannot be represented by the client's byte or count bound.
    Overflow { rule: Rule },
    /// A call within the tool payload could exceed the completion answer.
    InputAnswer { input: u32, answer: u32 },
    /// Completion slots fall short of the declared tool-call count.
    OutputItemsCalls { output_items: u32, calls: u32 },
    /// A call's argument text cannot fit the longest retained string.
    StringsInput { strings: u32, input: u32 },
    /// A reasoning item cannot fit the longest retained string.
    StringsReasoning { strings: u32, reasoning: u32 },
}

/// A derived limit named when its checked arithmetic fails.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Rule {
    /// Encoded request bytes.
    Request,
    /// Model-written completion bytes.
    Answer,
    /// One call's received argument bytes.
    Input,
    /// One reasoning item's bytes.
    Reasoning,
    /// Completion blocks and content indexes.
    OutputItems,
    /// One retained decoded string.
    Strings,
    /// Retained event records.
    Tokens,
    /// Retained decoded event text.
    Retained,
    /// One call's answer reservation.
    Receiving,
    /// Scanned event wire bytes.
    Skip,
    /// Tools represented in an encoded request.
    Tools,
    /// History messages and blocks represented in an encoded request.
    HistoryItems,
}

mod arithmetic;

// Archived response.headers rendered as HTTP/1.1 200 OK plus each field and
// the blank line: tool-result-final reaches 1935 bytes and all three have 42
// fields. Its message id/phase replay is 86 compact bytes; tool-call's item id
// is 67. Selective projections reach 30 records at response.completed and 196
// fixed decoded bytes at tool-call's output_item.added, excluding arguments,
// answer text and encrypted_content. These measurements travel with fixtures.
const CODEX: arithmetic::Constants = arithmetic::Constants {
    response_head: 1935,
    response_fields: 42,
    error_bytes: 16 * 1024,
    detail_bytes: 1024,
    metadata: 86,
    // Captured function-tool member spelling, with a one-byte nonempty name,
    // empty description/schema and strict:false: 78 encoded bytes.
    smallest_tool: 78,
    // The smallest replayable native envelope has a one-byte type: {"type":"a"}.
    smallest_item: 12,
    fixed_tokens: 30,
    fixed_text: 196,
    // function_call_arguments.delta is the longest archived SSE event name.
    event_field: 38,
    // Preserve the merged nesting budget; archives reach 7, the golden 10.
    depth: 32,
};

// All four archived response heads reach 1435 bytes and 31 fields.
// message_start's kept fields reach 29 records and 202 decoded fixed bytes;
// other block metadata is not replayed by this dialect. Thinking/unknown
// envelopes belong to reasoning. Error/detail are policy (llm.md, 4.2 and 7).
const ANTHROPIC: arithmetic::Constants = arithmetic::Constants {
    response_head: 1435,
    response_fields: 31,
    error_bytes: 16 * 1024,
    detail_bytes: 1024,
    metadata: 0,
    // Captured Messages tool spelling, one-byte name, empty description/schema.
    smallest_tool: 47,
    smallest_item: 12,
    fixed_tokens: 29,
    fixed_text: 202,
    event_field: 19,
    // Preserve the merged nesting budget; the deepest archive reaches 7.
    depth: 32,
};
