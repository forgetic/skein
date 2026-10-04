//! Owned vocabulary shared by LLM providers. Replay data remains opaque to callers.
use crate::Json;
use alloc::boxed::Box;
use skein_http::Header;
use skein_lib::{Duration, Token, bytes};

/// A concrete wire dialect. More providers can be added without changing messages.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Provider {
    /// OAuth subscription access through the `ChatGPT` Codex Responses route.
    OpenAiCodex,
    /// OAuth bearer access to Anthropic's Messages subscription dialect.
    Anthropic,
}
/// The conversation participant; instructions live separately in [`Prompt`].
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Role {
    User,
    Assistant,
}
/// Provider-owned metadata needed to resend a completed block faithfully.
///
/// Retain this value with its block. Its schema belongs to the producing
/// provider, and admission rejects using it with a different provider.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Replay {
    /// The dialect that produced and understands this metadata.
    pub provider: Provider,
    /// Opaque replay metadata, not application text or tool arguments.
    pub value: Json,
}
/// One owned conversation block, retained in provider output order.
///
/// Text is UTF-8. Assistant blocks may carry replay metadata; tool results
/// belong to user messages and reference the corresponding tool call's id.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Block {
    /// User input or completed assistant text.
    Text { text: Box<[u8]>, replay: Option<Replay> },
    /// A completed assistant refusal, distinct from ordinary text.
    Refusal { text: Box<[u8]>, replay: Option<Replay> },
    /// A completed assistant tool call. `id` correlates the result; `arguments`
    /// are raw JSON bytes and can be malformed in received output. Inspect
    /// them before executing a tool. Sending requires a valid JSON object.
    ToolCall { id: Box<[u8]>, name: Box<[u8]>, arguments: Box<[u8]>, replay: Option<Replay> },
    /// A result for the tool call named by `id`. Codex lacks a native error
    /// flag, so `is_error` prefixes the wire text with `Error: ` when true;
    /// successful text is sent unchanged.
    ToolResult { id: Box<[u8]>, text: Box<[u8]>, is_error: bool },
    /// Provider-owned reasoning, including encrypted or signed replay data.
    /// The payload is opaque; visible reasoning summaries arrive as deltas.
    Reasoning { replay: Replay },
}
/// One conversation turn, with blocks in the order they occurred.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Message {
    pub role: Role,
    pub content: Box<[Block]>,
}
/// A callable tool definition. Execution and result creation belong to the caller.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Tool {
    pub name: Box<[u8]>,
    pub description: Box<[u8]>,
    /// A JSON Schema object describing the tool's argument object.
    pub schema: Json,
}
/// Owned input to one call. Admission validates counts, bytes and replay data.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Prompt {
    /// Provider model identifier, nonempty UTF-8.
    pub model: Box<[u8]>,
    /// System-level instructions, separate from user/assistant history.
    pub instructions: Box<[u8]>,
    pub tools: Box<[Tool]>,
    /// Chronological conversation history, including prior replay metadata.
    pub messages: Box<[Message]>,
    /// An optional provider-supported reasoning effort value.
    pub reasoning_effort: Option<Box<[u8]>>,
    /// Optional provider prompt-cache affinity key; it does not enable storage.
    pub cache_key: Option<Box<[u8]>>,
    /// Anthropic's `max_tokens`; `None` uses 4096. Codex subscription calls
    /// require `None` because that route does not support token caps.
    pub max_output_tokens: Option<u32>,
}
/// Why a successfully decoded response finished.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Stop {
    EndTurn,
    ToolUse,
    MaxTokens,
    Refusal,
}
/// Provider-reported token accounting. Absent usage and optional cache counts are zero.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Usage {
    /// Uncached input tokens; excludes both cache fields below.
    pub input_tokens: u64,
    /// Output tokens, including provider-counted reasoning tokens.
    pub output_tokens: u64,
    /// Input tokens read from an existing prompt cache.
    pub cache_read_tokens: u64,
    /// Input tokens written to a prompt cache, when the provider reports them.
    pub cache_write_tokens: u64,
}
impl Usage {
    pub const ZERO: Usage = Usage { input_tokens: 0, output_tokens: 0, cache_read_tokens: 0, cache_write_tokens: 0 };
}
/// A terminal successful response. Its blocks can be appended to history.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Completion {
    pub content: Box<[Block]>,
    pub stop: Stop,
    pub usage: Usage,
}
/// Incremental UTF-8 fragments for display, not replacements for completed blocks.
///
/// `index` is the provider output item index. Content and summary indices name
/// fragments within an item. Tool arguments may be incomplete JSON until done.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Delta {
    Text { index: u32, content_index: u32, text: Box<[u8]> },
    ToolArguments { index: u32, delta: Box<[u8]> },
    Reasoning { index: u32, summary_index: u32, text: Box<[u8]> },
}
/// A rejected call has produced no traffic and no terminal event.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Error {
    Invalid,
    Limit,
    Unsupported,
}
/// The terminal outcome of an accepted call. Retry policy belongs to its owner.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Failure {
    Unauthorized,
    Exhausted {
        retry_after: Duration,
    },
    RateLimited {
        retry_after: Duration,
    },
    Overloaded,
    Unavailable,
    ContextTooLong,
    Invalid,
    Limit,
    Protocol,
    Cancelled,
    /// The caller's timer expired and it aborted the call.
    TimedOut,
}
/// The wire dialect and HTTP destination; transport/TLS are caller-owned.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Endpoint {
    pub provider: Provider,
    /// HTTP authority, optionally including a port, with no URL scheme.
    pub authority: Box<[u8]>,
    /// Origin-form request target, such as `/backend-api/codex/responses`.
    pub target: Box<[u8]>,
    /// Additional caller identity/route headers. Reserved framing and credential
    /// headers are rejected rather than allowing duplicate or conflicting fields.
    pub headers: Box<[Header]>,
}
impl Endpoint {
    /// Anthropic Messages with subscription OAuth bearer authentication.
    #[must_use]
    pub fn anthropic() -> Endpoint {
        Endpoint {
            provider: Provider::Anthropic,
            authority: bytes::copy_of(b"api.anthropic.com"),
            target: bytes::copy_of(b"/v1/messages"),
            headers: Box::new([]),
        }
    }
    /// The subscription Responses route. Caller headers identify the actual client.
    #[must_use]
    pub fn codex() -> Endpoint {
        Endpoint {
            provider: Provider::OpenAiCodex,
            authority: bytes::copy_of(b"chatgpt.com"),
            target: bytes::copy_of(b"/backend-api/codex/responses"),
            headers: Box::new([]),
        }
    }
}
/// Credentials are caller-owned OAuth material; no Debug implementation exposes them.
#[expect(missing_debug_implementations, reason = "OAuth credential bytes must not enter debug traces")]
pub struct Credential {
    /// OAuth bearer access token; acquisition and renewal belong to the caller.
    pub access_token: Box<[u8]>,
    /// Codex account identifier sent in `chatgpt-account-id`. Must be empty
    /// for Anthropic, which authenticates using only the bearer token.
    pub account_id: Box<[u8]>,
}
impl Credential {
    /// Uses a caller-obtained Anthropic OAuth access token.
    #[must_use]
    pub fn anthropic(access_token: Box<[u8]>) -> Credential {
        Credential { access_token, account_id: Box::new([]) }
    }
}
/// One owner-correlated request. Admission consumes it whether accepted or rejected.
#[expect(missing_debug_implementations, reason = "a call owns credentials that must not enter debug traces")]
pub struct Call {
    pub owner: Token,
    pub prompt: Prompt,
    pub endpoint: Endpoint,
    pub credential: Credential,
}
