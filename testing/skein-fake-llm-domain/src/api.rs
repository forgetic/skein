//! The provider's API, as its domain layer sees it once the protocol layer has
//! parsed a request.
//!
//! Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, sections 4.4 and 6.3.

use alloc::boxed::Box;

use skein_lib::Duration;

/// Sender of one provider conversation message.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Role {
    /// User or tool-result side of the conversation.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    User,
    /// Provider-produced assistant side of the conversation.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Assistant,
}

/// Caller-supplied deliberately invalid call input; the fake interprets neither name nor body.
/// Fixed wrappers and payloads share the configured script byte allowance.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct InvalidInput {
    /// Optional literal replacement tool name; `None` keeps the offered tool's name.
    pub name: Option<Box<[u8]>>,
    /// Whole caller-supplied malformed or semantically invalid body.
    pub arguments: Box<[u8]>,
}

/// Application-owned inputs for random tool stories. Empty menus generate no random tool calls.
/// Menu storage and scripts jointly fit `Config::script_bytes` before admission.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Menu {
    /// Whole argument bodies copied for randomly selected offered tools.
    pub arguments: Box<[Box<[u8]>]>,
    /// Whole deliberate invalid inputs selected when the malformed roll wins.
    pub invalid: Box<[InvalidInput]>,
}

/// One bounded block accepted from the provider stream or neutral fake API.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Part {
    /// Provider-owned replay data, preserved verbatim and never interpreted by the domain.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Opaque {
        /// Owned payload bytes charged against the enclosing provider limit.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        bytes: Box<[u8]>,
    },
    /// Owned bounded text in its original provider position.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Text {
        /// Owned UTF-8 text, bounded by the enclosing message or output cap.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        text: Box<[u8]>,
    },
    /// The model calls a tool with `arguments`, a JSON object.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    ToolCall {
        /// Provider-issued tool-call identifier, preserved verbatim in its result.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        id: Box<[u8]>,
        /// Boundary name, compared byte for byte; it carries no authority by itself.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        name: Box<[u8]>,
        /// Provider-written JSON tool arguments, retained within the enclosing byte cap.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        arguments: Box<[u8]>,
    },
    /// The client's answer to the tool call `id`.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    ToolOutput {
        /// Provider-issued tool-call identifier, preserved verbatim in its result.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        id: Box<[u8]>,
        /// Owned output bytes retained within the enclosing output cap.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        output: Box<[u8]>,
        /// Whether the supplied tool result represents a failure.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        is_error: bool,
    },
}

/// One ordered conversation message, with sender and owned bounded content.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Message {
    /// Sender of this message in the provider-neutral conversation.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub role: Role,
    /// Maximum message parts, tool definitions or retained output parts.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub parts: Box<[Part]>,
}

/// A tool the client offers. `parameters` is a JSON schema.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct ToolSpec {
    /// Boundary name, compared byte for byte; it carries no authority by itself.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub name: Box<[u8]>,
    /// Owned display text for the offered tool; the model alone interprets it.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub description: Box<[u8]>,
    /// Owned bounded JSON schema for the tool's arguments.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub parameters: Box<[u8]>,
}

/// A request for the next assistant message.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Query {
    /// Provider model name, treated as bytes by the domain.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub model: Box<[u8]>,
    /// System instructions supplied by the opener, retained within the enclosing byte cap.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub system: Box<[u8]>,
    /// Tools offered or granted by this record; their names confer no additional authority.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub tools: Box<[ToolSpec]>,
    /// Oldest-first conversation messages, with provider call/result pairing preserved.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub messages: Box<[Message]>,
    /// Maximum output tokens requested for one completion.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub max_tokens: u32,
}

/// One fake-provider terminal, carrying ordered response parts, stop classification and independent token usage.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Answer {
    /// Maximum message parts, tool definitions or retained output parts.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub parts: Box<[Part]>,
    /// Provider stop classification or permission to offer finish, as named by the enclosing record.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub finish: Finish,
    /// Provider-reported token usage, charged exactly once when its completion ends.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub usage: Usage,
}

/// Why the fake provider stopped; the agent translator supplies its own corresponding vocabulary.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Finish {
    /// The fake finished its assistant turn.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Stop,
    /// The fake asks for tools, including deliberately malformed empty batches.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    ToolCalls,
    /// The fake cut its answer at the requested output-token limit.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Length,
    /// The fake refused by its scripted content filter.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    ContentFilter,
}

/// Tokens a call took: the prompt's, read afresh or from the cache, the
/// cache's new entries, and the answer's.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Usage {
    /// Fresh provider prompt tokens reported for this completion.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub prompt_tokens: u64,
    /// Provider prompt tokens served from cache.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub cached_tokens: u64,
    /// Provider prompt tokens written to cache.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub cache_creation_tokens: u64,
    /// Provider output tokens reported for this completion.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub completion_tokens: u64,
}

/// Typed refusal or terminal failure of the fake peer, independent of the agent's policy.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Error {
    /// The provider is temporarily overloaded.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Overloaded,
    /// The provider asks the client to wait before retrying.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    RateLimited {
        /// Optional provider cooldown, retained in the representation of this boundary.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        retry_after: Duration,
    },
    /// The service failed, or could not be reached.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Unavailable,
    /// The query does not fit the model's context window.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    ContextTooLong,
    /// The client's credentials were refused.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Unauthorized,
    /// The provider reports its account allowance spent.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Exhausted {
        /// Optional provider cooldown, retained in the representation of this boundary.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        retry_after: Duration,
    },
    /// The neutral fake rejected the client's conversation structure.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    InvalidRequest,
}

/// A conversation the fake plays from a script rather than at random: one
/// whose system text holds `cue`.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Script {
    /// Script-selection bytes matched at the earliest position in system text.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub cue: Box<[u8]>,
    /// The answers, in order: the first answers a conversation with no
    /// assistant message yet, the next one with one, and so on.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub turns: Box<[Turn]>,
}

/// One scripted answer: what it says, why it stops, and the tokens it takes
/// to say.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Turn {
    /// Scripted answer parts, emitted in their declared order.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub lines: Box<[Line]>,
    /// Provider stop classification or permission to offer finish, as named by the enclosing record.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub finish: Finish,
    /// Maximum JSON tokens or scripted output tokens, as named by the enclosing record.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    pub tokens: u64,
}

/// A piece of a scripted answer. The fake names its calls.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Line {
    /// Whole provider-owned replay block supplied by the caller's script.
    /// Retained without interpretation within the aggregate script byte cap.
    Opaque {
        /// Complete native replay value; the peer protocol checks its wire syntax.
        bytes: Box<[u8]>,
    },
    /// Owned bounded text in its original provider position.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Text {
        /// Owned UTF-8 text, bounded by the enclosing message or output cap.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        text: Box<[u8]>,
    },
    /// A call to the tool `name` with `arguments`, which may be anything: a
    /// script may call a tool that was not offered, or write what is not an
    /// object.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    Call {
        /// Boundary name, compared byte for byte; it carries no authority by itself.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        name: Box<[u8]>,
        /// Provider-written JSON tool arguments, retained within the enclosing byte cap.
        ///
        /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
        arguments: Box<[u8]>,
    },
}
