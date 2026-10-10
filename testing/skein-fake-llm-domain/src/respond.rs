//! The fake's script: what it answers, from the query and its random state.
//!
//! - With the configured chances, the call fails as overloaded, rate-limited,
//!   unavailable, too long for the context window, or unauthorised.
//! - The query must end with a user message, and every user message's tool
//!   outputs must answer exactly the tool calls of the assistant message
//!   before it. Real providers reject anything else, and so does the fake: the
//!   call fails as invalid.
//! - With the configured chances, the answer is refused, or says it calls
//!   tools and calls none.
//! - A conversation a script cues, by its system text holding the script's
//!   cue (the script whose cue comes first in it, when several do), is
//!   answered with the script's turn for the assistant messages it has had,
//!   the fake naming its calls; and once the script has no more, with the
//!   text "done".
//! - While the conversation has had fewer rounds of tool calls than
//!   configured since the client last wrote, and the query offers tools, the
//!   answer calls some of them, picked at random, with arguments from a small
//!   menu; with the configured chance, a call is malformed instead.
//! - Otherwise the answer is the text "done".
//! - An answer longer than the query's `max_tokens` is cut short there, in
//!   its first tool call if it makes any.
//! - The prompt takes a token for every four bytes of text. All of it but the
//!   last message is read from the cache, written by the call before; the
//!   last message is read afresh, and cached for the next call.
//!
//! Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, sections 4.4 and 6.3.

use alloc::boxed::Box;

use skein_lib::bytes::copy_of;
use skein_lib::{List, Rng};

use crate::api::{Answer, Error, Finish, Line, Message, Part, Query, Role, Script, Turn, Usage};
use crate::domain::Config;
use crate::limits;

pub(crate) fn respond(
    rng: &mut Rng,
    minted: &mut u64,
    config: &Config,
    scripts: &[Script],
    menu: &crate::api::Menu,
    query: &Query,
) -> Result<Answer, Error> {
    let roll = rng.below(1000);
    let failures = [
        (config.overloaded, Error::Overloaded),
        (config.rate_limited, Error::RateLimited { retry_after: config.retry_after }),
        (config.unavailable, Error::Unavailable),
        (config.too_long, Error::ContextTooLong),
        (config.unauthorized, Error::Unauthorized),
    ];
    let mut below: u64 = 0;
    for (chance, error) in failures {
        below = below.saturating_add(u64::from(chance));
        if roll < below {
            return Err(error);
        }
    }
    if !valid(&query.messages) || !valid_choice(query) {
        return Err(Error::InvalidRequest);
    }
    let roll = rng.below(1000);
    let refused = u64::from(config.refused);
    if roll < refused {
        return Ok(answer(
            query,
            Box::new([Part::Text { text: copy_of(b"no") }]),
            Finish::ContentFilter,
            1,
            cut_text(),
        ));
    }
    if roll < refused.saturating_add(u64::from(config.no_calls)) {
        let parts = Box::new([Part::Text { text: copy_of(b"let me call a tool") }]);
        return Ok(answer(query, parts, Finish::ToolCalls, 4, cut_text()));
    }
    if let Some(script) = cued(scripts, &query.system) {
        let turns = &scripts.get(script).expect("found among the scripts").turns;
        if let Some(turn) = turns.get(said(&query.messages)) {
            if !limits::fits(limits::scripted_answer(turn), config.answer_bytes) {
                return Err(Error::ContextTooLong);
            }
            return scripted(minted, query, turn);
        }
        return Ok(answer(query, Box::new([Part::Text { text: copy_of(b"done") }]), Finish::Stop, 1, cut_text()));
    }
    let outside = rng.chance(config.outside_choice) && tool_count(query, true) != 0;
    if tool_rounds(&query.messages) < config.tool_rounds
        && tool_count(query, outside) != 0
        && !menu.arguments.is_empty()
    {
        let count = u32::try_from(rng.between(1, config.calls_per_answer.max(1).into())).expect("drawn below a u32");
        if !limits::fits(limits::random_answer(query, menu, count), config.answer_bytes) {
            return Err(Error::ContextTooLong);
        }
        let mut calls = List::with_capacity(count);
        for _ in 0..count {
            calls.push(call(rng, minted, config, menu, query, outside)?).expect("room for every call");
        }
        // Cut short, the answer ends partway into its first call.
        let first = match calls.get(0) {
            Some(Part::ToolCall { id, name, arguments }) => {
                Part::ToolCall { id: id.clone(), name: name.clone(), arguments: cut_arguments(arguments) }
            }
            Some(Part::Opaque { .. } | Part::Text { .. } | Part::ToolOutput { .. }) | None => {
                unreachable!("an answer that calls tools starts with a call")
            }
        };
        let tokens = u64::from(count).saturating_mul(8);
        return Ok(answer(query, calls.into_boxed(), Finish::ToolCalls, tokens, Box::new([first])));
    }
    let tokens = rng.between(1, config.answer_tokens.max(1).into());
    Ok(answer(query, Box::new([Part::Text { text: copy_of(b"done") }]), Finish::Stop, tokens, cut_text()))
}

/// Only names offered distinct tools; the independent domain validates its inputs.
fn valid_choice(query: &Query) -> bool {
    match &query.choice {
        crate::api::ToolChoice::Auto | crate::api::ToolChoice::None => true,
        crate::api::ToolChoice::Only(names) => {
            if names.is_empty() {
                return false;
            }
            for (index, name) in names.iter().enumerate() {
                let mut offered = false;
                for tool in &query.tools {
                    offered |= tool.name == *name;
                }
                if !offered {
                    return false;
                }
                for previous in names.get(..index).expect("an index through the names") {
                    if previous == name {
                        return false;
                    }
                }
            }
            true
        }
    }
}

fn allowed(choice: &crate::api::ToolChoice, name: &[u8]) -> bool {
    match choice {
        crate::api::ToolChoice::Auto => true,
        crate::api::ToolChoice::None => false,
        crate::api::ToolChoice::Only(names) => {
            for offered in names {
                if offered.as_ref() == name {
                    return true;
                }
            }
            false
        }
    }
}

fn tool_count(query: &Query, outside: bool) -> u64 {
    let mut count: u64 = 0;
    for tool in &query.tools {
        if allowed(&query.choice, &tool.name) != outside {
            count = count.checked_add(1).expect("a count through a bounded tool array");
        }
    }
    count
}

/// What a text answer cut short says.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
fn cut_text() -> Box<[Part]> {
    Box::new([Part::Text { text: copy_of(b"do") }])
}

/// Copies a bounded caller input for a randomly selected offered tool; no application vocabulary is known.
fn call(
    rng: &mut Rng,
    minted: &mut u64,
    config: &Config,
    menu: &crate::api::Menu,
    query: &Query,
    outside: bool,
) -> Result<Part, Error> {
    let mut pick = rng.below(tool_count(query, outside));
    let mut selected = None;
    for tool in &query.tools {
        if allowed(&query.choice, &tool.name) != outside {
            if pick == 0 {
                selected = Some(tool);
                break;
            }
            pick = pick.checked_sub(1).expect("a nonzero remaining tool index");
        }
    }
    let tool = selected.expect("picked below the eligible tool count");
    *minted = minted.checked_add(1).ok_or(Error::InvalidRequest)?;
    let id = call_id(*minted);
    if rng.chance(config.malformed) && !menu.invalid.is_empty() {
        let count = u64::try_from(menu.invalid.len()).expect("bounded menu");
        let pick = usize::try_from(rng.below(count)).expect("bounded menu index");
        let input = menu.invalid.get(pick).expect("picked below menu count");
        let name = match &input.name {
            Some(name) => name.clone(),
            None => tool.name.clone(),
        };
        return Ok(Part::ToolCall { id, name, arguments: input.arguments.clone() });
    }
    let count = u64::try_from(menu.arguments.len()).expect("bounded menu");
    let pick = usize::try_from(rng.below(count)).expect("bounded menu index");
    let arguments = menu.arguments.get(pick).expect("picked below menu count").clone();
    Ok(Part::ToolCall { id, name: tool.name.clone(), arguments })
}

/// A literal prefix of caller bytes when its requested output token allowance cuts a call.
fn cut_arguments(arguments: &[u8]) -> Box<[u8]> {
    copy_of(arguments.get(..arguments.len().min(3)).expect("bounded caller prefix"))
}

/// The answer of `parts`, which take `tokens` to say; or, past the query's
/// `max_tokens`, of `cut`, which takes them all.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
fn answer(query: &Query, parts: Box<[Part]>, finish: Finish, tokens: u64, cut: Box<[Part]>) -> Answer {
    let most = u64::from(query.max_tokens);
    if tokens > most {
        return Answer { parts: cut, finish: Finish::Length, usage: usage(query, most) };
    }
    Answer { parts, finish, usage: usage(query, tokens) }
}

/// Which of `scripts` cues the conversation of `system`: the one whose cue
/// comes first in it, the first listed of those that come at once.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
fn cued(scripts: &[Script], system: &[u8]) -> Option<usize> {
    // One pass, from each place in the text on, the scripts in order at each.
    // Bounded by the text: visit every byte suffix, including the empty one.
    for offset in 0..=system.len() {
        let text = system.get(offset..).expect("an offset through the text is a suffix");
        for (index, script) in scripts.iter().enumerate() {
            let leads = match script.cue.first() {
                Some(first) => text.first() == Some(first),
                None => true,
            };
            if leads && text.starts_with(&script.cue) {
                return Some(index);
            }
        }
    }
    None
}

/// The scripted answer `turn`, its calls named afresh; cut short past the
/// query's `max_tokens` as any answer is, partway into its first call if it
/// starts with one.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
fn scripted(minted: &mut u64, query: &Query, turn: &Turn) -> Result<Answer, Error> {
    let count = u32::try_from(turn.lines.len()).expect("a script's answer fits a u32");
    let mut parts = List::with_capacity(count);
    for line in &turn.lines {
        let part = match line {
            Line::Text { text } => Part::Text { text: copy_of(text) },
            Line::Opaque { bytes } => Part::Opaque { bytes: bytes.clone() },
            Line::Call { name, arguments } => {
                *minted = minted.checked_add(1).ok_or(Error::InvalidRequest)?;
                Part::ToolCall { id: call_id(*minted), name: copy_of(name), arguments: copy_of(arguments) }
            }
        };
        parts.push(part).expect("room for every line");
    }
    let cut = match parts.get(0) {
        Some(Part::ToolCall { id, name, arguments }) => {
            Box::new([Part::ToolCall { id: id.clone(), name: name.clone(), arguments: cut_arguments(arguments) }])
        }
        Some(Part::Opaque { .. } | Part::Text { .. } | Part::ToolOutput { .. }) | None => cut_text(),
    };
    Ok(answer(query, parts.into_boxed(), turn.finish, turn.tokens, cut))
}

/// The assistant messages a conversation has had.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
fn said(messages: &[Message]) -> usize {
    let mut said: usize = 0;
    for message in messages {
        match message.role {
            Role::Assistant => said = said.saturating_add(1),
            Role::User => {}
        }
    }
    said
}

/// Output accounting; the domain fills prompt accounting from its actual cache table.
fn usage(_query: &Query, output: u64) -> Usage {
    Usage { input: None, cache_read: None, cache_write: None, output: Some(output), reasoning: Some(0) }
}

/// Whether the conversation ends with a user message, and every user message's
/// tool outputs answer exactly the tool calls of the assistant message before
/// it: no call goes unanswered, and no output answers nothing.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
fn valid(messages: &[Message]) -> bool {
    match messages.last() {
        Some(last) => match last.role {
            Role::User => {}
            Role::Assistant => return false,
        },
        None => return false,
    }
    let mut previous: Option<&Message> = None;
    for message in messages {
        for part in &message.parts {
            match message.role {
                Role::User => match part {
                    Part::ToolCall { .. } => return false,
                    Part::ToolOutput { .. } | Part::Text { .. } | Part::Opaque { .. } => {}
                },
                Role::Assistant => match part {
                    Part::ToolOutput { .. } => return false,
                    Part::ToolCall { .. } | Part::Text { .. } | Part::Opaque { .. } => {}
                },
            }
        }
        let calls: &[Part] = match previous {
            Some(previous) => match previous.role {
                Role::Assistant => &previous.parts,
                Role::User => &[],
            },
            None => &[],
        };
        let paired = match message.role {
            Role::User => answers(&message.parts, calls),
            Role::Assistant => !calls_any(calls),
        };
        if !paired {
            return false;
        }
        previous = Some(message);
    }
    true
}

/// Whether the tool outputs among `parts` answer exactly the tool calls among
/// `calls`.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
fn answers(parts: &[Part], calls: &[Part]) -> bool {
    for part in parts {
        match part {
            Part::ToolOutput { id, .. } => {
                if calls_with(calls, id) != 1 || outputs_for(parts, id) != 1 {
                    return false;
                }
            }
            Part::Opaque { .. } | Part::Text { .. } | Part::ToolCall { .. } => {}
        }
    }
    for part in calls {
        match part {
            Part::ToolCall { id, .. } => {
                if outputs_for(parts, id) != 1 || calls_with(calls, id) != 1 {
                    return false;
                }
            }
            Part::Opaque { .. } | Part::Text { .. } | Part::ToolOutput { .. } => {}
        }
    }
    true
}

fn calls_with(parts: &[Part], wanted: &[u8]) -> u32 {
    let mut count: u32 = 0;
    for part in parts {
        match part {
            Part::ToolCall { id, .. } => {
                if **id == *wanted {
                    count = count.saturating_add(1);
                }
            }
            Part::Opaque { .. } | Part::Text { .. } | Part::ToolOutput { .. } => {}
        }
    }
    count
}

fn outputs_for(parts: &[Part], wanted: &[u8]) -> u32 {
    let mut count: u32 = 0;
    for part in parts {
        match part {
            Part::ToolOutput { id, .. } => {
                if **id == *wanted {
                    count = count.saturating_add(1);
                }
            }
            Part::Opaque { .. } | Part::Text { .. } | Part::ToolCall { .. } => {}
        }
    }
    count
}

/// Assistant messages that call tools, since the last user message that the
/// client wrote (one with text, not only tool outputs).
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
fn tool_rounds(messages: &[Message]) -> u32 {
    let mut rounds: u32 = 0;
    for message in messages {
        match message.role {
            Role::Assistant => {
                if calls_any(&message.parts) {
                    rounds = rounds.saturating_add(1);
                }
            }
            Role::User => {
                if says_any(&message.parts) {
                    rounds = 0;
                }
            }
        }
    }
    rounds
}

fn says_any(parts: &[Part]) -> bool {
    for part in parts {
        match part {
            Part::Text { .. } => return true,
            Part::Opaque { .. } | Part::ToolCall { .. } | Part::ToolOutput { .. } => {}
        }
    }
    false
}

fn calls_any(parts: &[Part]) -> bool {
    for part in parts {
        match part {
            Part::ToolCall { .. } => return true,
            Part::Opaque { .. } | Part::Text { .. } | Part::ToolOutput { .. } => {}
        }
    }
    false
}

/// "call_" and sixteen hex digits of `n`.
///
/// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
fn call_id(n: u64) -> Box<[u8]> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut id = *b"call_0000000000000000";
    let mut rest = n;
    for digit in id.iter_mut().rev().take(16) {
        let nibble = usize::try_from(rest & 0xF).expect("a nibble fits in a usize");
        *digit = *HEX.get(nibble).expect("a nibble indexes sixteen digits");
        rest = rest.wrapping_shr(4);
    }
    copy_of(&id)
}

#[cfg(test)]
mod tests {
    use alloc::boxed::Box;

    use skein_lib::bytes::copy_of;
    use skein_lib::{Duration, Rng};

    use super::{call_id, respond, valid};
    use crate::api::{Error, Finish, Line, Message, Part, Query, Role, Script, ToolSpec, Turn};
    use crate::domain::Config;

    const CONFIG: Config = Config {
        calls: 4,
        query_bytes: 1 << 20,
        script_bytes: 1 << 20,
        answer_bytes: 1 << 20,
        latency_min: Duration::ZERO,
        latency_max: Duration::ZERO,
        overloaded: 0,
        rate_limited: 0,
        retry_after: Duration::ZERO,
        unavailable: 0,
        too_long: 0,
        unauthorized: 0,
        refused: 0,
        no_calls: 0,
        answer_tokens: 1,
        calls_per_answer: 1,
        malformed: 0,
        outside_choice: 0,
        tool_rounds: 1,
        cache_lifetime: Duration::from_secs(300),
        cache_entries: 8,
        unscoped_reads: 0,
    };

    fn menu() -> crate::api::Menu {
        crate::api::Menu {
            arguments: Box::new([copy_of(br#"{"a":1}"#)]),
            invalid: Box::new([
                crate::api::InvalidInput { name: Some(copy_of(b"unknown")), arguments: copy_of(b"{}") },
                crate::api::InvalidInput { name: None, arguments: copy_of(b"{") },
            ]),
        }
    }

    fn user(parts: Box<[Part]>) -> Message {
        Message { role: Role::User, parts }
    }

    fn assistant(parts: Box<[Part]>) -> Message {
        Message { role: Role::Assistant, parts }
    }

    fn text() -> Part {
        Part::Text { text: copy_of(b"hi") }
    }

    fn call(id: &[u8]) -> Part {
        Part::ToolCall { id: copy_of(id), name: copy_of(b"ls"), arguments: copy_of(b"{}") }
    }

    fn output(id: &[u8]) -> Part {
        Part::ToolOutput { id: copy_of(id), output: copy_of(b"ok"), is_error: false }
    }

    fn query(messages: Box<[Message]>) -> Query {
        let tool = ToolSpec { name: copy_of(b"ls"), description: copy_of(b""), parameters: copy_of(b"{}") };
        Query {
            caching: crate::api::Caching::Unscoped,
            model: copy_of(b"fake"),
            system: copy_of(b""),
            tools: Box::new([tool]),
            messages,
            max_tokens: 100,
            choice: crate::api::ToolChoice::Auto,
        }
    }

    #[test]
    fn tool_outputs_must_answer_exactly_the_calls_before_them() {
        assert!(valid(&[user(Box::new([text()]))]));
        assert!(!valid(&[]));
        assert!(!valid(&[assistant(Box::new([text()]))]));
        let asked = assistant(Box::new([call(b"a"), call(b"b")]));
        assert!(valid(&[asked.clone(), user(Box::new([output(b"b"), output(b"a")]))]));
        assert!(!valid(&[user(Box::new([call(b"a")]))]), "a call cannot arrive on the user side");
        assert!(valid(&[assistant(Box::new([text()])), user(Box::new([text()]))]));
        assert!(
            !valid(&[assistant(Box::new([output(b"a")])), user(Box::new([text()]))]),
            "a result cannot arrive on the assistant side"
        );
        assert!(!valid(&[asked.clone(), user(Box::new([output(b"a")]))]));
        assert!(!valid(&[asked.clone(), user(Box::new([output(b"a"), output(b"b"), output(b"c")]))]));
        assert!(!valid(&[assistant(Box::new([call(b"a")])), user(Box::new([output(b"a"), output(b"a")]))]));
        assert!(!valid(&[assistant(Box::new([call(b"a"), call(b"a")])), user(Box::new([output(b"a")]))]));
        // Further back too: a call left unanswered, an output answering none.
        let answered = user(Box::new([output(b"a"), output(b"b")]));
        let later = [assistant(Box::new([text()])), user(Box::new([text()]))];
        assert!(valid(&[asked.clone(), answered.clone(), later[0].clone(), later[1].clone()]));
        assert!(!valid(&[asked.clone(), user(Box::new([text()])), later[0].clone(), later[1].clone()]));
        assert!(!valid(&[user(Box::new([output(b"a")])), later[0].clone(), later[1].clone()]));
        assert!(!valid(&[asked, later[0].clone(), later[1].clone()]));
    }

    /// The id and the name of the tool call `part`.
    ///
    /// Contract: docs/design/fake-llm.md, sections 2–5; programming-model.md, section 4.4.
    fn called(part: Option<&Part>) -> (&[u8], &[u8], &[u8]) {
        let Some(Part::ToolCall { id, name, arguments }) = part else {
            panic!("expected a tool call, not {part:?}");
        };
        (id, name, arguments)
    }

    #[test]
    fn the_script_calls_tools_for_the_configured_rounds_then_answers() {
        let mut rng = Rng::new(1);
        let mut minted = 0;
        let first = respond(&mut rng, &mut minted, &CONFIG, &[], &menu(), &query(Box::new([user(Box::new([text()]))])));
        let answer = first.expect("a valid query");
        assert_eq!(answer.finish, Finish::ToolCalls);
        let (id, name, arguments) = called(answer.parts.first());
        assert_eq!((id, name, arguments.first()), (&b"call_0000000000000001"[..], &b"ls"[..], Some(&b'{')));

        let called = assistant(answer.parts);
        let messages =
            Box::new([user(Box::new([text()])), called.clone(), user(Box::new([output(b"call_0000000000000001")]))]);
        let answer = respond(&mut rng, &mut minted, &CONFIG, &[], &menu(), &query(messages)).expect("a valid query");
        assert_eq!(answer.finish, Finish::Stop);

        // The client writes again: another round of tools, then the answer.
        let messages = Box::new([
            user(Box::new([text()])),
            called,
            user(Box::new([output(b"call_0000000000000001")])),
            assistant(answer.parts),
            user(Box::new([text()])),
        ]);
        let answer = respond(&mut rng, &mut minted, &CONFIG, &[], &menu(), &query(messages)).expect("a valid query");
        assert_eq!(answer.finish, Finish::ToolCalls);
    }

    #[test]
    fn the_script_fails_with_the_configured_chances() {
        let mut rng = Rng::new(1);
        let mut minted = 0;
        let config = Config { rate_limited: 1000, retry_after: Duration::from_secs(2), ..CONFIG };
        let result =
            respond(&mut rng, &mut minted, &config, &[], &menu(), &query(Box::new([user(Box::new([text()]))])));
        assert_eq!(result, Err(Error::RateLimited { retry_after: Duration::from_secs(2) }));
        let cases = [
            (Config { unavailable: 1000, ..CONFIG }, Error::Unavailable),
            (Config { too_long: 1000, ..CONFIG }, Error::ContextTooLong),
        ];
        for (config, error) in cases {
            let result =
                respond(&mut rng, &mut minted, &config, &[], &menu(), &query(Box::new([user(Box::new([text()]))])));
            assert_eq!(result, Err(error));
        }
        let config = Config { unauthorized: 1000, ..CONFIG };
        let result =
            respond(&mut rng, &mut minted, &config, &[], &menu(), &query(Box::new([user(Box::new([text()]))])));
        assert_eq!(result, Err(Error::Unauthorized));
    }

    #[test]
    fn the_script_refuses_or_names_no_tool_with_the_configured_chances() {
        let mut rng = Rng::new(1);
        let mut minted = 0;
        let first = || query(Box::new([user(Box::new([text()]))]));
        let config = Config { refused: 1000, ..CONFIG };
        let answer = respond(&mut rng, &mut minted, &config, &[], &menu(), &first()).expect("a valid query");
        assert_eq!(answer.finish, Finish::ContentFilter);
        let config = Config { no_calls: 1000, ..CONFIG };
        let answer = respond(&mut rng, &mut minted, &config, &[], &menu(), &first()).expect("a valid query");
        assert_eq!(answer.finish, Finish::ToolCalls);
        assert!(!super::calls_any(&answer.parts), "it names no tool");
    }

    #[test]
    fn an_answer_longer_than_its_max_tokens_is_cut_short() {
        let mut rng = Rng::new(1);
        let mut minted = 0;
        let mut first = query(Box::new([user(Box::new([text()]))]));
        first.max_tokens = 3;
        let answer = respond(&mut rng, &mut minted, &CONFIG, &[], &menu(), &first).expect("a valid query");
        assert_eq!((answer.finish, answer.usage.output), (Finish::Length, Some(3)));
        // It was to call a tool, and stops partway into the call.
        assert_eq!(answer.parts.len(), 1);
        assert_eq!(called(answer.parts.first()), (&b"call_0000000000000001"[..], &b"ls"[..], &br#"{"a"#[..]));
        let config = Config { tool_rounds: 0, answer_tokens: 2, ..CONFIG };
        let answer = respond(&mut rng, &mut minted, &config, &[], &menu(), &first).expect("a valid query");
        assert_eq!(answer.finish, Finish::Stop);
        assert!(answer.usage.output.expect("fake reports output") <= 2, "within the configured answer");
    }

    #[test]
    fn an_answer_makes_up_to_its_configured_calls_and_some_may_be_malformed() {
        let mut rng = Rng::new(1);
        let mut minted = 0;
        let first = || query(Box::new([user(Box::new([text()]))]));
        let config = Config { calls_per_answer: 3, ..CONFIG };
        let mut most: usize = 0;
        for _ in 0..20_u32 {
            let answer = respond(&mut rng, &mut minted, &config, &[], &menu(), &first()).expect("a valid query");
            assert!((1..=3).contains(&answer.parts.len()), "between one and three calls");
            most = most.max(answer.parts.len());
        }
        assert_eq!(most, 3);
        let config = Config { malformed: 1000, ..CONFIG };
        for _ in 0..20_u32 {
            let answer = respond(&mut rng, &mut minted, &config, &[], &menu(), &first()).expect("a valid query");
            let (_, name, arguments) = called(answer.parts.first());
            let unknown = name == b"unknown";
            let broken = arguments == b"{";
            assert!(unknown != broken, "malformed one way: {name:?} {arguments:?}");
        }
    }

    fn script(cue: &[u8], turns: Box<[Turn]>) -> Script {
        Script { cue: copy_of(cue), turns }
    }

    fn says(text: &[u8]) -> Turn {
        Turn { lines: Box::new([Line::Text { text: copy_of(text) }]), finish: Finish::Stop, tokens: 2 }
    }

    #[test]
    fn a_conversation_its_system_text_cues_plays_its_script_turn_by_turn() {
        let mut rng = Rng::new(1);
        let mut minted = 0;
        let calls = Turn {
            lines: Box::new([
                Line::Text { text: copy_of(b"looking") },
                Line::Call { name: copy_of(b"ls"), arguments: copy_of(b"{}") },
                Line::Call { name: copy_of(b"nope"), arguments: copy_of(b"[") },
            ]),
            finish: Finish::ToolCalls,
            tokens: 5,
        };
        let scripts = [script(b"@main", Box::new([calls, says(b"fixed")])), script(b"@child", Box::new([says(b"hi")]))];
        let mut first = query(Box::new([user(Box::new([text()]))]));
        first.system = copy_of(b"Do it. @main");
        let answer = respond(&mut rng, &mut minted, &CONFIG, &scripts, &menu(), &first).expect("a valid query");
        assert_eq!((answer.finish, answer.usage.output), (Finish::ToolCalls, Some(5)));
        let [Part::Text { .. }, Part::ToolCall { id, name, .. }, Part::ToolCall { arguments, .. }] = &*answer.parts
        else {
            panic!("expected the scripted calls, got {:?}", answer.parts);
        };
        assert_eq!((&**id, &**name, &**arguments), (&b"call_0000000000000001"[..], &b"ls"[..], &b"["[..]));

        // The next turn answers the conversation with one assistant message.
        let outputs = || user(Box::new([output(b"call_0000000000000001"), output(b"call_0000000000000002")]));
        let called = assistant(answer.parts);
        let mut second = query(Box::new([user(Box::new([text()])), called.clone(), outputs()]));
        second.system = copy_of(b"Do it. @main");
        let answer = respond(&mut rng, &mut minted, &CONFIG, &scripts, &menu(), &second).expect("a valid query");
        assert_eq!(&*answer.parts, &[Part::Text { text: copy_of(b"fixed") }]);

        // Past its end, it is done; and the cue that comes first wins.
        let history = [user(Box::new([text()])), called, outputs(), assistant(answer.parts), user(Box::new([text()]))];
        let mut third = query(Box::new(history));
        third.system = copy_of(b"Do it. @main");
        let answer = respond(&mut rng, &mut minted, &CONFIG, &scripts, &menu(), &third).expect("a valid query");
        assert_eq!((&*answer.parts, answer.finish), (&[Part::Text { text: copy_of(b"done") }][..], Finish::Stop));
        first.system = copy_of(b"@child, then @main");
        let answer = respond(&mut rng, &mut minted, &CONFIG, &scripts, &menu(), &first).expect("a valid query");
        assert_eq!(&*answer.parts, &[Part::Text { text: copy_of(b"hi") }]);
    }

    #[test]
    fn a_scripted_answer_past_its_max_tokens_is_cut_partway_into_its_first_call() {
        let mut rng = Rng::new(1);
        let mut minted = 0;
        let calls = Turn {
            lines: Box::new([Line::Call { name: copy_of(b"ls"), arguments: copy_of(b"{}") }]),
            finish: Finish::ToolCalls,
            tokens: 500,
        };
        let scripts = [script(b"@main", Box::new([calls]))];
        let mut first = query(Box::new([user(Box::new([text()]))]));
        first.system = copy_of(b"@main");
        let answer = respond(&mut rng, &mut minted, &CONFIG, &scripts, &menu(), &first).expect("a valid query");
        assert_eq!((answer.finish, answer.usage.output), (Finish::Length, Some(100)));
        assert_eq!(called(answer.parts.first()), (&b"call_0000000000000001"[..], &b"ls"[..], &b"{}"[..]));
    }

    #[test]
    fn call_ids_are_hex() {
        assert_eq!(&*call_id(0xAB), b"call_00000000000000ab");
        assert_eq!(&*call_id(u64::MAX), b"call_ffffffffffffffff");
    }
    #[test]
    fn random_tool_choices_honour_none_only_and_the_breach_chance() {
        for seed in 0..64 {
            let mut query = query(Box::new([user(Box::new([text()]))]));
            let other = ToolSpec { name: copy_of(b"other"), description: Box::new([]), parameters: copy_of(b"{}") };
            query.tools = Box::new([query.tools.first().expect("offered tool").clone(), other]);
            query.choice = crate::api::ToolChoice::None;
            let mut rng = Rng::new(seed);
            let mut minted = 0;
            let answer = respond(&mut rng, &mut minted, &CONFIG, &[], &menu(), &query).expect("valid choice");
            assert_eq!(answer.finish, Finish::Stop);
            assert!(!super::calls_any(&answer.parts), "None calls no tool");
            query.choice = crate::api::ToolChoice::Only(Box::new([copy_of(b"other")]));
            let answer = respond(&mut rng, &mut minted, &CONFIG, &[], &menu(), &query).expect("valid choice");
            assert_eq!(called(answer.parts.first()).1, b"other");
            let config = Config { outside_choice: 1000, ..CONFIG };
            let answer = respond(&mut rng, &mut minted, &config, &[], &menu(), &query).expect("configured breach");
            assert_eq!(called(answer.parts.first()).1, b"ls");
            query.choice = crate::api::ToolChoice::None;
            let answer = respond(&mut rng, &mut minted, &config, &[], &menu(), &query).expect("configured breach");
            assert_eq!(answer.finish, Finish::ToolCalls);
            assert!(super::calls_any(&answer.parts), "None breach still calls an offered tool");
        }
    }
}
