//! Checked rules over explicit constants; transport pieces stay with the owner.
use super::{Declared, ESCAPE, Rule, TOKEN_BYTES, Violation};
use crate::client;
use skein_http::{client as http, sse};
use skein_json::document;

pub(super) struct Constants {
    pub response_head: u32,
    pub response_fields: u32,
    pub error_bytes: u32,
    pub detail_bytes: u32,
    pub metadata: u32,
    pub smallest_tool: u32,
    pub smallest_item: u32,
    pub fixed_tokens: u32,
    pub fixed_text: u32,
    pub event_field: u32,
    pub depth: u32,
}

pub(super) fn dialect(declared: &Declared, constants: &Constants) -> Result<client::Limits, Violation> {
    let window = declared.window.checked_mul(TOKEN_BYTES).ok_or(Violation::Overflow { rule: Rule::Request })?;
    let request = window.checked_mul(ESCAPE).ok_or(Violation::Overflow { rule: Rule::Request })?;
    let answer = declared.output.checked_mul(TOKEN_BYTES).ok_or(Violation::Overflow { rule: Rule::Answer })?;
    let input = declared.tool_payload.checked_mul(ESCAPE).ok_or(Violation::Overflow { rule: Rule::Input })?;
    let reasoning = declared.reasoning_item;
    let call_items =
        declared.calls_per_response.checked_mul(2).ok_or(Violation::Overflow { rule: Rule::OutputItems })?;
    let output_items = call_items.checked_add(2).ok_or(Violation::Overflow { rule: Rule::OutputItems })?;
    let strings = answer.max(reasoning);
    let tokens = constants.fixed_tokens.checked_add(reasoning).ok_or(Violation::Overflow { rule: Rule::Tokens })?;
    let retained = answer
        .max(input)
        .max(reasoning)
        .checked_add(constants.fixed_text)
        .ok_or(Violation::Overflow { rule: Rule::Retained })?;
    let event = document::worst_case(&document::Limits { tokens, text: retained })
        .ok_or(Violation::Overflow { rule: Rule::Receiving })?;
    let event = u32::try_from(event).or(Err(Violation::Overflow { rule: Rule::Receiving }))?;
    let reasoning_reservation =
        output_items.checked_mul(reasoning).ok_or(Violation::Overflow { rule: Rule::Receiving })?;
    let held = answer.checked_add(reasoning_reservation).ok_or(Violation::Overflow { rule: Rule::Receiving })?;
    let receiving = held.checked_add(event).ok_or(Violation::Overflow { rule: Rule::Receiving })?;
    let escaped = receiving.checked_mul(ESCAPE).ok_or(Violation::Overflow { rule: Rule::Skip })?;
    let skip = request.checked_add(escaped).ok_or(Violation::Overflow { rule: Rule::Skip })?;
    let tools = request.checked_div(constants.smallest_tool).ok_or(Violation::Overflow { rule: Rule::Tools })?;
    let history_items =
        request.checked_div(constants.smallest_item).ok_or(Violation::Overflow { rule: Rule::HistoryItems })?;
    let limits = client::Limits {
        http: http::Limits {
            request: 0,
            head: constants.response_head,
            headers: constants.response_fields,
            read: 0,
            send: 0,
        },
        sse: sse::Limits { line: skip, event: skip, field: constants.event_field, chunk: 0 },
        request,
        answer,
        input,
        reasoning,
        metadata: constants.metadata,
        output_items,
        strings,
        tokens,
        retained,
        receiving,
        skip,
        tools,
        history_items,
        depth: constants.depth,
        error_bytes: constants.error_bytes,
        detail_bytes: constants.detail_bytes,
        drop_reasoning: false,
        declared_output_tokens: declared.output,
    };
    relationships(declared, &limits)?;
    Ok(limits)
}

fn relationships(declared: &Declared, limits: &client::Limits) -> Result<(), Violation> {
    if limits.input > limits.answer {
        return Err(Violation::InputAnswer { input: limits.input, answer: limits.answer });
    }
    if limits.output_items < declared.calls_per_response {
        return Err(Violation::OutputItemsCalls {
            output_items: limits.output_items,
            calls: declared.calls_per_response,
        });
    }
    if limits.strings < limits.input {
        return Err(Violation::StringsInput { strings: limits.strings, input: limits.input });
    }
    if limits.strings < limits.reasoning {
        return Err(Violation::StringsReasoning { strings: limits.strings, reasoning: limits.reasoning });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
