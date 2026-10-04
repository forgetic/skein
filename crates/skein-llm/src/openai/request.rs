use crate::openai::{DecodeError, Json, Limits, identity, json};
use alloc::boxed::Box;
use skein_json::{Token, writer::Encoder};
use skein_lib::{List, bytes};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Role {
    User,
    Assistant,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum Input {
    Message { role: Role, text: Box<[u8]>, id: Option<Box<[u8]>>, phase: Option<Box<[u8]>>, refusal: bool },
    FunctionCall { call_id: Box<[u8]>, item_id: Option<Box<[u8]>>, name: Box<[u8]>, arguments: Box<[u8]> },
    FunctionOutput { call_id: Box<[u8]>, output: Box<[u8]> },
    Opaque { value: Json },
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Tool {
    pub name: Box<[u8]>,
    pub description: Box<[u8]>,
    pub schema: Json,
}
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Request {
    pub model: Box<[u8]>,
    pub instructions: Box<[u8]>,
    pub tools: Box<[Tool]>,
    pub input: Box<[Input]>,
    pub effort: Option<Box<[u8]>>,
    pub prompt_cache_key: Option<Box<[u8]>>,
}
pub fn encode_request(request: &Request, limits: &Limits) -> Result<Box<[u8]>, DecodeError> {
    let len = measure_request(request, limits)?;
    let bounded = skein_json::writer::Limits { depth: limits.depth, length: limits.request_bytes };
    let mut write = Encoder::write(len, &bounded);
    write_request(&mut write, request);
    Ok(write.finish())
}
/// Validates and measures without allocating the request's body.
pub fn measure_request(request: &Request, limits: &Limits) -> Result<u32, DecodeError> {
    validate(request, limits)?;
    let bounded = skein_json::writer::Limits { depth: limits.depth, length: limits.request_bytes };
    let mut measure = Encoder::measure(&bounded);
    write_request(&mut measure, request);
    crate::openai::common::measured(measure)
}
fn validate(request: &Request, limits: &Limits) -> Result<(), DecodeError> {
    let count = usize::try_from(limits.parts).expect("u32 fits usize");
    if request.input.len() > count || request.tools.len() > count {
        return Err(DecodeError::TooLarge);
    }
    for tool in &request.tools {
        if tool.schema.as_tokens().first() != Some(&Token::ObjectStart) {
            return Err(DecodeError::WrongType);
        }
    }
    for input in &request.input {
        match input {
            Input::FunctionCall { arguments, .. } => {
                let value = Json::from_bytes(arguments, limits)?;
                if value.as_tokens().first() != Some(&Token::ObjectStart) {
                    return Err(DecodeError::WrongType);
                }
            }
            Input::Opaque { value } => {
                if value.as_tokens().first() != Some(&Token::ObjectStart) {
                    return Err(DecodeError::WrongType);
                }
            }
            Input::Message { role, refusal, .. } => match role {
                Role::User if *refusal => return Err(DecodeError::WrongType),
                Role::User | Role::Assistant => {}
            },
            Input::FunctionOutput { .. } => {}
        }
    }
    Ok(())
}
pub(crate) fn write_tools(out: &mut Encoder, tools: &[Tool]) {
    out.array_start();
    for tool in tools {
        out.object_start();
        out.key(b"type");
        out.string(b"function");
        out.key(b"name");
        out.string(&tool.name);
        out.key(b"description");
        out.string(&tool.description);
        out.key(b"parameters");
        tool.schema.write(out);
        out.key(b"strict");
        out.boolean(false);
        out.object_end();
    }
    out.array_end();
}
fn write_request(out: &mut Encoder, request: &Request) {
    out.object_start();
    out.key(b"model");
    out.string(&request.model);
    out.key(b"instructions");
    out.string(&request.instructions);
    out.key(b"stream");
    out.boolean(true);
    out.key(b"store");
    out.boolean(false);
    out.key(b"include");
    out.array_start();
    out.string(b"reasoning.encrypted_content");
    out.array_end();
    out.key(b"tools");
    write_tools(out, &request.tools);
    out.key(b"tool_choice");
    out.string(b"auto");
    out.key(b"parallel_tool_calls");
    out.boolean(true);
    out.key(b"input");
    out.array_start();
    for input in &request.input {
        write_input(out, input);
    }
    out.array_end();
    if let Some(effort) = &request.effort {
        out.key(b"reasoning");
        out.object_start();
        out.key(b"effort");
        out.string(effort);
        out.object_end();
    }
    if let Some(key) = &request.prompt_cache_key {
        out.key(b"prompt_cache_key");
        out.string(key);
    }
    out.key(b"text");
    out.object_start();
    out.key(b"verbosity");
    out.string(identity::VERBOSITY);
    out.object_end();
    out.object_end();
}
fn write_input(out: &mut Encoder, input: &Input) {
    match input {
        Input::Opaque { value } => value.write(out),
        Input::Message { role, text, id, phase, refusal } => {
            out.object_start();
            out.key(b"type");
            out.string(b"message");
            out.key(b"role");
            out.string(match role {
                Role::User => b"user",
                Role::Assistant => b"assistant",
            });
            if let Some(id) = id {
                out.key(b"id");
                out.string(id);
            }
            if let Some(phase) = phase {
                out.key(b"phase");
                out.string(phase);
            }
            out.key(b"content");
            out.array_start();
            out.object_start();
            out.key(b"type");
            out.string(match role {
                Role::User => b"input_text",
                Role::Assistant => {
                    if *refusal {
                        b"refusal"
                    } else {
                        b"output_text"
                    }
                }
            });
            out.key(if *refusal { b"refusal" } else { b"text" });
            out.string(text);
            out.object_end();
            out.array_end();
            out.object_end();
        }
        Input::FunctionCall { call_id, item_id, name, arguments } => {
            out.object_start();
            out.key(b"type");
            out.string(b"function_call");
            out.key(b"call_id");
            out.string(call_id);
            if let Some(id) = item_id {
                out.key(b"id");
                out.string(id);
            }
            out.key(b"name");
            out.string(name);
            out.key(b"arguments");
            out.string(arguments);
            out.object_end();
        }
        Input::FunctionOutput { call_id, output } => {
            out.object_start();
            out.key(b"type");
            out.string(b"function_call_output");
            out.key(b"call_id");
            out.string(call_id);
            out.key(b"output");
            out.string(output);
            out.object_end();
        }
    }
}
pub fn decode_request(value: &Json, limits: &Limits) -> Result<Request, DecodeError> {
    let mut admission =
        Encoder::measure(&skein_json::writer::Limits { depth: limits.depth, length: limits.request_bytes });
    value.write(&mut admission);
    let _length = crate::openai::common::measured(admission)?;
    let tokens = value.as_tokens();
    if !json::boolean(json::value_at(tokens, json::required(tokens, b"stream")?)?)?
        || json::boolean(json::value_at(tokens, json::required(tokens, b"store")?)?)?
    {
        return Err(DecodeError::Malformed);
    }
    let instructions = json::text(json::value_at(tokens, json::required(tokens, b"instructions")?)?)?;
    let model = json::text(json::value_at(tokens, json::required(tokens, b"model")?)?)?;
    let mut tools = List::with_capacity(limits.parts);
    if let Some(values) = json::optional_at(tokens, json::field(tokens, b"tools")?)? {
        for &offset in &json::array(values, limits.parts)? {
            let tool = json::value_at(values, offset)?;
            if json::text_ref(json::value_at(tool, json::required(tool, b"type")?)?)? != b"function" {
                return Err(DecodeError::WrongType);
            }
            let description = match json::optional_at(tool, json::field(tool, b"description")?)? {
                Some(value) => json::text(value)?,
                None => bytes::copy_of(b""),
            };
            let tool = Tool {
                name: json::text(json::value_at(tool, json::required(tool, b"name")?)?)?,
                description,
                schema: Json::from_tokens(json::value_at(tool, json::required(tool, b"parameters")?)?, limits)?,
            };
            if tools.push(tool).is_err() {
                return Err(DecodeError::TooLarge);
            }
        }
    }
    let values = json::value_at(tokens, json::required(tokens, b"input")?)?;
    let mut input = List::with_capacity(limits.parts);
    for &offset in &json::array(values, limits.parts)? {
        let item = read_input(json::value_at(values, offset)?, limits)?;
        if input.push(item).is_err() {
            return Err(DecodeError::TooLarge);
        }
    }
    let effort = match json::optional_at(tokens, json::field(tokens, b"reasoning")?)? {
        Some(value) => optional_text(value, b"effort")?,
        None => None,
    };
    let prompt_cache_key = optional_text(tokens, b"prompt_cache_key")?;
    let request =
        Request { model, instructions, tools: tools.into_boxed(), input: input.into_boxed(), effort, prompt_cache_key };
    validate(&request, limits)?;
    Ok(request)
}
pub(crate) fn optional_text(tokens: &[Token], name: &[u8]) -> Result<Option<Box<[u8]>>, DecodeError> {
    match json::optional_at(tokens, json::field(tokens, name)?)? {
        Some(value) => match value {
            [Token::Null] => Ok(None),
            _ => Ok(Some(json::text(value)?)),
        },
        None => Ok(None),
    }
}
fn read_input(tokens: &[Token], limits: &Limits) -> Result<Input, DecodeError> {
    let kind = match json::optional_at(tokens, json::field(tokens, b"type")?)? {
        Some(value) => json::text_ref(value)?,
        None => b"message",
    };
    match kind {
        b"message" => {
            let role = match json::text_ref(json::value_at(tokens, json::required(tokens, b"role")?)?)? {
                b"user" => Role::User,
                b"assistant" => Role::Assistant,
                _ => return Err(DecodeError::WrongType),
            };
            let values = json::value_at(tokens, json::required(tokens, b"content")?)?;
            let mut text = List::with_capacity(limits.request_bytes);
            let mut refusal = false;
            let mut content_kind: Option<bool> = None;
            for &offset in &json::array(values, limits.parts)? {
                let part = json::value_at(values, offset)?;
                let kind = json::text_ref(json::value_at(part, json::required(part, b"type")?)?)?;
                let key = match kind {
                    b"input_text" | b"output_text" => b"text".as_slice(),
                    b"refusal" => {
                        refusal = true;
                        b"refusal"
                    }
                    _ => return Err(DecodeError::WrongType),
                };
                let is_refusal = kind == b"refusal";
                if let Some(previous) = content_kind
                    && previous != is_refusal
                {
                    return Err(DecodeError::WrongType);
                }
                content_kind = Some(is_refusal);
                crate::openai::common::append(
                    &mut text,
                    json::text_ref(json::value_at(part, json::required(part, key)?)?)?,
                )?;
            }
            Ok(Input::Message {
                role,
                text: text.into_boxed(),
                id: optional_text(tokens, b"id")?,
                phase: optional_text(tokens, b"phase")?,
                refusal,
            })
        }
        b"function_call" => Ok(Input::FunctionCall {
            call_id: json::text(json::value_at(tokens, json::required(tokens, b"call_id")?)?)?,
            item_id: optional_text(tokens, b"id")?,
            name: json::text(json::value_at(tokens, json::required(tokens, b"name")?)?)?,
            arguments: json::text(json::value_at(tokens, json::required(tokens, b"arguments")?)?)?,
        }),
        b"function_call_output" => Ok(Input::FunctionOutput {
            call_id: json::text(json::value_at(tokens, json::required(tokens, b"call_id")?)?)?,
            output: json::text(json::value_at(tokens, json::required(tokens, b"output")?)?)?,
        }),
        _ => Ok(Input::Opaque { value: Json::from_tokens(tokens, limits)? }),
    }
}
