//! Sized CDP command encoding.

#![expect(clippy::disallowed_types, reason = "a command is assembled once then moved to its pipe")]

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_lib::Decimal;

use super::decode::Document;

/// Why a command was refused before it reached the pipe.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum EncodeError {
    TooLong,
    Text,
    Params,
}

/// Encode a complete NUL terminated command. `params` is a JSON object,
/// commonly `b"{}"`; the empty slice also means an empty object.
pub fn command(
    id: u64,
    method: &[u8],
    session: Option<&[u8]>,
    params: &[u8],
    limit: u32,
) -> Result<Box<[u8]>, EncodeError> {
    if !valid_text(method)
        || match session {
            Some(part) => !valid_text(part),
            None => false,
        }
    {
        return Err(EncodeError::Text);
    }
    let params = if params.is_empty() { b"{}".as_slice() } else { params };
    let Ok(parsed) = Document::parse(params, limit) else {
        return Err(EncodeError::Params);
    };
    if !parsed.root().raw().starts_with(b"{") {
        return Err(EncodeError::Params);
    }
    let mut output = Vec::new();
    output.extend_from_slice(b"{\"id\":");
    output.extend_from_slice(Decimal::of(id).as_bytes());
    output.extend_from_slice(b",\"method\":");
    string(&mut output, method);
    if let Some(session) = session {
        output.extend_from_slice(b",\"sessionId\":");
        string(&mut output, session);
    }
    output.extend_from_slice(b",\"params\":");
    output.extend_from_slice(parsed.root().raw());
    output.push(b'}');
    let Ok(limit) = usize::try_from(limit) else {
        return Err(EncodeError::TooLong);
    };
    if output.len() > limit {
        return Err(EncodeError::TooLong);
    }
    output.push(0);
    Ok(output.into_boxed_slice())
}

fn string(out: &mut Vec<u8>, value: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(b'"');
    for byte in value {
        match byte {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0..=31 => {
                out.extend_from_slice(b"\\u00");
                out.push(*HEX.get(usize::from(byte >> 4_u32)).expect("a nibble indexes hex"));
                out.push(*HEX.get(usize::from(byte & 15)).expect("a nibble indexes hex"));
            }
            _ => out.push(*byte),
        }
    }
    out.push(b'"');
}

#[expect(clippy::disallowed_methods, reason = "CDP method and session names must be UTF-8 JSON strings")]
fn valid_text(bytes: &[u8]) -> bool {
    core::str::from_utf8(bytes).is_ok()
}

#[cfg(test)]
mod tests {
    use super::{EncodeError, command};

    #[test]
    fn command_with_session_and_params() {
        assert_eq!(
            command(5, b"Page.navigate", Some(b"a\"b"), br#"{"url":"about:blank"}"#, 200)
                .expect("valid command")
                .as_ref(),
            b"{\"id\":5,\"method\":\"Page.navigate\",\"sessionId\":\"a\\\"b\",\"params\":{\"url\":\"about:blank\"}}\0",
        );
    }

    #[test]
    fn size_and_params_refusal() {
        assert_eq!(command(1, b"Browser.close", None, b"{}", 10), Err(EncodeError::TooLong));
        assert_eq!(command(1, b"X", None, b"[1]", 100), Err(EncodeError::Params));
        assert_eq!(command(1, b"X", None, b"{bad}", 100), Err(EncodeError::Params));
    }
}
