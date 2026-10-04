//! Header fields (RFC 9110, 5): a name and a value, both bytes, the names
//! compared without regard to case; the lists some values are; and the
//! lines a head comes in.

use alloc::boxed::Box;

/// A header field, as a head carries it: its name, and its value with the
/// whitespace around it trimmed (RFC 9110, 5.5).
///
/// Names are compared without regard to case ([`Header::is`]); the bytes
/// are kept as the peer sent them. A value is not checked as UTF-8: a field
/// whose value is text is the application's to check.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Header {
    pub name: Box<[u8]>,
    pub value: Box<[u8]>,
}

impl Header {
    /// Whether this field is named `name`, without regard to case.
    #[must_use]
    pub fn is(&self, name: &[u8]) -> bool {
        self.name.eq_ignore_ascii_case(name)
    }
}

/// Whether `byte` may be part of a token, as a field's name or a method is
/// (RFC 9110, 5.6.2).
pub(crate) fn is_tchar(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte)
}

/// Whether `byte` may be part of a field's value (RFC 9110, 5.5): a visible
/// character, a space, a tab, or a byte past ASCII. No other control
/// character, and so no CR, LF or NUL.
pub(crate) fn is_field_byte(byte: u8) -> bool {
    byte == b'\t' || byte == b' ' || (0x21..=0x7E).contains(&byte) || byte >= 0x80
}

/// Whether `byte` is whitespace around a value: a space or a tab (RFC 9110,
/// 5.6.3).
pub(crate) fn is_ows(byte: u8) -> bool {
    byte == b' ' || byte == b'\t'
}

/// `bytes` without the spaces and tabs around them.
pub(crate) fn trim(bytes: &[u8]) -> &[u8] {
    let mut start = bytes.len();
    for (at, &byte) in bytes.iter().enumerate() {
        if !is_ows(byte) {
            start = at;
            break;
        }
    }
    let mut end = start;
    for (at, &byte) in bytes.iter().enumerate().skip(start) {
        if !is_ows(byte) {
            end = at.saturating_add(1);
        }
    }
    bytes.get(start..end).expect("within the bytes")
}

/// The next element of a comma-separated list (RFC 9110, 5.6.1) in `value`
/// from `at`, trimmed, and where the one after it begins; `None` once past
/// the end. Empty elements are elements here, the one after a trailing
/// comma among them: the caller skips them, as a recipient must.
pub(crate) fn element(value: &[u8], at: usize) -> Option<(&[u8], usize)> {
    let rest = value.get(at..)?;
    let mut end = rest.len();
    for (offset, &byte) in rest.iter().enumerate() {
        if byte == b',' {
            end = offset;
            break;
        }
    }
    let element = trim(rest.get(..end)?);
    Some((element, at.checked_add(end)?.checked_add(1)?))
}

/// Whether any field named `name` among `headers` lists `token`, without
/// regard to case: `Connection: close`, `Connection: keep-alive`.
pub(crate) fn lists(headers: &[Header], name: &[u8], token: &[u8]) -> bool {
    for header in headers {
        if !header.is(name) {
            continue;
        }
        let mut at = 0;
        // Bounded by the value: each element moves past its comma.
        for _ in 0..=header.value.len() {
            let Some((element, next)) = element(&header.value, at) else { break };
            if element.eq_ignore_ascii_case(token) {
                return true;
            }
            at = next;
        }
    }
    false
}

/// A line's content: the bytes before its LF, and before a CR that
/// precedes it (RFC 9112, 2.2). `None` for bytes that do not end with LF:
/// a scan that reached its maximum first.
pub(crate) fn content(bytes: &[u8]) -> Option<&[u8]> {
    let line = bytes.strip_suffix(b"\n")?;
    match line.strip_suffix(b"\r") {
        Some(line) => Some(line),
        None => Some(line),
    }
}
