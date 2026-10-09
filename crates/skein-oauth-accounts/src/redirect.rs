//! Bounded redirect reconstruction and query decoding (oauth.md, sections 1,
//! 5 and 6.5). The registered URI is compared byte for byte before the OAuth
//! machine sees query values; state authentication remains the machine's.

use alloc::boxed::Box;
use skein_lib::{List, Writer, bytes};
use skein_oauth::ClientLimits;

pub(crate) struct Redirect {
    pub(crate) uri: Box<[u8]>,
    pub(crate) state: Box<[u8]>,
    pub(crate) code: Option<Box<[u8]>>,
    pub(crate) error: Option<Box<[u8]>>,
}

pub(crate) fn parse(uri: &[u8], registered: &[u8], limits: &ClientLimits) -> Option<Redirect> {
    if uri.len() > usize::try_from(limits.url_bytes).ok()? || uri.contains(&b'#') {
        return None;
    }
    let rest = uri.strip_prefix(registered)?;
    let separator = if registered.contains(&b'?') { b'&' } else { b'?' };
    if rest.first() != Some(&separator) {
        return None;
    }
    let query = rest.get(1..)?;
    let mut fields = Fields { state: None, code: None, error: None };
    let mut start = 0;
    for index in 0..query.len() {
        if query.get(index) == Some(&b'&') {
            field(query.get(start..index)?, limits, &mut fields)?;
            start = index.checked_add(1)?;
        }
    }
    field(query.get(start..)?, limits, &mut fields)?;
    Some(Redirect { uri: bytes::copy_of(registered), state: fields.state?, code: fields.code, error: fields.error })
}

struct Fields {
    state: Option<Box<[u8]>>,
    code: Option<Box<[u8]>>,
    error: Option<Box<[u8]>>,
}

fn field(pair: &[u8], limits: &ClientLimits, fields: &mut Fields) -> Option<()> {
    let at = bytes::find(pair, b"=")?;
    let key = pair.get(..at)?;
    let value = pair.get(at.checked_add(1)?..)?;
    match key {
        b"state" => {
            if fields.state.is_some() {
                return None;
            }
            fields.state = Some(decode(value, limits.state_bytes)?);
        }
        b"code" => {
            if fields.code.is_some() {
                return None;
            }
            fields.code = Some(decode(value, limits.code_bytes)?);
        }
        b"error" => {
            if fields.error.is_some() {
                return None;
            }
            fields.error = Some(decode(value, limits.document.detail_bytes)?);
        }
        _ => {}
    }
    Some(())
}

fn decode(value: &[u8], bound: u32) -> Option<Box<[u8]>> {
    let mut output = List::with_capacity(bound);
    let mut skip = 0_u32;
    for index in 0..value.len() {
        if skip > 0 {
            skip = skip.checked_sub(1)?;
            continue;
        }
        let byte = *value.get(index)?;
        let decoded = match byte {
            b'%' => {
                let high = hex(*value.get(index.checked_add(1)?)?)?;
                let low = hex(*value.get(index.checked_add(2)?)?)?;
                skip = 2;
                high.checked_mul(16)?.checked_add(low)?
            }
            b'+' => b' ',
            byte => byte,
        };
        if output.push(decoded).is_err() {
            return None;
        }
    }
    Some(output.to_boxed())
}

fn hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => byte.checked_sub(b'0'),
        b'a'..=b'f' => byte.checked_sub(b'a')?.checked_add(10),
        b'A'..=b'F' => byte.checked_sub(b'A')?.checked_add(10),
        _ => None,
    }
}

pub(crate) fn from_head(call: &skein_http::server::Call, registered: &[u8], limits: &ClientLimits) -> Option<Redirect> {
    if call.method != skein_http::Method::Get {
        return None;
    }
    let mut count = 0_u32;
    let mut host = None;
    for header in &call.headers {
        if header.is(b"host") {
            count = count.checked_add(1)?;
            host = Some(header.value.as_ref());
        }
    }
    if count != 1 {
        return None;
    }
    let authority = registered.strip_prefix(b"http://")?;
    let slash = bytes::find(authority, b"/")?;
    if host? != authority.get(..slash)? {
        return None;
    }
    let origin = call.target.starts_with(b"/");
    let length =
        if origin { 7_usize.checked_add(host?.len())?.checked_add(call.target.len())? } else { call.target.len() };
    if length > usize::try_from(limits.url_bytes).ok()? {
        return None;
    }
    let mut uri = Writer::new(length);
    if origin {
        uri.put(b"http://").ok()?;
        uri.put(host?).ok()?;
    }
    uri.put(&call.target).ok()?;
    parse(&uri.finish(), registered, limits)
}
