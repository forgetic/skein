//! Historical `ChatGPT` subscription identity from tongs' redacted subject
//! recordings, 2026-06-13 (recorder f4e0a2b). These were successful exchanges
//! made by tongs; fresh captures of Codex itself remain a deployment task.
pub const PROVENANCE: &[u8] = b"tongs subject capture 2026-06-13 f4e0a2b";
pub const ORIGINATOR: &[u8] = b"pi";
pub const USER_AGENT: &[u8] = b"pi (linux; x86_64)";
pub const BETA: &[u8] = b"responses=experimental";
pub const ACCOUNT_HEADER: &[u8] = b"chatgpt-account-id";
pub const SESSION_HEADER: &[u8] = b"session_id";
pub const REQUEST_HEADER: &[u8] = b"x-request-id";
pub const VERBOSITY: &[u8] = b"low";

use alloc::boxed::Box;
use skein_lib::bytes;
/// Fixed identity headers; the owner supplies bearer, account and ids.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Header {
    pub name: Box<[u8]>,
    pub value: Box<[u8]>,
}
#[must_use]
pub fn headers() -> Box<[Header]> {
    Box::new([header(b"originator", ORIGINATOR), header(b"user-agent", USER_AGENT), header(b"OpenAI-Beta", BETA)])
}
fn header(name: &[u8], value: &[u8]) -> Header {
    Header { name: bytes::copy_of(name), value: bytes::copy_of(value) }
}
