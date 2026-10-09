//! Opt-in historical Claude Code identity values from Tongs' Anthropic adapter.
//!
//! These are archived compatibility data, not Skein's identity or a guarantee
//! of current subscription admission. The default endpoint does not install
//! them. Callers choosing this profile supply their own session and request
//! IDs and opt in to the system identity with [`instructions`].
use crate::Error;
use alloc::boxed::Box;
use skein_http::Header;
use skein_lib::{Writer, bytes};

pub const CLAUDE_CODE_SYSTEM_IDENTITY: &[u8] = b"You are Claude Code, Anthropic's official CLI for Claude.";
pub const CLAUDE_CODE_USER_AGENT: &[u8] = b"claude-cli/2.1.139 (external, sdk-cli)";
pub const OAUTH_BETAS: &[u8] = b"claude-code-20250219,oauth-2025-04-20,interleaved-thinking-2025-05-14,context-management-2025-06-27,prompt-caching-scope-2026-01-05,advisor-tool-2026-03-01,advanced-tool-use-2025-11-20,context-1m-2025-08-07,effort-2025-11-24,extended-cache-ttl-2025-04-11";

/// Opts in to the historical system identity while preserving the caller's
/// additional instructions. The request encoder sends the identity as the
/// first system text block and `extra` as a separate following block.
pub fn instructions(extra: &[u8]) -> Result<Box<[u8]>, Error> {
    if extra.is_empty() {
        return Ok(bytes::copy_of(CLAUDE_CODE_SYSTEM_IDENTITY));
    }
    let length = CLAUDE_CODE_SYSTEM_IDENTITY.len().checked_add(2).ok_or(Error::Invalid)?;
    let length = length.checked_add(extra.len()).ok_or(Error::Invalid)?;
    let mut out = Writer::new(length);
    out.put(CLAUDE_CODE_SYSTEM_IDENTITY).expect("identity length was measured");
    out.put(b"\n\n").expect("separator length was measured");
    out.put(extra).expect("additional instructions were measured");
    Ok(out.finish())
}

/// Historical static identity headers; the caller chooses whether to use them.
#[must_use]
pub fn claude_code_headers() -> Box<[Header]> {
    Box::new([
        header(b"anthropic-beta", OAUTH_BETAS),
        header(b"user-agent", CLAUDE_CODE_USER_AGENT),
        header(b"x-app", b"cli"),
        header(b"X-Stainless-Arch", b"x64"),
        header(b"X-Stainless-Lang", b"js"),
        header(b"X-Stainless-OS", b"Linux"),
        header(b"X-Stainless-Package-Version", b"0.93.0"),
        header(b"X-Stainless-Retry-Count", b"0"),
        header(b"X-Stainless-Runtime", b"node"),
        header(b"X-Stainless-Runtime-Version", b"v24.3.0"),
        header(b"X-Stainless-Timeout", b"600"),
    ])
}
fn header(name: &[u8], value: &[u8]) -> Header {
    Header { name: bytes::copy_of(name), value: bytes::copy_of(value) }
}
