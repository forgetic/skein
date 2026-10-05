//! Resource limits for one browser machine.

use core::mem::size_of;

use skein_lib::Duration;

/// Limits chosen by the test owner and fixed for the machine's lifetime.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Limits {
    pub persons: u32,
    pub pages: u32,
    pub ops: u32,
    pub commands: u32,
    pub message: u32,
    pub command: u32,
    pub matches: u32,
    pub text: u32,
    pub snapshot: u32,
    pub screenshot: u32,
    pub stderr: u32,
    pub poll: Duration,
    pub answer: Duration,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            persons: 4,
            pages: 16,
            ops: 64,
            commands: 128,
            message: 4_194_304,
            command: 65_536,
            matches: 128,
            text: 4096,
            snapshot: 262_144,
            screenshot: 2_097_152,
            stderr: 32_768,
            poll: Duration::from_millis(50),
            answer: Duration::from_secs(10),
        }
    }
}

/// The largest scan requested from the replies pipe, including NUL.
#[must_use]
pub fn largest_read(limits: &Limits) -> Option<u32> {
    limits.message.checked_add(1)
}

/// The largest room requested on the commands pipe, including NUL.
#[must_use]
pub fn largest_room(limits: &Limits) -> Option<u32> {
    limits.command.checked_add(1)
}

/// An upper bound for the machine's owned memory, or `None` on overflow.
/// The browser state adds its own slab sizes to this wire and boundary bound.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let page = u64::from(limits.pages).checked_mul(512)?;
    let person = u64::from(limits.persons).checked_mul(256)?;
    let op = u64::from(limits.ops).checked_mul(512)?;
    let command_entry = u64::try_from(size_of::<(u64, u64, u64)>()).ok()?;
    let commands = u64::from(limits.commands).checked_mul(command_entry.checked_add(u64::from(limits.command))?)?;
    let matches = u64::from(limits.matches).checked_mul(96_u64.checked_add(u64::from(limits.text).checked_mul(3)?)?)?;
    let bytes = u64::from(limits.message)
        .checked_add(u64::from(limits.command))?
        .checked_add(u64::from(limits.snapshot))?
        .checked_add(u64::from(limits.screenshot))?
        .checked_add(u64::from(limits.stderr))?;
    page.checked_add(person)?.checked_add(op)?.checked_add(commands)?.checked_add(matches)?.checked_add(bytes)
}
