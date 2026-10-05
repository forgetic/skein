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

/// A conservative upper bound for the machine's owned memory, or `None` on
/// overflow. It includes every entity table, in-flight command, queued
/// terminal, the largest simultaneous document and command encodings, and
/// per-operation matches and reports. The estimate intentionally counts a
/// screenshot and a snapshot for every operation and queued event at once.
#[must_use]
pub fn worst_case(limits: &Limits) -> Option<u64> {
    let text = u64::from(limits.text);
    let command = u64::from(limits.command).checked_add(1)?;
    let message = u64::from(limits.message).checked_add(1)?;
    let seen = u64::try_from(size_of::<crate::boundary::Seen>()).ok()?.checked_add(text.checked_mul(3)?)?;
    let matches = u64::from(limits.matches).checked_mul(seen)?;
    let report = matches.checked_add(u64::from(limits.snapshot))?.checked_add(u64::from(limits.screenshot))?;
    let persons = u64::from(limits.persons).checked_mul(512_u64.checked_add(text)?)?;
    let pages =
        u64::from(limits.pages).checked_mul(1024_u64.checked_add(command)?.checked_add(text.checked_mul(2)?)?)?;
    let ops = u64::from(limits.ops).checked_mul(1024_u64.checked_add(report)?.checked_add(command)?)?;
    let commands = u64::from(limits.commands).checked_mul(512_u64.checked_add(command.checked_mul(2)?)?)?;
    let events = u64::from(limits.persons)
        .checked_add(u64::from(limits.pages))?
        .checked_add(u64::from(limits.ops))?
        .checked_add(4)?;
    let events = events.checked_mul(u64::try_from(size_of::<crate::boundary::Event>()).ok()?.checked_add(report)?)?;
    let transient =
        message.checked_mul(4)?.checked_add(command.checked_mul(4)?)?.checked_add(u64::from(limits.stderr))?;
    persons.checked_add(pages)?.checked_add(ops)?.checked_add(commands)?.checked_add(events)?.checked_add(transient)
}
