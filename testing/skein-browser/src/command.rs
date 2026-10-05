//! The command and descriptor layout the owner uses to spawn Chromium.

#![expect(clippy::disallowed_types, reason = "the profile argument is assembled once before spawn")]

use alloc::boxed::Box;
use alloc::vec::Vec;

use skein_lib::List;

/// A child pipe requested from the process spawner.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Pipe {
    pub descriptor: u32,
    pub direction: Direction,
}

/// Direction, as seen by the child.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Direction {
    Read,
    Write,
}

/// Spawn configuration; the profile path must name a fresh directory owned
/// by this browser and removed by its owner after child exit.
#[derive(Debug)]
pub struct Command {
    pub program: Box<[u8]>,
    pub args: List<Box<[u8]>>,
    pub env: List<Variable>,
    pub pipes: [Pipe; 3],
}

/// One child environment variable.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Variable {
    pub name: Box<[u8]>,
    pub value: Box<[u8]>,
}

/// Build Chromium's quiet, offline command with a fixed locale and time zone.
#[must_use]
pub fn command(program: &[u8], profile: &[u8]) -> Command {
    assert!(!program.is_empty() && !program.contains(&0), "a program path has no NUL");
    assert!(!profile.is_empty() && !profile.contains(&0), "a profile path has no NUL");
    let mut args = List::with_capacity(16);
    for arg in [
        b"--headless".as_slice(),
        b"--remote-debugging-pipe",
        b"--no-first-run",
        b"--no-default-browser-check",
        b"--disable-background-networking",
        b"--disable-sync",
        b"--disable-extensions",
        b"--disable-component-update",
        b"--disable-default-apps",
        b"--disable-features=MediaRouter",
        b"--metrics-recording-only",
    ] {
        args.push(Box::from(arg)).expect("the argument list has room");
    }
    let mut profile_arg = Vec::new();
    profile_arg.extend_from_slice(b"--user-data-dir=");
    profile_arg.extend_from_slice(profile);
    args.push(profile_arg.into_boxed_slice()).expect("the argument list has room");
    args.push(Box::from(b"about:blank".as_slice())).expect("the argument list has room");
    let mut env = List::with_capacity(3);
    env.push(Variable { name: Box::from(b"LANG".as_slice()), value: Box::from(b"C.UTF-8".as_slice()) })
        .expect("room for locale");
    env.push(Variable { name: Box::from(b"LC_ALL".as_slice()), value: Box::from(b"C.UTF-8".as_slice()) })
        .expect("room for locale");
    env.push(Variable { name: Box::from(b"TZ".as_slice()), value: Box::from(b"UTC".as_slice()) })
        .expect("room for time zone");
    Command {
        program: Box::from(program),
        args,
        env,
        pipes: [
            Pipe { descriptor: 3, direction: Direction::Read },
            Pipe { descriptor: 4, direction: Direction::Write },
            Pipe { descriptor: 2, direction: Direction::Write },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::{Direction, command};

    #[test]
    fn chromium_pipe_command() {
        let command = command(b"/usr/bin/chromium", b"/tmp/fresh-profile");
        assert_eq!(command.program.as_ref(), b"/usr/bin/chromium");
        assert_eq!(command.args.last().expect("initial page").as_ref(), b"about:blank");
        let mut profile = false;
        for arg in &command.args {
            if arg.as_ref() == b"--user-data-dir=/tmp/fresh-profile" {
                profile = true;
            }
        }
        assert!(profile, "the command has a fresh profile");
        assert_eq!(command.pipes.first().expect("command pipe").descriptor, 3);
        assert_eq!(command.pipes.first().expect("command pipe").direction, Direction::Read);
        assert_eq!(command.pipes.get(1).expect("reply pipe").descriptor, 4);
        assert_eq!(command.pipes.get(1).expect("reply pipe").direction, Direction::Write);
        assert_eq!(command.pipes.get(2).expect("stderr pipe").descriptor, 2);
    }
}
