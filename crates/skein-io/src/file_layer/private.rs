//! Opening private roots and making directories (io.md, section 5.3).
//! The request holds an independently opened parent, its bounded leaf name,
//! and descriptors until cleanup. It never infers a user from a path.
//! `start`, `up`, and `stopped` run only through `FileIo`'s entry points.

use super::{FileIo, Pending};
use crate::file::{Event, Unsafe};
use crate::kernel::{Complete, Error, Fd, Kind, Op, OpenHow, Stat, Submit};
use alloc::boxed::Box;
use skein_lib::{Queue, Token};

#[derive(Clone, Copy, Debug)]
pub(super) enum Action {
    Make { mode: u32 },
    Private { user: u32 },
    Refusal { user: u32 },
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    ParentOpening,
    Making,
    Opening,
    Stating,
    DeniedOpening,
    DeniedStating,
    ClosingFile,
    ClosingParent,
}

#[derive(Debug)]
pub(super) struct Opening {
    owner: Token,
    action: Action,
    phase: Phase,
    name: Box<[u8]>,
    parent: Option<Fd>,
    file: Option<Fd>,
    terminal: Option<Event>,
    made: bool,
}

impl Opening {
    pub(super) const fn owner(&self) -> Token {
        self.owner
    }
    pub(super) fn can_abandon(&self) -> bool {
        match self.action {
            Action::Private { .. } => self.terminal.is_none(),
            Action::Refusal { .. } => false,
            Action::Make { .. } => match self.phase {
                Phase::ParentOpening => true,
                Phase::Making
                | Phase::Opening
                | Phase::Stating
                | Phase::DeniedOpening
                | Phase::DeniedStating
                | Phase::ClosingFile
                | Phase::ClosingParent => false,
            },
        }
    }

    pub(super) fn can_cancel(&self) -> bool {
        match self.phase {
            Phase::ParentOpening | Phase::Opening | Phase::DeniedOpening => true,
            Phase::Making | Phase::Stating | Phase::DeniedStating | Phase::ClosingFile | Phase::ClosingParent => false,
        }
    }
}

pub(super) fn unsafe_root(stat: Stat, expected: u32) -> Option<Unsafe> {
    if stat.owner != expected {
        return Some(Unsafe::Owner { found: stat.owner, expected });
    }
    if stat.kind == Kind::Symlink {
        return Some(Unsafe::Link);
    }
    if stat.kind != Kind::Directory {
        return Some(Unsafe::Kind(stat.kind));
    }
    if stat.mode & 0o077 != 0 {
        return Some(Unsafe::Mode(stat.mode));
    }
    None
}

pub(super) fn unsafe_file(stat: Stat, expected: u32) -> Option<Unsafe> {
    if stat.owner != expected {
        return Some(Unsafe::Owner { found: stat.owner, expected });
    }
    if stat.kind == Kind::Symlink {
        return Some(Unsafe::Link);
    }
    if stat.kind != Kind::File {
        return Some(Unsafe::Kind(stat.kind));
    }
    if stat.mode & 0o077 != 0 {
        return Some(Unsafe::Mode(stat.mode));
    }
    if stat.links != 1 {
        return Some(Unsafe::Links(stat.links));
    }
    None
}

pub(super) fn start(
    io: &mut FileIo,
    owner: Token,
    root: Token,
    path: Box<[u8]>,
    action: Action,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    let Some(fd) = io.file(root) else {
        super::terminal_failure(events, owner, Error::NotFound);
        return;
    };
    if path.len() >= 4096 || path.contains(&0) {
        super::terminal_failure(events, owner, Error::InvalidArgument);
        return;
    }
    let Some((parent, name)) = super::store::split_path(&path) else {
        super::terminal_failure(events, owner, Error::InvalidArgument);
        return;
    };
    match action {
        Action::Private { .. } if io.files.len() == io.files.capacity() => {
            super::terminal_failure(events, owner, Error::TooManyOpenFiles);
            return;
        }
        Action::Private { .. } | Action::Make { .. } => {}
        Action::Refusal { .. } => unreachable!("a refusal is started after a denied load"),
    }
    let no_follow = match action {
        Action::Private { .. } => true,
        Action::Refusal { .. } => unreachable!("a refusal has its root and path"),
        Action::Make { mode } => {
            if mode & !crate::kernel::PERMISSIONS != 0 {
                super::terminal_failure(events, owner, Error::InvalidArgument);
                return;
            }
            io.private_owner(root).is_some()
        }
    };
    let how = if no_follow { OpenHow::DirectoryNoFollow } else { OpenHow::Directory };
    let opening = Opening {
        owner,
        action,
        phase: Phase::ParentOpening,
        name,
        parent: None,
        file: None,
        terminal: None,
        made: false,
    };
    issue(io, opening, Op::Open { root: fd, path: parent, how }, subs);
}

fn issue(io: &mut FileIo, opening: Opening, operation: Op, subs: &mut Queue<Submit>) {
    io.pending = Some(Pending::Private(opening));
    io.issue(operation, subs);
}

fn open(io: &mut FileIo, mut opening: Opening, subs: &mut Queue<Submit>) {
    opening.phase = Phase::Opening;
    let operation =
        Op::Open { root: opening.parent.expect("parent open"), path: opening.name.clone(), how: OpenHow::ReadNoFollow };
    issue(io, opening, operation, subs);
}

fn cleanup(io: &mut FileIo, mut opening: Opening, events: &mut Queue<Event>, subs: &mut Queue<Submit>) {
    if opening.terminal.is_some()
        && let Some(fd) = opening.file.take()
    {
        opening.phase = Phase::ClosingFile;
        issue(io, opening, Op::Close { fd }, subs);
    } else if let Some(fd) = opening.parent.take() {
        opening.phase = Phase::ClosingParent;
        issue(io, opening, Op::Close { fd }, subs);
    } else {
        match opening.terminal {
            Some(terminal) => events.push(terminal),
            None => {
                let user = match opening.action {
                    Action::Private { user } => user,
                    Action::Make { .. } | Action::Refusal { .. } => {
                        unreachable!("non-admitting requests have their terminal")
                    }
                };
                let fd = opening.file.take().expect("private root held until parent closes");
                let file = io.insert_private(fd, user).expect("admission reserved an open file slot");
                events.push(Event::Opened { owner: opening.owner, file, len: 0 });
            }
        }
    }
}

#[expect(clippy::too_many_lines, reason = "one exhaustive transition match keeps refusal and cleanup together")]
pub(super) fn up(
    io: &mut FileIo,
    mut opening: Opening,
    complete: Complete,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match complete.result {
        Err(error) => {
            match opening.phase {
                Phase::Opening if error == Error::Permission => {
                    opening.terminal =
                        Some(Event::Failed { owner: opening.owner, error, committed: false, residue: None });
                    opening.phase = Phase::DeniedOpening;
                    let operation = Op::Open {
                        root: opening.parent.expect("the same opened parent"),
                        path: opening.name.clone(),
                        how: OpenHow::PathNoFollow,
                    };
                    issue(io, opening, operation, subs);
                    return;
                }
                Phase::Opening if error == Error::NotFound && !opening.made => {
                    opening.made = true;
                    opening.phase = Phase::Making;
                    let operation = Op::MakeDirectory {
                        dir: opening.parent.expect("parent open"),
                        name: opening.name.clone(),
                        mode: 0o700,
                    };
                    issue(io, opening, operation, subs);
                    return;
                }
                Phase::Making => match opening.action {
                    Action::Private { .. } if error == Error::Exists => {
                        open(io, opening, subs);
                        return;
                    }
                    Action::Make { .. } | Action::Private { .. } => {}
                    Action::Refusal { .. } => unreachable!("a denial makes nothing"),
                },
                Phase::ParentOpening | Phase::Opening if error == Error::TooManyLinks => match opening.action {
                    Action::Private { .. } => {
                        opening.terminal = Some(Event::Refused { owner: opening.owner, found: Unsafe::Link });
                    }
                    Action::Make { .. } => {}
                    Action::Refusal { .. } => unreachable!("a refusal opens only a path"),
                },
                Phase::Opening if error == Error::NotAFile => {
                    opening.terminal = Some(Event::Refused { owner: opening.owner, found: Unsafe::Kind(Kind::Other) });
                }
                Phase::ParentOpening
                | Phase::Opening
                | Phase::Stating
                | Phase::DeniedOpening
                | Phase::DeniedStating
                | Phase::ClosingFile
                | Phase::ClosingParent => {}
            }
            if opening.terminal.is_none() {
                opening.terminal = Some(Event::Failed { owner: opening.owner, error, committed: false, residue: None });
            }
            cleanup(io, opening, events, subs);
        }
        Ok(done) => match opening.phase {
            Phase::ParentOpening => {
                let fd = super::store::done_fd(done);
                opening.parent = Some(fd);
                match opening.action {
                    Action::Private { .. } => open(io, opening, subs),
                    Action::Make { mode } => {
                        opening.phase = Phase::Making;
                        let operation = Op::MakeDirectory { dir: fd, name: opening.name.clone(), mode };
                        issue(io, opening, operation, subs);
                    }
                    Action::Refusal { .. } => unreachable!("a denial already names its root"),
                }
            }
            Phase::Making => {
                super::store::done_nothing(done);
                match opening.action {
                    Action::Private { .. } => open(io, opening, subs),
                    Action::Refusal { .. } => unreachable!("a denial makes nothing"),
                    Action::Make { .. } => {
                        opening.terminal = Some(Event::Made { owner: opening.owner });
                        cleanup(io, opening, events, subs);
                    }
                }
            }
            Phase::Opening => {
                let fd = super::store::done_fd(done);
                opening.file = Some(fd);
                opening.phase = Phase::Stating;
                issue(io, opening, Op::Stat { fd }, subs);
            }
            Phase::Stating => {
                let stat = super::store::done_stat(done);
                let user = match opening.action {
                    Action::Private { user } => user,
                    Action::Make { .. } | Action::Refusal { .. } => unreachable!("only a private root is admitted"),
                };
                if let Some(found) = unsafe_root(stat, user) {
                    opening.terminal = Some(Event::Refused { owner: opening.owner, found });
                }
                cleanup(io, opening, events, subs);
            }
            Phase::DeniedOpening => {
                let fd = super::store::done_fd(done);
                opening.file = Some(fd);
                opening.phase = Phase::DeniedStating;
                issue(io, opening, Op::Stat { fd }, subs);
            }
            Phase::DeniedStating => {
                let stat = super::store::done_stat(done);
                let found = match opening.action {
                    Action::Private { user } => unsafe_root(stat, user),
                    Action::Refusal { user } => unsafe_file(stat, user),
                    Action::Make { .. } => unreachable!("a directory maker has no denied read"),
                };
                if let Some(found) = found {
                    opening.terminal = Some(Event::Refused { owner: opening.owner, found });
                }
                cleanup(io, opening, events, subs);
            }
            Phase::ClosingFile | Phase::ClosingParent => {
                super::store::done_nothing(done);
                cleanup(io, opening, events, subs);
            }
        },
    }
}

pub(super) fn stopped(
    io: &mut FileIo,
    mut opening: Opening,
    complete: Complete,
    reason: Error,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    match opening.phase {
        Phase::ParentOpening => {
            if let Ok(done) = complete.result {
                opening.parent = Some(super::store::done_fd(done));
            }
        }
        Phase::Opening | Phase::DeniedOpening => {
            if let Ok(done) = complete.result {
                opening.file = Some(super::store::done_fd(done));
            }
        }
        Phase::Making | Phase::Stating | Phase::DeniedStating | Phase::ClosingFile | Phase::ClosingParent => {}
    }
    if opening.terminal.is_none() {
        opening.terminal = Some(if reason == Error::Cancelled {
            Event::Cancelled { owner: opening.owner }
        } else {
            Event::Failed { owner: opening.owner, error: reason, committed: false, residue: None }
        });
    }
    cleanup(io, opening, events, subs);
}

/// Kernel path refusals that already establish unsafe private metadata.
pub(super) fn unsafe_open(error: Error, private: bool) -> Option<Unsafe> {
    if private && error == Error::TooManyLinks {
        return Some(Unsafe::Link);
    }
    if private && error == Error::NotAFile {
        return Some(Unsafe::Kind(Kind::Other));
    }
    None
}

pub(super) fn refuse_file(
    io: &mut FileIo,
    owner: Token,
    fd: Fd,
    found: Unsafe,
    events: &mut Queue<Event>,
    subs: &mut Queue<Submit>,
) {
    let opening = Opening {
        owner,
        action: Action::Make { mode: 0 },
        phase: Phase::Stating,
        name: Box::from([]),
        parent: None,
        file: Some(fd),
        terminal: Some(Event::Refused { owner, found }),
        made: false,
    };
    cleanup(io, opening, events, subs);
}

/// A denied private load is probed only to name its refusal, never to admit bytes.
pub(super) fn denied_load(io: &mut FileIo, owner: Token, loading: super::Loading, subs: &mut Queue<Submit>) {
    let operation = Op::Open { root: loading.root, path: loading.path.clone(), how: OpenHow::PathNoFollow };
    let opening = Opening {
        owner,
        action: Action::Refusal { user: loading.user },
        phase: Phase::DeniedOpening,
        name: loading.path,
        parent: None,
        file: None,
        terminal: Some(Event::Failed { owner, error: Error::Permission, committed: false, residue: None }),
        made: false,
    };
    issue(io, opening, operation, subs);
}
