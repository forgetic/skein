//! Synchronous child creation and signalling for the ring adapter.

#![expect(
    clippy::disallowed_types,
    reason = "OwnedFd closes temporary spawn descriptors on every failure path inside the unsafe adapter"
)]

use std::ffi::CString;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
use std::ptr;

use skein_io::kernel::{Done, Error, Fd, Signal, Spawn, Way};

unsafe extern "C" {
    fn pidfd_spawn(
        pidfd: *mut libc::c_int,
        path: *const libc::c_char,
        actions: *const libc::posix_spawn_file_actions_t,
        attr: *const libc::posix_spawnattr_t,
        argv: *const *mut libc::c_char,
        envp: *const *mut libc::c_char,
    ) -> libc::c_int;
}

fn fd(raw: i32) -> Result<OwnedFd, i32> {
    if raw < 0 {
        Err(super::last_errno())
    } else {
        // SAFETY: this syscall just made `raw`, uniquely owned here.
        Ok(unsafe { OwnedFd::from_raw_fd(raw) })
    }
}

fn cstring(bytes: &[u8]) -> Result<CString, i32> {
    CString::new(bytes).map_err(|_| libc::EINVAL)
}

fn action(rc: i32) -> Result<(), i32> {
    if rc == 0 { Ok(()) } else { Err(rc) }
}

/// A pipe write after the child closes its read end returns EPIPE instead of
/// terminating the service with SIGPIPE. The kernel is tied to this thread.
pub(super) fn block_sigpipe() -> Result<(), i32> {
    // SAFETY: sigset_t is a C signal set that sigemptyset initializes.
    let mut set: libc::sigset_t = unsafe { std::mem::zeroed() };
    // SAFETY: `set` is a valid writable signal set for both calls.
    action(unsafe { libc::sigemptyset(ptr::from_mut(&mut set)) })?;
    // SAFETY: `set` remains live and writable.
    action(unsafe { libc::sigaddset(ptr::from_mut(&mut set), libc::SIGPIPE) })?;
    // SAFETY: only this kernel thread's mask is changed; a child gets a
    // cleared mask through its spawn attributes below.
    action(unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, ptr::from_ref(&set), ptr::null_mut()) })
}

/// Blocks SIGINT and SIGTERM on the service thread and opens the signalfd
/// that io adopts (shell.md, section 6; io.md, section 7). The caller owns
/// the returned descriptor until io adopts it. Call before opening the ring
/// and before starting other threads.
pub(super) fn open_termination_signals() -> Result<Fd, i32> {
    // SAFETY: sigemptyset initializes this plain C signal set.
    let mut set: libc::sigset_t = unsafe { std::mem::zeroed() };
    // SAFETY: each call writes only within the live signal set.
    if unsafe { libc::sigemptyset(ptr::from_mut(&mut set)) } != 0 {
        return Err(super::last_errno());
    }
    for signal in [libc::SIGINT, libc::SIGTERM] {
        // SAFETY: set is initialized and writable.
        if unsafe { libc::sigaddset(ptr::from_mut(&mut set), signal) } != 0 {
            return Err(super::last_errno());
        }
    }
    // SAFETY: sigset_t is a plain C signal set, initialized below by pthread_sigmask.
    let mut old: libc::sigset_t = unsafe { std::mem::zeroed() };
    // SAFETY: pthread_sigmask writes the prior mask into old and changes
    // only this thread's mask. The child spawn path clears its mask.
    action(unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, ptr::from_ref(&set), ptr::from_mut(&mut old)) })?;
    // SAFETY: signalfd reads the initialized mask and returns a new owned fd.
    let raw = unsafe { libc::signalfd(-1, ptr::from_ref(&set), libc::SFD_CLOEXEC) };
    if raw < 0 {
        let errno = super::last_errno();
        // SAFETY: restore the prior mask after failing to create a source.
        let _restored = unsafe { libc::pthread_sigmask(libc::SIG_SETMASK, ptr::from_ref(&old), ptr::null_mut()) };
        return Err(errno);
    }
    Ok(Fd::new(raw))
}

fn directory(command: &Spawn) -> Result<OwnedFd, i32> {
    let name = if command.dir.is_empty() { b".".as_slice() } else { command.dir.as_ref() };
    let path = cstring(name)?;
    // SAFETY: open_how is a plain Linux integer structure; zero is valid.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = u64::try_from(libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC).expect("positive flags");
    how.resolve = libc::RESOLVE_BENEATH | libc::RESOLVE_NO_MAGICLINKS;
    // SAFETY: the path and open_how are valid for the duration of the syscall.
    let opened = unsafe {
        libc::syscall(libc::SYS_openat2, command.root.raw(), path.as_ptr(), &how, size_of::<libc::open_how>())
    };
    fd(i32::try_from(opened).unwrap_or(-1))
}

use std::mem::size_of;

type PreparedPipes = (OwnedFd, Vec<OwnedFd>, Vec<OwnedFd>, Vec<Option<OwnedFd>>);

fn prepare(command: &Spawn) -> Result<PreparedPipes, i32> {
    let dir = directory(command)?;
    let maximum = command.pipes.iter().map(|pipe| pipe.child).max().unwrap_or(2).max(2);
    let start = i32::try_from(maximum).map_err(|_| libc::EINVAL)?.checked_add(1).ok_or(libc::EINVAL)?;
    let mut parents = Vec::with_capacity(command.pipes.len());
    let mut children = Vec::with_capacity(command.pipes.len());
    for pipe in &command.pipes {
        let mut pair = [0_i32; 2];
        // SAFETY: `pair` has two writable descriptor slots.
        if unsafe { libc::pipe2(pair.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
            return Err(super::last_errno());
        }
        // SAFETY: `pipe2` made both ends, now owned exactly once.
        let read = unsafe { OwnedFd::from_raw_fd(pair[0]) };
        // SAFETY: pipe2 made the second descriptor separately.
        let write = unsafe { OwnedFd::from_raw_fd(pair[1]) };
        let (parent, child) = match pipe.way {
            Way::In => (write, read),
            Way::Out => (read, write),
        };
        // Move the source above every child target: ordered dup2 actions can
        // then never overwrite the source of a later action.
        // SAFETY: `child` is open, and fcntl duplicates it with CLOEXEC.
        let high = fd(unsafe { libc::fcntl(child.as_raw_fd(), libc::F_DUPFD_CLOEXEC, start) })?;
        parents.push(parent);
        children.push(high);
    }
    let mut nulls = Vec::with_capacity(3);
    for standard in 0..3_u32 {
        if command.pipes.iter().any(|pipe| pipe.child == standard) {
            nulls.push(None);
            continue;
        }
        let flags = if standard == 0 { libc::O_RDONLY } else { libc::O_WRONLY };
        let name = c"/dev/null";
        // SAFETY: static NUL-terminated path.
        let null = fd(unsafe { libc::open(name.as_ptr(), flags | libc::O_CLOEXEC) })?;
        // Keep every source above every child target, including /dev/null.
        // SAFETY: `null` is open and fcntl makes a separate CLOEXEC source.
        let high = fd(unsafe { libc::fcntl(null.as_raw_fd(), libc::F_DUPFD_CLOEXEC, start) })?;
        nulls.push(Some(high));
    }
    Ok((dir, parents, children, nulls))
}

pub(super) fn spawn(command: &mut Spawn) -> Result<Done, Error> {
    spawn_inner(command).map_err(map_error)
}

fn spawn_inner(command: &mut Spawn) -> Result<Done, i32> {
    let path = cstring(&command.program)?;
    let (dir, parents, children, nulls) = prepare(command)?;
    let argv = std::iter::once(&command.program)
        .chain(command.args.iter())
        .map(|v| cstring(v))
        .collect::<Result<Vec<_>, _>>()?;
    let env = command.env.iter().map(|v| cstring(v)).collect::<Result<Vec<_>, _>>()?;
    let mut argv_ptrs: Vec<*mut libc::c_char> = argv.iter().map(|v| v.as_ptr().cast_mut()).collect();
    argv_ptrs.push(ptr::null_mut());
    let mut env_ptrs: Vec<*mut libc::c_char> = env.iter().map(|v| v.as_ptr().cast_mut()).collect();
    env_ptrs.push(ptr::null_mut());
    let mut actions = MaybeUninit::<libc::posix_spawn_file_actions_t>::uninit();
    // SAFETY: initialization writes the action structure and returns 0 when valid.
    action(unsafe { libc::posix_spawn_file_actions_init(actions.as_mut_ptr()) })?;
    // SAFETY: initialized above; destroy after all additions and spawn.
    let mut actions = unsafe { actions.assume_init() };
    let result = (|| {
        // SAFETY: the actions pointer stays live through pidfd_spawn.
        action(unsafe { libc::posix_spawn_file_actions_addfchdir_np(ptr::from_mut(&mut actions), dir.as_raw_fd()) })?;
        for (pipe, child) in command.pipes.iter().zip(&children) {
            let to = i32::try_from(pipe.child).map_err(|_| libc::EINVAL)?;
            // SAFETY: both descriptors are valid, and actions is initialized.
            action(unsafe {
                libc::posix_spawn_file_actions_adddup2(ptr::from_mut(&mut actions), child.as_raw_fd(), to)
            })?;
        }
        for (to, maybe_null) in nulls.iter().enumerate() {
            if let Some(null) = maybe_null {
                // SAFETY: the /dev/null descriptor is valid.
                action(unsafe {
                    libc::posix_spawn_file_actions_adddup2(
                        ptr::from_mut(&mut actions),
                        null.as_raw_fd(),
                        i32::try_from(to).expect("0..3"),
                    )
                })?;
            }
        }
        let after = command.pipes.iter().map(|pipe| pipe.child).max().unwrap_or(2).max(2);
        let from = i32::try_from(after).map_err(|_| libc::EINVAL)?.checked_add(1).ok_or(libc::EINVAL)?;
        // SAFETY: close all inherited descriptors above the requested child
        // targets, including the temporary pipe sources and cwd descriptor.
        action(unsafe { libc::posix_spawn_file_actions_addclosefrom_np(ptr::from_mut(&mut actions), from) })?;
        let mut attributes = MaybeUninit::<libc::posix_spawnattr_t>::uninit();
        // SAFETY: init writes the structure, which is used only on success.
        action(unsafe { libc::posix_spawnattr_init(attributes.as_mut_ptr()) })?;
        // SAFETY: initialized above; destroyed after the spawn attempt.
        let mut attributes = unsafe { attributes.assume_init() };
        let spawned = (|| {
            // SAFETY: sigemptyset initializes this plain C signal set.
            let mut empty: libc::sigset_t = unsafe { std::mem::zeroed() };
            // SAFETY: empty is writable and lives through the attribute copy.
            action(unsafe { libc::sigemptyset(ptr::from_mut(&mut empty)) })?;
            // SAFETY: the initialized attributes receive a copy of the mask.
            action(unsafe { libc::posix_spawnattr_setsigmask(ptr::from_mut(&mut attributes), ptr::from_ref(&empty)) })?;
            // SAFETY: the initialized attributes accept the flag.
            action(unsafe {
                libc::posix_spawnattr_setflags(
                    ptr::from_mut(&mut attributes),
                    i16::try_from(libc::POSIX_SPAWN_SETSIGMASK).expect("spawn flag fits i16"),
                )
            })?;
            let mut pidfd = -1_i32;
            // SAFETY: all arrays are NUL-terminated and stable for the call;
            // the initialized actions and attributes live through it.
            let rc = unsafe {
                pidfd_spawn(
                    ptr::from_mut(&mut pidfd),
                    path.as_ptr(),
                    ptr::from_ref(&actions),
                    ptr::from_ref(&attributes),
                    argv_ptrs.as_ptr(),
                    env_ptrs.as_ptr(),
                )
            };
            action(rc)?;
            fd(pidfd)
        })();
        // SAFETY: no spawn call can still read the attributes.
        let _destroyed = unsafe { libc::posix_spawnattr_destroy(ptr::from_mut(&mut attributes)) };
        let pidfd = spawned?;
        for (pipe, parent) in command.pipes.iter_mut().zip(parents) {
            pipe.parent = Some(Fd::new(parent.into_raw_fd()));
        }
        Ok(Done::Spawned { pidfd: Fd::new(pidfd.into_raw_fd()) })
    })();
    // SAFETY: the actions object was initialized and is no longer used.
    let _destroyed = unsafe { libc::posix_spawn_file_actions_destroy(ptr::from_mut(&mut actions)) };
    result
}

pub(super) fn signal_child(pidfd: Fd, signal: Signal) -> Result<Done, Error> {
    let number = match signal {
        Signal::Terminate => libc::SIGTERM,
        Signal::Kill => libc::SIGKILL,
    };
    // SAFETY: pidfd_send_signal takes no borrowed memory when siginfo is null.
    let sent =
        unsafe { libc::syscall(libc::SYS_pidfd_send_signal, pidfd.raw(), number, ptr::null::<libc::siginfo_t>(), 0) };
    if sent == 0 { Ok(Done::Nothing) } else { Err(map_error(super::last_errno())) }
}

fn map_error(errno: i32) -> Error {
    match errno {
        libc::ENOENT => Error::NotFound,
        libc::EACCES | libc::EPERM => Error::Permission,
        libc::ENOTDIR => Error::NotADirectory,
        libc::ELOOP => Error::TooManyLinks,
        libc::ENAMETOOLONG => Error::NameTooLong,
        libc::EXDEV => Error::Escape,
        libc::EMFILE | libc::ENFILE => Error::TooManyOpenFiles,
        libc::ENOMEM | libc::ENOBUFS => Error::NoBufferSpace,
        libc::EINVAL => Error::InvalidArgument,
        other => Error::Other(other),
    }
}

/// Descriptors made for a service hosted by a real-world harness.
#[derive(Debug)]
pub struct HostedPipes {
    pub pidfd: Fd,
    pub pipes: Vec<(u32, Fd)>,
    pub signal: Fd,
    pub signal_writer: Fd,
}

/// Makes the same parent/child pipe pairs as spawn, without executing a program.
pub(super) fn hosted_pipes(command: &mut Spawn) -> Result<HostedPipes, Error> {
    hosted_pipes_inner(command).map_err(map_error)
}

fn hosted_pipes_inner(command: &mut Spawn) -> Result<HostedPipes, i32> {
    let (_dir, parents, children, _nulls) = prepare(command)?;
    let (signal, signal_writer) = signal_pipe_inner()?;
    // SAFETY: eventfd makes a separate CLOEXEC descriptor used only as an
    // identity placeholder; the harness intercepts wait, signal and close.
    let placeholder = fd(unsafe { libc::eventfd(0, libc::EFD_CLOEXEC) })?;
    let pipes =
        command.pipes.iter().zip(children).map(|(pipe, child)| (pipe.child, Fd::new(child.into_raw_fd()))).collect();
    for (pipe, parent) in command.pipes.iter_mut().zip(parents) {
        pipe.parent = Some(Fd::new(parent.into_raw_fd()));
    }
    Ok(HostedPipes {
        pidfd: Fd::new(placeholder.into_raw_fd()),
        pipes,
        signal: Fd::new(signal.into_raw_fd()),
        signal_writer: Fd::new(signal_writer.into_raw_fd()),
    })
}

fn signal_pipe_inner() -> Result<(OwnedFd, OwnedFd), i32> {
    let mut pair = [0_i32; 2];
    // SAFETY: pair has two writable descriptor slots.
    if unsafe { libc::pipe2(pair.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(super::last_errno());
    }
    // SAFETY: pipe2 returned two separately owned descriptors.
    let read = unsafe { OwnedFd::from_raw_fd(pair[0]) };
    // SAFETY: pipe2 made this separate writing descriptor.
    let write = unsafe { OwnedFd::from_raw_fd(pair[1]) };
    // Keep a referee sending repeated signals from blocking this one loop.
    // SAFETY: write is open, and these are integer status flags.
    action(if unsafe { libc::fcntl(write.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK) } < 0 {
        super::last_errno()
    } else {
        0
    })?;
    Ok((read, write))
}

/// A harness signal uses the same record that a signalfd read returns.
pub(super) fn write_signal(writer: Fd, signal: skein_io::kernel::ServiceSignal) -> Result<(), Error> {
    // SAFETY: signalfd_siginfo is a plain C integer record; zero is valid.
    let mut record: libc::signalfd_siginfo = unsafe { std::mem::zeroed() };
    record.ssi_signo = u32::try_from(match signal {
        skein_io::kernel::ServiceSignal::Interrupt => libc::SIGINT,
        skein_io::kernel::ServiceSignal::Terminate => libc::SIGTERM,
    })
    .expect("positive signal");
    // SAFETY: the pointer names one initialized record, alive for this call.
    // Its size is below PIPE_BUF, so a successful write is atomic and whole.
    let wrote =
        unsafe { libc::write(writer.raw(), ptr::from_ref(&record).cast(), size_of::<libc::signalfd_siginfo>()) };
    if wrote < 0 { Err(map_error(super::last_errno())) } else { Ok(()) }
}

/// Makes a signal-record pipe for a second service in the same real loop.
pub(super) fn signal_pipe() -> Result<(Fd, Fd), Error> {
    let (reader, writer) = signal_pipe_inner().map_err(map_error)?;
    Ok((Fd::new(reader.into_raw_fd()), Fd::new(writer.into_raw_fd())))
}

/// Queues a termination signal for this thread's already-opened signalfd.
pub(super) fn signal_current_thread(signal: skein_io::kernel::ServiceSignal) -> Result<(), Error> {
    let number = match signal {
        skein_io::kernel::ServiceSignal::Interrupt => libc::SIGINT,
        skein_io::kernel::ServiceSignal::Terminate => libc::SIGTERM,
    };
    // SAFETY: pthread_self returns the live calling thread's identifier.
    let thread = unsafe { libc::pthread_self() };
    // SAFETY: the caller opened its termination signalfd before sending,
    // so this signal is blocked on the live thread.
    let result = unsafe { libc::pthread_kill(thread, number) };
    action(result).map_err(map_error)
}
