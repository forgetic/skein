//! Process-global Unix signal registry.
//!
//! Implements the classic self-pipe pattern:
//!
//! 1. [`forward_signal`] is installed (via `sigaction`) as the handler for
//!    every signal that has at least one listener. It does the only things
//!    that are async-signal-safe: load an atomic and `write(2)` the signal
//!    number into a non-blocking pipe.
//! 2. A detached dispatcher thread (`skein-signal`) blocks on the read end
//!    of that pipe. For every byte it receives, it marks all registered
//!    listeners of that signal as pending and wakes their tasks.
//! 3. [`ListenerState::poll_recv`] is plain waker-based polling — it needs
//!    no reactor or runtime context, so signal streams work under any
//!    executor, including a bare `block_on`.
//!
//! # Semantics
//!
//! - Signal deliveries **coalesce**: N deliveries between two polls wake the
//!   listener once, matching standard Unix signal semantics.
//! - Handlers are installed once per signal number and stay installed for
//!   the lifetime of the process, even after the last listener is dropped
//!   (later deliveries are dispatched to nobody). In particular, the default
//!   action remains suppressed — e.g. after listening for SIGINT once,
//!   Ctrl+C no longer kills the process by default.
//! - If the pipe is full (the dispatcher is severely backlogged), the
//!   handler's write fails with `EAGAIN` and that delivery is dropped. This
//!   is benign for the same reason coalescing is: pending listeners are
//!   woken by the deliveries that did fit.

// The crate denies unsafe code by default; this module needs three narrow
// exemptions, all in support of the signal handler: installing it with
// `sigaction`, the `write(2)` call inside it, and saving/restoring `errno`
// around that write. Each site carries its own SAFETY comment.
#![allow(unsafe_code)]

use nix::sys::signal::{SaFlags, SigAction, SigHandler, SigSet, Signal as NixSignal, sigaction};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::io;
use std::os::fd::{IntoRawFd, OwnedFd};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::task::{Context, Poll, Waker};

use super::SignalKind;

/// Write end of the self-pipe. `-1` until the registry is initialized.
///
/// Read by [`forward_signal`] in signal-handler context; written exactly once
/// during registry initialization, before any handler is installed.
static WRITE_FD: AtomicI32 = AtomicI32::new(-1);

/// Returns a pointer to the calling thread's `errno`.
///
/// Used only by [`forward_signal`] to save and restore `errno` around its
/// `write(2)` call, as required of well-behaved signal handlers.
unsafe fn errno_location() -> *mut libc::c_int {
    // SAFETY: each of these libc accessors returns a valid pointer to the
    // calling thread's errno and is async-signal-safe.
    unsafe {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            libc::__errno_location()
        }
        #[cfg(any(target_os = "macos", target_os = "ios", target_os = "freebsd"))]
        {
            libc::__error()
        }
        #[cfg(any(target_os = "openbsd", target_os = "netbsd"))]
        {
            libc::__errno()
        }
    }
}

/// The signal handler: forwards the signal number through the self-pipe.
///
/// Async-signal-safety: loads one atomic, calls `write(2)`, and saves and
/// restores `errno`. It must never allocate, lock, panic, or touch any other
/// shared state.
extern "C" fn forward_signal(signum: libc::c_int) {
    let fd = WRITE_FD.load(Ordering::Acquire);
    if fd < 0 {
        return;
    }
    // SAFETY: reading/writing the thread-local errno and calling write(2)
    // (async-signal-safe per POSIX) on a pipe fd that is kept open for the
    // lifetime of the process. The buffer is a valid 1-byte stack slot.
    unsafe {
        let errno = errno_location();
        let saved = *errno;
        let byte = [signum as u8];
        let _ = libc::write(fd, byte.as_ptr().cast(), 1);
        *errno = saved;
    }
}

/// Per-listener notification state shared between a `Signal` stream and the
/// dispatcher thread.
#[derive(Debug, Default)]
pub(crate) struct ListenerState {
    /// Set by the dispatcher when a signal arrives; cleared by `poll_recv`.
    pending: AtomicBool,
    /// Waker of the task currently blocked in `poll_recv`, if any.
    waker: Mutex<Option<Waker>>,
}

impl ListenerState {
    /// Polls for a signal notification. Deliveries coalesce: any number of
    /// deliveries since the last poll yields one `Ready`.
    pub(crate) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<()>> {
        if self.pending.swap(false, Ordering::AcqRel) {
            return Poll::Ready(Some(()));
        }
        {
            let mut slot = self.waker.lock();
            if !slot.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
                *slot = Some(cx.waker().clone());
            }
        }
        // Re-check after publishing the waker: a dispatch that ran between
        // the first check and the store would otherwise be lost.
        if self.pending.swap(false, Ordering::AcqRel) {
            Poll::Ready(Some(()))
        } else {
            Poll::Pending
        }
    }
}

/// Listener table plus the set of signals whose handler is installed.
struct Registry {
    listeners: Mutex<HashMap<i32, Vec<Weak<ListenerState>>>>,
    installed: Mutex<HashSet<i32>>,
}

/// Initialized once on first use; `Err` if the self-pipe could not be set up.
static REGISTRY: OnceLock<Result<&'static Registry, String>> = OnceLock::new();

impl Registry {
    fn global() -> io::Result<&'static Self> {
        REGISTRY
            .get_or_init(Self::init)
            .as_ref()
            .copied()
            .map_err(|message| io::Error::other(message.clone()))
    }

    fn init() -> Result<&'static Self, String> {
        let (read_end, write_end) =
            nix::unistd::pipe().map_err(|e| format!("signal pipe creation failed: {e}"))?;

        configure_pipe_ends(&read_end, &write_end)
            .map_err(|e| format!("signal pipe configuration failed: {e}"))?;

        let registry: &'static Self = Box::leak(Box::new(Self {
            listeners: Mutex::new(HashMap::new()),
            installed: Mutex::new(HashSet::new()),
        }));

        // Publish the write end for the handler before any handler can be
        // installed. The fd is intentionally leaked: it must stay valid for
        // as long as signal handlers may run, i.e. forever.
        WRITE_FD.store(write_end.into_raw_fd(), Ordering::Release);

        std::thread::Builder::new()
            .name("skein-signal".to_string())
            .spawn(move || registry.dispatch_loop(&read_end))
            .map_err(|e| format!("signal dispatcher thread spawn failed: {e}"))?;

        Ok(registry)
    }

    /// Blocks on the pipe's read end and fans each signal number out to its
    /// registered listeners.
    fn dispatch_loop(&'static self, read_end: &OwnedFd) {
        let mut buf = [0u8; 64];
        loop {
            match nix::unistd::read(read_end, &mut buf) {
                Ok(0) => return, // write end closed: cannot happen, but exit cleanly
                Ok(n) => {
                    for &byte in &buf[..n] {
                        self.dispatch(i32::from(byte));
                    }
                }
                Err(nix::errno::Errno::EINTR) => {}
                Err(_) => return,
            }
        }
    }

    /// Marks all live listeners of `signum` pending and wakes their tasks.
    fn dispatch(&self, signum: i32) {
        let mut wakers = Vec::new();
        {
            let mut listeners = self.listeners.lock();
            if let Some(list) = listeners.get_mut(&signum) {
                list.retain(|weak| {
                    let Some(state) = weak.upgrade() else {
                        return false;
                    };
                    // pending is set before the waker is taken so that a
                    // concurrent poll_recv either sees pending or publishes
                    // a waker we are about to take — never neither.
                    state.pending.store(true, Ordering::Release);
                    if let Some(waker) = state.waker.lock().take() {
                        wakers.push(waker);
                    }
                    true
                });
            }
        }
        // Wake outside the registry lock: wakers can run arbitrary code.
        for waker in wakers {
            waker.wake();
        }
    }

    /// Installs [`forward_signal`] for `signum` if not yet installed.
    fn install_handler(&self, kind: SignalKind) -> io::Result<()> {
        let signum = kind.as_raw_value();
        let mut installed = self.installed.lock();
        if installed.contains(&signum) {
            return Ok(());
        }

        let nix_signal = NixSignal::try_from(signum)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        let action = SigAction::new(
            SigHandler::Handler(forward_signal),
            SaFlags::SA_RESTART,
            SigSet::empty(),
        );
        // SAFETY: forward_signal is async-signal-safe (see its docs), and the
        // self-pipe it writes to has already been published via WRITE_FD.
        unsafe { sigaction(nix_signal, &action) }
            .map_err(|e| io::Error::from_raw_os_error(e as i32))?;

        installed.insert(signum);
        Ok(())
    }
}

/// Makes the write end non-blocking (the handler must never block) and marks
/// both ends close-on-exec so children don't inherit the pipe.
fn configure_pipe_ends(read_end: &OwnedFd, write_end: &OwnedFd) -> nix::Result<()> {
    use nix::fcntl::{FcntlArg, FdFlag, OFlag, fcntl};
    for fd in [read_end, write_end] {
        fcntl(fd, FcntlArg::F_SETFD(FdFlag::FD_CLOEXEC))?;
    }
    let flags = fcntl(write_end, FcntlArg::F_GETFL)?;
    let flags = OFlag::from_bits_truncate(flags) | OFlag::O_NONBLOCK;
    fcntl(write_end, FcntlArg::F_SETFL(flags))?;
    Ok(())
}

/// Registers a new listener for `kind`, installing the process-wide handler
/// on first use.
pub(crate) fn register(kind: SignalKind) -> io::Result<Arc<ListenerState>> {
    let registry = Registry::global()?;
    registry.install_handler(kind)?;

    let state = Arc::new(ListenerState::default());
    let mut listeners = registry.listeners.lock();
    let list = listeners.entry(kind.as_raw_value()).or_default();
    // Opportunistically drop entries whose Signal was dropped so the list
    // doesn't grow without bound under repeated register/drop cycles.
    list.retain(|weak| weak.strong_count() > 0);
    list.push(Arc::downgrade(&state));
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_lite::future;
    use nix::sys::signal::raise;
    use std::task::{Context, Wake};

    fn init_test(name: &str) {
        crate::test_utils::init_test_logging();
        crate::test_phase!(name);
    }

    struct NoopWaker;

    impl Wake for NoopWaker {
        fn wake(self: Arc<Self>) {}
    }

    async fn recv(state: &ListenerState) {
        std::future::poll_fn(|cx| state.poll_recv(cx).map(|_| ())).await;
    }

    // Each test uses a distinct signal so concurrently running tests cannot
    // observe each other's deliveries.

    #[test]
    fn listener_receives_raised_signal() {
        init_test("listener_receives_raised_signal");
        let state = register(SignalKind::user_defined1()).expect("register SIGUSR1");
        raise(NixSignal::SIGUSR1).expect("raise");
        future::block_on(recv(&state));
        crate::test_complete!("listener_receives_raised_signal");
    }

    #[test]
    fn all_listeners_of_a_signal_are_notified() {
        init_test("all_listeners_of_a_signal_are_notified");
        let first = register(SignalKind::user_defined2()).expect("register SIGUSR2");
        let second = register(SignalKind::user_defined2()).expect("register SIGUSR2");
        raise(NixSignal::SIGUSR2).expect("raise");
        future::block_on(recv(&first));
        future::block_on(recv(&second));
        crate::test_complete!("all_listeners_of_a_signal_are_notified");
    }

    #[test]
    fn deliveries_coalesce() {
        init_test("deliveries_coalesce");
        let state = register(SignalKind::hangup()).expect("register SIGHUP");
        raise(NixSignal::SIGHUP).expect("raise");
        raise(NixSignal::SIGHUP).expect("raise");
        future::block_on(recv(&state));

        // Both deliveries were consumed by the single recv above; the stream
        // must now be idle.
        let waker = Waker::from(Arc::new(NoopWaker));
        let mut cx = Context::from_waker(&waker);
        let idle = state.poll_recv(&mut cx).is_pending();
        crate::assert_with_log!(idle, "stream idle after coalesced recv", true, idle);
        crate::test_complete!("deliveries_coalesce");
    }

    #[test]
    fn dropped_listeners_are_pruned() {
        init_test("dropped_listeners_are_pruned");
        let kind = SignalKind::window_change();
        drop(register(kind).expect("register SIGWINCH"));
        drop(register(kind).expect("register SIGWINCH"));
        let _live = register(kind).expect("register SIGWINCH");

        let registry = Registry::global().expect("registry");
        let listeners = registry.listeners.lock();
        let live = listeners
            .get(&kind.as_raw_value())
            .map_or(0, |list| list.iter().filter(|w| w.strong_count() > 0).count());
        crate::assert_with_log!(live == 1, "one live listener", 1, live);
        crate::test_complete!("dropped_listeners_are_pruned");
    }
}
