//! Async signal stream for Unix signals.
//!
//! On Unix, signal streams are backed by the process-global self-pipe
//! registry in [`super::registry`]: a minimal async-signal-safe handler
//! forwards each delivery through a pipe to a dispatcher thread, which wakes
//! every registered stream. Polling is purely waker-based, so streams work
//! under any executor — no reactor or runtime context is required.
//!
//! On non-Unix platforms, [`signal`] returns an error.
//!
//! # Cancel Safety
//!
//! - `Signal::recv`: Cancel-safe, can be cancelled at any await point.

use std::io;
use std::task::{Context, Poll};

use super::SignalKind;

/// Error returned when signal handling is not available.
#[derive(Debug, Clone)]
pub struct SignalError {
    kind: SignalKind,
    message: &'static str,
}

impl SignalError {
    fn unsupported(kind: SignalKind) -> Self {
        Self {
            kind,
            message: "Signal handling is not supported on this platform",
        }
    }
}

impl std::fmt::Display for SignalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} ({})", self.message, self.kind.name(), self.kind)
    }
}

impl std::error::Error for SignalError {}

impl From<SignalError> for io::Error {
    fn from(e: SignalError) -> Self {
        Self::new(io::ErrorKind::Unsupported, e)
    }
}

/// An async stream that receives signals of a particular kind.
///
/// Deliveries coalesce, matching Unix semantics: any number of deliveries
/// between two `recv` calls completes the next `recv` exactly once.
///
/// Dropping the stream stops its notifications, but the process-wide signal
/// handler stays installed — the signal's default action remains suppressed
/// for the lifetime of the process.
///
/// # Example
///
/// ```ignore
/// use skein::signal::{signal, SignalKind};
///
/// async fn handle_signals() -> std::io::Result<()> {
///     let mut sigterm = signal(SignalKind::terminate())?;
///
///     loop {
///         sigterm.recv().await;
///         println!("Received SIGTERM");
///         break;
///     }
///     Ok(())
/// }
/// ```
#[derive(Debug)]
pub struct Signal {
    kind: SignalKind,
    #[cfg(unix)]
    state: std::sync::Arc<super::registry::ListenerState>,
}

impl Signal {
    /// Creates a new signal stream for the given signal kind.
    ///
    /// # Errors
    ///
    /// Returns an error if signal handling is not available on this platform
    /// or the handler could not be installed.
    #[cfg(unix)]
    fn new(kind: SignalKind) -> io::Result<Self> {
        let state = super::registry::register(kind)?;
        Ok(Self { kind, state })
    }

    /// Creates a new signal stream for the given signal kind.
    ///
    /// # Errors
    ///
    /// Always errors: signal handling is only supported on Unix.
    #[cfg(not(unix))]
    fn new(kind: SignalKind) -> io::Result<Self> {
        Err(SignalError::unsupported(kind).into())
    }

    /// Receives the next signal notification.
    ///
    /// Returns `None` if the signal stream has been closed. (The current
    /// Unix implementation never closes the stream, so this returns
    /// `Some(())` for every delivery.)
    ///
    /// # Cancel Safety
    ///
    /// This method is cancel-safe. If you use it as the event in a `select!`
    /// statement and some other branch completes first, no signal notification
    /// is lost.
    pub async fn recv(&mut self) -> Option<()> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }

    /// Polls for the next signal notification.
    ///
    /// Like [`recv`](Self::recv), but usable from manual `Future`
    /// implementations.
    pub fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<()>> {
        #[cfg(unix)]
        {
            self.state.poll_recv(cx)
        }
        #[cfg(not(unix))]
        {
            let _ = cx;
            Poll::Pending
        }
    }

    /// Returns the signal kind this stream is listening for.
    #[must_use]
    pub fn kind(&self) -> SignalKind {
        self.kind
    }
}

/// Creates a new stream that receives signals of the given kind.
///
/// # Errors
///
/// Returns an error if signal handling is not available.
///
/// # Example
///
/// ```ignore
/// use skein::signal::{signal, SignalKind};
///
/// let mut sigterm = signal(SignalKind::terminate())?;
/// sigterm.recv().await;
/// ```
pub fn signal(kind: SignalKind) -> io::Result<Signal> {
    Signal::new(kind)
}

/// Creates a stream for SIGINT (Ctrl+C on Unix).
///
/// # Errors
///
/// Returns an error if signal handling is not available.
#[cfg(unix)]
pub fn sigint() -> io::Result<Signal> {
    signal(SignalKind::interrupt())
}

/// Creates a stream for SIGTERM.
///
/// # Errors
///
/// Returns an error if signal handling is not available.
#[cfg(unix)]
pub fn sigterm() -> io::Result<Signal> {
    signal(SignalKind::terminate())
}

/// Creates a stream for SIGHUP.
///
/// # Errors
///
/// Returns an error if signal handling is not available.
#[cfg(unix)]
pub fn sighup() -> io::Result<Signal> {
    signal(SignalKind::hangup())
}

/// Creates a stream for SIGUSR1.
///
/// # Errors
///
/// Returns an error if signal handling is not available.
#[cfg(unix)]
pub fn sigusr1() -> io::Result<Signal> {
    signal(SignalKind::user_defined1())
}

/// Creates a stream for SIGUSR2.
///
/// # Errors
///
/// Returns an error if signal handling is not available.
#[cfg(unix)]
pub fn sigusr2() -> io::Result<Signal> {
    signal(SignalKind::user_defined2())
}

/// Creates a stream for SIGQUIT.
///
/// # Errors
///
/// Returns an error if signal handling is not available.
#[cfg(unix)]
pub fn sigquit() -> io::Result<Signal> {
    signal(SignalKind::quit())
}

/// Creates a stream for SIGCHLD.
///
/// # Errors
///
/// Returns an error if signal handling is not available.
#[cfg(unix)]
pub fn sigchld() -> io::Result<Signal> {
    signal(SignalKind::child())
}

/// Creates a stream for SIGWINCH.
///
/// # Errors
///
/// Returns an error if signal handling is not available.
#[cfg(unix)]
pub fn sigwinch() -> io::Result<Signal> {
    signal(SignalKind::window_change())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_test(name: &str) {
        crate::test_utils::init_test_logging();
        crate::test_phase!(name);
    }

    #[test]
    fn signal_error_display() {
        init_test("signal_error_display");
        let err = SignalError::unsupported(SignalKind::Terminate);
        let msg = format!("{err}");
        let has_sigterm = msg.contains("SIGTERM");
        crate::assert_with_log!(has_sigterm, "contains SIGTERM", true, has_sigterm);
        let has_reason = msg.contains("not supported");
        crate::assert_with_log!(has_reason, "contains reason", true, has_reason);
        crate::test_complete!("signal_error_display");
    }

    #[cfg(unix)]
    #[test]
    fn signal_creates_stream() {
        init_test("signal_creates_stream");
        let stream = signal(SignalKind::terminate());
        let is_ok = stream.is_ok();
        crate::assert_with_log!(is_ok, "stream created", true, is_ok);
        let kind = stream.expect("stream").kind();
        crate::assert_with_log!(
            kind == SignalKind::Terminate,
            "kind",
            SignalKind::Terminate,
            kind
        );
        crate::test_complete!("signal_creates_stream");
    }

    #[cfg(not(unix))]
    #[test]
    fn signal_unsupported_off_unix() {
        init_test("signal_unsupported_off_unix");
        let err = signal(SignalKind::terminate()).expect_err("must error off unix");
        crate::assert_with_log!(
            err.kind() == io::ErrorKind::Unsupported,
            "err kind",
            io::ErrorKind::Unsupported,
            err.kind()
        );
        crate::test_complete!("signal_unsupported_off_unix");
    }

    #[cfg(unix)]
    #[test]
    fn unix_signal_helpers() {
        init_test("unix_signal_helpers");
        let cases = [
            (sigint().expect("sigint"), SignalKind::Interrupt),
            (sigterm().expect("sigterm"), SignalKind::Terminate),
            (sighup().expect("sighup"), SignalKind::Hangup),
            (sigusr1().expect("sigusr1"), SignalKind::User1),
            (sigusr2().expect("sigusr2"), SignalKind::User2),
            (sigquit().expect("sigquit"), SignalKind::Quit),
            (sigchld().expect("sigchld"), SignalKind::Child),
            (sigwinch().expect("sigwinch"), SignalKind::WindowChange),
        ];
        for (stream, expected) in cases {
            let kind = stream.kind();
            crate::assert_with_log!(kind == expected, "helper kind", expected, kind);
        }
        crate::test_complete!("unix_signal_helpers");
    }

    #[cfg(unix)]
    #[test]
    fn signal_stream_receives_delivery() {
        init_test("signal_stream_receives_delivery");
        // SIGALRM is used by this test only (registry tests use other kinds),
        // so concurrently running tests cannot interfere.
        let mut stream = signal(SignalKind::alarm()).expect("stream");
        nix::sys::signal::raise(nix::sys::signal::Signal::SIGALRM).expect("raise");
        let received = futures_lite::future::block_on(stream.recv());
        crate::assert_with_log!(received.is_some(), "received", true, received.is_some());
        crate::test_complete!("signal_stream_receives_delivery");
    }
}
