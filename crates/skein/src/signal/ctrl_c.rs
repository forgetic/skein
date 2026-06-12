//! Cross-platform Ctrl+C handling.
//!
//! Provides a simple async function to wait for Ctrl+C (SIGINT on Unix).
//! On Unix this is backed by the signal stream infrastructure in
//! [`super::signal`]; on other platforms it returns an error.

use std::io;

/// Error returned when Ctrl+C handling is not available.
#[derive(Debug, Clone)]
pub struct CtrlCError {
    message: &'static str,
}

impl CtrlCError {
    const fn unsupported() -> Self {
        Self {
            message: "Ctrl+C handling is not supported on this platform",
        }
    }
}

impl std::fmt::Display for CtrlCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CtrlCError {}

impl From<CtrlCError> for io::Error {
    fn from(e: CtrlCError) -> Self {
        Self::new(io::ErrorKind::Unsupported, e)
    }
}

/// Waits for Ctrl+C (SIGINT on Unix).
///
/// This is the cross-platform way to handle graceful shutdown triggered
/// by the user pressing Ctrl+C in the terminal.
///
/// Note that the SIGINT handler installed by the first call stays installed
/// for the lifetime of the process: after calling this, Ctrl+C no longer
/// terminates the process by default — it completes this future instead.
///
/// # Errors
///
/// Returns an error if Ctrl+C handling is not available on this platform
/// or if the handler could not be registered.
///
/// # Cancel Safety
///
/// This function is cancel-safe. If cancelled, no Ctrl+C event is lost.
///
/// # Example
///
/// ```ignore
/// use skein::signal::ctrl_c;
///
/// async fn run_server() -> std::io::Result<()> {
///     println!("Server starting. Press Ctrl+C to stop.");
///
///     // Run until Ctrl+C
///     ctrl_c().await?;
///
///     println!("Shutting down...");
///     Ok(())
/// }
/// ```
#[cfg(unix)]
pub async fn ctrl_c() -> io::Result<()> {
    let mut sigint = super::signal::signal(super::SignalKind::interrupt())?;
    sigint.recv().await;
    Ok(())
}

/// Waits for Ctrl+C.
///
/// # Errors
///
/// Always errors: Ctrl+C handling is only supported on Unix.
#[cfg(not(unix))]
pub async fn ctrl_c() -> io::Result<()> {
    Err(CtrlCError::unsupported().into())
}

/// Checks if Ctrl+C handling is available on this platform.
///
/// Returns `true` if `ctrl_c()` can successfully register a handler.
#[must_use]
pub fn is_available() -> bool {
    cfg!(unix)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_test(name: &str) {
        crate::test_utils::init_test_logging();
        crate::test_phase!(name);
    }

    #[test]
    fn ctrl_c_availability_matches_platform() {
        init_test("ctrl_c_availability_matches_platform");
        let available = is_available();
        crate::assert_with_log!(
            available == cfg!(unix),
            "availability",
            cfg!(unix),
            available
        );
        crate::test_complete!("ctrl_c_availability_matches_platform");
    }

    #[test]
    fn ctrl_c_error_display() {
        init_test("ctrl_c_error_display");
        let err = CtrlCError::unsupported();
        let msg = format!("{err}");
        let contains = msg.contains("not supported");
        crate::assert_with_log!(contains, "contains reason", true, contains);
        crate::test_complete!("ctrl_c_error_display");
    }

    /// Regression test for daemons that park on `ctrl_c().await`: the future
    /// must register a SIGINT handler and complete when SIGINT arrives,
    /// rather than erroring out immediately (which made callers fall through
    /// to their shutdown path during startup).
    #[cfg(unix)]
    #[test]
    fn ctrl_c_completes_on_sigint() {
        init_test("ctrl_c_completes_on_sigint");

        // Pin the SIGINT disposition to our handler before any raise can
        // happen, so the default action (process termination) is never
        // reachable in this test.
        let _pin = super::super::signal::sigint().expect("pin SIGINT handler");

        let raiser = std::thread::spawn(|| {
            // ctrl_c() registers its listener on first poll, which happens
            // as soon as block_on starts below — long before this delay ends.
            std::thread::sleep(std::time::Duration::from_millis(100));
            nix::sys::signal::raise(nix::sys::signal::Signal::SIGINT).expect("raise SIGINT");
        });

        let result = futures_lite::future::block_on(ctrl_c());
        raiser.join().expect("raiser thread");
        crate::assert_with_log!(result.is_ok(), "ctrl_c ok", true, result.is_ok());
        crate::test_complete!("ctrl_c_completes_on_sigint");
    }
}
