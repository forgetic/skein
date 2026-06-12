//! Async signal handling and graceful shutdown.
//!
//! This module provides primitives for handling Unix signals and implementing
//! graceful shutdown patterns in async applications.
//!
//! # Components
//!
//! - [`SignalKind`]: Enumeration of Unix signal types
//! - [`Signal`]: Async stream for receiving signals
//! - [`ctrl_c`]: Cross-platform Ctrl+C handling
//! - [`ShutdownController`]: Coordinated graceful shutdown
//! - [`ShutdownReceiver`]: Handle for receiving shutdown notifications
//! - [`with_graceful_shutdown`]: Run tasks with shutdown support
//!
//! # Implementation
//!
//! On Unix, signal streams are backed by a process-global self-pipe: a
//! minimal async-signal-safe handler forwards each delivery through a pipe
//! to a dedicated dispatcher thread, which wakes every registered stream.
//! Polling is purely waker-based, so signal futures work under any executor
//! and need no reactor. See the `registry` module for details. On non-Unix
//! platforms, constructing a signal stream returns an error.
//!
//! # Example
//!
//! ```ignore
//! use skein::signal::{ShutdownController, with_graceful_shutdown, GracefulOutcome};
//!
//! async fn run_server() {
//!     let controller = ShutdownController::new();
//!
//!     // Subscribe to shutdown notifications
//!     let receiver = controller.subscribe();
//!
//!     // Run a task with graceful shutdown support
//!     let result = with_graceful_shutdown(
//!         async { /* server loop */ 42 },
//!         receiver,
//!     ).await;
//!
//!     match result {
//!         GracefulOutcome::Completed(value) => println!("Completed: {value}"),
//!         GracefulOutcome::ShutdownSignaled => println!("Shutdown requested"),
//!     }
//! }
//! ```
//!
//! # Cancel Safety
//!
//! - `Signal::recv`: Cancel-safe
//! - `ShutdownReceiver::wait`: Cancel-safe
//! - `ctrl_c`: Cancel-safe

mod ctrl_c;
mod graceful;
mod kind;
#[cfg(unix)]
mod registry;
mod shutdown;
mod signal;

pub use ctrl_c::{CtrlCError, ctrl_c, is_available};
pub use graceful::{
    GracePeriodGuard, GracefulBuilder, GracefulConfig, GracefulOutcome, with_graceful_shutdown,
};
pub use kind::SignalKind;
pub use shutdown::{ShutdownController, ShutdownReceiver};
pub use signal::{Signal, SignalError, signal};

// Unix-specific signal helpers
#[cfg(unix)]
pub use signal::{sigchld, sighup, sigint, sigquit, sigterm, sigusr1, sigusr2, sigwinch};
