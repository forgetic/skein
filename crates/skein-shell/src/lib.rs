//! The shell kit (shell.md): what a service's `main` runs its
//! loop on (programming-model.md, section 2).
//!
//! - [`Kernel`]: the io_uring backend of io's kernel records
//!   (`skein_io::kernel`; shell.md, 3), opened once, then submitted to
//!   and reaped from once per iteration.
//! - [`open_root`]: a directory opened at startup as a root for io's files.
//! - [`open_termination_signals`]: the blocked SIGINT/SIGTERM source io adopts.
//! - [`Clock`]: monotonic and wall time, read together once per iteration.
//! - [`seed`]: the random seed, from `getrandom`, once at startup.
//!
//! skein provides no `run`: the loop belongs to the service.
//!
//! This is ordinary Rust (programming-model.md, 10.2), and the only `unsafe`
//! in skein that a service runs lives here, in one module, the ring adapter:
//! the crate's lints deny it everywhere else. (The counting allocator,
//! test-only, has the other: testing.md, 6.)

mod clock;
mod ring;
mod seed;
#[cfg(test)]
mod tests;

pub use clock::{Clock, Now};
pub use ring::{
    Config, HostedPipes, Kernel, OpenError, Wait, hosted_pipes, open_root, open_termination_signals,
    write_service_signal,
};
pub use seed::seed;
