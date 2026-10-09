//! The shell kit (shell.md): what a service's `main` runs its
//! loop on (programming-model.md, section 2).
//!
//! - [`Kernel`]: the io_uring backend of io's kernel records
//!   (`skein_io::kernel`; shell.md, 3), opened once, then submitted to
//!   and reaped from once per iteration.
//! - [`open_append`]: a regular file opened beneath a root for output at startup.
//! - [`open_root`]: a directory opened at startup as a root for io's files.
//! - [`open_termination_signals`]: the blocked SIGINT/SIGTERM source io adopts.
//! - [`Clock`]: monotonic and wall time, read together once per iteration.
//! - [`seed`]: the random seed, from `getrandom`, once at startup.
//!
//! [`Host`] adapts the service; [`drive`] runs it until settlement (shell.md, 12 and 13).
//!
//! This is ordinary Rust (programming-model.md, 10.2), and the only `unsafe`
//! in skein that a service runs lives here, in one module, the ring adapter:
//! the crate's lints deny it everywhere else. (The counting allocator,
//! test-only, has the other: testing.md, 6.)

mod clock;
mod drive;
mod ring;
mod seed;
#[cfg(test)]
mod tests;

pub use clock::{Clock, Now};
pub use drive::{Host, drive};
pub use ring::{
    Config, HostedPipes, Kernel, OpenError, OutputKind, Wait, abandon_binary, close_keeper_fd, hosted_pipes,
    make_subreaper, open_append, open_cgroup, open_pidfd, open_root, open_signal_pipe, open_termination_signals,
    pidfd_exited, poll_child, prepare_output, set_subreaper, signal_current_thread, signal_kept_child, start_binary,
    start_binary_in_cgroup, subreaper, wait_cgroup_change, write_service_signal,
};
pub use seed::seed;
