//! The shell kit (overview.md, section 8): what a service's `main` runs its
//! loop on (consumers.md, section 4).
//!
//! - [`Kernel`]: the io_uring backend of io's kernel records
//!   (`skein_io::kernel`; overview.md, 7.1), opened once, then submitted to
//!   and reaped from once per iteration.
//! - [`Clock`]: monotonic and wall time, read together once per iteration.
//! - [`seed`]: the random seed, from `getrandom`, once at startup.
//!
//! skein provides no `run`: the loop belongs to the service.
//!
//! This is ordinary Rust (programming-style.md, 9.2), and the only `unsafe`
//! in skein lives here, in one module, the ring adapter: the crate's lints
//! deny it everywhere else.

mod clock;
mod ring;
mod seed;

pub use clock::{Clock, Now};
pub use ring::{Config, Kernel, OpenError, Wait};
pub use seed::seed;
