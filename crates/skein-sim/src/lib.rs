//! The simulated kernel (simulator.md): a backend that
//! answers the records of `skein_io::kernel` as its contract allows, for
//! every process of one world, with every choice drawn from one seed.
//!
//! Ordinary Rust (programming-model.md, 10.2): std is in, but no
//! clock, no thread, no OS randomness and no hash map with a random seed, so
//! a seed replays to the same [`trace`](Sim::trace).
//!
//! # Driving it
//!
//! - [`Sim::new`] makes a world from a seed and a [`Config`]
//!   ([`Config::calm`] is a well-behaved network, [`Config::chaos`] turns on
//!   every fault). [`Sim::spawn_process`] adds a process.
//! - Per process, the two calls the shell's `Kernel` offers a service:
//!   [`Sim::submit`] takes every record of a `Queue<Submit>`, and
//!   [`Sim::reap`] moves delivered completions into a `Queue<Complete>`, up
//!   to its room.
//! - Time moves only when asked: [`Sim::advance`] jumps to the next thing
//!   due (a late completion, a raced cancel, a connect's SYN timeout),
//!   [`Sim::advance_to`] to a given instant. The world is idle when nothing
//!   is due and no process has [`deferred`](Sim::deferred) work, which
//!   waits for the process to enter, not for time. A world that hosts
//!   services moves time to the earlier of [`Sim::next_due`] and their
//!   earliest deadline.
//! - Files go to the embedder's fake machine, through the machine seam
//!   (simulator.md, 3.1): after each submit, the world takes the [`Call`]s
//!   waiting ([`Sim::calls`]), has its machine answer each, and hands the
//!   [`Answer`]s back ([`Sim::answer`]), which completes the operations. A
//!   root the shell would open at startup is the machine's [`Handle`] of a
//!   directory, given to a process with [`Sim::root`].
//! - A child spawn asks the machine which program it names; the simulator
//!   keeps the child's pidfd, pipes, waiting and exit state.
//! - [`Sim::assert_quiescent`] and [`Sim::assert_no_open_fds`] check a
//!   process at the end; [`Sim::render_trace`] prints the run with its seed,
//!   every fault drawn included.
//!
//! # What it checks
//!
//! Every broken invariant of the contract fails the world at submit, with a
//! panic naming the seed and the end of the trace, as does any operation on a
//! descriptor the process does not have open. Every completion it makes is
//! checked with `Complete::is_valid`, and each token completes once. So is
//! every answer of the machine: one per call, of its call's shape, no more
//! bytes or entries than asked, and a handle no process holds.
//!
//! # Conformance
//!
//! The conformance suite (`testing/skein-conformance`; kernel.md, 8;
//! testing-strategy.md, 5) holds the simulator and the ring to the same
//! contract: its scenarios run against the simulator in
//! `tests/conformance/sim`, and against the ring in `tests/conformance/ring`.
//!
//! # Its choices, where the contract leaves one
//!
//! - **Descriptors** count up from 3 in each process and are never reused,
//!   up to `Config::max_fds` open at once.
//! - **Effects happen when the operation is decided:** at submit, or for a
//!   waiting operation (a `Connect` whose connection was made among them)
//!   once it can proceed and its process enters the kernel. The ring runs
//!   such completions only when the loop enters it, after the entry's
//!   submissions: so does the end of [`Sim::submit`]. [`Sim::reap`] decides
//!   them too, standing in for the loop's wait inside its submit, since the
//!   ring's own reap never enters it. Until then a `Cancel` still stops the
//!   operation. Latency delays only the delivery of a completion. A
//!   completion decided but not reaped is still in flight.
//! - **Decided at once, whatever the process:** what the network does
//!   (bytes landing in a buffer, a connection established into a
//!   listener's queue, an end of stream or a reset reaching a socket), a
//!   connect's SYN timeout, the refusal of the connects waiting on a
//!   listener that closes, and a raced cancel landing.
//! - **A cancel** of an operation still waiting wins (`Ok(Nothing)`, the
//!   target `Err(Cancelled)`, in a random order). Of one already decided, or
//!   of a token not in flight, it is `Err(TooLate)`. A stopped `Connect`
//!   whose connection was made leaves it made: the server accepts it, and
//!   this end only closes. With the `cancel_race` fault it lands late: the
//!   target may be decided first, or be interrupted (`TooLate`, and the
//!   target `Cancelled`). With `cancel_unsubmitted` it fails with
//!   `Other(11)` and the target runs on.
//! - **Ports:** port 0 picks a free port of the ephemeral range from a random
//!   start. Only a listener on an overlapping address clashes, as
//!   `SO_REUSEADDR` makes it on Linux. The unspecified address binds, and
//!   listens on every loopback address of its family; a connection's source
//!   is `127.0.0.1` or `::1`.
//! - **Buffers:** one receive buffer per socket, of `Config::buffer` bytes;
//!   sent bytes land in the peer's at once.
//! - **Faults beyond loopback:** `refuse`, `reset` and `timed_out` model a
//!   remote network, which loopback never is. A timed-out end reports
//!   `TimedOut`; its peer sees a reset.
//! - **Files are the machine's, the kernel's part the simulator's:** the
//!   descriptors (a file's counted with the sockets' against
//!   `Config::max_fds`, an `Open` the machine has yet to answer holding its
//!   place), the checks, and the faults. An operation on files is decided
//!   when the machine answers it, which a world does as soon as the process
//!   submits; latency then delays its delivery, as any other's.
//! - **Faults of files:** a short `Read` is cut from what the machine read,
//!   so it is short of what is there; a short `Write` gives the machine
//!   fewer bytes. `no_space`, `read_only` and `io_error` (`Other(5)`), and
//!   `no_buffer`, model a disk beyond a healthy scratch directory, and fall
//!   instead of the call, before the machine is asked: the operation did
//!   nothing. `hung` parks an `Open`, `Read`, `Write` or `Sync` with no
//!   call at all, until a `Cancel` stops it.
//! - **A cancel of an operation on files** stops one that hangs. One whose
//!   call waits for the world, or is with the machine, is being done, as an
//!   ring's worker would be doing it: the `Cancel` is too late.
#![forbid(unsafe_code)]

extern crate alloc;

mod config;
mod machine;
mod net;
mod sim;
mod trace;

pub use config::{Config, Faults};
pub use machine::{Answer, Ask, Call, Handle, Program, Reply, Ticket};
pub use sim::{Pid, Sim};
pub use trace::{Entry, Event, Fault, Summary, Text, render};
