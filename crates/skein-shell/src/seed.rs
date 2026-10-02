//! The seed (overview.md, section 8; programming-style.md, section 8).

use crate::ring;

/// A random seed for the layers' `Rng`s, from `getrandom`. Read once, at
/// startup: everything random after it is drawn from the layers' state.
///
/// Fails with the error number when the kernel refuses `getrandom` (a
/// seccomp profile), or a signal interrupts it while the entropy pool is
/// still initialising (`EINTR`).
pub fn seed() -> Result<u64, i32> {
    ring::random()
}
