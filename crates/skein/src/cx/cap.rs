//! Typed capability sets for `Cx`.
//!
//! The capability set is represented at the type level so that operations
//! requiring certain effects (spawn/time/random/io) can be gated
//! at compile time.
//!
//! # Capability Rows
//!
//! A capability row is a fixed-width vector of booleans — one per effect:
//! `[SPAWN, TIME, RANDOM, IO]`. The [`CapSet`] struct encodes this
//! row as const generics, making it a zero-sized type with no runtime cost.
//!
//! The subset relation ([`SubsetOf`]) is the pointwise ≤ ordering on rows.
//! Narrowing (dropping capabilities) always succeeds; widening (gaining
//! capabilities) is a compile-time error.
//!
//! # Forging Prevention
//!
//! Capability marker traits are sealed to prevent external crates from
//! implementing them for arbitrary types. This ensures only the runtime's
//! `CapSet` types can grant access to gated APIs.
//!
//! # Narrowing is Monotone
//!
//! If `A: SubsetOf<B>`, then every method available on `Cx<A>` is a
//! subset of those available on `Cx<B>`. Narrowing cannot introduce new
//! effects because each `Has*` marker is gated on a single boolean
//! position, and the `SubsetOf` impl requires each bit in the sub to
//! be ≤ the corresponding bit in the super.
//!
//! # Trusted Roots
//!
//! - The runtime constructs full contexts internally (e.g., via `RuntimeState`).
//! - Test-only constructors (e.g., `Cx::for_testing*`) are permitted for harnesses.
//!
//! # Compile-time rejection of widening
//!
//! ```compile_fail
//! use skein::cx::cap::{CapSet, SubsetOf};
//!
//! // WebCaps (no spawn) cannot widen to GrpcCaps (has spawn):
//! fn widen<Sub: SubsetOf<Super>, Super>() {}
//! type WebCaps = CapSet<false, true, false, true>;
//! type GrpcCaps = CapSet<true, true, false, true>;
//! widen::<GrpcCaps, WebCaps>(); // ERROR: GrpcCaps is NOT a subset of WebCaps
//! ```
//!
//! ```compile_fail
//! use skein::cx::cap::{CapSet, None, SubsetOf};
//!
//! // Cannot widen from None to any capability:
//! fn widen<Sub: SubsetOf<Super>, Super>() {}
//! type SpawnOnly = CapSet<true, false, false, false>;
//! widen::<SpawnOnly, None>(); // ERROR: SpawnOnly is NOT a subset of None
//! ```

mod sealed {
    pub trait Sealed {}

    /// Type-level capability bit for subset reasoning.
    ///
    /// Kept inside `sealed` so external crates cannot construct or
    /// implement traits on these types, preserving anti-forgery.
    pub struct Bit<const V: bool>;

    /// Ordering on capability bits: `false ≤ false`, `false ≤ true`, `true ≤ true`.
    ///
    /// The missing impl `(Bit<true>, Bit<false>)` encodes that widening
    /// (gaining a capability you don't have) is statically rejected.
    pub trait Le {}
    impl Le for (Bit<false>, Bit<false>) {}
    impl Le for (Bit<false>, Bit<true>) {}
    impl Le for (Bit<true>, Bit<true>) {}
}

/// Type-level capability set.
///
/// Each boolean controls whether the capability is present:
/// - `SPAWN`: spawn tasks/regions
/// - `TIME`: timers, timeouts
/// - `RANDOM`: entropy and random values
/// - `IO`: async I/O capability
#[derive(Debug, Clone, Copy, Default)]
pub struct CapSet<const SPAWN: bool, const TIME: bool, const RANDOM: bool, const IO: bool>;

impl<const SPAWN: bool, const TIME: bool, const RANDOM: bool, const IO: bool> sealed::Sealed
    for CapSet<SPAWN, TIME, RANDOM, IO>
{
}

/// Full capability set (default).
pub type All = CapSet<true, true, true, true>;

/// No capabilities.
pub type None = CapSet<false, false, false, false>;

/// Marker: spawn capability.
///
/// ```compile_fail
/// use skein::cx::HasSpawn;
///
/// struct FakeCaps;
/// impl HasSpawn for FakeCaps {}
/// ```
pub trait HasSpawn: sealed::Sealed {}
impl<const TIME: bool, const RANDOM: bool, const IO: bool> HasSpawn
    for CapSet<true, TIME, RANDOM, IO>
{
}

/// Marker: time capability.
pub trait HasTime: sealed::Sealed {}
impl<const SPAWN: bool, const RANDOM: bool, const IO: bool> HasTime
    for CapSet<SPAWN, true, RANDOM, IO>
{
}

/// Marker: random/entropy capability.
pub trait HasRandom: sealed::Sealed {}
impl<const SPAWN: bool, const TIME: bool, const IO: bool> HasRandom
    for CapSet<SPAWN, TIME, true, IO>
{
}

/// Marker: I/O capability.
pub trait HasIo: sealed::Sealed {}
impl<const SPAWN: bool, const TIME: bool, const RANDOM: bool> HasIo
    for CapSet<SPAWN, TIME, RANDOM, true>
{
}

/// Marker: subset relation between capability sets.
///
/// `Sub: SubsetOf<Super>` holds when every capability enabled in `Sub` is
/// also enabled in `Super`. This is the pointwise ≤ ordering on boolean
/// capability rows and guarantees that narrowing is **monotone**: you can
/// only drop capabilities, never gain them.
///
/// # Monotonicity argument
///
/// Because `sealed::Le` is only implemented for `(false,false)`,
/// `(false,true)`, and `(true,true)` — but *not* `(true,false)` — the
/// compiler rejects any attempt to widen a capability set. Combined with
/// the `Sealed` supertrait, external crates cannot forge `SubsetOf`
/// implementations.
///
/// # Properties
///
/// - **Reflexive**: `CapSet<S,T,R,I>: SubsetOf<CapSet<S,T,R,I>>`
/// - **Transitive**: if `A: SubsetOf<B>` and `B: SubsetOf<C>`, then
///   `A: SubsetOf<C>` (follows from bit-level ≤ transitivity)
/// - **Antisymmetric**: `A: SubsetOf<B>` and `B: SubsetOf<A>` implies A = B
///
/// ```compile_fail
/// use skein::cx::SubsetOf;
///
/// struct FakeCaps;
/// impl SubsetOf<FakeCaps> for FakeCaps {}
/// ```
pub trait SubsetOf<Super>: sealed::Sealed {}

// General pointwise subset: Sub ⊆ Super iff each capability bit in Sub ≤ Super.
impl<
    const S1: bool,
    const T1: bool,
    const R1: bool,
    const I1: bool,
    const S2: bool,
    const T2: bool,
    const R2: bool,
    const I2: bool,
> SubsetOf<CapSet<S2, T2, R2, I2>> for CapSet<S1, T1, R1, I1>
where
    (sealed::Bit<S1>, sealed::Bit<S2>): sealed::Le,
    (sealed::Bit<T1>, sealed::Bit<T2>): sealed::Le,
    (sealed::Bit<R1>, sealed::Bit<R2>): sealed::Le,
    (sealed::Bit<I1>, sealed::Bit<I2>): sealed::Le,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: assert a type satisfies SubsetOf at compile time.
    fn assert_subset<Sub: SubsetOf<Super>, Super>() {}

    // Helper: assert a marker trait at compile time.
    fn assert_has_spawn<C: HasSpawn>() {}
    fn assert_has_time<C: HasTime>() {}
    fn assert_has_random<C: HasRandom>() {}
    fn assert_has_io<C: HasIo>() {}

    // --- Reflexivity ---

    #[test]
    fn subset_reflexive_all() {
        assert_subset::<All, All>();
    }

    #[test]
    fn subset_reflexive_none() {
        assert_subset::<None, None>();
    }

    #[test]
    fn subset_reflexive_mixed() {
        // CapSet<true, false, true, false> ⊆ itself
        assert_subset::<CapSet<true, false, true, false>, CapSet<true, false, true, false>>();
    }

    // --- Bottom and top ---

    #[test]
    fn none_subset_of_all() {
        assert_subset::<None, All>();
    }

    #[test]
    fn none_subset_of_any() {
        assert_subset::<None, CapSet<false, true, false, false>>();
        assert_subset::<None, CapSet<true, false, false, false>>();
    }

    #[test]
    fn any_subset_of_all() {
        assert_subset::<CapSet<true, false, true, false>, All>();
        assert_subset::<CapSet<false, false, false, true>, All>();
    }

    // --- Intermediate narrowing (framework wrapper types) ---

    #[test]
    fn background_subset_of_grpc() {
        // Background = <true, true, false, false>
        // Grpc       = <true, true, false, true>
        // Background ⊆ Grpc (Background drops IO)
        type BackgroundCaps = CapSet<true, true, false, false>;
        type GrpcCaps = CapSet<true, true, false, true>;
        assert_subset::<BackgroundCaps, GrpcCaps>();
    }

    #[test]
    fn web_subset_of_all() {
        // Web = <false, true, false, true>
        type WebCaps = CapSet<false, true, false, true>;
        assert_subset::<WebCaps, All>();
    }

    #[test]
    fn pure_subset_of_web() {
        // Pure = None = <false, false, false, false>
        // Web  = <false, true, false, true>
        type WebCaps = CapSet<false, true, false, true>;
        assert_subset::<None, WebCaps>();
    }

    #[test]
    fn single_cap_subset_of_multi() {
        // <false, true, false, false> ⊆ <true, true, false, true>
        assert_subset::<CapSet<false, true, false, false>, CapSet<true, true, false, true>>();
    }

    // --- Transitivity (demonstrated, not mechanized) ---

    #[test]
    fn transitive_none_background_grpc() {
        type BackgroundCaps = CapSet<true, true, false, false>;
        type GrpcCaps = CapSet<true, true, false, true>;
        // None ⊆ Background ⊆ Grpc, therefore None ⊆ Grpc
        assert_subset::<None, BackgroundCaps>();
        assert_subset::<BackgroundCaps, GrpcCaps>();
        assert_subset::<None, GrpcCaps>();
    }

    // --- Marker traits ---

    #[test]
    fn all_has_every_capability() {
        assert_has_spawn::<All>();
        assert_has_time::<All>();
        assert_has_random::<All>();
        assert_has_io::<All>();
    }

    #[test]
    fn partial_caps_have_correct_markers() {
        // <true, true, false, true> has Spawn+Time+Io but not Random
        assert_has_spawn::<CapSet<true, true, false, true>>();
        assert_has_time::<CapSet<true, true, false, true>>();
        assert_has_io::<CapSet<true, true, false, true>>();
    }

    // --- ZST property ---

    #[test]
    fn capset_is_zero_sized() {
        assert_eq!(std::mem::size_of::<All>(), 0);
        assert_eq!(std::mem::size_of::<None>(), 0);
        assert_eq!(std::mem::size_of::<CapSet<true, false, true, false>>(), 0);
    }

    // --- Compile-fail doctests for anti-forgery are on HasSpawn and SubsetOf above ---
}
