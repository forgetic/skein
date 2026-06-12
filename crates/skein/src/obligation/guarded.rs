//! Guarded Recursion Lens: Time-Indexed Behavior for Leases.
//!
//! This module captures the guarded recursion / "later" modality lens as a
//! concrete design note tied to lease and region APIs in skein.
//!
//! # Background: What is Guarded Recursion?
//!
//! In type theory, the **later modality** (written `▸A` or `later(A)`) delays
//! a type by one time step. Guarded recursion allows defining infinite
//! (coinductive) behaviors safely: a self-referencing definition is productive
//! if every recursive occurrence is guarded by `▸`.
//!
//! For example, an infinite stream `Stream A = A × ▸(Stream A)` is well-defined
//! because the recursive reference `Stream A` appears only under `▸`.
//!
//! # How This Maps to Skein
//!
//! Skein's runtime is inherently step-indexed via its deterministic lab
//! runtime (virtual time advances in discrete steps) and its explicit state
//! machines. The "later" modality is realized concretely:
//!
//! ## Leases as Time-Indexed Obligations
//!
//! A lease is a time-indexed obligation that transitions through states:
//!
//! ```text
//! LeaseEvolution(t) =
//!   | Active { expires_at }  if t < expires_at
//!   | Expired                if t ≥ expires_at ∧ ¬renewed
//!   | Released               if holder released before expiry
//! ```
//!
//! The renewal operation extends the "active" window:
//!
//! ```text
//! renew(lease, duration, now):
//!   requires: lease.state(now) = Active
//!   ensures:  lease.expires_at' = now + duration
//!   effect:   shifts the ▸ boundary forward in time
//! ```
//!
//! Key insight: **renewal is a re-guarding operation**. It moves the
//! `▸`-boundary (expiry) into the future, keeping the lease "productive"
//! (active). Without renewal, the lease becomes "non-productive" (expired)
//! after a fixed number of time steps.
//!
//! ## Region Lifecycle as Step-Indexed Protocol
//!
//! ```text
//! RegionProtocol = Open → ▸(Closing → ▸(Draining → ▸(Finalizing → ▸(Closed))))
//! ```
//!
//! Each transition is guarded: a region cannot skip phases. The guard ensures
//! that children complete before the parent finalizes (structured concurrency).
//!
//! ## Budget Consumption as Decreasing Sequence
//!
//! A budget is a monotonically decreasing resource:
//!
//! ```text
//! budget(t+1) ≤ budget(t)     (deadline approaches, polls consumed)
//! ```
//!
//! This is the dual of guarded recursion: instead of producing one step of
//! output, we consume one step of resource. The budget reaching zero is the
//! inductive termination condition.
//!
//! # Practical Payoff
//!
//! The primary payoff of the guarded recursion lens is **time-indexed lease
//! renewal reasoning**:
//!
//! 1. **Liveness proof**: If a lease is renewed every `d` units with duration
//!    `d`, it is always active. This is a coinductive argument: `active(t)`
//!    implies `active(t + d)` via renewal.
//!
//! 2. **Expiry proof**: An unrenewed lease expires at exactly
//!    `created_at + initial_duration`. This is an inductive argument with
//!    base case at creation time.
//!
//! 3. **Nested deadline monotonicity**: If a lease lives inside a region with
//!    deadline `D`, the lease's expiry cannot exceed `D`. This is the
//!    deadline monotonicity invariant: `lease.expires_at ≤ region.deadline`.
//!
//! # Constraints for Future Phases
//!
//! See [`PreservationConstraint`] for the machine-readable checklist.

use crate::types::Time;
use std::time::Duration;

/// Time-indexed invariants that the runtime must preserve to keep the
/// guarded recursion lens valid.
///
/// Each invariant corresponds to a concrete runtime property that, if
/// violated, would break the time-indexed reasoning described above.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeIndexedInvariant {
    /// Lease expiry is monotonically determined by time: once
    /// `now ≥ expires_at`, the lease is expired regardless of state.
    /// No operation other than `renew` can move `expires_at` forward.
    LeaseTimeMonotonicity,

    /// Lease renewal extends expiry from `now`, not from `expires_at`.
    /// This prevents "banking" renewals: you must be active to renew.
    LeaseRenewalFromNow,

    /// Lease state transitions are irreversible for terminal states:
    /// `Released` and `Expired` cannot transition back to `Active`.
    LeaseTerminalIrreversibility,

    /// Region phases progress strictly forward:
    /// `Open → Closing → Draining → Finalizing → Closed`.
    /// No backward transitions.
    RegionPhaseMonotonicity,

    /// Budget consumption is monotonically decreasing within a region:
    /// polls and cost only decrease, deadline only approaches.
    BudgetMonotonicConsumption,

    /// Deadline monotonicity across the region tree: child deadline
    /// cannot exceed parent deadline.
    DeadlineTreeMonotonicity,
}

/// Constraints that future phases must preserve to keep the guarded
/// recursion / "later" modality lens valid.
///
/// Each constraint is a "don't break this" note for later phases.
#[derive(Debug, Clone)]
pub struct PreservationConstraint {
    /// The invariant this constraint protects.
    pub invariant: TimeIndexedInvariant,
    /// Human-readable description of what must be preserved.
    pub constraint: &'static str,
    /// Why breaking this would invalidate the lens.
    pub rationale: &'static str,
}

/// Returns the full checklist of preservation constraints.
#[must_use]
pub fn preservation_constraints() -> Vec<PreservationConstraint> {
    vec![
        PreservationConstraint {
            invariant: TimeIndexedInvariant::LeaseTimeMonotonicity,
            constraint: "Lease expiry is determined solely by time comparison",
            rationale: "The time-indexed model requires that lease validity is a pure \
                        function of current time: active(t) = (t < expires_at). Any \
                        state that overrides this (e.g., 'force-active despite expired') \
                        would break the time-indexing.",
        },
        PreservationConstraint {
            invariant: TimeIndexedInvariant::LeaseRenewalFromNow,
            constraint: "Lease renewal sets expires_at = now + duration",
            rationale: "Renewal from 'now' ensures the lease cannot accumulate unbounded \
                        future credit. This is the re-guarding operation: it places a \
                        new guard exactly 'duration' steps ahead.",
        },
        PreservationConstraint {
            invariant: TimeIndexedInvariant::LeaseTerminalIrreversibility,
            constraint: "Released and Expired lease states are terminal",
            rationale: "Terminal states in the lease state machine correspond to \
                        coinductive termination. Allowing transitions back to Active \
                        would create an inconsistent state history.",
        },
        PreservationConstraint {
            invariant: TimeIndexedInvariant::RegionPhaseMonotonicity,
            constraint: "Region lifecycle phases are strictly forward",
            rationale: "The step-indexed protocol Open → Closing → ... → Closed \
                        is a finite unfolding. Backward transitions would violate \
                        the well-foundedness of the region shutdown sequence.",
        },
        PreservationConstraint {
            invariant: TimeIndexedInvariant::BudgetMonotonicConsumption,
            constraint: "Budget resources only decrease during execution",
            rationale: "Budget is the dual of guarded recursion: a decreasing measure \
                        that guarantees termination. Adding budget mid-execution would \
                        break the inductive termination argument.",
        },
        PreservationConstraint {
            invariant: TimeIndexedInvariant::DeadlineTreeMonotonicity,
            constraint: "Child deadline <= parent deadline in the region tree",
            rationale: "Nested time bounds must tighten inward. A child with a longer \
                        deadline than its parent would outlive the parent's scope, \
                        violating structured concurrency.",
        },
    ]
}

/// A lightweight model of lease evolution for testing time-indexed properties.
///
/// This mirrors the real `Lease` struct but is self-contained for unit testing
/// the guarded recursion invariants without depending on `RuntimeState`.
#[derive(Debug, Clone)]
pub struct LeaseModel {
    /// When the lease was created.
    pub created_at: Time,
    /// Current expiry time.
    pub expires_at: Time,
    /// Initial duration for reference.
    pub initial_duration: Duration,
    /// How many times renewed.
    pub renewal_count: u32,
    /// Whether explicitly released.
    pub released: bool,
}

impl LeaseModel {
    /// Create a new active lease model.
    #[must_use]
    pub fn new(created_at: Time, duration: Duration) -> Self {
        Self {
            created_at,
            expires_at: created_at + duration,
            initial_duration: duration,
            renewal_count: 0,
            released: false,
        }
    }

    /// Is the lease active at the given time?
    #[must_use]
    pub fn is_active(&self, now: Time) -> bool {
        !self.released && now < self.expires_at
    }

    /// Is the lease expired at the given time?
    #[must_use]
    pub fn is_expired(&self, now: Time) -> bool {
        !self.released && now >= self.expires_at
    }

    /// Renew the lease from `now`.
    ///
    /// Returns `false` if the lease is already expired or released.
    pub fn renew(&mut self, duration: Duration, now: Time) -> bool {
        if self.released || now >= self.expires_at {
            return false;
        }
        self.expires_at = now + duration;
        self.renewal_count += 1;
        true
    }

    /// Release the lease.
    ///
    /// Returns `false` if already released or expired.
    pub fn release(&mut self, now: Time) -> bool {
        if self.released || now >= self.expires_at {
            return false;
        }
        self.released = true;
        true
    }

    /// Remaining time until expiry, or zero.
    #[must_use]
    pub fn remaining(&self, now: Time) -> Duration {
        if now >= self.expires_at {
            Duration::ZERO
        } else {
            Duration::from_nanos(self.expires_at.duration_since(now))
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn t(nanos: u64) -> Time {
        Time::from_nanos(nanos)
    }

    fn dur(nanos: u64) -> Duration {
        Duration::from_nanos(nanos)
    }

    // ========================================================================
    // Constraint catalog tests
    // ========================================================================

    #[test]
    fn preservation_constraints_nonempty() {
        let constraints = preservation_constraints();
        assert!(!constraints.is_empty());
        // Each invariant should appear exactly once
        let unique: std::collections::HashSet<TimeIndexedInvariant> =
            constraints.iter().map(|c| c.invariant).collect();
        assert_eq!(
            unique.len(),
            constraints.len(),
            "duplicate invariant in constraints"
        );
    }

    #[test]
    fn all_invariants_have_constraints() {
        let constraints = preservation_constraints();
        let covered: std::collections::HashSet<TimeIndexedInvariant> =
            constraints.iter().map(|c| c.invariant).collect();

        let all = [
            TimeIndexedInvariant::LeaseTimeMonotonicity,
            TimeIndexedInvariant::LeaseRenewalFromNow,
            TimeIndexedInvariant::LeaseTerminalIrreversibility,
            TimeIndexedInvariant::RegionPhaseMonotonicity,
            TimeIndexedInvariant::BudgetMonotonicConsumption,
            TimeIndexedInvariant::DeadlineTreeMonotonicity,
        ];

        for inv in &all {
            assert!(
                covered.contains(inv),
                "invariant {inv:?} has no preservation constraint"
            );
        }
    }

    // ========================================================================
    // Lease time-indexed properties
    // ========================================================================

    /// Liveness: a lease renewed every `d` with duration `d` is always active.
    #[test]
    fn lease_renewal_liveness() {
        let d = dur(1000);
        let mut lease = LeaseModel::new(t(0), d);

        // Simulate 10 renewal cycles
        for step in 0..10u64 {
            let now = t(step * 500); // renew at half the duration
            assert!(
                lease.is_active(now),
                "lease should be active at step {step}"
            );
            let renewed = lease.renew(d, now);
            assert!(renewed, "renewal should succeed at step {step}");
        }
        // After 10 renewals, still active
        assert!(lease.is_active(t(5000)));
        assert_eq!(lease.renewal_count, 10);
    }

    /// Expiry: an unrenewed lease expires at created_at + initial_duration.
    #[test]
    fn lease_expiry_without_renewal() {
        let lease = LeaseModel::new(t(100), dur(500));

        // Active before expiry
        assert!(lease.is_active(t(100)));
        assert!(lease.is_active(t(599)));

        // Expired at exactly expires_at
        assert!(lease.is_expired(t(600)));
        assert!(lease.is_expired(t(1000)));

        // Remaining
        assert_eq!(lease.remaining(t(100)), dur(500));
        assert_eq!(lease.remaining(t(350)), dur(250));
        assert_eq!(lease.remaining(t(600)), Duration::ZERO);
    }

    /// Terminal irreversibility: released lease cannot be renewed.
    #[test]
    fn lease_terminal_irreversibility_released() {
        let mut lease = LeaseModel::new(t(0), dur(1000));
        assert!(lease.release(t(100)));

        // Cannot renew after release
        assert!(!lease.renew(dur(1000), t(200)));

        // Cannot release again
        assert!(!lease.release(t(300)));
    }

    /// Terminal irreversibility: expired lease cannot be renewed.
    #[test]
    fn lease_terminal_irreversibility_expired() {
        let mut lease = LeaseModel::new(t(0), dur(100));

        // Let it expire
        assert!(lease.is_expired(t(200)));

        // Cannot renew after expiry
        assert!(!lease.renew(dur(1000), t(200)));

        // Cannot release after expiry
        assert!(!lease.release(t(200)));
    }

    /// Renewal extends from `now`, not from `expires_at`.
    #[test]
    fn lease_renewal_from_now_not_from_expiry() {
        let mut lease = LeaseModel::new(t(0), dur(1000));

        // Renew early at t=200 with duration 500
        assert!(lease.renew(dur(500), t(200)));
        // New expiry should be 200 + 500 = 700, NOT 1000 + 500 = 1500
        assert_eq!(lease.expires_at, t(700));
        assert!(lease.is_active(t(699)));
        assert!(lease.is_expired(t(700)));
    }

    /// Lease cannot "bank" renewals — renewal doesn't accumulate future credit.
    #[test]
    fn lease_no_credit_banking() {
        let mut lease = LeaseModel::new(t(0), dur(1000));

        // Renew with a shorter duration → expiry moves backward
        assert!(lease.renew(dur(100), t(50)));
        assert_eq!(lease.expires_at, t(150));
        // The original 1000ns of lease time is gone
        assert!(lease.is_expired(t(150)));
    }

    /// Nested deadline monotonicity: lease inside a region with earlier deadline.
    #[test]
    fn lease_respects_region_deadline() {
        let region_deadline = t(500);
        let lease = LeaseModel::new(t(0), dur(1000));

        // Lease expires at t=1000 but region deadline is t=500.
        // The effective active window should be limited by the region.
        // This is a constraint that the runtime must enforce:
        // lease.expires_at should be min(lease.expires_at, region.deadline)
        let effective_expiry = lease.expires_at.min(region_deadline);
        assert_eq!(effective_expiry, t(500));

        // At t=500, the region is closing, so the lease is effectively over
        // even though lease.expires_at = t(1000)
        assert!(Time::from_nanos(500) >= effective_expiry);
    }

    // ========================================================================
    // Region phase monotonicity
    // ========================================================================

    /// Region phases must progress strictly forward.
    #[test]
    fn region_phase_ordering() {
        // Use u8 ordinals to model phase ordering
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        #[repr(u8)]
        enum Phase {
            Open = 0,
            Closing = 1,
            Draining = 2,
            Finalizing = 3,
            Closed = 4,
        }

        let sequence = [
            Phase::Open,
            Phase::Closing,
            Phase::Draining,
            Phase::Finalizing,
            Phase::Closed,
        ];

        // Verify strict forward progress
        for window in sequence.windows(2) {
            assert!(
                window[0] < window[1],
                "phase {:?} should be before {:?}",
                window[0],
                window[1]
            );
        }

        // Verify total count
        assert_eq!(sequence.len(), 5);
    }

    // ========================================================================
    // Budget decreasing measure
    // ========================================================================

    /// Budget consumption is monotonically decreasing.
    #[test]
    fn budget_monotonic_decrease() {
        // Model budget as a decreasing counter
        let mut polls_remaining: u32 = 100;
        let mut cost_remaining: u64 = 1000;

        for _ in 0..10 {
            let prev_polls = polls_remaining;
            let prev_cost = cost_remaining;
            polls_remaining -= 1;
            cost_remaining -= 50;
            assert!(polls_remaining < prev_polls);
            assert!(cost_remaining < prev_cost);
        }

        assert_eq!(polls_remaining, 90);
        assert_eq!(cost_remaining, 500);
    }

    /// Deadline approaches monotonically as time advances.
    #[test]
    fn deadline_approaches_monotonically() {
        let deadline = t(10_000);

        let mut prev_remaining = u64::MAX;
        for step in 0..10 {
            let now = t(step * 1000);
            let remaining = if now >= deadline {
                0
            } else {
                deadline.duration_since(now)
            };
            assert!(
                remaining <= prev_remaining,
                "remaining time should decrease"
            );
            prev_remaining = remaining;
        }
    }
}
