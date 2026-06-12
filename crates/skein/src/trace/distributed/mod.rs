//! Distributed tracing infrastructure.
//!
//! This module provides:
//!
//! - **Trace identifiers and context** (`id`, `context`, `span`, `collector`):
//!   W3C-compatible trace IDs, symbol-level span recording, and in-process collection.
//! - **Vector clocks** (`vclock`): Causal ordering for distributed events.
//!   Events are partially ordered: concurrent events remain unordered.
//! - **Convergent state lattice** (`lattice`): Join-semilattice for obligation
//!   and lease state that converges via monotone merge.

pub mod collector;
pub mod context;
pub mod id;
pub mod lattice;
pub mod span;
pub mod vclock;

pub use collector::{SymbolTraceCollector, TraceRecord, TraceSummary};
pub use context::{RegionTag, SymbolTraceContext, TraceFlags};
pub use id::{SymbolSpanId, TraceId};
pub use lattice::{LatticeState, LeaseLatticeState, ObligationEntry, ObligationLattice};
pub use span::{SymbolSpan, SymbolSpanKind, SymbolSpanStatus};
pub use vclock::{
    CausalEvent, CausalOrder, CausalTracker, HybridClock, HybridTime, LamportClock, LamportTime,
    LogicalClock, LogicalClockHandle, LogicalClockKind, LogicalClockMode, LogicalTime, VectorClock,
    VectorClockHandle,
};

/// Identifier for a node in a distributed trace.
///
/// Nodes are opaque identifiers: the runtime does not interpret them beyond
/// equality comparison and display. Vector clocks and convergent state are
/// keyed by `NodeId`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(String);

impl NodeId {
    /// Creates a new node identifier from a string.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Returns the node identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Node({})", self.0)
    }
}
