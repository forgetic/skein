//! Deterministic network simulation for distributed testing.

mod config;
mod network;

pub use config::{JitterModel, LatencyModel, NetworkConditions, NetworkConfig};
pub use network::{
    Fault, HostId, NetworkMetrics, NetworkTraceEvent, NetworkTraceKind, Packet, SimulatedNetwork,
};
