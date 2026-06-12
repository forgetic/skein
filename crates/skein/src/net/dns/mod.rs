//! Async DNS resolution with caching and Happy Eyeballs support.
//!
//! This module provides DNS resolution with configurable caching, retry logic,
//! and Happy Eyeballs (RFC 6555) support for optimal connection establishment.
//!
//! # Cancel Safety
//!
//! - `lookup_ip`: Cancel-safe, DNS query can be cancelled at any point.
//! - `happy_eyeballs_connect`: Cancel-safe, connection attempts are cancelled on drop.
//! - Cache updates are atomic and don't block on cancellation.
//!
//! # Phase 0 Implementation
//!
//! In Phase 0, DNS resolution uses `std::net::ToSocketAddrs` which performs
//! synchronous resolution. The async API is maintained for forward compatibility
//! with future async DNS implementations.
//!
//! # Example
//!
//! ```ignore
//! use asupersync::net::dns::{Resolver, ResolverConfig};
//!
//! let resolver = Resolver::new();
//!
//! // Simple IP lookup
//! let lookup = resolver.lookup_ip("example.com").await?;
//! for addr in lookup.addresses() {
//!     println!("{}", addr);
//! }
//!
//! // Happy Eyeballs connection (races IPv6/IPv4)
//! let stream = resolver.happy_eyeballs_connect("example.com", 443).await?;
//! ```

mod cache;
mod error;
mod lookup;
mod resolver;

pub use cache::{CacheConfig, CacheStats, DnsCache};
pub use error::DnsError;
pub use lookup::{HappyEyeballs, LookupIp, LookupMx, LookupSrv, LookupTxt, MxRecord, SrvRecord};
pub use resolver::{Resolver, ResolverConfig};
