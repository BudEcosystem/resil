//! # resil
//!
//! Rate limiting and resilience for services that sit in front of upstreams: a local-first,
//! Redis-synced rate and concurrency limiter that holds a limit across replicas without a network
//! round-trip per request, and the primitives that decide what to do when an upstream call fails.
//! Each module stands alone.
//!
//! | Module | Holds |
//! |---|---|
//! | [`policy`] | serde types for a per-target policy: rate limits, concurrency cap, retry, fallbacks |
//! | [`limit`] | local-first, store-synced rate and concurrency limiter |
//! | [`classify`] | failure → retry / failover / breaker verdict; `Retry-After` parsing |
//! | [`retry`] | retry with classification, delay hints and a deadline |
//! | [`breaker`] | keyed circuit breakers, per target and per provider |
//! | [`fallback`] | ordered fallback chains with cycle detection |
//! | [`http`] | 429 bodies, `X-RateLimit-*` headers, CORS expose list |

pub mod breaker;
pub mod classify;
pub mod fallback;
pub mod http;
pub mod limit;
pub mod policy;
pub mod retry;

pub use limit::{Decision, Limiter, LimiterOptions, RateHeaders};
pub use policy::{DeploymentPolicy, RateLimitAlgorithm, RateLimitConfig, RetryConfig};
