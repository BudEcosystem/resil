//! # resil
//!
//! Traffic policy for Bud's gateways (budgateway, WaaV): everything that turns a deployment's
//! **Rate limiting** and **Resilience** settings into behaviour, in one place.
//!
//! | Module | Holds |
//! |---|---|
//! | [`policy`] | serde types for the deployment-settings contract (`model_table` / `voice_table`) |
//! | [`limit`] | local-first, store-synced rate and concurrency limiter |
//! | [`classify`] | failure → retry / failover / breaker verdict; `Retry-After` parsing |
//! | [`retry`] | retry with classification, delay hints and a deadline |
//! | [`breaker`] | keyed circuit breakers, deployment and vendor tiers |
//! | [`fallback`] | ordered fallback chains with cycle detection |
//! | [`http`] | 429 bodies, `X-RateLimit-*` headers, CORS expose list |
//!
//! Design: `specs/022-gateway-rate-limit-resilience/FRD.md` in bud-runtime.

pub mod breaker;
pub mod classify;
pub mod fallback;
pub mod http;
pub mod limit;
pub mod policy;
pub mod retry;

pub use limit::{Decision, Limiter, LimiterOptions, RateHeaders};
pub use policy::{DeploymentPolicy, RateLimitAlgorithm, RateLimitConfig, RetryConfig};
