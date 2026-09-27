//! A per-target traffic policy as data: rate limits, a concurrency cap, retry and fallbacks.
//!
//! Plain serde types, so the same policy can come from JSON published by a control plane or from
//! a TOML config file, and every service that reads it parses it the same way. Parsing is lenient
//! where it matters (see [`DeploymentPolicy::from_json_lenient`]).

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;

use crate::limit::algo::{Algorithm, WindowSpec};

/// The admin's algorithm choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitAlgorithm {
    /// Counter per aligned window. Cheapest; up to 2x the limit across a window boundary.
    FixedWindow,
    /// Weighted current + previous window counters (Cloudflare's approximation).
    #[default]
    SlidingWindow,
    /// GCRA: smooth rate with a burst allowance (`burst_size`).
    TokenBucket,
}

impl From<RateLimitAlgorithm> for Algorithm {
    fn from(a: RateLimitAlgorithm) -> Self {
        match a {
            RateLimitAlgorithm::FixedWindow => Algorithm::FixedWindow,
            RateLimitAlgorithm::SlidingWindow => Algorithm::SlidingWindow,
            RateLimitAlgorithm::TokenBucket => Algorithm::TokenBucket,
        }
    }
}

/// Rate limits for one target (the `rate_limits` block).
///
/// No `deny_unknown_fields`: older and newer publishers may carry keys this version does not know.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RateLimitConfig {
    #[serde(default)]
    pub algorithm: RateLimitAlgorithm,
    pub requests_per_second: Option<u32>,
    pub requests_per_minute: Option<u32>,
    pub requests_per_hour: Option<u32>,
    /// Bucket depth for `token_bucket`; ignored by the other algorithms.
    pub burst_size: Option<u32>,
    /// `false` turns every limit off while keeping the values (an off switch that remembers).
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Longest a store-backed view may be relied on while the store is failing, before the
    /// replica falls back to its local share (`on_store_unavailable`).
    #[serde(default = "default_cache_ttl_ms")]
    pub cache_ttl_ms: u64,
    /// Longest a request waits on the store: an early sync when its replica's credit is spent,
    /// or a last-mile decision.
    #[serde(default = "default_redis_timeout_ms")]
    pub redis_timeout_ms: u64,
    /// `a`: the fraction of the unreserved budget one sync may grant, split across replicas.
    /// `>= 1.0` means local-only mode: each replica enforces the full limit on its own.
    #[serde(default = "default_local_allowance")]
    pub local_allowance: f64,
    /// Sync period for a key that is busy (above half its limit or spending its credit fast).
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,
}

fn default_enabled() -> bool {
    true
}
fn default_cache_ttl_ms() -> u64 {
    200
}
fn default_redis_timeout_ms() -> u64 {
    10
}
fn default_local_allowance() -> f64 {
    0.1
}
fn default_sync_interval_ms() -> u64 {
    100
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            algorithm: RateLimitAlgorithm::default(),
            requests_per_second: None,
            requests_per_minute: None,
            requests_per_hour: None,
            burst_size: None,
            enabled: default_enabled(),
            cache_ttl_ms: default_cache_ttl_ms(),
            redis_timeout_ms: default_redis_timeout_ms(),
            local_allowance: default_local_allowance(),
            sync_interval_ms: default_sync_interval_ms(),
        }
    }
}

impl RateLimitConfig {
    /// Any window configured (regardless of `enabled`).
    pub fn has_limits(&self) -> bool {
        self.requests_per_second.is_some()
            || self.requests_per_minute.is_some()
            || self.requests_per_hour.is_some()
    }

    /// Enabled and at least one non-zero window.
    pub fn is_active(&self) -> bool {
        self.enabled && !self.windows().is_empty()
    }

    /// `local_allowance >= 1.0`: per-replica limits, no store.
    pub fn is_local_only(&self) -> bool {
        self.local_allowance >= 1.0
    }

    /// The configured windows, shortest first. A zero limit disables its window.
    pub fn windows(&self) -> Vec<WindowSpec> {
        let burst = match self.algorithm {
            RateLimitAlgorithm::TokenBucket => self.burst_size.filter(|b| *b > 0),
            _ => None,
        };
        [
            (self.requests_per_second, 1_000u64),
            (self.requests_per_minute, 60_000),
            (self.requests_per_hour, 3_600_000),
        ]
        .into_iter()
        .filter_map(|(limit, window_ms)| {
            let limit = limit.filter(|l| *l > 0)?;
            Some(WindowSpec {
                limit: u64::from(limit),
                window_ms,
                burst: u64::from(burst.unwrap_or(limit)),
            })
        })
        .collect()
    }

    /// The lowest-rate window: `(limit, window)`.
    pub fn most_restrictive_limit(&self) -> Option<(u32, Duration)> {
        self.windows()
            .into_iter()
            .min_by(|a, b| {
                let ra = a.limit as f64 / a.window_ms as f64;
                let rb = b.limit as f64 / b.window_ms as f64;
                ra.partial_cmp(&rb).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|w| (w.limit as u32, Duration::from_millis(w.window_ms)))
    }

    /// Merge with `other` taking precedence: limits come from `other` when set, and tuning fields
    /// are taken from `other` only when they differ from the default.
    pub fn merge(self, other: Self) -> Self {
        Self {
            algorithm: other.algorithm,
            requests_per_second: other.requests_per_second.or(self.requests_per_second),
            requests_per_minute: other.requests_per_minute.or(self.requests_per_minute),
            requests_per_hour: other.requests_per_hour.or(self.requests_per_hour),
            burst_size: other.burst_size.or(self.burst_size),
            enabled: other.enabled,
            cache_ttl_ms: if other.cache_ttl_ms != default_cache_ttl_ms() {
                other.cache_ttl_ms
            } else {
                self.cache_ttl_ms
            },
            redis_timeout_ms: if other.redis_timeout_ms != default_redis_timeout_ms() {
                other.redis_timeout_ms
            } else {
                self.redis_timeout_ms
            },
            local_allowance: if (other.local_allowance - default_local_allowance()).abs()
                > f64::EPSILON
            {
                other.local_allowance
            } else {
                self.local_allowance
            },
            sync_interval_ms: if other.sync_interval_ms != default_sync_interval_ms() {
                other.sync_interval_ms
            } else {
                self.sync_interval_ms
            },
        }
    }

    /// Clamp the tuning knobs into sane ranges (a bad publish must not wedge a gateway).
    pub fn sanitized(&self) -> Self {
        let mut c = self.clone();
        if !c.local_allowance.is_finite() || c.local_allowance <= 0.0 {
            c.local_allowance = default_local_allowance();
        }
        c.sync_interval_ms = c.sync_interval_ms.clamp(10, 10_000);
        c.cache_ttl_ms = c.cache_ttl_ms.clamp(50, 60_000);
        c.redis_timeout_ms = c.redis_timeout_ms.clamp(1, 5_000);
        c
    }
}

/// Retry policy (the `retry_config` block).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RetryConfig {
    pub num_retries: usize,
    pub max_delay_s: f32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            num_retries: 0,
            max_delay_s: 10.0,
        }
    }
}

/// Everything a deployment's Rate limiting and Resilience settings say, as one gateway sees it.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeploymentPolicy {
    pub rate_limits: Option<RateLimitConfig>,
    pub max_concurrent: Option<u32>,
    pub retry_config: Option<RetryConfig>,
    pub fallback_models: Vec<Arc<str>>,
}

/// At most this many fallbacks per target.
pub const MAX_FALLBACKS: usize = 5;

impl DeploymentPolicy {
    /// Parse the policy blocks from a JSON object **leniently**: a malformed block is dropped with
    /// a warning and never takes the record that carries it (or the other blocks)
    /// down with it. Returns the policy and the warnings.
    pub fn from_json_lenient(obj: &Value) -> (Self, Vec<String>) {
        let mut warnings = Vec::new();
        let mut policy = DeploymentPolicy::default();

        match obj.get("rate_limits") {
            None | Some(Value::Null) => {}
            Some(v) => match serde_json::from_value::<RateLimitConfig>(v.clone()) {
                Ok(c) => policy.rate_limits = Some(c),
                Err(e) => warnings.push(format!("rate_limits ignored: {e}")),
            },
        }
        match obj.get("max_concurrent") {
            None | Some(Value::Null) => {}
            Some(v) => match v.as_u64() {
                Some(n) if n > 0 && n <= u64::from(u32::MAX) => {
                    policy.max_concurrent = Some(n as u32)
                }
                _ => warnings.push(format!(
                    "max_concurrent ignored: {v} is not a positive integer"
                )),
            },
        }
        match obj.get("retry_config") {
            None | Some(Value::Null) => {}
            Some(v) => match serde_json::from_value::<RetryConfig>(v.clone()) {
                Ok(c) if c.max_delay_s.is_finite() && c.max_delay_s >= 0.0 => {
                    policy.retry_config = Some(c)
                }
                Ok(_) => warnings.push("retry_config ignored: max_delay_s out of range".into()),
                Err(e) => warnings.push(format!("retry_config ignored: {e}")),
            },
        }
        match obj.get("fallback_models") {
            None | Some(Value::Null) => {}
            Some(Value::Array(items)) => {
                for item in items {
                    match item.as_str() {
                        Some(s) if !s.is_empty() => {
                            if policy.fallback_models.len() < MAX_FALLBACKS {
                                policy.fallback_models.push(Arc::from(s));
                            } else {
                                warnings.push(format!(
                                    "fallback_models truncated to {MAX_FALLBACKS} entries"
                                ));
                                break;
                            }
                        }
                        _ => warnings.push(format!("fallback_models entry ignored: {item}")),
                    }
                }
            }
            Some(v) => warnings.push(format!("fallback_models ignored: {v} is not a list")),
        }
        (policy, warnings)
    }

    pub fn is_empty(&self) -> bool {
        self.rate_limits.is_none()
            && self.max_concurrent.is_none()
            && self.retry_config.is_none()
            && self.fallback_models.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_are_stable() {
        let c = RateLimitConfig::default();
        assert_eq!(c.algorithm, RateLimitAlgorithm::SlidingWindow);
        assert!(c.enabled);
        assert_eq!(c.cache_ttl_ms, 200);
        assert_eq!(c.redis_timeout_ms, 10);
        assert_eq!(c.local_allowance, 0.1);
        assert_eq!(c.sync_interval_ms, 100);
    }

    #[test]
    fn parses_a_published_policy() {
        let c: RateLimitConfig = serde_json::from_value(json!({
            "algorithm": "token_bucket", "requests_per_second": 10, "requests_per_minute": null,
            "requests_per_hour": 5000, "burst_size": 15, "enabled": true, "cache_ttl_ms": 500,
            "local_allowance": 0.8, "sync_interval_ms": 100
        }))
        .unwrap();
        assert_eq!(c.algorithm, RateLimitAlgorithm::TokenBucket);
        let w = c.windows();
        assert_eq!(w.len(), 2);
        assert_eq!((w[0].limit, w[0].window_ms, w[0].burst), (10, 1000, 15));
        assert_eq!(
            (w[1].limit, w[1].window_ms, w[1].burst),
            (5000, 3_600_000, 15)
        );
    }

    #[test]
    fn tolerates_unknown_keys() {
        let c: RateLimitConfig = serde_json::from_value(json!({
            "requests_per_minute": 60, "redis_connection_pool_size": 8, "local_cache_size": 1000
        }))
        .unwrap();
        assert_eq!(c.requests_per_minute, Some(60));
    }

    #[test]
    fn burst_only_applies_to_token_bucket() {
        for alg in [
            RateLimitAlgorithm::FixedWindow,
            RateLimitAlgorithm::SlidingWindow,
        ] {
            let c = RateLimitConfig {
                algorithm: alg,
                requests_per_minute: Some(60),
                burst_size: Some(500),
                ..Default::default()
            };
            assert_eq!(c.windows()[0].burst, 60, "{alg:?} must ignore burst_size");
        }
    }

    #[test]
    fn zero_limit_disables_window_and_disabled_is_inactive() {
        let c = RateLimitConfig {
            requests_per_second: Some(0),
            requests_per_minute: Some(10),
            ..Default::default()
        };
        assert_eq!(c.windows().len(), 1);
        assert!(c.is_active());
        let off = RateLimitConfig {
            enabled: false,
            ..c
        };
        assert!(!off.is_active());
        assert!(off.has_limits());
    }

    #[test]
    fn most_restrictive_is_lowest_rate() {
        let c = RateLimitConfig {
            requests_per_second: Some(10),
            requests_per_minute: Some(300),
            requests_per_hour: Some(1000),
            ..Default::default()
        };
        assert_eq!(
            c.most_restrictive_limit(),
            Some((1000, Duration::from_secs(3600)))
        );
    }

    #[test]
    fn merge_takes_the_override_where_set() {
        let base = RateLimitConfig {
            requests_per_minute: Some(60),
            cache_ttl_ms: 300,
            ..Default::default()
        };
        let over = RateLimitConfig {
            requests_per_second: Some(2),
            cache_ttl_ms: 500,
            ..Default::default()
        };
        let m = base.merge(over);
        assert_eq!(m.requests_per_minute, Some(60));
        assert_eq!(m.requests_per_second, Some(2));
        assert_eq!(m.cache_ttl_ms, 500);
    }

    #[test]
    fn sanitized_clamps_bad_knobs() {
        let c = RateLimitConfig {
            local_allowance: f64::NAN,
            sync_interval_ms: 0,
            redis_timeout_ms: 0,
            ..Default::default()
        }
        .sanitized();
        assert_eq!(c.local_allowance, 0.1);
        assert_eq!(c.sync_interval_ms, 10);
        assert_eq!(c.redis_timeout_ms, 1);
    }

    #[test]
    fn lenient_policy_drops_only_the_bad_block() {
        let (p, w) = DeploymentPolicy::from_json_lenient(&json!({
            "vendor": "elevenlabs",
            "rate_limits": {"algorithm": "leaky_bucket", "requests_per_second": 5},
            "max_concurrent": 20,
            "retry_config": {"num_retries": 2, "max_delay_s": 5.0},
            "fallback_models": ["a", 7, "b"]
        }));
        assert!(p.rate_limits.is_none());
        assert_eq!(p.max_concurrent, Some(20));
        assert_eq!(p.retry_config.unwrap().num_retries, 2);
        assert_eq!(
            p.fallback_models,
            vec![Arc::<str>::from("a"), Arc::from("b")]
        );
        assert_eq!(w.len(), 2, "{w:?}");
    }

    #[test]
    fn lenient_policy_rejects_nonpositive_concurrency_and_caps_fallbacks() {
        let (p, w) = DeploymentPolicy::from_json_lenient(&json!({
            "max_concurrent": 0,
            "fallback_models": ["1", "2", "3", "4", "5", "6"]
        }));
        assert!(p.max_concurrent.is_none());
        assert_eq!(p.fallback_models.len(), MAX_FALLBACKS);
        assert_eq!(w.len(), 2);
    }

    #[test]
    fn old_entry_has_no_policy() {
        let (p, w) = DeploymentPolicy::from_json_lenient(&json!({"vendor": "deepgram"}));
        assert!(p.is_empty());
        assert!(w.is_empty());
    }
}
