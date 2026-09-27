//! Keyed circuit breakers, in two tiers:
//!
//! | Tier | Key | Opens on |
//! |---|---|---|
//! | deployment | target id | its credential/quota errors (401/402/403, 429) and 5xx/timeouts |
//! | vendor | provider + API host | 5xx/timeouts/connect errors from ≥ 2 distinct targets |
//!
//! One tenant's bad key opens only its own target's breaker; a provider outage opens the vendor
//! tier, which every target on that provider consults, so they fail over without each paying the
//! failure volume first.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use tokio::time::Instant;

use crate::classify::{BreakerSignal, Verdict};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BreakerConfig {
    /// Failures inside `window` that open a closed breaker.
    pub failure_threshold: u32,
    /// Only failures this recent count.
    pub window: Duration,
    /// How long a breaker stays open before letting one probe through.
    pub open_for: Duration,
    /// Distinct sources (deployments) that must have failed: 1 for the deployment tier, 2 for
    /// the vendor tier.
    pub min_sources: usize,
}

impl BreakerConfig {
    pub fn deployment() -> Self {
        Self {
            failure_threshold: 5,
            window: Duration::from_secs(60),
            open_for: Duration::from_secs(30),
            min_sources: 1,
        }
    }
    pub fn vendor() -> Self {
        Self {
            failure_threshold: 5,
            window: Duration::from_secs(60),
            open_for: Duration::from_secs(30),
            min_sources: 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Deployment,
    Vendor,
}

impl Tier {
    fn label(self) -> &'static str {
        match self {
            Tier::Deployment => "deployment",
            Tier::Vendor => "vendor",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerState {
    Closed,
    Open,
    HalfOpen,
}

/// The breaker refused the call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Open {
    pub tier: Tier,
    pub retry_in: Duration,
}

#[derive(Debug)]
enum St {
    Closed,
    Open { until: Instant },
    HalfOpen { probe_started: Option<Instant> },
}

#[derive(Debug)]
struct Entry {
    st: St,
    failures: VecDeque<(Instant, Arc<str>)>,
}

/// One tier of keyed breakers.
pub struct Breakers {
    cfg: BreakerConfig,
    tier: Tier,
    map: DashMap<Arc<str>, Mutex<Entry>, foldhash::fast::RandomState>,
}

impl Breakers {
    pub fn new(tier: Tier, cfg: BreakerConfig) -> Self {
        Self {
            cfg,
            tier,
            map: DashMap::with_hasher(foldhash::fast::RandomState::default()),
        }
    }

    fn with<R>(&self, key: &str, f: impl FnOnce(&mut Entry) -> R) -> R {
        if let Some(e) = self.map.get(key) {
            let mut g = e.lock().unwrap_or_else(|p| p.into_inner());
            return f(&mut g);
        }
        let e = self.map.entry(Arc::from(key)).or_insert_with(|| {
            Mutex::new(Entry {
                st: St::Closed,
                failures: VecDeque::new(),
            })
        });
        let mut g = e.lock().unwrap_or_else(|p| p.into_inner());
        f(&mut g)
    }

    fn gauge(&self, key: &str, open: bool) {
        metrics::gauge!("resil_breaker_open", "tier" => self.tier.label(), "key" => key.to_owned())
            .set(if open { 1.0 } else { 0.0 });
    }

    /// May a call go to `key`? An open breaker whose time is up lets exactly one probe through.
    pub fn check(&self, key: &str) -> Result<(), Open> {
        if !self.map.contains_key(key) {
            return Ok(());
        }
        let now = Instant::now();
        let open_for = self.cfg.open_for;
        let tier = self.tier;
        self.with(key, |e| match e.st {
            St::Closed => Ok(()),
            St::Open { until } if now < until => Err(Open {
                tier,
                retry_in: until - now,
            }),
            St::Open { .. } => {
                e.st = St::HalfOpen {
                    probe_started: Some(now),
                };
                Ok(())
            }
            St::HalfOpen { probe_started } => match probe_started {
                // A probe whose outcome was never reported must not wedge the breaker.
                Some(t) if now.duration_since(t) < open_for => Err(Open {
                    tier,
                    retry_in: Duration::from_secs(1),
                }),
                _ => {
                    e.st = St::HalfOpen {
                        probe_started: Some(now),
                    };
                    Ok(())
                }
            },
        })
    }

    pub fn state(&self, key: &str) -> BreakerState {
        let Some(e) = self.map.get(key) else {
            return BreakerState::Closed;
        };
        let g = e.lock().unwrap_or_else(|p| p.into_inner());
        match g.st {
            St::Closed => BreakerState::Closed,
            St::Open { until } if Instant::now() < until => BreakerState::Open,
            St::Open { .. } | St::HalfOpen { .. } => BreakerState::HalfOpen,
        }
    }

    pub fn record_success(&self, key: &str) {
        if !self.map.contains_key(key) {
            return;
        }
        let closed = self.with(key, |e| {
            e.failures.clear();
            let was_open = !matches!(e.st, St::Closed);
            e.st = St::Closed;
            was_open
        });
        if closed {
            self.gauge(key, false);
        }
    }

    /// A failure attributed to `source` (the deployment, for the vendor tier).
    pub fn record_failure(&self, key: &str, source: &str) {
        let now = Instant::now();
        let cfg = self.cfg;
        let opened = self.with(key, |e| {
            if let St::HalfOpen { .. } = e.st {
                e.st = St::Open {
                    until: now + cfg.open_for,
                };
                return true;
            }
            while e
                .failures
                .front()
                .is_some_and(|(t, _)| now.duration_since(*t) > cfg.window)
            {
                e.failures.pop_front();
            }
            e.failures.push_back((now, Arc::from(source)));
            if e.failures.len() > 64 {
                e.failures.pop_front();
            }
            let mut sources: Vec<&str> = e.failures.iter().map(|(_, s)| &**s).collect();
            sources.sort_unstable();
            sources.dedup();
            if matches!(e.st, St::Closed)
                && e.failures.len() as u32 >= cfg.failure_threshold
                && sources.len() >= cfg.min_sources
            {
                e.st = St::Open {
                    until: now + cfg.open_for,
                };
                e.failures.clear();
                return true;
            }
            false
        });
        if opened {
            tracing::warn!(tier = self.tier.label(), key, "circuit breaker opened");
            self.gauge(key, true);
        }
    }

    /// Open for exactly `d` (a vendor 429 with `Retry-After`: "accelerated circuit breaking").
    pub fn open_for(&self, key: &str, d: Duration) {
        let until = Instant::now() + d;
        self.with(key, |e| {
            let longer = match e.st {
                St::Open { until: u } => until > u,
                _ => true,
            };
            if longer {
                e.st = St::Open { until };
            }
        });
        self.gauge(key, true);
    }
}

/// The deployment tier and the vendor tier together.
pub struct TwoTier {
    pub deployment: Breakers,
    pub vendor: Breakers,
}

impl Default for TwoTier {
    fn default() -> Self {
        Self::new(BreakerConfig::deployment(), BreakerConfig::vendor())
    }
}

impl TwoTier {
    pub fn new(deployment: BreakerConfig, vendor: BreakerConfig) -> Self {
        Self {
            deployment: Breakers::new(Tier::Deployment, deployment),
            vendor: Breakers::new(Tier::Vendor, vendor),
        }
    }

    /// Both tiers must be closed (or probing).
    pub fn check(&self, deployment: &str, vendor: &str) -> Result<(), Open> {
        self.vendor.check(vendor)?;
        self.deployment.check(deployment)
    }

    pub fn record_success(&self, deployment: &str, vendor: &str) {
        self.deployment.record_success(deployment);
        self.vendor.record_success(vendor);
    }

    /// Feed a classified failure to the tiers it concerns.
    pub fn record_failure(&self, deployment: &str, vendor: &str, v: &Verdict) {
        match v.breaker {
            BreakerSignal::Ignore => {}
            BreakerSignal::Deployment => self.deployment.record_failure(deployment, deployment),
            BreakerSignal::Vendor => {
                self.deployment.record_failure(deployment, deployment);
                self.vendor.record_failure(vendor, deployment);
            }
            BreakerSignal::OpenFor(d) => self.deployment.open_for(deployment, d),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::{classify, Failure};

    fn v(code: u16) -> Verdict {
        classify(&Failure::Status {
            code,
            headers: None,
            body: None,
        })
    }

    #[tokio::test(start_paused = true)]
    async fn opens_after_threshold_and_probes_after_open_for() {
        let b = Breakers::new(Tier::Deployment, BreakerConfig::deployment());
        for _ in 0..4 {
            b.record_failure("d1", "d1");
        }
        assert!(b.check("d1").is_ok());
        b.record_failure("d1", "d1");
        let open = b.check("d1").unwrap_err();
        assert_eq!(open.tier, Tier::Deployment);
        assert!(open.retry_in <= Duration::from_secs(30));
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(b.check("d1").is_ok(), "one probe");
        assert!(b.check("d1").is_err(), "only one probe at a time");
        b.record_success("d1");
        assert_eq!(b.state("d1"), BreakerState::Closed);
    }

    #[tokio::test(start_paused = true)]
    async fn failed_probe_reopens() {
        let b = Breakers::new(Tier::Deployment, BreakerConfig::deployment());
        b.open_for("d1", Duration::from_secs(5));
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(b.check("d1").is_ok());
        b.record_failure("d1", "d1");
        assert_eq!(b.state("d1"), BreakerState::Open);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_failures_do_not_accumulate() {
        let b = Breakers::new(Tier::Deployment, BreakerConfig::deployment());
        for _ in 0..4 {
            b.record_failure("d1", "d1");
            tokio::time::advance(Duration::from_secs(20)).await;
        }
        b.record_failure("d1", "d1");
        assert!(b.check("d1").is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn tenant_isolation_one_bad_key_opens_only_its_deployment() {
        let t = TwoTier::default();
        for _ in 0..10 {
            t.record_failure("tenant-a", "elevenlabs", &v(401));
        }
        assert!(t.check("tenant-a", "elevenlabs").is_err());
        assert!(t.check("tenant-b", "elevenlabs").is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn vendor_tier_needs_two_deployments() {
        let t = TwoTier::default();
        for _ in 0..10 {
            t.record_failure("a", "deepgram", &v(503));
        }
        assert!(
            t.check("c", "deepgram").is_ok(),
            "one deployment's 503s are not a vendor outage"
        );
        let t = TwoTier::default();
        for i in 0..6 {
            t.record_failure(if i % 2 == 0 { "a" } else { "b" }, "deepgram", &v(503));
        }
        let open = t.check("c", "deepgram").unwrap_err();
        assert_eq!(open.tier, Tier::Vendor);
    }

    #[tokio::test(start_paused = true)]
    async fn caller_errors_never_trip() {
        let t = TwoTier::default();
        for _ in 0..50 {
            t.record_failure("a", "x", &v(400));
            t.record_failure("a", "x", &v(422));
        }
        assert!(t.check("a", "x").is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_429_opens_for_exactly_that_long() {
        let t = TwoTier::default();
        let mut h = http::HeaderMap::new();
        h.insert("retry-after", http::HeaderValue::from_static("20"));
        let verdict = classify(&Failure::Status {
            code: 429,
            headers: Some(&h),
            body: None,
        });
        t.record_failure("a", "x", &verdict);
        let open = t.check("a", "x").unwrap_err();
        assert_eq!(open.retry_in, Duration::from_secs(20));
        tokio::time::advance(Duration::from_secs(20)).await;
        assert!(t.check("a", "x").is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn unreported_probe_does_not_wedge() {
        let b = Breakers::new(Tier::Deployment, BreakerConfig::deployment());
        b.open_for("d", Duration::from_secs(1));
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(b.check("d").is_ok()); // probe, outcome never reported
        tokio::time::advance(Duration::from_secs(31)).await;
        assert!(b.check("d").is_ok());
    }
}
