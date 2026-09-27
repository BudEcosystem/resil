//! Local-first distributed rate and concurrency limiting.
//!
//! Every replica decides locally from **credit**: a reservation of the shared budget granted by the
//! store at the last sync. Admission is an atomic add and a compare — no I/O. A background task
//! pushes admitted hits in batches and asks for more credit; the store grants credit only out of
//! the budget nobody else has reserved (`limit − usage − Σ other replicas' reservations`), so the
//! replicas together never admit more than the limit while the store is healthy.
//!
//! A request waits on the store (at most `redis_timeout_ms`) only when its replica's credit is
//! spent before the early sync refilled it, or when the remaining budget is too small to split
//! across replicas (last-mile: the store admits waiting requests one by one).

pub mod algo;
pub mod clock;
mod conc;
mod rate;
pub mod redis;
pub(crate) mod stats;
pub mod store;
mod sync;

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed};
use std::sync::Arc;

use tokio::sync::{Notify, Semaphore};

use crate::policy::RateLimitConfig;
use clock::{AtomicF64, Clock, ClockSrc, SystemClock};
use conc::ConcKey;
pub use conc::{ConcurrencyDenied, ConcurrencyGuard};
use rate::RateKey;
pub use stats::Outcome;
use stats::Stats;
use store::{MemStore, Store};

/// What a replica does once the store has been unreachable for longer than `cache_ttl_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OnStoreUnavailable {
    /// Fail-static: enforce `⌈L / N_last⌉` locally, so the cluster total stays ≈ `L`.
    #[default]
    LocalShare,
    /// Fail-open: admit everything (hits are still counted and pushed on reconnect).
    Allow,
}

/// How a key decides once its budget is too small to split across the replicas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LastMile {
    /// Ask the store per request (atomic check-and-debit, bounded by `redis_timeout_ms`).
    #[default]
    Sync,
    /// Never wait: each replica may admit one unreserved request per sync interval (up to `N×` on
    /// tiny limits).
    Local,
}

#[derive(Debug, Clone)]
pub struct LimiterOptions {
    /// Namespace for store keys and metric labels: one per replica set (e.g. `my-gateway`).
    pub service: String,
    /// Unique replica id. Defaults to `$HOSTNAME` plus a random suffix.
    pub pod_id: Option<String>,
    pub on_store_unavailable: OnStoreUnavailable,
    pub last_mile: LastMile,
    /// Replica registry heartbeat period; a replica is live for 3 heartbeats.
    pub heartbeat_ms: u64,
    /// Sync period of a key that is well under its limit (adaptive sync).
    pub slow_sync_ms: u64,
    /// How long the store keeps a replica's reservation without hearing from it.
    pub lease_ttl_ms: u64,
    /// Deadline for a background sync round-trip.
    pub round_timeout_ms: u64,
    /// Scan period of the sync task.
    pub tick_ms: u64,
    /// Keys per round-trip.
    pub max_keys_per_round: usize,
}

impl Default for LimiterOptions {
    fn default() -> Self {
        Self {
            service: "resil".into(),
            pod_id: None,
            on_store_unavailable: OnStoreUnavailable::LocalShare,
            last_mile: LastMile::Sync,
            heartbeat_ms: 1_000,
            slow_sync_ms: 1_000,
            lease_ttl_ms: 3_000,
            round_timeout_ms: 1_000,
            tick_ms: 20,
            max_keys_per_round: 512,
        }
    }
}

/// Rate-limit headers (`X-RateLimit-*`, `Retry-After`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateHeaders {
    /// Limit of the most restrictive window.
    pub limit: u64,
    /// Minimum across windows.
    pub remaining: u64,
    /// Unix seconds.
    pub reset: u64,
    /// Whole seconds, at least 1; only on denial.
    pub retry_after: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// No active rate limit for this subject: no headers.
    Unlimited,
    Allow(RateHeaders),
    Deny(RateHeaders),
}

impl Decision {
    pub fn is_allowed(&self) -> bool {
        !matches!(self, Decision::Deny(_))
    }
    pub fn headers(&self) -> Option<&RateHeaders> {
        match self {
            Decision::Unlimited => None,
            Decision::Allow(h) | Decision::Deny(h) => Some(h),
        }
    }
}

pub(crate) struct Key {
    pub(crate) rate: Option<Arc<RateKey>>,
    pub(crate) conc: Option<Arc<ConcKey>>,
}

type KeyMap = papaya::HashMap<Arc<str>, Arc<Key>, foldhash::fast::RandomState>;

pub(crate) struct Inner {
    pub(crate) opts: LimiterOptions,
    pub(crate) service: &'static str,
    pub(crate) pod: Arc<str>,
    pub(crate) pods_key: Arc<str>,
    pub(crate) store: Option<Arc<dyn Store>>,
    /// Engine for local-only keys and for the degraded fallback.
    pub(crate) local: MemStore,
    pub(crate) keys: KeyMap,
    pub(crate) clock: ClockSrc,
    pub(crate) replicas: AtomicU32,
    /// store_ms − mono_ms.
    pub(crate) offset: AtomicF64,
    pub(crate) offset_known: AtomicBool,
    pub(crate) last_ok: AtomicF64,
    pub(crate) failing: AtomicBool,
    /// First successful round-trip after a failure (local mono ms).
    pub(crate) recovered_at: AtomicF64,
    pub(crate) last_heartbeat: AtomicF64,
    pub(crate) wake: Notify,
    pub(crate) stats: Stats,
    pub(crate) rounds: Arc<Semaphore>,
    pub(crate) stopped: AtomicBool,
    pub(crate) sync_failures: AtomicU64,
    pub(crate) rounds_total: AtomicU64,
}

/// The limiter. Cheap to clone (an `Arc`).
#[derive(Clone)]
pub struct Limiter {
    pub(crate) inner: Arc<Inner>,
}

fn default_pod_id() -> String {
    let host = std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("POD_NAME"))
        .unwrap_or_else(|_| "replica".into());
    format!("{host}-{:08x}", fastrand::u32(..))
}

impl Limiter {
    /// A limiter backed by `store` (`None`: every key is local-only).
    pub fn new(opts: LimiterOptions, store: Option<Arc<dyn Store>>) -> Self {
        Self::build(opts, store, ClockSrc::System(SystemClock::default()))
    }

    /// A limiter on a custom clock (tests drive a [`clock::ManualClock`]).
    pub fn with_clock(
        opts: LimiterOptions,
        store: Option<Arc<dyn Store>>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self::build(opts, store, ClockSrc::Custom(clock))
    }

    fn build(opts: LimiterOptions, store: Option<Arc<dyn Store>>, clock: ClockSrc) -> Self {
        let pod: Arc<str> = Arc::from(opts.pod_id.clone().unwrap_or_else(default_pod_id));
        let service: &'static str = Box::leak(opts.service.clone().into_boxed_str());
        let pods_key: Arc<str> = Arc::from(format!("rl2:{{{service}}}:pods"));
        let initial_offset = clock.unix_ms() - clock.mono_ms();
        let local_epoch = initial_offset;
        let inner = Inner {
            service,
            pod,
            pods_key,
            store,
            // The local engine is only ever called with an explicit store time.
            local: MemStore::new(Arc::new(move || local_epoch)),
            keys: papaya::HashMap::with_hasher(foldhash::fast::RandomState::default()),
            clock,
            replicas: AtomicU32::new(1),
            offset: AtomicF64::new(initial_offset),
            offset_known: AtomicBool::new(false),
            last_ok: AtomicF64::new(0.0),
            failing: AtomicBool::new(false),
            recovered_at: AtomicF64::new(f64::NEG_INFINITY),
            last_heartbeat: AtomicF64::new(f64::NEG_INFINITY),
            wake: Notify::new(),
            stats: Stats::default(),
            rounds: Arc::new(Semaphore::new(4)),
            stopped: AtomicBool::new(false),
            sync_failures: AtomicU64::new(0),
            rounds_total: AtomicU64::new(0),
            opts,
        };
        let inner = Arc::new(inner);
        inner.last_ok.store(inner.clock.mono_ms());
        Self { inner }
    }

    pub fn pod_id(&self) -> &str {
        &self.inner.pod
    }

    /// Live replicas of this service, as last counted.
    pub fn replicas(&self) -> u32 {
        self.inner.replicas.load(Relaxed)
    }

    pub fn has_store(&self) -> bool {
        self.inner.store.is_some()
    }

    /// Register this replica and start the background sync task. Requires a Tokio runtime.
    pub async fn start(&self) -> tokio::task::JoinHandle<()> {
        if self.inner.store.is_some() {
            let _ = self.inner.round(true, true).await;
        }
        let inner = self.inner.clone();
        tokio::spawn(async move { sync::run(inner).await })
    }

    /// Push outstanding hits, release this replica's reservations and deregister it.
    pub async fn shutdown(&self) {
        self.inner.stopped.store(true, Relaxed);
        self.inner.wake.notify_one();
        let _ = self.inner.final_round().await;
    }

    /// Install or replace a subject's policy. An unchanged configuration keeps its live state
    /// (callers may re-apply their whole policy table periodically).
    pub fn set_policy(
        &self,
        subject: &str,
        rate_limits: Option<&RateLimitConfig>,
        max_concurrent: Option<u32>,
    ) {
        let subject_arc: Arc<str> = Arc::from(subject);
        let existing = self.inner.keys.pin().get(subject).cloned();
        let rate = rate_limits.filter(|c| c.is_active()).map(|c| {
            let c = c.sanitized();
            match existing.as_ref().and_then(|k| k.rate.clone()) {
                Some(r) if r.same_config(&c, self.inner.store.is_some()) => r,
                _ => Arc::new(RateKey::new(&self.inner, subject_arc.clone(), c)),
            }
        });
        let conc = max_concurrent.filter(|m| *m > 0).map(|m| {
            let tuning = rate_limits.cloned().unwrap_or_default().sanitized();
            match existing.as_ref().and_then(|k| k.conc.clone()) {
                Some(c) if c.same_config(m, &tuning, self.inner.store.is_some()) => c,
                _ => Arc::new(ConcKey::new(&self.inner, subject_arc.clone(), m, &tuning)),
            }
        });
        if rate.is_none() && conc.is_none() {
            self.inner.keys.pin().remove(subject);
            return;
        }
        let unchanged = existing
            .as_ref()
            .is_some_and(|k| opt_ptr_eq(&k.rate, &rate) && opt_ptr_eq(&k.conc, &conc));
        if !unchanged {
            self.inner
                .keys
                .pin()
                .insert(subject_arc, Arc::new(Key { rate, conc }));
        }
    }

    /// Drop a subject's policy.
    pub fn remove(&self, subject: &str) {
        self.inner.keys.pin().remove(subject);
    }

    /// Replace every policy at once: subjects missing from `policies` are removed.
    pub fn sync_policies<'a, I>(&self, policies: I)
    where
        I: IntoIterator<Item = (&'a str, Option<&'a RateLimitConfig>, Option<u32>)>,
    {
        let mut seen = std::collections::HashSet::new();
        for (subject, rl, mc) in policies {
            self.set_policy(subject, rl, mc);
            seen.insert(subject.to_owned());
        }
        self.inner.keys.pin().retain(|k, _| seen.contains(&**k));
    }

    pub fn has_policy(&self, subject: &str) -> bool {
        self.inner.keys.pin().contains_key(subject)
    }

    /// Number of subjects with a policy.
    pub fn len(&self) -> usize {
        self.inner.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.keys.len() == 0
    }

    /// Decide one request. Completes without I/O unless the replica's credit is spent.
    pub async fn check(&self, subject: &str) -> Decision {
        let now = self.inner.clock.mono_ms();
        let rate = {
            let guard = self.inner.keys.guard();
            let Some(key) = self.inner.keys.get(subject, &guard) else {
                return Decision::Unlimited;
            };
            let Some(rate) = key.rate.as_ref() else {
                return Decision::Unlimited;
            };
            if let Some(d) = self.inner.fast(rate, now, Outcome::AllowLocal) {
                return d;
            }
            rate.clone()
        };
        self.inner.slow(rate, now).await
    }

    /// The no-I/O part of [`check`](Self::check): `None` means the caller must use `check`.
    pub fn try_check(&self, subject: &str) -> Option<Decision> {
        let now = self.inner.clock.mono_ms();
        let guard = self.inner.keys.guard();
        let key = self.inner.keys.get(subject, &guard)?;
        match key.rate.as_ref() {
            None => Some(Decision::Unlimited),
            Some(rate) => self.inner.fast(rate, now, Outcome::AllowLocal),
        }
    }

    /// Take a concurrency slot (`max_concurrent`). `Ok(None)`: the subject has no cap. The slot is
    /// released when the guard drops, on every exit path.
    pub async fn acquire(
        &self,
        subject: &str,
    ) -> Result<Option<ConcurrencyGuard>, ConcurrencyDenied> {
        let now = self.inner.clock.mono_ms();
        let conc = {
            let guard = self.inner.keys.guard();
            let Some(key) = self.inner.keys.get(subject, &guard) else {
                return Ok(None);
            };
            let Some(conc) = key.conc.as_ref() else {
                return Ok(None);
            };
            conc.clone()
        };
        self.inner.acquire(conc, now).await.map(Some)
    }

    /// Slots this replica holds for `subject`.
    pub fn active(&self, subject: &str) -> u64 {
        self.inner
            .keys
            .pin()
            .get(subject)
            .and_then(|k| k.conc.as_ref().map(|c| c.active()))
            .unwrap_or(0)
    }

    /// Run one sync round now. `force` syncs every key regardless of its schedule. Tests and
    /// shutdown use it; production relies on the background task.
    pub async fn sync_now(&self, force: bool) -> Result<(), store::StoreError> {
        self.inner.round(force, force).await
    }

    /// Decision counters (tests, diagnostics).
    pub fn outcome_count(&self, o: Outcome) -> u64 {
        self.inner.stats.total(o)
    }

    /// Store round-trips that failed or timed out.
    pub fn sync_failures(&self) -> u64 {
        self.inner.sync_failures.load(Relaxed)
    }

    /// Store round-trips attempted.
    pub fn sync_rounds(&self) -> u64 {
        self.inner.rounds_total.load(Relaxed)
    }
}

fn opt_ptr_eq<T>(a: &Option<Arc<T>>, b: &Option<Arc<T>>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        (None, None) => true,
        _ => false,
    }
}

impl Inner {
    #[inline]
    pub(crate) fn store_now(&self, mono: f64) -> f64 {
        mono + self.offset.load()
    }

    /// Store unavailable long enough to stop trusting reservations.
    #[inline]
    pub(crate) fn degraded(&self, now: f64, cache_ttl_ms: u64) -> bool {
        self.failing.load(Relaxed) && now - self.last_ok.load() > cache_ttl_ms as f64
    }

    /// Just back from a store outage: every replica is still pushing the hits it admitted while
    /// degraded, so nobody may take new budget until those have landed.
    #[inline]
    pub(crate) fn recovering(&self, now: f64, sync_interval_ms: u64) -> bool {
        // A request built while the store is failing is the first push after it comes back.
        self.failing.load(Relaxed)
            || now - self.recovered_at.load() < 2.0 * sync_interval_ms.max(self.opts.tick_ms) as f64
    }

    pub(crate) fn credit_ttl_ms(&self) -> f64 {
        self.opts.lease_ttl_ms as f64 * 0.75
    }
}

/// A stable 64-bit FNV-1a hash (config hashes appear in store keys, so they must not depend on
/// the process's hasher seed).
pub(crate) fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}
