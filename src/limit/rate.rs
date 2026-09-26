//! Rate keys: the hot path, the slow path (cold start, waiting on a sync, degraded mode) and how a
//! sync reply is applied.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Notify;

use super::algo::{self, Algorithm, WindowSpec, WindowState};
use super::clock::AtomicF64;
use super::stats::Outcome;
use super::store::{RateReply, RateSync};
use super::{fnv1a, Decision, Inner, LastMile, OnStoreUnavailable, RateHeaders};
use crate::policy::RateLimitConfig;

/// The last store state this replica saw for a key.
pub(crate) struct RateView {
    /// Local monotonic ms when this view arrived.
    pub(crate) at_mono: f64,
    pub(crate) windows: Vec<WindowState>,
    pub(crate) reserved_others: u64,
    /// `false` until the first sync reply.
    pub(crate) fresh: bool,
}

/// Fields every request touches, on their own cache line.
#[repr(align(128))]
pub(crate) struct Hot {
    /// Requests admitted from credit (monotonic).
    pub(crate) admitted: AtomicU64,
    /// `admitted` may grow up to this value.
    pub(crate) credit: AtomicU64,
    /// Local monotonic ms after which the credit is no longer backed by a store reservation.
    pub(crate) valid_until: AtomicF64,
    /// Early sync once less than this much credit is left.
    pub(crate) half: AtomicU64,
    pub(crate) wanted: AtomicBool,
    /// Headers precomputed at the last sync: `remaining` when `admitted` was `hdr_admitted`, and
    /// the reset second. The hot path counts `remaining` down from local admits instead of
    /// re-evaluating every window per request.
    pub(crate) hdr_remaining: AtomicU64,
    pub(crate) hdr_admitted: AtomicU64,
    pub(crate) hdr_reset: AtomicU64,
    /// Local-only keys: the next window boundary (store ms). The first request past it refreshes
    /// inline so earlier admits are charged to the window they were made in. `+∞` otherwise.
    pub(crate) boundary: AtomicF64,
}

pub(crate) struct RateKey {
    pub(crate) store_key: Arc<str>,
    pub(crate) degraded_key: Arc<str>,
    pub(crate) alg: Algorithm,
    pub(crate) windows: Arc<[WindowSpec]>,
    pub(crate) cfg: RateLimitConfig,
    pub(crate) cfg_hash: u64,
    pub(crate) local_only: bool,
    /// Index of the most restrictive window (for `X-RateLimit-Limit` / `Reset`).
    pub(crate) head: usize,
    /// Smallest capacity across windows.
    pub(crate) min_capacity: u64,
    pub(crate) hot: Hot,
    // ---- owned by the sync path ----
    pub(crate) pushed: AtomicU64,
    pub(crate) inflight: AtomicU64,
    pub(crate) syncing: AtomicBool,
    pub(crate) waiters: AtomicU64,
    pub(crate) denied: AtomicU64,
    pub(crate) synced: Notify,
    pub(crate) send_seq: AtomicU64,
    pub(crate) done_seq: AtomicU64,
    pub(crate) last_sync: AtomicF64,
    /// Admits per ms (EWMA).
    pub(crate) ewma: AtomicF64,
    pub(crate) exhausted_until: AtomicF64,
    /// Admits known to precede the latest window boundary (snapshot ~2 ms before it) and the store
    /// time of that snapshot: the next push charges them to the window they were made in.
    pub(crate) old_a: AtomicU64,
    pub(crate) old_t: AtomicF64,
    /// Shortest window: every longer window's boundaries are on its grid.
    pub(crate) min_window_ms: u64,
    pub(crate) view: ArcSwap<RateView>,
    pub(crate) credit_lock: Mutex<()>,
    pub(crate) degraded_windows: Mutex<(u32, Arc<[WindowSpec]>)>,
}

/// What a sync sent for one key, to apply or revert its reply.
pub(crate) struct RateSent {
    pub(crate) key: Arc<RateKey>,
    pub(crate) a_send: u64,
    pub(crate) pushed_before: u64,
    pub(crate) hits: u64,
    pub(crate) send_mono: f64,
    pub(crate) seq: u64,
    pub(crate) ewma: f64,
}

pub(crate) fn config_hash(alg: Algorithm, windows: &[WindowSpec]) -> u64 {
    let mut b = vec![alg.code()];
    for w in windows {
        b.extend_from_slice(&w.limit.to_le_bytes());
        b.extend_from_slice(&w.window_ms.to_le_bytes());
        b.extend_from_slice(&w.burst.to_le_bytes());
    }
    fnv1a(&b)
}

impl RateKey {
    pub(crate) fn new(inner: &Inner, subject: Arc<str>, cfg: RateLimitConfig) -> Self {
        let alg: Algorithm = cfg.algorithm.into();
        let windows: Arc<[WindowSpec]> = Arc::from(cfg.windows());
        let cfg_hash = config_hash(alg, &windows);
        let store_key: Arc<str> = Arc::from(format!(
            "rl2:{{{}:{}}}:{:016x}",
            inner.service, subject, cfg_hash
        ));
        let degraded_key: Arc<str> = Arc::from(format!("{store_key}:local"));
        let head = windows
            .iter()
            .enumerate()
            .min_by(|(_, a), (_, b)| {
                (a.limit as f64 / a.window_ms as f64)
                    .partial_cmp(&(b.limit as f64 / b.window_ms as f64))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(i, _)| i)
            .unwrap_or(0);
        let min_capacity = windows
            .iter()
            .map(|w| w.capacity(alg) as u64)
            .min()
            .unwrap_or(1);
        let local_only = cfg.is_local_only() || inner.store.is_none();
        let n = windows.len();
        Self {
            store_key,
            degraded_key,
            alg,
            windows: windows.clone(),
            cfg,
            cfg_hash,
            local_only,
            head,
            min_capacity,
            hot: Hot {
                admitted: AtomicU64::new(0),
                credit: AtomicU64::new(0),
                valid_until: AtomicF64::new(f64::NEG_INFINITY),
                half: AtomicU64::new(0),
                wanted: AtomicBool::new(false),
                hdr_remaining: AtomicU64::new(0),
                hdr_admitted: AtomicU64::new(0),
                hdr_reset: AtomicU64::new(0),
                boundary: AtomicF64::new(if local_only { 0.0 } else { f64::INFINITY }),
            },
            pushed: AtomicU64::new(0),
            inflight: AtomicU64::new(0),
            syncing: AtomicBool::new(false),
            waiters: AtomicU64::new(0),
            denied: AtomicU64::new(0),
            synced: Notify::new(),
            send_seq: AtomicU64::new(0),
            done_seq: AtomicU64::new(0),
            last_sync: AtomicF64::new(f64::NEG_INFINITY),
            ewma: AtomicF64::new(0.0),
            exhausted_until: AtomicF64::new(f64::NEG_INFINITY),
            old_a: AtomicU64::new(0),
            old_t: AtomicF64::new(0.0),
            min_window_ms: windows.iter().map(|w| w.window_ms).min().unwrap_or(1_000),
            view: ArcSwap::from_pointee(RateView {
                at_mono: f64::NEG_INFINITY,
                windows: vec![WindowState::default(); n],
                reserved_others: 0,
                fresh: false,
            }),
            credit_lock: Mutex::new(()),
            degraded_windows: Mutex::new((0, windows)),
        }
    }

    pub(crate) fn same_config(&self, c: &RateLimitConfig, has_store: bool) -> bool {
        let alg: Algorithm = c.algorithm.into();
        config_hash(alg, &c.windows()) == self.cfg_hash
            && (c.is_local_only() || !has_store) == self.local_only
            && self.cfg == *c
    }

    /// Hits this replica admitted that the view does not reflect yet.
    #[inline]
    pub(crate) fn local_extra(&self) -> f64 {
        let a = self.hot.admitted.load(Relaxed);
        let p = self.pushed.load(Relaxed);
        (a.saturating_sub(p) + self.inflight.load(Relaxed)) as f64
    }

    /// Unused credit and whether it is still backed by a reservation.
    pub(crate) fn unused(&self) -> u64 {
        self.hot
            .credit
            .load(Relaxed)
            .saturating_sub(self.hot.admitted.load(Relaxed))
    }

    /// Estimated usage fraction of the busiest window (adaptive sync).
    pub(crate) fn load_fraction(&self, t: f64) -> f64 {
        let v = self.view.load();
        let extra = self.local_extra();
        self.windows
            .iter()
            .zip(v.windows.iter())
            .map(|(w, s)| (algo::usage(self.alg, w, *s, t) + extra) / w.capacity(self.alg))
            .fold(0.0, f64::max)
    }

    /// Refresh the precomputed headers (after a sync).
    pub(crate) fn snapshot_headers(&self, t: f64) {
        let a = self.hot.admitted.load(Relaxed);
        let h = self.headers(t, self.local_extra(), None);
        self.hot.hdr_admitted.store(a, Relaxed);
        self.hot.hdr_remaining.store(h.remaining, Relaxed);
        self.hot.hdr_reset.store(h.reset, Relaxed);
    }

    /// Headers for an admit whose `admitted` count became `a + 1`.
    #[inline]
    fn allow_headers(&self, t: f64, a: u64) -> RateHeaders {
        let reset = self.hot.hdr_reset.load(Relaxed);
        if t < reset as f64 * 1000.0 {
            let since = (a + 1).saturating_sub(self.hot.hdr_admitted.load(Relaxed));
            RateHeaders {
                limit: self.windows[self.head].limit,
                remaining: self.hot.hdr_remaining.load(Relaxed).saturating_sub(since),
                reset,
                retry_after: None,
            }
        } else {
            // The window rolled since the last sync: evaluate it.
            self.headers(t, self.local_extra(), None)
        }
    }

    fn headers(&self, t: f64, extra: f64, retry_after_ms: Option<f64>) -> RateHeaders {
        let v = self.view.load();
        let mut remaining = f64::INFINITY;
        for (w, s) in self.windows.iter().zip(v.windows.iter()) {
            remaining = remaining.min(algo::free(self.alg, w, *s, t) - extra);
        }
        let head_w = &self.windows[self.head];
        let head_s = v.windows[self.head];
        let reset_ms = algo::reset_ms(self.alg, head_w, head_s, t);
        RateHeaders {
            limit: head_w.limit,
            remaining: remaining.max(0.0).floor() as u64,
            reset: ((t + reset_ms) / 1000.0).ceil().max(0.0) as u64,
            retry_after: retry_after_ms.map(|ms| ((ms / 1000.0).ceil() as u64).max(1)),
        }
    }

    /// Time until every window admits one more request given `reserved` extra units.
    fn retry_after_ms(&self, t: f64, reserved: f64) -> f64 {
        let v = self.view.load();
        self.windows
            .iter()
            .zip(v.windows.iter())
            .map(|(w, s)| algo::retry_after_ms(self.alg, w, *s, t, reserved))
            .fold(0.0, f64::max)
    }

    /// Some window certainly cannot take another request (usage alone, no reservations).
    fn certainly_full(&self, t: f64, extra: f64) -> bool {
        let v = self.view.load();
        self.windows
            .iter()
            .zip(v.windows.iter())
            .any(|(w, s)| algo::free(self.alg, w, *s, t) - extra < 1.0)
    }

    /// Credit a cold key may use before its first sync: at most its replica share and one
    /// interval of the configured rate, and never more than the last view shows as free.
    fn cold_credit(&self, t: f64, replicas: u32, allowance: f64) -> u64 {
        let n = f64::from(replicas.max(1));
        let v = self.view.load();
        let extra = self.local_extra();
        let mut c = f64::INFINITY;
        for (w, s) in self.windows.iter().zip(v.windows.iter()) {
            let cap = w.capacity(self.alg);
            let share = (allowance.min(1.0) * cap / n).floor();
            let interval = (cap * self.cfg.sync_interval_ms as f64 / w.window_ms as f64).ceil();
            let mut room = share.min(interval);
            if v.fresh {
                room = room.min((algo::free(self.alg, w, *s, t) - extra).floor());
            }
            c = c.min(room);
        }
        c.max(0.0) as u64
    }
}

impl Inner {
    /// Admit from credit. No I/O, no locks.
    #[inline]
    pub(crate) fn fast(&self, r: &RateKey, now: f64, outcome: Outcome) -> Option<Decision> {
        let h = &r.hot;
        if now >= h.valid_until.load() {
            return None;
        }
        let t = self.store_now(now);
        if t >= h.boundary.load() {
            return None;
        }
        let a = h.admitted.fetch_add(1, Relaxed);
        let c = h.credit.load(Relaxed);
        if a >= c {
            h.admitted.fetch_sub(1, Relaxed);
            return None;
        }
        if c - a - 1 < h.half.load(Relaxed) && !h.wanted.load(Relaxed) {
            h.wanted.store(true, Relaxed);
            self.wake.notify_one();
        }
        self.stats.record(outcome);
        Some(Decision::Allow(r.allow_headers(t, a)))
    }

    fn deny(&self, r: &RateKey, now: f64, outcome: Outcome, with_reservations: bool) -> Decision {
        let t = self.store_now(now);
        let extra = r.local_extra();
        // Others may hold admits this replica cannot see yet: their reservations, plus about one
        // unreserved cold-start admit each. Counting them keeps Retry-After from undershooting.
        let unseen = f64::from(self.replicas.load(Relaxed).max(1) - 1);
        let reserved = if with_reservations {
            extra + r.view.load().reserved_others as f64 + unseen
        } else {
            extra + unseen
        };
        let ra = r.retry_after_ms(t, reserved).max(1.0);
        tracing::trace!(
            store_key = %r.store_key, ?outcome, t, reserved, extra, retry_after_ms = ra,
            "resil: deny"
        );
        r.denied.fetch_add(1, Relaxed);
        self.stats.record(outcome);
        let mut h = r.headers(t, extra, Some(ra));
        h.remaining = 0;
        Decision::Deny(h)
    }

    /// Everything that is not a credit admit.
    pub(crate) async fn slow(self: &Arc<Self>, r: Arc<RateKey>, now: f64) -> Decision {
        if r.local_only {
            return self.local_only(&r, now);
        }
        if self.degraded(now, r.cfg.cache_ttl_ms) {
            return self.degraded_decide(&r, now);
        }
        if now < r.exhausted_until.load() {
            return self.deny(&r, now, Outcome::DenyLocal, true);
        }
        let t = self.store_now(now);
        let (fresh, age) = {
            let v = r.view.load();
            (v.fresh, now - v.at_mono)
        };
        if fresh && r.certainly_full(t, r.local_extra()) {
            if age <= r.cfg.cache_ttl_ms as f64 {
                self.wake.notify_one();
                return self.deny(&r, now, Outcome::DenyLocal, true);
            }
            // Too old to deny on (others may have freed or used budget since): ask the store.
            return self.wait_for_sync(r).await;
        }
        // Cold (or lapsed) credit: admit from a small unreserved share and sync right away.
        if now >= r.hot.valid_until.load() && !r.syncing.load(Relaxed) {
            if let Some(d) = self.cold(&r, now) {
                return d;
            }
        }
        self.wait_for_sync(r).await
    }

    fn cold(self: &Arc<Self>, r: &Arc<RateKey>, now: f64) -> Option<Decision> {
        {
            let _g = r.credit_lock.lock().unwrap_or_else(|e| e.into_inner());
            if now < r.hot.valid_until.load() {
                drop(_g);
                return self.fast(r, now, Outcome::AllowLocal);
            }
            if self.recovering(now, r.cfg.sync_interval_ms) {
                return None;
            }
            let c0 = r.cold_credit(
                self.store_now(now),
                self.replicas.load(Relaxed),
                r.cfg.local_allowance,
            );
            if c0 == 0 {
                return None;
            }
            let a0 = r.hot.admitted.load(Relaxed);
            r.hot.credit.store(a0 + c0, Relaxed);
            r.hot.half.store(c0 / 2, Relaxed);
            r.hot.valid_until.store(now + self.credit_ttl_ms());
        }
        self.kick(r);
        self.fast(r, now, Outcome::AllowCold)
    }

    /// Start a sync of this key now unless one is in flight.
    pub(crate) fn kick(self: &Arc<Self>, r: &Arc<RateKey>) {
        if r.syncing
            .compare_exchange(false, true, Relaxed, Relaxed)
            .is_ok()
        {
            let inner = self.clone();
            let r = r.clone();
            tokio::spawn(async move {
                let _ = inner.sync_claimed(vec![r], vec![], false).await;
            });
        }
    }

    async fn wait_for_sync(self: &Arc<Self>, r: Arc<RateKey>) -> Decision {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(r.cfg.redis_timeout_ms);
        r.waiters.fetch_add(1, Relaxed);
        // A sync that was built before we registered does not carry our `need`.
        let registered = r.send_seq.load(Relaxed);
        let mut timed_out = false;
        loop {
            let notified = r.synced.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.kick(&r);
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                timed_out = true;
                break;
            }
            let now = self.clock.mono_ms();
            if let Some(d) = self.fast(&r, now, Outcome::AllowSync) {
                r.waiters.fetch_sub(1, Relaxed);
                return d;
            }
            if r.done_seq.load(Relaxed) > registered || self.failing.load(Relaxed) {
                break;
            }
        }
        r.waiters.fetch_sub(1, Relaxed);
        let now = self.clock.mono_ms();
        if let Some(d) = self.fast(&r, now, Outcome::AllowSync) {
            return d;
        }
        if timed_out || self.failing.load(Relaxed) {
            // The store did not answer in time: decide on the (stale) view.
            let t = self.store_now(now);
            if !r.certainly_full(t, r.local_extra()) {
                r.hot.admitted.fetch_add(1, Relaxed);
                self.stats.record(Outcome::AllowOverrun);
                return Decision::Allow(r.headers(t, r.local_extra(), None));
            }
            return self.deny(&r, now, Outcome::DenyLocal, false);
        }
        self.deny(&r, now, Outcome::DenySync, true)
    }

    /// Store unavailable: fail-static at `⌈L/N_last⌉` per replica, or fail-open.
    fn degraded_decide(&self, r: &RateKey, now: f64) -> Decision {
        // Credit still backed by a live reservation is safe to use.
        if let Some(d) = self.fast(r, now, Outcome::AllowLocal) {
            return d;
        }
        let t = self.store_now(now);
        if self.opts.on_store_unavailable == OnStoreUnavailable::Allow {
            r.hot.admitted.fetch_add(1, Relaxed);
            self.stats.record(Outcome::AllowDegraded);
            return Decision::Allow(r.headers(t, r.local_extra(), None));
        }
        let n = self.replicas.load(Relaxed).max(1);
        let windows = {
            let mut g = r.degraded_windows.lock().unwrap_or_else(|e| e.into_inner());
            if g.0 != n {
                let scaled: Vec<WindowSpec> = r
                    .windows
                    .iter()
                    .map(|w| WindowSpec {
                        limit: w.limit.div_ceil(u64::from(n)).max(1),
                        window_ms: w.window_ms,
                        burst: w.burst.div_ceil(u64::from(n)).max(1),
                    })
                    .collect();
                *g = (n, Arc::from(scaled));
            }
            g.1.clone()
        };
        let reply = self.local.sync_rate(&RateSync {
            key: r.degraded_key.clone(),
            alg: r.alg,
            windows: windows.clone(),
            pod: self.pod.clone(),
            hits: 0,
            hits_old: 0,
            t_old_ms: 0.0,
            unused: 0,
            want: 0,
            need: 1,
            allowance: 1.0,
            replicas: 1,
            lease_ttl_ms: 1,
            now_override_ms: Some(t),
        });
        if reply.direct == 1 {
            // Counted, so the store gets paid back when it returns.
            r.hot.admitted.fetch_add(1, Relaxed);
            self.stats.record(Outcome::AllowDegraded);
            return Decision::Allow(r.headers(t, r.local_extra(), None));
        }
        let ra = windows
            .iter()
            .zip(reply.windows.iter())
            .map(|(w, s)| algo::retry_after_ms(r.alg, w, *s, t, 0.0))
            .fold(1.0, f64::max);
        self.stats.record(Outcome::DenyDegraded);
        r.denied.fetch_add(1, Relaxed);
        let mut h = r.headers(t, r.local_extra(), Some(ra));
        h.remaining = 0;
        Decision::Deny(h)
    }

    /// `local_allowance >= 1` or no store: the whole budget is this replica's. Credit is refilled
    /// inline from the in-process engine, so there is never a wait.
    fn local_only(&self, r: &RateKey, now: f64) -> Decision {
        let t = self.store_now(now);
        {
            let _g = r.credit_lock.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(d) = self.fast(r, now, Outcome::AllowLocal) {
                return d;
            }
            let a = r.hot.admitted.load(Relaxed);
            let p = r.pushed.load(Relaxed);
            let pending = a.saturating_sub(p);
            // Past a window boundary: what was admitted so far belongs to the old window.
            let boundary = r.hot.boundary.load();
            let crossed = boundary > 0.0 && t >= boundary;
            let reply = self.local.sync_rate(&RateSync {
                key: r.store_key.clone(),
                alg: r.alg,
                windows: r.windows.clone(),
                pod: self.pod.clone(),
                hits: if crossed { 0 } else { pending },
                hits_old: if crossed { pending } else { 0 },
                t_old_ms: boundary - 1.0,
                unused: r.unused(),
                want: r.min_capacity.max(1),
                need: 0,
                allowance: 1.0,
                replicas: 1,
                lease_ttl_ms: u64::MAX / 4,
                now_override_ms: Some(t),
            });
            r.view.store(Arc::new(RateView {
                at_mono: now,
                windows: reply.windows,
                reserved_others: 0,
                fresh: true,
            }));
            r.pushed.store(a, Relaxed);
            r.hot.credit.store(a + reply.kept + reply.granted, Relaxed);
            r.hot.half.store(0, Relaxed);
            r.hot.valid_until.store(f64::INFINITY);
            let w = r.min_window_ms as f64;
            r.hot.boundary.store(((t / w).floor() + 1.0) * w);
            r.snapshot_headers(t);
            if let Some(d) = self.fast(r, now, Outcome::AllowLocal) {
                return d;
            }
        }
        self.deny(r, now, Outcome::DenyLocal, false)
    }

    /// Build the sync request for a claimed key.
    pub(crate) fn rate_request(&self, r: &Arc<RateKey>, now: f64) -> (RateSync, RateSent) {
        let a = r.hot.admitted.load(Relaxed);
        let p = r.pushed.load(Relaxed);
        let old_a = r.old_a.load(Relaxed).min(a);
        let (hits_old, t_old_ms) = if old_a > p {
            (old_a - p, r.old_t.load())
        } else {
            (0, 0.0)
        };
        let hits = a.saturating_sub(p) - hits_old;
        let last = r.last_sync.load();
        let prev = r.ewma.load();
        let ewma = if last.is_finite() {
            let sample = (hits + hits_old) as f64 / (now - last).max(1.0);
            if prev == 0.0 {
                sample
            } else {
                0.5 * prev + 0.5 * sample
            }
        } else {
            // First sync of a cold key: assume its hits arrived within one interval, so the first
            // grant is generous rather than a trickle of early syncs while the estimate builds.
            (hits + hits_old).max(1) as f64 / r.cfg.sync_interval_ms as f64
        };
        // Same notion of "busy" as the scheduler (sync.rs): an early sync is not a reason to
        // shrink the horizon, or small grants feed more early syncs.
        let hot = r.denied.load(Relaxed) > 0 || r.load_fraction(self.store_now(now)) > 0.5;
        let horizon = if hot {
            r.cfg.sync_interval_ms
        } else {
            self.opts.slow_sync_ms
        } as f64;
        let recovering = self.recovering(now, r.cfg.sync_interval_ms);
        let need = if recovering {
            0
        } else {
            r.waiters.load(Relaxed)
        };
        // Enough for 1.5 intervals before the half-credit early sync fires, so a steady key
        // refills on its schedule rather than early.
        let target = ((ewma * horizon * 3.0).ceil() as u64)
            .max(1)
            .min(r.min_capacity.max(1));
        let unused = r.unused().min(target);
        let want = if recovering {
            0
        } else {
            target.saturating_sub(unused)
        };
        tracing::trace!(
            store_key = %r.store_key, hits, ewma, target, unused, want, need, "resil: sync request"
        );
        let seq = r.send_seq.fetch_add(1, Relaxed) + 1;
        r.pushed.store(a, Relaxed);
        r.inflight.store(hits + hits_old, Relaxed);
        r.last_sync.store(now);
        let req = RateSync {
            key: r.store_key.clone(),
            alg: r.alg,
            windows: r.windows.clone(),
            pod: self.pod.clone(),
            hits,
            hits_old,
            t_old_ms,
            unused,
            want,
            need,
            allowance: r.cfg.local_allowance.min(1.0),
            replicas: self.replicas.load(Relaxed).max(1),
            lease_ttl_ms: self.opts.lease_ttl_ms,
            now_override_ms: None,
        };
        let sent = RateSent {
            key: r.clone(),
            a_send: a,
            pushed_before: p,
            hits: hits + hits_old,
            send_mono: now,
            seq,
            ewma,
        };
        (req, sent)
    }

    pub(crate) fn apply_rate(&self, s: RateSent, reply: RateReply, recv_mono: f64) {
        let r = &s.key;
        {
            let _g = r.credit_lock.lock().unwrap_or_else(|e| e.into_inner());
            r.view.store(Arc::new(RateView {
                at_mono: recv_mono,
                windows: reply.windows,
                reserved_others: reply.reserved_others,
                fresh: true,
            }));
            r.pushed.store(s.a_send + reply.direct, Relaxed);
            r.inflight.store(0, Relaxed);
            let own = reply.kept + reply.granted + reply.direct;
            let mut credit = s.a_send + own;
            if own == 0 && reply.avail >= 1 && self.opts.last_mile == LastMile::Local {
                credit += 1;
            }
            r.hot.credit.store(credit, Relaxed);
            r.hot.half.store((reply.kept + reply.granted) / 2, Relaxed);
            r.hot.valid_until.store(s.send_mono + self.credit_ttl_ms());
            r.hot.wanted.store(false, Relaxed);
            r.denied.store(0, Relaxed);
            r.ewma.store(s.ewma);
            // Nothing left to grant anyone: deny locally until the budget can have moved.
            // (avail ≥ 1 with no grant is last-mile mode: requests keep asking the store.)
            if own == 0 && credit == s.a_send && reply.avail < 1 {
                let t = self.store_now(recv_mono);
                let reserved = reply.reserved_others as f64
                    + r.local_extra()
                    + f64::from(self.replicas.load(Relaxed).max(1) - 1);
                let ra = r
                    .retry_after_ms(t, reserved)
                    .clamp(1.0, r.cfg.sync_interval_ms as f64);
                r.exhausted_until.store(recv_mono + ra);
            } else {
                r.exhausted_until.store(f64::NEG_INFINITY);
            }
            r.done_seq.store(s.seq, Relaxed);
            if r.old_a.load(Relaxed) <= s.a_send {
                r.old_a.store(0, Relaxed);
            }
            r.snapshot_headers(self.store_now(recv_mono));
        }
        r.syncing.store(false, Relaxed);
        r.synced.notify_waiters();
    }

    pub(crate) fn revert_rate(&self, s: RateSent) {
        let r = &s.key;
        {
            let _g = r.credit_lock.lock().unwrap_or_else(|e| e.into_inner());
            r.pushed.store(s.pushed_before, Relaxed);
            r.inflight.store(0, Relaxed);
            let _ = s.hits;
        }
        r.syncing.store(false, Relaxed);
        r.synced.notify_waiters();
    }
}
