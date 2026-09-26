//! Concurrency caps (`max_concurrent`, FRD §5.8): the same credit scheme applied to a count. Each
//! replica publishes its absolute count plus a small reservation; the sum over live replicas never
//! exceeds the cap while the store is healthy, and a crashed replica's slots expire with its lease.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use super::clock::AtomicF64;
use super::stats::Outcome;
use super::store::{ConcReply, ConcSync};
use super::{Inner, OnStoreUnavailable};
use crate::policy::RateLimitConfig;

#[repr(align(128))]
pub(crate) struct ConcHot {
    pub(crate) active: AtomicU64,
    pub(crate) allowed: AtomicU64,
    pub(crate) valid_until: AtomicF64,
}

pub(crate) struct ConcKey {
    pub(crate) subject: Arc<str>,
    pub(crate) store_key: Arc<str>,
    pub(crate) max: u64,
    pub(crate) tuning: RateLimitConfig,
    pub(crate) local_only: bool,
    pub(crate) hot: ConcHot,
    pub(crate) acquired: AtomicU64,
    pub(crate) acquired_at_sync: AtomicU64,
    pub(crate) syncing: AtomicBool,
    pub(crate) waiters: AtomicU64,
    pub(crate) synced: Notify,
    pub(crate) send_seq: AtomicU64,
    pub(crate) done_seq: AtomicU64,
    pub(crate) last_sync: AtomicF64,
    pub(crate) ewma: AtomicF64,
    pub(crate) others: AtomicU64,
    pub(crate) credit_lock: Mutex<()>,
}

pub(crate) struct ConcSent {
    pub(crate) key: Arc<ConcKey>,
    pub(crate) active_send: u64,
    pub(crate) acquired_send: u64,
    pub(crate) send_mono: f64,
    pub(crate) seq: u64,
    pub(crate) ewma: f64,
}

/// A held concurrency slot; released on drop.
pub struct ConcurrencyGuard {
    key: Arc<ConcKey>,
}

impl std::fmt::Debug for ConcurrencyGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConcurrencyGuard")
            .field("subject", &self.key.subject)
            .finish()
    }
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        self.key.hot.active.fetch_sub(1, Relaxed);
    }
}

/// `max_concurrent` reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConcurrencyDenied {
    pub max: u64,
    /// Whole seconds (always 1: a slot can free at any moment).
    pub retry_after: u64,
}

impl ConcKey {
    pub(crate) fn new(
        inner: &Inner,
        subject: Arc<str>,
        max: u32,
        tuning: &RateLimitConfig,
    ) -> Self {
        let store_key: Arc<str> = Arc::from(format!("cc2:{{{}:{}}}:{max}", inner.service, subject));
        Self {
            subject,
            store_key,
            max: u64::from(max),
            tuning: tuning.clone(),
            local_only: tuning.is_local_only() || inner.store.is_none(),
            hot: ConcHot {
                active: AtomicU64::new(0),
                allowed: AtomicU64::new(0),
                valid_until: AtomicF64::new(f64::NEG_INFINITY),
            },
            acquired: AtomicU64::new(0),
            acquired_at_sync: AtomicU64::new(0),
            syncing: AtomicBool::new(false),
            waiters: AtomicU64::new(0),
            synced: Notify::new(),
            send_seq: AtomicU64::new(0),
            done_seq: AtomicU64::new(0),
            last_sync: AtomicF64::new(f64::NEG_INFINITY),
            ewma: AtomicF64::new(0.0),
            others: AtomicU64::new(0),
            credit_lock: Mutex::new(()),
        }
    }

    pub(crate) fn same_config(&self, max: u32, tuning: &RateLimitConfig, has_store: bool) -> bool {
        self.max == u64::from(max)
            && (tuning.is_local_only() || !has_store) == self.local_only
            && self.tuning.sync_interval_ms == tuning.sync_interval_ms
            && self.tuning.redis_timeout_ms == tuning.redis_timeout_ms
            && self.tuning.cache_ttl_ms == tuning.cache_ttl_ms
            && self.tuning.local_allowance == tuning.local_allowance
    }

    pub(crate) fn active(&self) -> u64 {
        self.hot.active.load(Relaxed)
    }

    fn denied(&self) -> ConcurrencyDenied {
        ConcurrencyDenied {
            max: self.max,
            retry_after: 1,
        }
    }
}

impl Inner {
    #[inline]
    fn conc_fast(
        &self,
        c: &Arc<ConcKey>,
        now: f64,
        cap: Option<u64>,
        outcome: Outcome,
    ) -> Option<ConcurrencyGuard> {
        let h = &c.hot;
        let limit = match cap {
            Some(cap) => cap,
            None => {
                if now >= h.valid_until.load() {
                    return None;
                }
                h.allowed.load(Relaxed)
            }
        };
        let a = h.active.fetch_add(1, Relaxed);
        if a >= limit {
            h.active.fetch_sub(1, Relaxed);
            return None;
        }
        c.acquired.fetch_add(1, Relaxed);
        self.stats.record(outcome);
        Some(ConcurrencyGuard { key: c.clone() })
    }

    pub(crate) async fn acquire(
        self: &Arc<Self>,
        c: Arc<ConcKey>,
        now: f64,
    ) -> Result<ConcurrencyGuard, ConcurrencyDenied> {
        if c.local_only {
            return self
                .conc_fast(&c, now, Some(c.max), Outcome::AllowLocal)
                .ok_or_else(|| {
                    self.stats.record(Outcome::DenyLocal);
                    c.denied()
                });
        }
        if let Some(g) = self.conc_fast(&c, now, None, Outcome::AllowLocal) {
            return Ok(g);
        }
        if self.degraded(now, c.tuning.cache_ttl_ms) {
            if self.opts.on_store_unavailable == OnStoreUnavailable::Allow {
                c.hot.active.fetch_add(1, Relaxed);
                c.acquired.fetch_add(1, Relaxed);
                self.stats.record(Outcome::AllowDegraded);
                return Ok(ConcurrencyGuard { key: c.clone() });
            }
            let n = u64::from(self.replicas.load(Relaxed).max(1));
            return self
                .conc_fast(
                    &c,
                    now,
                    Some(c.max.div_ceil(n).max(1)),
                    Outcome::AllowDegraded,
                )
                .ok_or_else(|| {
                    self.stats.record(Outcome::DenyDegraded);
                    c.denied()
                });
        }
        // Cold: a small unreserved allowance, and sync at once.
        if now >= c.hot.valid_until.load()
            && !c.syncing.load(Relaxed)
            && !self.recovering(now, c.tuning.sync_interval_ms)
        {
            let granted = {
                let _g = c.credit_lock.lock().unwrap_or_else(|e| e.into_inner());
                if now >= c.hot.valid_until.load() {
                    let n = f64::from(self.replicas.load(Relaxed).max(1));
                    let a = c.tuning.local_allowance.min(1.0);
                    let known_others = c.others.load(Relaxed);
                    let free = c.max.saturating_sub(known_others) as f64;
                    let c0 = (a * free / n).floor() as u64;
                    if c0 > 0 {
                        let active = c.hot.active.load(Relaxed);
                        c.hot.allowed.store(active + c0, Relaxed);
                        c.hot.valid_until.store(now + self.credit_ttl_ms());
                        true
                    } else {
                        false
                    }
                } else {
                    true
                }
            };
            if granted {
                self.kick_conc(&c);
                if let Some(g) = self.conc_fast(&c, now, None, Outcome::AllowCold) {
                    return Ok(g);
                }
            }
        }
        // Wait for a sync (bounded).
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(c.tuning.redis_timeout_ms);
        c.waiters.fetch_add(1, Relaxed);
        let registered = c.send_seq.load(Relaxed);
        loop {
            let notified = c.synced.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.kick_conc(&c);
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                break;
            }
            let now = self.clock.mono_ms();
            if let Some(g) = self.conc_fast(&c, now, None, Outcome::AllowSync) {
                c.waiters.fetch_sub(1, Relaxed);
                return Ok(g);
            }
            if c.done_seq.load(Relaxed) > registered || self.failing.load(Relaxed) {
                break;
            }
        }
        c.waiters.fetch_sub(1, Relaxed);
        let now = self.clock.mono_ms();
        if let Some(g) = self.conc_fast(&c, now, None, Outcome::AllowSync) {
            return Ok(g);
        }
        self.stats.record(Outcome::DenySync);
        Err(c.denied())
    }

    pub(crate) fn kick_conc(self: &Arc<Self>, c: &Arc<ConcKey>) {
        if c.syncing
            .compare_exchange(false, true, Relaxed, Relaxed)
            .is_ok()
        {
            let inner = self.clone();
            let c = c.clone();
            tokio::spawn(async move {
                let _ = inner.sync_claimed(vec![], vec![c], false).await;
            });
        }
    }

    pub(crate) fn conc_request(
        &self,
        c: &Arc<ConcKey>,
        now: f64,
        horizon_ms: f64,
    ) -> (ConcSync, ConcSent) {
        let active = c.hot.active.load(Relaxed);
        let allowed = c.hot.allowed.load(Relaxed);
        let acquired = c.acquired.load(Relaxed);
        let since = acquired.saturating_sub(c.acquired_at_sync.load(Relaxed));
        let last = c.last_sync.load();
        let prev = c.ewma.load();
        let ewma = if last.is_finite() {
            let sample = since as f64 / (now - last).max(1.0);
            if prev == 0.0 {
                sample
            } else {
                0.5 * prev + 0.5 * sample
            }
        } else {
            prev
        };
        let recovering = self.recovering(now, c.tuning.sync_interval_ms);
        let need = if recovering {
            0
        } else {
            c.waiters.load(Relaxed)
        };
        let target = if recovering {
            0
        } else if since > 0 || need > 0 {
            ((ewma * horizon_ms * 2.0).ceil() as u64).max(1).min(c.max)
        } else {
            0
        };
        let unused = allowed.saturating_sub(active).min(target);
        let want = target.saturating_sub(unused);
        let seq = c.send_seq.fetch_add(1, Relaxed) + 1;
        c.last_sync.store(now);
        let req = ConcSync {
            key: c.store_key.clone(),
            pod: self.pod.clone(),
            active,
            unused,
            want,
            need,
            max: c.max,
            allowance: c.tuning.local_allowance.min(1.0),
            replicas: self.replicas.load(Relaxed).max(1),
            lease_ttl_ms: self.opts.lease_ttl_ms.max(5_000),
            now_override_ms: None,
        };
        (
            req,
            ConcSent {
                key: c.clone(),
                active_send: active,
                acquired_send: acquired,
                send_mono: now,
                seq,
                ewma,
            },
        )
    }

    pub(crate) fn apply_conc(&self, s: ConcSent, reply: ConcReply) {
        let c = &s.key;
        {
            let _g = c.credit_lock.lock().unwrap_or_else(|e| e.into_inner());
            c.hot.allowed.store(
                s.active_send + reply.kept + reply.granted + reply.direct,
                Relaxed,
            );
            c.hot.valid_until.store(s.send_mono + self.credit_ttl_ms());
            c.acquired_at_sync.store(s.acquired_send, Relaxed);
            c.others.store(reply.others, Relaxed);
            c.ewma.store(s.ewma);
            c.done_seq.store(s.seq, Relaxed);
        }
        c.syncing.store(false, Relaxed);
        c.synced.notify_waiters();
    }

    pub(crate) fn revert_conc(&self, s: ConcSent) {
        s.key.syncing.store(false, Relaxed);
        s.key.synced.notify_waiters();
    }
}
