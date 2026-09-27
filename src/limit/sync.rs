//! The sync task: pick the keys that are due, push them in one round-trip, apply the replies.

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;

use super::conc::{ConcKey, ConcSent};
use super::rate::{RateKey, RateSent};
use super::store::{Batch, Heartbeat, StoreError};
use super::Inner;

/// Snapshot admits this long before each second boundary (store time): the grid every window
/// (1 s, 1 min, 1 h) is aligned to.
const BOUNDARY_LEAD_MS: f64 = 2.0;

pub(crate) async fn run(inner: Arc<Inner>) {
    let mut tick = tokio::time::interval(Duration::from_millis(inner.opts.tick_ms.max(1)));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_export = inner.clock.mono_ms();
    loop {
        let boundary = inner.next_boundary();
        tokio::select! {
            _ = tick.tick() => {}
            _ = inner.wake.notified() => {}
            _ = tokio::time::sleep(boundary.1) => inner.snapshot_boundary(boundary.0),
        }
        if inner.stopped.load(Relaxed) {
            return;
        }
        if inner.store.is_some() {
            // Never block the loop on a slow store: rounds run concurrently, bounded.
            if let Ok(permit) = inner.rounds.clone().try_acquire_owned() {
                let inner2 = inner.clone();
                tokio::spawn(async move {
                    let _p = permit;
                    let _ = inner2.round(false, false).await;
                });
            }
        }
        let now = inner.clock.mono_ms();
        if now - last_export >= 1_000.0 {
            last_export = now;
            inner.stats.export(inner.service);
            metrics::gauge!("resil_replicas", "svc" => inner.service)
                .set(f64::from(inner.replicas.load(Relaxed)));
            inner.export_active();
        }
    }
}

impl Inner {
    /// `resil_active{svc,key}`: concurrency slots this replica holds per capped key.
    pub(crate) fn export_active(&self) {
        for (subject, key) in self.keys.pin().iter() {
            if let Some(c) = &key.conc {
                metrics::gauge!("resil_active", "svc" => self.service, "key" => subject.to_string())
                    .set(c.active() as f64);
            }
        }
    }

    /// The next second boundary in store time and how long until the snapshot before it.
    fn next_boundary(&self) -> (f64, Duration) {
        let now = self.clock.mono_ms();
        let t = self.store_now(now);
        let mut b = ((t / 1000.0).floor() + 1.0) * 1000.0;
        if b - BOUNDARY_LEAD_MS <= t {
            b += 1000.0;
        }
        (
            b,
            Duration::from_secs_f64((b - BOUNDARY_LEAD_MS - t).max(0.0) / 1000.0),
        )
    }

    /// Just before a boundary: whatever each key admitted so far belongs to the ending window.
    pub(crate) fn snapshot_boundary(&self, boundary: f64) {
        let t_old = boundary - BOUNDARY_LEAD_MS;
        for (_, key) in self.keys.pin().iter() {
            let Some(r) = &key.rate else { continue };
            if r.local_only || boundary % r.min_window_ms as f64 != 0.0 {
                continue;
            }
            let a = r.hot.admitted.load(Relaxed);
            if a > r.pushed.load(Relaxed) {
                r.old_t.store(t_old);
                r.old_a.store(a, Relaxed);
            }
        }
    }

    /// Collect due keys and sync them. `force` syncs every key; `heartbeat` forces a heartbeat.
    pub(crate) async fn round(
        self: &Arc<Self>,
        force: bool,
        heartbeat: bool,
    ) -> Result<(), StoreError> {
        if self.store.is_none() {
            return Ok(());
        }
        let now = self.clock.mono_ms();
        let mut rates: Vec<Arc<RateKey>> = Vec::new();
        let mut concs: Vec<Arc<ConcKey>> = Vec::new();
        let cap = self.opts.max_keys_per_round.max(1);
        let since_hb = now - self.last_heartbeat.load();
        let hb_strict = heartbeat || since_hb >= self.opts.heartbeat_ms as f64;
        // Strict pass first; if a round-trip happens anyway, keys (and the heartbeat) that are at
        // least half due ride along, so a quiet replica makes ~one round-trip per second.
        for slack in [1.0, 0.5] {
            if slack < 1.0 && rates.is_empty() && concs.is_empty() && !hb_strict {
                break;
            }
            let keys = self.keys.pin();
            for (_, key) in keys.iter() {
                if rates.len() + concs.len() >= cap {
                    break;
                }
                if let Some(r) = &key.rate {
                    if !r.local_only && (force || self.rate_due(r, now, slack)) && claim(&r.syncing)
                    {
                        rates.push(r.clone());
                    }
                }
                if let Some(c) = &key.conc {
                    if !c.local_only && (force || self.conc_due(c, now, slack)) && claim(&c.syncing)
                    {
                        concs.push(c.clone());
                    }
                }
            }
        }
        if rates.is_empty() && concs.is_empty() && !hb_strict {
            return Ok(());
        }
        let hb_due = hb_strict || since_hb >= self.opts.heartbeat_ms as f64 / 2.0;
        self.sync_claimed(rates, concs, hb_due).await
    }

    fn rate_due(&self, r: &RateKey, now: f64, slack: f64) -> bool {
        if r.syncing.load(Relaxed) {
            return false;
        }
        if r.hot.wanted.load(Relaxed) || r.waiters.load(Relaxed) > 0 {
            return true;
        }
        let since = now - r.last_sync.load();
        let pending = r.hot.admitted.load(Relaxed) > r.pushed.load(Relaxed);
        let denied = r.denied.load(Relaxed) > 0;
        if !pending && !denied {
            // Idle: give back a reservation this replica no longer needs.
            return r.unused() > 1
                && now < r.hot.valid_until.load()
                && since >= self.opts.slow_sync_ms as f64 * slack
                && r.ewma.load() * self.opts.slow_sync_ms as f64 * 2.0 < r.unused() as f64;
        }
        let busy = denied || r.load_fraction(self.store_now(now)) > 0.5;
        let interval = if busy {
            r.cfg.sync_interval_ms
        } else {
            self.opts.slow_sync_ms.max(r.cfg.sync_interval_ms)
        };
        since >= interval as f64 * slack
    }

    fn conc_due(&self, c: &ConcKey, now: f64, slack: f64) -> bool {
        if c.syncing.load(Relaxed) {
            return false;
        }
        if c.waiters.load(Relaxed) > 0 {
            return true;
        }
        let since = now - c.last_sync.load();
        let active = c.hot.active.load(Relaxed);
        let acquiring = c.acquired.load(Relaxed) > c.acquired_at_sync.load(Relaxed);
        let reserved = c.hot.allowed.load(Relaxed) > active;
        if !acquiring && active == 0 && !reserved {
            return false;
        }
        let busy = acquiring && (active + c.others.load(Relaxed)) * 2 > c.max;
        let interval = if busy {
            c.tuning.sync_interval_ms
        } else {
            self.opts.slow_sync_ms.max(c.tuning.sync_interval_ms)
        };
        since >= interval as f64 * slack
    }

    /// Sync keys whose `syncing` flag this caller already holds.
    pub(crate) async fn sync_claimed(
        self: &Arc<Self>,
        rates: Vec<Arc<RateKey>>,
        concs: Vec<Arc<ConcKey>>,
        heartbeat: bool,
    ) -> Result<(), StoreError> {
        let Some(store) = self.store.clone() else {
            for r in rates {
                r.syncing.store(false, Relaxed);
            }
            for c in concs {
                c.syncing.store(false, Relaxed);
            }
            return Ok(());
        };
        let now = self.clock.mono_ms();
        let mut batch = Batch::default();
        let mut rate_sent: Vec<RateSent> = Vec::with_capacity(rates.len());
        let mut conc_sent: Vec<ConcSent> = Vec::with_capacity(concs.len());
        for r in &rates {
            let (req, sent) = self.rate_request(r, now);
            batch.rate.push(req);
            rate_sent.push(sent);
        }
        for c in &concs {
            let horizon = self.opts.slow_sync_ms as f64;
            let (req, sent) = self.conc_request(c, now, horizon);
            batch.conc.push(req);
            conc_sent.push(sent);
        }
        if heartbeat {
            self.last_heartbeat.store(now);
            batch.heartbeat = Some(Heartbeat {
                key: self.pods_key.clone(),
                pod: self.pod.clone(),
                live_window_ms: self.opts.heartbeat_ms * 3,
                leave: false,
                now_override_ms: None,
            });
        }
        self.rounds_total.fetch_add(1, Relaxed);
        tracing::trace!(
            pod = %self.pod, rates = rates.len(), concs = concs.len(), heartbeat, "resil: sync round"
        );
        let started = std::time::Instant::now();
        let res = tokio::time::timeout(
            Duration::from_millis(self.opts.round_timeout_ms),
            store.round_trip(&batch),
        )
        .await;
        let recv = self.clock.mono_ms();
        metrics::histogram!("resil_sync_seconds", "svc" => self.service)
            .record(started.elapsed().as_secs_f64());
        match res {
            Ok(Ok(reply))
                if reply.rate.len() == rate_sent.len() && reply.conc.len() == conc_sent.len() =>
            {
                if self.failing.swap(false, Relaxed) {
                    self.recovered_at.store(recv);
                }
                self.last_ok.store(recv);
                let mid = (now + recv) / 2.0;
                if let Some(first) = reply
                    .heartbeat
                    .as_ref()
                    .map(|h| h.now_ms)
                    .or_else(|| reply.rate.first().map(|r| r.now_ms))
                    .or_else(|| reply.conc.first().map(|c| c.now_ms))
                {
                    let sample = first - mid;
                    if self.offset_known.swap(true, Relaxed) {
                        let prev = self.offset.load();
                        self.offset.store(prev + 0.2 * (sample - prev));
                    } else {
                        self.offset.store(sample);
                    }
                }
                if let Some(h) = &reply.heartbeat {
                    self.replicas.store(h.replicas.max(1), Relaxed);
                }
                for (s, rep) in rate_sent.into_iter().zip(reply.rate) {
                    self.apply_rate(s, rep, recv);
                }
                for (s, rep) in conc_sent.into_iter().zip(reply.conc) {
                    self.apply_conc(s, rep);
                }
                Ok(())
            }
            other => {
                let err = match other {
                    Ok(Ok(_)) => StoreError("store reply did not match the request".into()),
                    Ok(Err(e)) => e,
                    Err(_) => StoreError(format!(
                        "store round-trip exceeded {} ms",
                        self.opts.round_timeout_ms
                    )),
                };
                self.failing.store(true, Relaxed);
                self.sync_failures.fetch_add(1, Relaxed);
                metrics::counter!("resil_sync_failures_total", "svc" => self.service).increment(1);
                tracing::debug!(error = %err, "resil: limit store sync failed");
                for s in rate_sent {
                    self.revert_rate(s);
                }
                for s in conc_sent {
                    self.revert_conc(s);
                }
                Err(err)
            }
        }
    }

    /// Shutdown: push every outstanding hit, drop this replica's reservations, deregister.
    pub(crate) async fn final_round(self: &Arc<Self>) -> Result<(), StoreError> {
        let Some(store) = self.store.clone() else {
            return Ok(());
        };
        let _ = self.round(true, false).await;
        let mut batch = Batch::default();
        for (_, key) in self.keys.pin().iter() {
            if let Some(r) = &key.rate {
                batch.release.push((r.store_key.clone(), self.pod.clone()));
            }
            if let Some(c) = &key.conc {
                if c.hot.active.load(Relaxed) == 0 {
                    batch.release.push((c.store_key.clone(), self.pod.clone()));
                }
            }
        }
        batch.heartbeat = Some(Heartbeat {
            key: self.pods_key.clone(),
            pod: self.pod.clone(),
            live_window_ms: self.opts.heartbeat_ms * 3,
            leave: true,
            now_override_ms: None,
        });
        tokio::time::timeout(
            Duration::from_millis(self.opts.round_timeout_ms),
            store.round_trip(&batch),
        )
        .await
        .map_err(|_| StoreError("shutdown round-trip timed out".into()))?
        .map(|_| ())
    }
}

fn claim(flag: &std::sync::atomic::AtomicBool) -> bool {
    flag.compare_exchange(false, true, Relaxed, Relaxed).is_ok()
}

#[cfg(test)]
mod tests {
    use crate::limit::{Limiter, LimiterOptions};
    use crate::RateLimitConfig;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// The exported series exist with their labels: decisions per outcome/mode, and the
    /// per-key active concurrency gauge.
    #[tokio::test]
    async fn exports_decisions_and_active_slots() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let limiter = Limiter::new(
            LimiterOptions {
                service: "metrics-test".into(),
                ..Default::default()
            },
            None,
        );
        let local = RateLimitConfig {
            requests_per_minute: Some(1),
            local_allowance: 1.0,
            ..Default::default()
        };
        limiter.set_policy("dep", Some(&local), Some(3));
        let _ = limiter.check("dep").await;
        let _ = limiter.check("dep").await;
        let _slot = limiter.acquire("dep").await.unwrap();
        metrics::with_local_recorder(&recorder, || {
            limiter.inner.stats.export(limiter.inner.service);
            limiter.inner.export_active();
        });
        let snap = snapshotter.snapshot().into_vec();
        let find = |name: &str, label: (&str, &str)| {
            snap.iter().find(|(k, _, _, _)| {
                k.key().name() == name
                    && k.key()
                        .labels()
                        .any(|l| l.key() == label.0 && l.value() == label.1)
            })
        };
        let allow = find("resil_decisions_total", ("outcome", "allow")).expect("allow series");
        let deny = find("resil_decisions_total", ("outcome", "deny")).expect("deny series");
        let total = |v: &DebugValue| match v {
            DebugValue::Counter(c) => *c,
            _ => 0,
        };
        assert!(total(&allow.3) >= 1 && total(&deny.3) >= 1);
        let active = find("resil_active", ("key", "dep")).expect("active gauge");
        assert!(matches!(active.3, DebugValue::Gauge(g) if g.into_inner() == 1.0));
    }
}
