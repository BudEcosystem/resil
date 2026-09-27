//! N limiter replicas sharing the reference store, on a virtual clock.
//! The store round-trip is instantaneous here; `tests/redis_cluster.rs` repeats the key
//! properties against a real Redis in real time.

use std::sync::Arc;

use resil::limit::clock::ManualClock;
use resil::limit::store::MemStore;
use resil::limit::{Decision, Limiter, LimiterOptions, Outcome};
use resil::{RateLimitAlgorithm, RateLimitConfig};

const SUBJECT: &str = "deployment-1";

struct Cluster {
    clock: Arc<ManualClock>,
    store: Arc<MemStore>,
    reps: Vec<Limiter>,
}

impl Cluster {
    async fn new(n: usize) -> Self {
        Self::with(n, LimiterOptions::default()).await
    }

    async fn with(n: usize, opts: LimiterOptions) -> Self {
        let clock = Arc::new(ManualClock::default());
        let c2 = clock.clone();
        let store = Arc::new(MemStore::new(Arc::new(move || c2.now())));
        let mut reps = Vec::new();
        for i in 0..n {
            let l = Limiter::with_clock(
                LimiterOptions {
                    service: "sim".into(),
                    pod_id: Some(format!("pod-{i}")),
                    ..opts.clone()
                },
                Some(store.clone()),
                clock.clone(),
            );
            reps.push(l);
        }
        let c = Self { clock, store, reps };
        for r in &c.reps {
            r.sync_now(true).await.unwrap();
        }
        // second pass so every replica sees the final N
        for r in &c.reps {
            r.sync_now(true).await.unwrap();
        }
        c
    }

    fn set(&self, cfg: &RateLimitConfig) {
        for r in &self.reps {
            r.set_policy(SUBJECT, Some(cfg), None);
        }
    }

    /// One replica's sync task: run whatever is due.
    async fn sync(&self) {
        for r in &self.reps {
            let _ = r.sync_now(false).await;
        }
        tokio::task::yield_now().await;
    }
}

fn cfg(alg: RateLimitAlgorithm, rps: Option<u32>, rpm: Option<u32>) -> RateLimitConfig {
    RateLimitConfig {
        algorithm: alg,
        requests_per_second: rps,
        requests_per_minute: rpm,
        local_allowance: 0.8,
        cache_ttl_ms: 500,
        ..Default::default()
    }
}

#[derive(Clone, Copy)]
struct Arrival {
    t: f64,
    admitted: bool,
}

/// Drive `rate_per_s` Poisson arrivals for `dur_ms`, placed on replicas by `pick`.
async fn drive(
    c: &Cluster,
    dur_ms: f64,
    rate_per_s: f64,
    seed: u64,
    pick: impl Fn(&mut fastrand::Rng) -> usize,
) -> Vec<Arrival> {
    let mut rng = fastrand::Rng::with_seed(seed);
    let step = 1.0;
    let per_step = rate_per_s / 1000.0 * step;
    let mut out = Vec::new();
    let start = c.clock.now();
    while c.clock.now() - start < dur_ms {
        // Poisson(per_step) by inversion
        let mut k = 0;
        let l = (-per_step).exp();
        let mut p = rng.f64();
        while p > l {
            k += 1;
            p *= rng.f64();
        }
        for _ in 0..k {
            let r = &c.reps[pick(&mut rng)];
            let d = r.check(SUBJECT).await;
            out.push(Arrival {
                t: c.clock.now(),
                admitted: d.is_allowed(),
            });
        }
        c.sync().await;
        c.clock.advance(step);
    }
    out
}

fn per_window(arr: &[Arrival], window_ms: f64) -> Vec<(u64, u64)> {
    let mut m = std::collections::BTreeMap::new();
    for a in arr {
        let w = (a.t / window_ms).floor() as u64;
        let e = m.entry(w).or_insert((0u64, 0u64));
        e.1 += 1;
        if a.admitted {
            e.0 += 1;
        }
    }
    m.into_values().collect()
}

// per-window admits ≤ L for N ∈ {1, 2, 4, 8} at 0.5×…50× the limit.
#[tokio::test]
async fn tc_lf_01_never_exceeds_limit() {
    for alg in [
        RateLimitAlgorithm::FixedWindow,
        RateLimitAlgorithm::SlidingWindow,
    ] {
        for n in [1usize, 2, 4, 8] {
            for mult in [0.5, 1.0, 5.0, 50.0] {
                let c = Cluster::new(n).await;
                c.set(&cfg(alg, Some(40), None));
                let arr = drive(&c, 5_000.0, 40.0 * mult, n as u64 * 31, |r| r.usize(0..n)).await;
                for (i, (admitted, _)) in per_window(&arr, 1000.0).into_iter().enumerate() {
                    assert!(
                        admitted <= 40,
                        "{alg:?} N={n} ×{mult}: window {i} admitted {admitted} > 40"
                    );
                }
            }
        }
    }
}

// For the token bucket: any interval τ admits ≤ b + τ/T.
#[tokio::test]
async fn tc_lf_01_token_bucket_envelope() {
    for n in [1usize, 4] {
        let c = Cluster::new(n).await;
        let mut conf = cfg(RateLimitAlgorithm::TokenBucket, Some(20), None);
        conf.burst_size = Some(5);
        c.set(&conf);
        let arr = drive(&c, 5_000.0, 200.0, 7, |r| r.usize(0..n)).await;
        let admits: Vec<f64> = arr.iter().filter(|a| a.admitted).map(|a| a.t).collect();
        for (i, &t0) in admits.iter().enumerate() {
            for &t1 in &admits[i..] {
                let tau = t1 - t0;
                let count = admits[i..].iter().filter(|&&t| t <= t1).count() as f64;
                assert!(
                    count <= 5.0 + tau / 50.0 + 1.0,
                    "N={n}: {count} admits in {tau} ms"
                );
            }
        }
    }
}

// at 5× the limit, admits ≥ 0.95·L per window (no starvation from the shares).
#[tokio::test]
async fn tc_lf_02_no_starvation() {
    for alg in [
        RateLimitAlgorithm::FixedWindow,
        RateLimitAlgorithm::SlidingWindow,
    ] {
        for n in [1usize, 4] {
            let c = Cluster::new(n).await;
            c.set(&cfg(alg, Some(100), None));
            let arr = drive(&c, 6_000.0, 500.0, 3, |r| r.usize(0..n)).await;
            let w = per_window(&arr, 1000.0);
            for (i, (admitted, _)) in w.iter().enumerate().skip(1).take(w.len() - 2) {
                assert!(
                    *admitted >= 95,
                    "{alg:?} N={n}: window {i} admitted only {admitted}"
                );
            }
        }
    }
}

// all load on one replica of 4 still reaches ≥ 0.9·L per window.
#[tokio::test]
async fn tc_lf_03_skewed_traffic() {
    let c = Cluster::new(4).await;
    c.set(&cfg(RateLimitAlgorithm::FixedWindow, Some(100), None));
    let arr = drive(&c, 6_000.0, 400.0, 11, |_| 0).await;
    let w = per_window(&arr, 1000.0);
    for (i, (admitted, _)) in w.iter().enumerate().skip(1).take(w.len() - 2) {
        assert!(*admitted >= 90, "window {i}: {admitted}");
        assert!(*admitted <= 100, "window {i}: {admitted}");
    }
}

// token bucket long-run rate within 1 % of L; a burst of b admitted from idle.
#[tokio::test]
async fn tc_lf_04_token_bucket_rate_and_burst() {
    let c = Cluster::new(2).await;
    let mut conf = cfg(RateLimitAlgorithm::TokenBucket, Some(50), None);
    conf.burst_size = Some(20);
    c.set(&conf);
    // burst from idle: 20 at once
    let mut burst = 0;
    for i in 0..30 {
        if c.reps[i % 2].check(SUBJECT).await.is_allowed() {
            burst += 1;
        }
        tokio::task::yield_now().await;
    }
    assert!((19..=20).contains(&burst), "burst admitted {burst}");
    c.clock.advance(1_000.0);
    let arr = drive(&c, 60_000.0, 150.0, 5, |r| r.usize(0..2)).await;
    let admitted = arr.iter().filter(|a| a.admitted).count() as f64;
    let expected = 50.0 * 60.0 + 20.0;
    assert!(
        (admitted - expected).abs() / expected < 0.01,
        "admitted {admitted}, expected ≈{expected}"
    );
}

// a retry at exactly Retry-After is admitted in ≥ 99 % of cases.
#[tokio::test]
async fn tc_lf_06_retry_after_never_undershoots() {
    for alg in [
        RateLimitAlgorithm::FixedWindow,
        RateLimitAlgorithm::SlidingWindow,
        RateLimitAlgorithm::TokenBucket,
    ] {
        let c = Cluster::new(2).await;
        c.set(&cfg(alg, None, Some(30)));
        let mut trials = 0;
        let mut ok = 0;
        let mut rng = fastrand::Rng::with_seed(9);
        for _ in 0..40 {
            // saturate
            let mut ra = None;
            for _ in 0..200 {
                let d = c.reps[rng.usize(0..2)].check(SUBJECT).await;
                c.sync().await;
                if let Decision::Deny(h) = d {
                    ra = h.retry_after;
                    break;
                }
                c.clock.advance(rng.f64() * 20.0);
            }
            let Some(ra) = ra else { continue };
            // wait exactly Retry-After, keep the view fresh meanwhile
            let target = c.clock.now() + ra as f64 * 1000.0;
            while c.clock.now() < target {
                c.clock.advance(50.0f64.min(target - c.clock.now()));
                c.sync().await;
            }
            trials += 1;
            if c.reps[rng.usize(0..2)].check(SUBJECT).await.is_allowed() {
                ok += 1;
            }
        }
        assert!(trials >= 20, "{alg:?}: only {trials} trials");
        assert!(
            ok * 100 >= trials * 99,
            "{alg:?}: {ok}/{trials} admitted at Retry-After"
        );
    }
}

// store down → each replica enforces ⌈L/N⌉; cluster total within L ± N.
#[tokio::test]
async fn tc_lf_07_store_down_fail_static() {
    let c = Cluster::new(4).await;
    c.set(&cfg(RateLimitAlgorithm::FixedWindow, Some(100), None));
    drive(&c, 1_000.0, 50.0, 1, |r| r.usize(0..4)).await;
    c.store.set_failing(true);
    // let the failure be noticed and the credit lapse
    for _ in 0..30 {
        c.clock.advance(100.0);
        c.sync().await;
    }
    let arr = drive(&c, 5_000.0, 1_000.0, 2, |r| r.usize(0..4)).await;
    let w = per_window(&arr, 1000.0);
    for (i, (admitted, _)) in w.iter().enumerate().skip(1).take(w.len() - 2) {
        assert!(
            (96..=104).contains(admitted),
            "degraded window {i}: {admitted} (want 100 ± 4)"
        );
    }
    assert!(c.reps[0].outcome_count(Outcome::AllowDegraded) > 0);
}

// a cold key on 4 replicas at once: first-interval admits ≤ L.
#[tokio::test]
async fn tc_lf_09_cold_start() {
    for limit in [8u32, 40, 400] {
        let c = Cluster::new(4).await;
        c.set(&cfg(RateLimitAlgorithm::FixedWindow, Some(limit), None));
        c.clock.set((c.clock.now() / 1000.0).ceil() * 1000.0 + 1.0);
        let mut admitted = 0;
        for i in 0..(limit as usize * 5) {
            if c.reps[i % 4].check(SUBJECT).await.is_allowed() {
                admitted += 1;
            }
        }
        assert!(admitted <= limit, "limit {limit}: cold admits {admitted}");
    }
}

// a config change starts from fresh state.
#[tokio::test]
async fn tc_lf_10_config_change_resets() {
    let c = Cluster::new(1).await;
    c.set(&cfg(RateLimitAlgorithm::FixedWindow, None, Some(10)));
    let mut n = 0;
    for _ in 0..20 {
        n += c.reps[0].check(SUBJECT).await.is_allowed() as u32;
        c.sync().await;
    }
    assert_eq!(n, 10);
    c.set(&cfg(RateLimitAlgorithm::FixedWindow, None, Some(20)));
    let mut m = 0;
    for _ in 0..30 {
        m += c.reps[0].check(SUBJECT).await.is_allowed() as u32;
        c.sync().await;
    }
    assert_eq!(m, 20, "new limit applies at once, no restart (D5)");
    // an identical republish keeps the live state
    c.set(&cfg(RateLimitAlgorithm::FixedWindow, None, Some(20)));
    assert!(!c.reps[0].check(SUBJECT).await.is_allowed());
}

// a replica dies holding slots; the others regain them within the lease TTL.
#[tokio::test]
async fn tc_lf_11_concurrency_lease_expiry() {
    let mut c = Cluster::new(2).await;
    for r in &c.reps {
        r.set_policy(SUBJECT, None, Some(3));
    }
    let mut held = Vec::new();
    for _ in 0..3 {
        held.push(c.reps[0].acquire(SUBJECT).await.unwrap().unwrap());
        c.sync().await;
    }
    c.clock.advance(200.0);
    c.sync().await;
    assert!(
        c.reps[1].acquire(SUBJECT).await.is_err(),
        "3 of 3 held on pod-0"
    );
    // pod-0 dies without releasing (the guards leak with it)
    std::mem::forget(held);
    let dead = c.reps.remove(0);
    std::mem::forget(dead);
    let mut regained_after = None;
    for step in 0..100 {
        c.clock.advance(100.0);
        c.sync().await;
        if let Ok(Some(g)) = c.reps[0].acquire(SUBJECT).await {
            drop(g);
            regained_after = Some(step as f64 * 100.0);
            break;
        }
    }
    let after = regained_after.expect("slots never came back");
    assert!(after <= 6_000.0, "regained after {after} ms");
}

// last-mile — 3 rps on 4 replicas → ≤ 3 admits per second cluster-wide.
#[tokio::test]
async fn tc_lf_13_last_mile() {
    let c = Cluster::new(4).await;
    c.set(&cfg(RateLimitAlgorithm::FixedWindow, Some(3), None));
    let arr = drive(&c, 5_000.0, 100.0, 4, |r| r.usize(0..4)).await;
    let w = per_window(&arr, 1000.0);
    for (i, (admitted, _)) in w.iter().enumerate() {
        assert!(*admitted <= 3, "window {i}: {admitted}");
    }
    for (i, (admitted, _)) in w.iter().enumerate().skip(1).take(w.len() - 2) {
        assert_eq!(*admitted, 3, "window {i} should still use its budget");
    }
}

// at 0.9× the limit, < 1 % of requests wait on the store.
#[tokio::test]
async fn tc_lf_15_early_sync_keeps_waits_rare() {
    let c = Cluster::new(4).await;
    c.set(&cfg(RateLimitAlgorithm::SlidingWindow, None, Some(6_000)));
    let arr = drive(&c, 20_000.0, 90.0, 8, |r| r.usize(0..4)).await;
    let total = arr.len() as u64;
    let waited: u64 = c
        .reps
        .iter()
        .map(|r| r.outcome_count(Outcome::AllowSync) + r.outcome_count(Outcome::DenySync))
        .sum();
    let denied = arr.iter().filter(|a| !a.admitted).count();
    assert_eq!(denied, 0, "0.9× the limit must not be denied");
    assert!(waited * 100 < total, "{waited} of {total} requests waited");
}

// hits admitted during an outage are pushed and paid back.
#[tokio::test]
async fn tc_lf_16_outage_payback() {
    let c = Cluster::new(2).await;
    c.set(&cfg(RateLimitAlgorithm::FixedWindow, None, Some(100)));
    c.clock
        .set((c.clock.now() / 60_000.0).ceil() * 60_000.0 + 1.0);
    c.store.set_failing(true);
    for _ in 0..10 {
        c.clock.advance(100.0);
        c.sync().await;
    }
    let mut during = 0;
    for i in 0..100 {
        during += c.reps[i % 2].check(SUBJECT).await.is_allowed() as u32;
    }
    assert!(
        (96..=100).contains(&during),
        "degraded admitted {during} (≈ L)"
    );
    c.store.set_failing(false);
    for _ in 0..30 {
        c.clock.advance(50.0);
        c.sync().await;
    }
    // the store now knows about them: nothing left in this window
    let mut after = 0;
    for i in 0..50 {
        after += c.reps[i % 2].check(SUBJECT).await.is_allowed() as u32;
        c.sync().await;
    }
    assert!(during + after <= 104, "{during} + {after} > L + N");
}

// disabled limits → unlimited, no headers.
#[tokio::test]
async fn tc_pa_03_disabled_is_unlimited() {
    let c = Cluster::new(1).await;
    let mut conf = cfg(RateLimitAlgorithm::FixedWindow, Some(1), None);
    conf.enabled = false;
    c.set(&conf);
    for _ in 0..10 {
        assert_eq!(c.reps[0].check(SUBJECT).await, Decision::Unlimited);
    }
    assert_eq!(c.reps[0].check("unknown").await, Decision::Unlimited);
}

// a key at 10 % of its limit syncs ≤ 1× per second per replica.
#[tokio::test]
async fn tc_pa_09_adaptive_sync() {
    let c = Cluster::new(2).await;
    c.set(&cfg(RateLimitAlgorithm::FixedWindow, None, Some(6_000)));
    drive(&c, 1_000.0, 10.0, 1, |r| r.usize(0..2)).await;
    let before: Vec<u64> = c.reps.iter().map(|r| r.sync_rounds()).collect();
    drive(&c, 10_000.0, 10.0, 2, |r| r.usize(0..2)).await;
    for (r, b) in c.reps.iter().zip(before) {
        let rounds = r.sync_rounds() - b;
        // ≤ ~1 round-trip/s: key syncs every second, the heartbeat rides along
        assert!(rounds <= 13, "{rounds} rounds in 10 s");
    }
}

// Local-only mode (`local_allowance ≥ 1`): each replica enforces the full limit, no store.
#[tokio::test]
async fn local_only_mode_is_per_replica() {
    let c = Cluster::new(2).await;
    let mut conf = cfg(RateLimitAlgorithm::FixedWindow, None, Some(10));
    conf.local_allowance = 1.0;
    c.set(&conf);
    for r in &c.reps {
        let mut n = 0;
        for _ in 0..20 {
            n += r.check(SUBJECT).await.is_allowed() as u32;
        }
        assert_eq!(n, 10);
    }
}

// Headers: limit of the most restrictive window, remaining counts down, exact Retry-After.
#[tokio::test]
async fn headers_are_exact() {
    let c = Cluster::new(1).await;
    c.clock.set((c.clock.now() / 60_000.0).ceil() * 60_000.0);
    c.set(&cfg(RateLimitAlgorithm::FixedWindow, Some(100), Some(5)));
    let mut last = None;
    for _ in 0..5 {
        let d = c.reps[0].check(SUBJECT).await;
        c.sync().await;
        let Decision::Allow(h) = d else {
            panic!("{d:?}")
        };
        assert_eq!(h.limit, 5);
        if let Some(prev) = last {
            assert!(h.remaining < prev, "{} !< {prev}", h.remaining);
        }
        last = Some(h.remaining);
    }
    assert_eq!(last, Some(0));
    c.clock.advance(15_000.0);
    let Decision::Deny(h) = c.reps[0].check(SUBJECT).await else {
        panic!()
    };
    assert_eq!(
        h.retry_after,
        Some(45),
        "rest of the minute, not the whole window"
    );
}
