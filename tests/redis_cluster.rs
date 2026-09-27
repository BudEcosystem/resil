//! Real time, real Redis: N replicas with their background sync tasks, many concurrent callers.
//! `RESIL_TEST_REDIS_URL=redis://127.0.0.1:6390 cargo test --release --features redis-0-31`.
#![cfg(feature = "redis-0-31")]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use redis031 as redis;
use resil::limit::redis::RedisStore;
use resil::limit::store::{Batch, BatchReply, BoxFuture, Store, StoreError};
use resil::limit::{Limiter, LimiterOptions, Outcome};
use resil::{RateLimitAlgorithm, RateLimitConfig};

async fn store() -> Option<Arc<dyn Store>> {
    let url = std::env::var("RESIL_TEST_REDIS_URL").ok()?;
    let client = redis::Client::open(url).unwrap();
    let conn = redis::aio::ConnectionManager::new(client).await.unwrap();
    Some(Arc::new(RedisStore::new(conn)))
}

fn unix_ms() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
        * 1000.0
}

async fn cluster(
    n: usize,
    svc: &str,
    wrap: impl Fn(Arc<dyn Store>) -> Arc<dyn Store>,
) -> Option<Vec<Limiter>> {
    let mut reps = Vec::new();
    for i in 0..n {
        let s = wrap(store().await?);
        let l = Limiter::new(
            LimiterOptions {
                service: svc.into(),
                pod_id: Some(format!("{svc}-pod-{i}")),
                ..Default::default()
            },
            Some(s),
        );
        reps.push(l);
    }
    for r in &reps {
        r.start().await;
    }
    // let every replica see N
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    Some(reps)
}

fn percentile(v: &mut [Duration], p: f64) -> Duration {
    v.sort_unstable();
    v[((v.len() - 1) as f64 * p) as usize]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cluster_wide_limit_holds_in_real_time() {
    let svc = format!("it{}", fastrand::u32(..));
    let Some(reps) = cluster(4, &svc, |s| s).await else {
        eprintln!("RESIL_TEST_REDIS_URL unset: skipping");
        return;
    };
    for r in &reps {
        assert_eq!(r.replicas(), 4);
    }
    let cfg = RateLimitConfig {
        algorithm: RateLimitAlgorithm::FixedWindow,
        requests_per_second: Some(50),
        local_allowance: 0.8,
        cache_ttl_ms: 500,
        ..Default::default()
    };
    for r in &reps {
        r.set_policy("dep", Some(&cfg), None);
    }
    // Start just before a second boundary: cold start and the first rollover overlap (the case
    // that once starved the first full window).
    let phase: f64 = std::env::var("RESIL_PHASE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(950.0);
    let wait = (phase - unix_ms() % 1000.0).rem_euclid(1000.0);
    tokio::time::sleep(Duration::from_millis(wait as u64)).await;
    // 16 callers, ~250 rps total (5× the limit), 4 s
    let reps = Arc::new(reps);
    let monitor = {
        let reps = reps.clone();
        tokio::spawn(async move {
            let mut last = [0u64; 8];
            for _ in 0..45 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let now: Vec<u64> = Outcome::ALL
                    .iter()
                    .map(|o| reps.iter().map(|r| r.outcome_count(*o)).sum())
                    .collect();
                let d: Vec<u64> = now.iter().zip(last.iter()).map(|(a, b)| a - b).collect();
                if std::env::var("RESIL_TRACE").is_ok() {
                    eprintln!(
                        "t={:.0} {:?}",
                        unix_ms() % 10_000.0,
                        Outcome::ALL
                            .iter()
                            .zip(d.iter())
                            .filter(|(_, n)| **n > 0)
                            .collect::<Vec<_>>()
                    );
                }
                last.copy_from_slice(&now);
            }
        })
    };
    let mut tasks = Vec::new();
    let stop = Instant::now() + Duration::from_secs(4);
    for t in 0..16usize {
        let reps = reps.clone();
        tasks.push(tokio::spawn(async move {
            let mut out = Vec::new();
            let mut i = t;
            while Instant::now() < stop {
                let r = &reps[i % 4];
                i += 1;
                let started = Instant::now();
                let d = r.check("dep").await;
                out.push((unix_ms(), d.is_allowed(), started.elapsed()));
                tokio::time::sleep(Duration::from_millis(64)).await;
            }
            out
        }));
    }
    let mut all = Vec::new();
    for t in tasks {
        all.extend(t.await.unwrap());
    }
    monitor.abort();
    let mut per = std::collections::BTreeMap::<u64, (u64, u64)>::new();
    for (t, ok, _) in &all {
        let e = per.entry((*t / 1000.0) as u64).or_default();
        e.1 += 1;
        if *ok {
            e.0 += 1;
        }
    }
    let windows: Vec<(u64, u64)> = per.values().copied().collect();
    eprintln!("per-second (admitted, offered): {windows:?}");
    // local wall clock vs Redis TIME can put a boundary request in the neighbour window
    for (admitted, _) in &windows {
        assert!(*admitted <= 52, "admitted {admitted} > 50 (+2 boundary)");
    }
    for (admitted, _) in windows.iter().skip(1).take(windows.len().saturating_sub(2)) {
        assert!(*admitted >= 48, "starved: {admitted}");
    }
    let mut lat: Vec<Duration> = all.iter().map(|(_, _, l)| *l).collect();
    let p50 = percentile(&mut lat, 0.5);
    let p99 = percentile(&mut lat, 0.99);
    let max = *lat.last().unwrap();
    eprintln!("check latency p50={p50:?} p99={p99:?} max={max:?}");
    assert!(
        max < Duration::from_millis(15),
        "a request waited {max:?} (> redis_timeout)"
    );
    let waits: u64 = reps
        .iter()
        .map(|r| r.outcome_count(Outcome::AllowSync) + r.outcome_count(Outcome::DenySync))
        .sum();
    eprintln!("waited on Redis: {waits} of {}", all.len());
    for r in reps.iter() {
        r.shutdown().await;
    }
}

/// A store that answers after `delay`.
struct Slow {
    inner: Arc<dyn Store>,
    delay: Duration,
}

impl Store for Slow {
    fn round_trip<'a>(&'a self, b: &'a Batch) -> BoxFuture<'a, Result<BatchReply, StoreError>> {
        Box::pin(async move {
            tokio::time::sleep(self.delay).await;
            self.inner.round_trip(b).await
        })
    }
}

// Redis slower than redis_timeout_ms → nobody waits longer than the timeout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_store_never_blocks_past_the_timeout() {
    let svc = format!("slow{}", fastrand::u32(..));
    let Some(reps) = cluster(2, &svc, |s| {
        Arc::new(Slow {
            inner: s,
            delay: Duration::from_millis(40),
        }) as Arc<dyn Store>
    })
    .await
    else {
        return;
    };
    let cfg = RateLimitConfig {
        algorithm: RateLimitAlgorithm::SlidingWindow,
        requests_per_second: Some(5),
        local_allowance: 0.8,
        redis_timeout_ms: 10,
        cache_ttl_ms: 500,
        ..Default::default()
    };
    for r in &reps {
        r.set_policy("dep", Some(&cfg), None);
    }
    let mut worst = Duration::ZERO;
    for i in 0..200 {
        let started = Instant::now();
        let _ = reps[i % 2].check("dep").await;
        worst = worst.max(started.elapsed());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    eprintln!("worst wait with a 40 ms store: {worst:?}");
    assert!(worst < Duration::from_millis(20), "waited {worst:?}");
    let overruns: u64 = reps
        .iter()
        .map(|r| r.outcome_count(Outcome::AllowOverrun))
        .sum();
    eprintln!("overruns: {overruns}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrency_cap_is_cluster_wide() {
    let svc = format!("cc{}", fastrand::u32(..));
    let Some(reps) = cluster(3, &svc, |s| s).await else {
        return;
    };
    for r in &reps {
        r.set_policy("session", None, Some(5));
    }
    let mut held = Vec::new();
    let mut denied = 0;
    for i in 0..30 {
        match reps[i % 3].acquire("session").await {
            Ok(Some(g)) => held.push(g),
            Ok(None) => panic!("cap missing"),
            Err(_) => denied += 1,
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    eprintln!("held {} denied {denied}", held.len());
    assert!(held.len() <= 5, "held {} > 5", held.len());
    assert!(held.len() >= 4, "only {} of 5 slots usable", held.len());
    held.clear();
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let g = reps[1].acquire("session").await;
    assert!(matches!(g, Ok(Some(_))), "released slots come back");
}

// A burst after idle gets the whole burst, however the concurrent requests fall across syncs and
// whatever the Redis round-trip within `redis_timeout_ms`. Found live on a gateway: 3 concurrent
// requests to a token bucket (1/s, burst 2) after idle were admitted 1 or 2 at random.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_concurrent_burst_after_idle_gets_the_whole_burst() {
    for delay_ms in [0u64, 2, 4] {
        let svc = format!("burst{}", fastrand::u32(..));
        let Some(reps) = cluster(1, &svc, |s| {
            Arc::new(Slow {
                inner: s,
                delay: Duration::from_millis(delay_ms),
            }) as Arc<dyn Store>
        })
        .await
        else {
            return;
        };
        let l = Arc::new(reps.into_iter().next().unwrap());
        let cfg = RateLimitConfig {
            algorithm: RateLimitAlgorithm::TokenBucket,
            requests_per_second: Some(1),
            burst_size: Some(2),
            local_allowance: 0.8,
            redis_timeout_ms: 10,
            cache_ttl_ms: 500,
            ..Default::default()
        };
        l.set_policy("dep", Some(&cfg), None);
        let mut got = Vec::new();
        for _ in 0..6 {
            // Drain the bucket, then leave it idle long enough to refill completely.
            let drained = burst(&l, 5).await;
            tokio::time::sleep(Duration::from_millis(2_600)).await;
            got.push((drained, burst(&l, 3).await));
        }
        eprintln!("store delay {delay_ms} ms: (drain, after idle) admits {got:?}");
        for (drained, after) in &got {
            assert!(*drained <= 2, "delay {delay_ms}: {drained} > burst 2");
            assert_eq!(
                *after, 2,
                "delay {delay_ms}: the refilled burst of 2 was not honoured: {got:?}"
            );
        }
        l.shutdown().await;
    }
}

/// `n` concurrent checks; how many were admitted.
async fn burst(l: &Arc<Limiter>, n: usize) -> usize {
    let tasks: Vec<_> = (0..n)
        .map(|_| {
            let l = l.clone();
            tokio::spawn(async move { l.check("dep").await })
        })
        .collect();
    let mut admitted = 0;
    for t in tasks {
        if matches!(t.await.unwrap(), resil::Decision::Allow(_)) {
            admitted += 1;
        }
    }
    admitted
}

// A token bucket 1/s burst 2 PLUS a non-binding per-minute window (which, being a token bucket, gets
// burst 2 too), with requests arriving a few ms apart as they do through a load balancer: the
// second window must not cost the refilled burst a token.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn a_staggered_burst_after_idle_gets_the_whole_burst_with_a_second_window() {
    let mut failures = Vec::new();
    for rpm in [1_000u32, 3_000, 6_000] {
        for gap_ms in [0u64, 1, 3, 8] {
            let svc = format!("stag{}", fastrand::u32(..));
            let Some(reps) = cluster(1, &svc, |s| s).await else {
                return;
            };
            let l = Arc::new(reps.into_iter().next().unwrap());
            let cfg = RateLimitConfig {
                algorithm: RateLimitAlgorithm::TokenBucket,
                requests_per_second: Some(1),
                requests_per_minute: Some(rpm),
                burst_size: Some(2),
                local_allowance: 0.8,
                redis_timeout_ms: 10,
                cache_ttl_ms: 500,
                ..Default::default()
            };
            l.set_policy("dep", Some(&cfg), None);
            for round in 0..4 {
                let _ = staggered(&l, 5, gap_ms).await;
                tokio::time::sleep(Duration::from_millis(2_600)).await;
                let after = staggered(&l, 3, gap_ms).await;
                if after != 2 {
                    failures.push((rpm, gap_ms, round, after));
                }
            }
            l.shutdown().await;
        }
    }
    assert!(
        failures.is_empty(),
        "(rpm, gap_ms, round, admitted of a refilled burst of 2): {failures:?}"
    );
}

/// `n` checks started `gap_ms` apart, running concurrently; how many were admitted.
async fn staggered(l: &Arc<Limiter>, n: usize, gap_ms: u64) -> usize {
    let mut tasks = Vec::new();
    for _ in 0..n {
        let l = l.clone();
        tasks.push(tokio::spawn(async move { l.check("dep").await }));
        if gap_ms > 0 {
            tokio::time::sleep(Duration::from_millis(gap_ms)).await;
        }
    }
    let mut admitted = 0;
    for t in tasks {
        if matches!(t.await.unwrap(), resil::Decision::Allow(_)) {
            admitted += 1;
        }
    }
    admitted
}
