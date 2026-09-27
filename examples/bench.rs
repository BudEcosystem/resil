//! Hot-path cost of `resil::Limiter::check` against a common cached-counter design (a per-replica
//! governor plus a Redis count cached for 200 ms, most decisions local — reproduced with governor
//! 0.6.3, moka 0.12.10, dashmap 6.1) and against a pure per-replica governor. Limits are set high enough that everything is admitted: this measures the admit
//! path, which is what nearly every request pays.
//!
//! `cargo run --release --example bench -- [today8|gov|resil]` — one variant per process, so one
//! variant's background work (moka's housekeeper, resil's sync task) never lands in another's time.
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use governor::{clock::QuantaClock, state::InMemoryState, state::NotKeyed, Quota, RateLimiter};
use moka::future::Cache;
use resil::limit::store::MemStore;
use resil::{Limiter, LimiterOptions, RateLimitAlgorithm, RateLimitConfig};

const HUGE: u32 = 2_000_000_000;
const ITERS: u64 = 400_000;
type Gov = RateLimiter<NotKeyed, InMemoryState, QuantaClock>;

// ---- the cached-counter limiter, its local_allowance < 1 path
#[derive(Clone)]
struct Cached {
    remaining: u32,
    used: Arc<AtomicU32>,
    at: Instant,
}
struct Today {
    govs: DashMap<String, Arc<[Gov; 3]>>,
    cache: Arc<Cache<String, Cached>>,
    consumption: DashMap<String, Arc<AtomicU32>>,
    local_allowance: f64,
}
impl Today {
    fn new(models: &[String], local_allowance: f64) -> Self {
        let govs = DashMap::new();
        let clock = QuantaClock::default();
        for m in models {
            let q = |w: fn(NonZeroU32) -> Quota| {
                RateLimiter::direct_with_clock(w(NonZeroU32::new(HUGE).unwrap()), &clock)
            };
            govs.insert(
                m.clone(),
                Arc::new([
                    q(Quota::per_second),
                    q(Quota::per_minute),
                    q(Quota::per_hour),
                ]),
            );
        }
        Self {
            govs,
            cache: Arc::new(
                Cache::builder()
                    .max_capacity(10_000)
                    .time_to_live(Duration::from_secs(300))
                    .build(),
            ),
            consumption: DashMap::new(),
            local_allowance,
        }
    }
    async fn check(&self, model: &str, key: &str) -> bool {
        if self.local_allowance >= 1.0 {
            let Some(g) = self.govs.get(model) else {
                return true;
            };
            return g.iter().all(|g| g.check().is_ok());
        }
        let ck = format!("{model}:{key}");
        if let Some(c) = self.cache.get(&ck).await {
            if c.at.elapsed() < Duration::from_millis(200) {
                let n = c.used.fetch_add(1, Relaxed) + 1;
                if n <= c.remaining {
                    return true;
                }
            }
        }
        let Some(g) = self.govs.get(model).map(|g| g.clone()) else {
            return true;
        };
        let cache = self.cache.clone();
        let k = ck.clone();
        tokio::spawn(async move {
            cache
                .insert(
                    k,
                    Cached {
                        remaining: 1000,
                        used: Arc::new(AtomicU32::new(0)),
                        at: Instant::now(),
                    },
                )
                .await;
        });
        let ok = g.iter().all(|g| g.check().is_ok());
        if ok {
            self.consumption
                .entry(ck.clone())
                .or_insert_with(|| Arc::new(AtomicU32::new(0)))
                .fetch_add(1, Relaxed);
            self.cache
                .insert(
                    ck,
                    Cached {
                        remaining: HUGE - 1,
                        used: Arc::new(AtomicU32::new(1)),
                        at: Instant::now(),
                    },
                )
                .await;
        }
        ok
    }
}

fn report(label: &str, par: usize, total: u64, el: Duration) {
    println!(
        "{label:<58} {:>7.1} ns/op {:>8.2} M ops/s",
        el.as_nanos() as f64 * par as f64 / total as f64,
        total as f64 / el.as_secs_f64() / 1e6
    );
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let keys: Arc<Vec<String>> = Arc::new(
        (0..10_000)
            .map(|i| format!("0b6e1c52-4f1e-4d7e-9a1b-{i:012}"))
            .collect(),
    );
    let api: Arc<Vec<String>> = Arc::new((0..50).map(|i| format!("client_key_{i:040}")).collect());
    let workers = std::thread::available_parallelism().unwrap().get();

    // resil: store in-process (MemStore) behind the background sync task, like Redis would be.
    let store = Arc::new(MemStore::new(Arc::new(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
            * 1000.0
    })));
    let lim = Limiter::new(
        LimiterOptions {
            service: "bench".into(),
            ..Default::default()
        },
        Some(store),
    );
    let cfg = RateLimitConfig {
        algorithm: RateLimitAlgorithm::SlidingWindow,
        requests_per_second: Some(HUGE),
        requests_per_minute: Some(HUGE),
        requests_per_hour: Some(HUGE),
        local_allowance: 0.8,
        cache_ttl_ms: 500,
        ..Default::default()
    };
    for k in keys.iter() {
        lim.set_policy(k, Some(&cfg), None);
    }
    lim.start().await;
    let only = std::env::args().nth(1).unwrap_or_default();
    let today8 = Arc::new(Today::new(
        if only == "today8" || only.is_empty() {
            &keys[..]
        } else {
            &keys[..0]
        },
        0.8,
    ));
    let today1 = Arc::new(Today::new(
        if only == "gov" || only.is_empty() {
            &keys[..]
        } else {
            &keys[..0]
        },
        1.0,
    ));
    if !(only.is_empty() || only == "resil") {
        for k in keys.iter() {
            lim.remove(k);
        }
    }

    for &(tasks, spread) in &[(1usize, false), (workers, false), (workers, true)] {
        println!(
            "--- {tasks} concurrent tasks, {} (10 000 keys, 3 windows)",
            if spread {
                "traffic over 10 000 deployments"
            } else {
                "1 hot deployment"
            }
        );
        let par = tasks.min(workers);
        // warm up resil's credit for the keys in play
        if only.is_empty() || only == "resil" {
            for i in 0..20_000u64 {
                let k = if spread {
                    &keys[(i as usize * 7) % keys.len()]
                } else {
                    &keys[0]
                };
                let _ = lim.check(k).await;
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        for (label, which, name) in [
            ("cached counter, local_allowance 0.8", 0, "today8"),
            (
                "today, local_allowance 1.0 (pure governor, per pod)",
                1,
                "gov",
            ),
            ("resil (credit, cluster-wide, sync running)", 2, "resil"),
        ] {
            if !(only.is_empty() || only == name) {
                continue;
            }
            let start = Instant::now();
            let mut hs = Vec::new();
            for t in 0..tasks {
                let (keys, api, lim, t8, t1) = (
                    keys.clone(),
                    api.clone(),
                    lim.clone(),
                    today8.clone(),
                    today1.clone(),
                );
                hs.push(tokio::spawn(async move {
                    let mut ok = 0u64;
                    for i in 0..ITERS {
                        let k = if spread {
                            &keys[(i as usize * 7 + t * 13) % keys.len()]
                        } else {
                            &keys[0]
                        };
                        ok += match which {
                            0 => t8.check(k, &api[(i as usize + t) % api.len()]).await,
                            1 => t1.check(k, "").await,
                            _ => lim.check(k).await.is_allowed(),
                        } as u64;
                        if i % 64 == 0 {
                            tokio::task::yield_now().await;
                        }
                    }
                    ok
                }));
            }
            let mut ok = 0;
            for h in hs {
                ok += h.await.unwrap();
            }
            let total = tasks as u64 * ITERS;
            report(label, par, total, start.elapsed());
            assert_eq!(ok, total, "{label}: everything should be admitted");
        }
    }
    let waited = lim.outcome_count(resil::limit::Outcome::AllowSync)
        + lim.outcome_count(resil::limit::Outcome::AllowCold);
    println!("resil admits that waited on the store or used cold credit: {waited}");
}
