//! The Lua scripts must behave exactly like `MemStore`. Random operation sequences (fixed store
//! time via `now_override_ms`) run against both, and every reply is compared.
//!
//! Needs a Redis/Valkey: `RESIL_TEST_REDIS_URL=redis://127.0.0.1:6390 cargo test --features redis-0-31`.
//! Skipped (passes) when the variable is unset.
#![cfg(feature = "redis-0-31")]

use std::sync::Arc;

use redis031 as redis;
use resil::limit::algo::{Algorithm, WindowSpec};
use resil::limit::redis::RedisStore;
use resil::limit::store::{Batch, ConcSync, Heartbeat, MemStore, RateSync, Store};

async fn redis_store() -> Option<RedisStore<redis::aio::ConnectionManager>> {
    let url = std::env::var("RESIL_TEST_REDIS_URL").ok()?;
    let client = redis::Client::open(url).expect("redis url");
    let conn = redis::aio::ConnectionManager::new(client)
        .await
        .expect("connect to redis");
    Some(RedisStore::new(conn))
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-6 * a.abs().max(1.0)
}

#[tokio::test]
async fn lua_matches_reference_store() {
    let Some(redis) = redis_store().await else {
        eprintln!("RESIL_TEST_REDIS_URL unset: skipping");
        return;
    };
    let mem = MemStore::new(Arc::new(|| 0.0));
    let run_id = fastrand::u64(..);
    let mut rng = fastrand::Rng::with_seed(0xC0FFEE);
    let pods: Vec<Arc<str>> = ["p1", "p2", "p3"].into_iter().map(Arc::from).collect();

    for case in 0..60 {
        let alg = [
            Algorithm::FixedWindow,
            Algorithm::SlidingWindow,
            Algorithm::TokenBucket,
        ][case % 3];
        let mut windows = Vec::new();
        for (w, lim) in [(1_000u64, 1..50u64), (60_000, 10..500)] {
            if rng.bool() || windows.is_empty() {
                let limit = rng.u64(lim);
                let burst = if alg == Algorithm::TokenBucket {
                    rng.u64(1..=limit)
                } else {
                    limit
                };
                windows.push(WindowSpec {
                    limit,
                    window_ms: w,
                    burst,
                });
            }
        }
        let windows: Arc<[WindowSpec]> = Arc::from(windows);
        let key: Arc<str> = Arc::from(format!("rl2:{{parity:{run_id}:{case}}}:x"));
        let ckey: Arc<str> = Arc::from(format!("cc2:{{parity:{run_id}:{case}}}:c"));
        let max = rng.u64(1..20);
        let mut now = 1_750_000_000_000.0 + rng.f64() * 1e6;

        for step in 0..80 {
            now += rng.f64() * 400.0;
            let pod = pods[rng.usize(0..pods.len())].clone();
            let mut batch = Batch::default();
            batch.rate.push(RateSync {
                key: key.clone(),
                alg,
                windows: windows.clone(),
                pod: pod.clone(),
                hits: rng.u64(0..30),
                hits_old: if rng.bool() { rng.u64(0..10) } else { 0 },
                t_old_ms: now - rng.f64() * 2_500.0,
                unused: rng.u64(0..10),
                want: rng.u64(0..40),
                need: rng.u64(0..4),
                allowance: [0.1, 0.5, 0.8, 1.0][rng.usize(0..4)],
                replicas: rng.u32(1..5),
                lease_ttl_ms: [300, 3_000][rng.usize(0..2)],
                now_override_ms: Some(now),
            });
            batch.conc.push(ConcSync {
                key: ckey.clone(),
                pod: pod.clone(),
                active: rng.u64(0..max + 2),
                unused: rng.u64(0..5),
                want: rng.u64(0..5),
                need: rng.u64(0..3),
                max,
                allowance: 0.8,
                replicas: rng.u32(1..4),
                lease_ttl_ms: 5_000,
                now_override_ms: Some(now),
            });
            batch.heartbeat = Some(Heartbeat {
                key: Arc::from(format!("rl2:{{parity:{run_id}}}:pods")),
                pod: pod.clone(),
                live_window_ms: 3_000,
                leave: rng.u8(0..10) == 0,
                now_override_ms: Some(now),
            });
            if rng.u8(0..20) == 0 {
                batch.release.push((key.clone(), pod.clone()));
            }
            let want = mem.round_trip(&batch).await.unwrap();
            let got = redis.round_trip(&batch).await.unwrap();

            let ctx = format!("case {case} step {step} alg {alg:?} windows {windows:?}");
            let (w, g) = (&want.rate[0], &got.rate[0]);
            assert!(close(w.now_ms, g.now_ms), "{ctx}: now {w:?} vs {g:?}");
            assert_eq!(
                (w.kept, w.avail, w.granted, w.direct, w.reserved_others),
                (g.kept, g.avail, g.granted, g.direct, g.reserved_others),
                "{ctx}: rate reply\nmem   {w:?}\nredis {g:?}"
            );
            for (a, b) in w.windows.iter().zip(g.windows.iter()) {
                assert_eq!((a.idx, a.cur, a.prev), (b.idx, b.cur, b.prev), "{ctx}");
                assert!(close(a.tat, b.tat), "{ctx}: tat {a:?} vs {b:?}");
            }
            let (w, g) = (&want.conc[0], &got.conc[0]);
            assert_eq!(
                (w.kept, w.granted, w.direct, w.others),
                (g.kept, g.granted, g.direct, g.others),
                "{ctx}: conc reply\nmem   {w:?}\nredis {g:?}"
            );
            assert_eq!(
                want.heartbeat.as_ref().unwrap().replicas,
                got.heartbeat.as_ref().unwrap().replicas,
                "{ctx}: heartbeat"
            );
        }
    }
}

#[tokio::test]
async fn redis_time_is_used_without_override() {
    let Some(redis) = redis_store().await else {
        return;
    };
    let batch = Batch {
        heartbeat: Some(Heartbeat {
            key: Arc::from(format!("rl2:{{time:{}}}:pods", fastrand::u64(..))),
            pod: Arc::from("p"),
            live_window_ms: 3_000,
            leave: false,
            now_override_ms: None,
        }),
        ..Default::default()
    };
    let r = redis.round_trip(&batch).await.unwrap();
    let unix_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as f64;
    let hb = r.heartbeat.unwrap();
    assert!(
        (hb.now_ms - unix_ms).abs() < 5_000.0,
        "{} vs {unix_ms}",
        hb.now_ms
    );
    assert_eq!(hb.replicas, 1);
}

#[tokio::test]
async fn survives_script_flush() {
    let Some(redis) = redis_store().await else {
        return;
    };
    let url = std::env::var("RESIL_TEST_REDIS_URL").unwrap();
    let batch = Batch {
        heartbeat: Some(Heartbeat {
            key: Arc::from(format!("rl2:{{flush:{}}}:pods", fastrand::u64(..))),
            pod: Arc::from("p"),
            live_window_ms: 3_000,
            leave: false,
            now_override_ms: None,
        }),
        ..Default::default()
    };
    redis.round_trip(&batch).await.unwrap();
    // A Redis restart or failover empties the script cache.
    let client = redis::Client::open(url).unwrap();
    let mut c = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async(&mut c)
        .await
        .unwrap();
    redis
        .round_trip(&batch)
        .await
        .expect("reloads scripts after NOSCRIPT");
}

/// Leases expire at exactly `expiry <= now`, in both implementations.
#[tokio::test]
async fn lease_expiry_boundary_matches() {
    let Some(redis) = redis_store().await else {
        return;
    };
    let mem = MemStore::new(Arc::new(|| 0.0));
    let id = fastrand::u64(..);
    let t0 = 1_750_000_000_000.0;
    for (i, (pod, now)) in [("a", t0), ("b", t0 + 1_000.0), ("b", t0 + 999.0)]
        .into_iter()
        .enumerate()
    {
        let mut b = Batch::default();
        b.rate.push(RateSync {
            key: Arc::from(format!("rl2:{{edge:{id}:{i}}}:x")),
            alg: Algorithm::FixedWindow,
            windows: Arc::from(vec![WindowSpec {
                limit: 100,
                window_ms: 60_000,
                burst: 100,
            }]),
            pod: Arc::from(pod),
            hits: 0,
            hits_old: 0,
            t_old_ms: 0.0,
            unused: 0,
            want: 50,
            need: 0,
            allowance: 1.0,
            replicas: 1,
            lease_ttl_ms: 1_000,
            now_override_ms: Some(now),
        });
        b.conc.push(ConcSync {
            key: Arc::from(format!("cc2:{{edge:{id}:{i}}}:c")),
            pod: Arc::from(pod),
            active: 2,
            unused: 0,
            want: 0,
            need: 0,
            max: 10,
            allowance: 1.0,
            replicas: 1,
            lease_ttl_ms: 1_000,
            now_override_ms: Some(now),
        });
        // Each index is its own key: seed pod "a" at t0 first, then query as "b".
        let mut seed = b.clone();
        seed.rate[0].pod = Arc::from("a");
        seed.rate[0].now_override_ms = Some(t0);
        seed.conc[0].pod = Arc::from("a");
        seed.conc[0].now_override_ms = Some(t0);
        mem.round_trip(&seed).await.unwrap();
        redis.round_trip(&seed).await.unwrap();
        let w = mem.round_trip(&b).await.unwrap();
        let g = redis.round_trip(&b).await.unwrap();
        assert_eq!(
            w.rate[0].reserved_others, g.rate[0].reserved_others,
            "rate {pod}@{now}"
        );
        assert_eq!(w.conc[0].others, g.conc[0].others, "conc {pod}@{now}");
        if pod == "b" && now == t0 + 1_000.0 {
            assert_eq!(
                g.rate[0].reserved_others, 0,
                "lease expiring exactly now is gone"
            );
            assert_eq!(g.conc[0].others, 0);
        }
        if pod == "b" && now == t0 + 999.0 {
            assert_eq!(g.rate[0].reserved_others, 50);
            assert_eq!(g.conc[0].others, 2);
        }
    }
}
