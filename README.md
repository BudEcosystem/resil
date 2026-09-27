# resil

Rate limiting and resilience for Rust services that sit in front of upstreams — gateways, proxies,
API servers. A **local-first, Redis-synced rate and concurrency limiter** that holds a limit across
every replica without a network round-trip per request, plus the pieces that decide what to do when
an upstream call fails: **classification, retry, circuit breaking and fallback chains**.

Each module is independent; use the limiter alone, or the resilience primitives alone.

| Module | Holds |
|---|---|
| `limit` | local-first, Redis-synced rate limiter (fixed window, sliding window, token bucket; per second / minute / hour) and concurrency caps, per key |
| `policy` | serde types for a per-target policy: `rate_limits`, `max_concurrent`, `retry_config`, `fallback_models` (lenient parsing: a malformed block is dropped, never the record that carries it) |
| `classify` | failure → retry / fail over / breaker verdict; `Retry-After` / `retry-after-ms` / HTTP-date; provider-concurrency 429s |
| `retry` | retry with classification, provider delay hints and a deadline; `standard` and `interactive` backoff profiles |
| `breaker` | keyed circuit breakers in two tiers: per target, and per provider (opens only when ≥ 2 targets fail) |
| `fallback` | ordered fallback chains: depth-first expansion, cycle guard, per-hop skip, shared deadline |
| `http` | OpenAI-style 429 bodies, `X-RateLimit-*` headers, CORS expose list |

A *target* (called a deployment in the types) is whatever a request is routed to: a model
deployment, an upstream service, a tenant's route. The limiter keys are arbitrary strings.

## The limiter

**Latency first.** Every replica admits from **credit** — a reservation of the shared budget it was
granted at its last sync. Admission is an atomic add and a compare: no I/O, no locks. A background
task pushes admitted hits to Redis in one pipelined round-trip per interval and asks for more
credit. Redis grants credit only out of the budget nobody has reserved:

```
avail = limit − usage − Σ other replicas' reservations
grant = min(want, ⌊local_allowance · avail / replicas⌋)
```

so the replicas together never admit more than the limit while Redis is healthy, however hard any
one of them is hit. A request waits on Redis (at most `redis_timeout_ms`, default 10 ms) only when
its replica's credit ran out before the early sync (at half credit) refilled it, or when the budget
left is too small to split across replicas (*last-mile*: Redis admits waiting requests one by one).

| Situation | Behaviour |
|---|---|
| Redis unreachable longer than `cache_ttl_ms` | fail-static: each replica enforces `⌈L / N⌉` locally (or fail-open, `OnStoreUnavailable::Allow`); hits are pushed on recovery and paid back |
| Redis slower than `redis_timeout_ms` | the waiting request is decided on the last view (counted as an overrun) |
| Replica crash | its reservations and concurrency slots expire with its lease (3 s / 5 s) |
| Cold key | a small unreserved share (≤ one sync interval of the rate), then an immediate sync |
| Quiet key | syncs once a second (adaptive), not every 100 ms |
| Changed limit | starts from a fresh window; unchanged limits keep their live state |
| `local_allowance ≥ 1.0` | local-only: each replica enforces the full limit, no Redis |

Hits are charged to the window they were admitted in: the sync task snapshots each key's admits
2 ms before every second boundary, so a push that lands after a boundary does not starve the next
window.

### Measured

`tests/redis_cluster.rs` — 4 replicas, 16 callers at 5× a 50 rps limit, real Valkey:

```
per-second (admitted, offered): [(16, 16), (50, 256), (50, 240), (50, 240), (50, 240)]
check latency p50=430ns p99=905µs max=1.65ms
```

`examples/bench.rs` — hot path, 10 000 keys × 3 windows, 16-core host, one variant per process
(`cargo run --release --example bench -- resil|gov|today8`). Baselines: a per-replica `governor`
limiter, and a common cached-counter design (per-replica governor plus a Redis count refreshed every
200 ms, 80 % of decisions made locally):

| | cached counter (`local_allowance 0.8`) | `governor` (per replica) | **resil** (cluster-wide) |
|---|---|---|---|
| 1 task | 424 ns/op | 181–215 ns/op | **83–95 ns/op** |
| 16 tasks, one hot key | 3.3 M ops/s | 4.6–5.9 M ops/s | **18–22 M ops/s** |
| 16 tasks, 10 000 keys | 0.28 M ops/s | 14–15 M ops/s | **55–64 M ops/s** |

## Use

```toml
[dependencies]
# pick the adapter for the `redis` crate version your service already links
resil = { git = "https://github.com/BudEcosystem/resil", rev = "<sha>", features = ["redis-0-31"] }
```

```rust
use std::sync::Arc;
use resil::limit::redis::RedisStore;
use resil::{Decision, Limiter, LimiterOptions, RateLimitConfig};

let conn = redis::aio::ConnectionManager::new(redis::Client::open(url)?).await?;
let limiter = Limiter::new(
    // `service` namespaces the Redis keys and metric labels: one per replica set.
    LimiterOptions { service: "my-gateway".into(), ..Default::default() },
    Some(Arc::new(RedisStore::new(conn))),
);
limiter.start().await;                       // heartbeat + background sync
limiter.set_policy(key, Some(&rate_limits), max_concurrent);

match limiter.check(key).await {
    Decision::Unlimited => {}                // no limit: no headers
    Decision::Allow(h) => { /* add X-RateLimit-* from h */ }
    Decision::Deny(h) => { /* resil::http::rate_limited(&h) */ }
}
let _slot = limiter.acquire(key).await;      // Ok(None) when no max_concurrent
```

Resilience around an upstream call:

```rust
use resil::retry::{retry, RetryPolicy};

let policy = RetryPolicy::interactive(&retry_config); // or ::standard for background work
let outcome = retry(&policy, Some(deadline), |_attempt| call_upstream(), |e| e.verdict()).await;
```

Other Redis clients: implement `resil::limit::redis::RedisExec` (one method: run commands as a
pipeline).

## Tests

```sh
docker run -d -p 6390:6379 valkey/valkey:8-alpine
RESIL_TEST_REDIS_URL=redis://127.0.0.1:6390 cargo test --release --features redis-0-31
```

| Suite | Checks |
|---|---|
| unit | algorithm maths (randomised: `Retry-After` never undershoots), policy parsing, classification, retry, breakers, fallback |
| `tests/redis_parity.rs` | the Lua scripts reply exactly like the in-process reference store on random operation sequences |
| `tests/simulation.rs` | N replicas on a virtual clock: never over the limit for N = 1–8 at 0.5–50× load, no starvation, skewed traffic, last-mile, fail-static, outage payback, lease expiry, adaptive sync |
| `tests/redis_cluster.rs` | the same in real time against Redis, a slow Redis, cluster-wide concurrency caps |

MSRV 1.88.
