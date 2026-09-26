# resil

Traffic policy for Bud's gateways — **budgateway** (LLM plane) and **WaaV** (audio plane). One
crate turns a deployment's *Rate limiting* and *Resilience* settings (budadmin → budapp →
`model_table` / `voice_table`) into behaviour, identically in both gateways.

| Module | Holds |
|---|---|
| `policy` | serde types for the deployment-settings contract: `rate_limits`, `max_concurrent`, `retry_config`, `fallback_models` (lenient parsing: a bad block is dropped, never the endpoint) |
| `limit` | local-first, Redis-synced rate limiter (fixed window, sliding window, token bucket; per second / minute / hour) and concurrency caps |
| `classify` | failure → retry / fail over / breaker verdict; `Retry-After` / `retry-after-ms`; vendor-concurrency 429s |
| `retry` | retry with classification, vendor delay hints and a deadline; budgateway and WaaV backoff profiles |
| `breaker` | keyed circuit breakers in two tiers: per deployment, per vendor (≥ 2 deployments failing) |
| `fallback` | ordered fallback chains: depth-first expansion, cycle guard, per-hop skip, shared deadline |
| `http` | 429 bodies, `X-RateLimit-*` headers, CORS expose list |

Design and rationale: `specs/022-gateway-rate-limit-resilience/` in
[bud-runtime](https://github.com/BudEcosystem/bud-runtime).

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

`examples/bench.rs` — hot path, 10 000 deployments × 3 windows, 16-core host, one variant per
process (`cargo run --release --example bench -- resil|gov|today8`):

| | budgateway today (`local_allowance 0.8`) | pure governor (per pod) | **resil** (cluster-wide) |
|---|---|---|---|
| 1 task | 424 ns/op | 181–215 ns/op | **83–95 ns/op** |
| 16 tasks, one hot deployment | 3.3 M ops/s | 4.6–5.9 M ops/s | **18–22 M ops/s** |
| 16 tasks, 10 000 deployments | 0.28 M ops/s | 14–15 M ops/s | **55–64 M ops/s** |

## Use

```toml
[dependencies]
# pick the adapter for the `redis` version your service already links
resil = { git = "https://github.com/BudEcosystem/resil", rev = "<sha>", features = ["redis-0-31"] }
```

```rust
use std::sync::Arc;
use resil::limit::redis::RedisStore;
use resil::{Decision, Limiter, LimiterOptions, RateLimitConfig};

let conn = redis::aio::ConnectionManager::new(redis::Client::open(url)?).await?;
let limiter = Limiter::new(
    LimiterOptions { service: "budgateway".into(), ..Default::default() },
    Some(Arc::new(RedisStore::new(conn))),
);
limiter.start().await;                       // heartbeat + background sync
limiter.set_policy(endpoint_id, Some(&rate_limits), max_concurrent);

match limiter.check(endpoint_id).await {
    Decision::Unlimited => {}                // no limit: no headers
    Decision::Allow(h) => { /* add X-RateLimit-* from h */ }
    Decision::Deny(h) => { /* resil::http::rate_limited(&h) */ }
}
let _slot = limiter.acquire(endpoint_id).await; // Ok(None) when no max_concurrent
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
| `tests/simulation.rs` | TEST_CASES §2 (TC-LF-*) on a virtual clock: never over the limit for N = 1–8 at 0.5–50× load, no starvation, skewed traffic, last-mile, fail-static, outage payback, lease expiry, adaptive sync |
| `tests/redis_cluster.rs` | the same in real time against Redis, a slow Redis, cluster-wide concurrency caps |

MSRV 1.88 (budgateway's toolchain).
