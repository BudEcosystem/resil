//! The shared store: request/reply types, the [`Store`] trait, and [`MemStore`] — the in-process
//! reference implementation. The Redis scripts must behave exactly like `MemStore`; the parity
//! test (`tests/redis_parity.rs`) runs the same operation sequences against both.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use super::algo::{self, Algorithm, WindowSpec, WindowState};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Store failure. The limiter treats every error the same way (the store is unavailable).
#[derive(Debug, Clone)]
pub struct StoreError(pub String);

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "limit store error: {}", self.0)
    }
}
impl std::error::Error for StoreError {}

/// One rate key's sync: push `hits`, report the credit still held, ask for more.
#[derive(Debug, Clone)]
pub struct RateSync {
    /// Full store key, hash-tagged on the limit key (`rl2:{svc:subject}:cfg`).
    pub key: Arc<str>,
    pub alg: Algorithm,
    pub windows: Arc<[WindowSpec]>,
    pub pod: Arc<str>,
    /// Requests this replica admitted locally since its last successful push (after `t_old`).
    pub hits: u64,
    /// Of the pushed hits, those admitted before a window boundary the push crossed...
    pub hits_old: u64,
    /// ...at this store time (just before the boundary). Ignored when `hits_old` is 0.
    pub t_old_ms: f64,
    /// Credit this replica still holds (its reservation carries over).
    pub unused: u64,
    /// Additional credit wanted for the coming interval.
    pub want: u64,
    /// Requests waiting right now; admitted directly (last-mile) when the budget is too small to
    /// split.
    pub need: u64,
    pub allowance: f64,
    pub replicas: u32,
    pub lease_ttl_ms: u64,
    /// Tests only: use this store time instead of the store clock.
    pub now_override_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RateReply {
    /// Store time at execution (ms).
    pub now_ms: f64,
    /// How much of the reported `unused` credit the store still honours (all of it while this
    /// replica's lease is live; re-validated against the free budget once the lease has lapsed).
    pub kept: u64,
    /// Unreserved budget before this replica's reservation, floored (min across windows).
    pub avail: i64,
    pub granted: u64,
    /// Waiting requests admitted directly (already recorded as hits).
    pub direct: u64,
    /// Credit held by other replicas.
    pub reserved_others: u64,
    /// Window states after the push.
    pub windows: Vec<WindowState>,
}

/// One concurrency key's sync: publish this replica's absolute count and reservation.
#[derive(Debug, Clone)]
pub struct ConcSync {
    pub key: Arc<str>,
    pub pod: Arc<str>,
    pub active: u64,
    pub unused: u64,
    pub want: u64,
    pub need: u64,
    pub max: u64,
    pub allowance: f64,
    pub replicas: u32,
    pub lease_ttl_ms: u64,
    pub now_override_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ConcReply {
    pub now_ms: f64,
    pub kept: u64,
    pub granted: u64,
    pub direct: u64,
    /// Slots held or reserved by other live replicas.
    pub others: u64,
}

/// Replica registry heartbeat (`N`).
#[derive(Debug, Clone)]
pub struct Heartbeat {
    pub key: Arc<str>,
    pub pod: Arc<str>,
    /// A replica is live if it heart-beat within this window.
    pub live_window_ms: u64,
    /// Deregister (graceful shutdown).
    pub leave: bool,
    pub now_override_ms: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeartbeatReply {
    pub now_ms: f64,
    pub replicas: u32,
}

/// Everything one sync round sends, in one round-trip.
#[derive(Debug, Clone, Default)]
pub struct Batch {
    pub rate: Vec<RateSync>,
    pub conc: Vec<ConcSync>,
    pub heartbeat: Option<Heartbeat>,
    /// Release this replica's reservations on these keys (graceful shutdown).
    pub release: Vec<(Arc<str>, Arc<str>)>,
}

#[derive(Debug, Clone, Default)]
pub struct BatchReply {
    pub rate: Vec<RateReply>,
    pub conc: Vec<ConcReply>,
    pub heartbeat: Option<HeartbeatReply>,
}

/// A shared limit store. One call is one round-trip.
pub trait Store: Send + Sync + 'static {
    fn round_trip<'a>(&'a self, batch: &'a Batch) -> BoxFuture<'a, Result<BatchReply, StoreError>>;
}

// --------------------------------------------------------------------------------------------
// Reference implementation
// --------------------------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct RateKey {
    windows: Vec<WindowState>,
    /// pod → (credit, expiry)
    leases: HashMap<Arc<str>, (u64, f64)>,
}

#[derive(Debug, Clone, Default)]
struct ConcKey {
    /// pod → (allowed, expiry)
    held: HashMap<Arc<str>, (u64, f64)>,
}

#[derive(Default)]
struct MemInner {
    rate: HashMap<Arc<str>, RateKey>,
    conc: HashMap<Arc<str>, ConcKey>,
    pods: HashMap<Arc<str>, HashMap<Arc<str>, f64>>,
}

/// In-process store with the exact semantics of the Redis scripts. Used as the reference model in
/// tests, and as the engine of local-only and degraded modes.
pub struct MemStore {
    inner: Mutex<MemInner>,
    clock: Arc<dyn Fn() -> f64 + Send + Sync>,
    fail: std::sync::atomic::AtomicBool,
}

impl MemStore {
    /// `clock` returns the store's "now" in ms.
    pub fn new(clock: Arc<dyn Fn() -> f64 + Send + Sync>) -> Self {
        Self {
            inner: Mutex::new(MemInner::default()),
            clock,
            fail: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Tests: make every round-trip fail (a store outage).
    pub fn set_failing(&self, failing: bool) {
        self.fail
            .store(failing, std::sync::atomic::Ordering::SeqCst);
    }

    fn now(&self, over: Option<f64>) -> f64 {
        over.unwrap_or_else(|| (self.clock)())
    }

    pub fn sync_rate(&self, r: &RateSync) -> RateReply {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = self.now(r.now_override_ms);
        let st = g.rate.entry(r.key.clone()).or_default();
        if st.windows.len() != r.windows.len() {
            st.windows = vec![WindowState::default(); r.windows.len()];
        }
        for (s, spec) in st.windows.iter_mut().zip(r.windows.iter()) {
            *s = algo::apply_past(r.alg, spec, *s, r.t_old_ms, r.hits_old);
            *s = algo::apply(r.alg, spec, *s, now, r.hits);
        }
        st.leases.retain(|_, (_, exp)| *exp > now);
        let others: u64 = st
            .leases
            .iter()
            .filter(|(p, _)| **p != r.pod)
            .map(|(_, (c, _))| *c)
            .sum();
        let mut avail = f64::INFINITY;
        for (s, spec) in st.windows.iter().zip(r.windows.iter()) {
            avail = avail.min(algo::free(r.alg, spec, *s, now) - others as f64);
        }
        let avail = avail.floor();
        let kept = match st.leases.get(&r.pod) {
            Some((held, _)) => r.unused.min(*held),
            None => r.unused.min(avail.max(0.0) as u64),
        };
        let (granted, direct) =
            algo::grant(avail - kept as f64, r.want, r.need, r.allowance, r.replicas);
        if direct > 0 {
            for (s, spec) in st.windows.iter_mut().zip(r.windows.iter()) {
                *s = algo::apply(r.alg, spec, *s, now, direct);
            }
        }
        let mine = kept + granted;
        if mine > 0 {
            st.leases
                .insert(r.pod.clone(), (mine, now + r.lease_ttl_ms as f64));
        } else {
            st.leases.remove(&r.pod);
        }
        RateReply {
            now_ms: now,
            kept,
            avail: avail as i64,
            granted,
            direct,
            reserved_others: others,
            windows: st.windows.clone(),
        }
    }

    pub fn sync_conc(&self, c: &ConcSync) -> ConcReply {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = self.now(c.now_override_ms);
        let st = g.conc.entry(c.key.clone()).or_default();
        st.held.retain(|_, (_, exp)| *exp > now);
        let others: u64 = st
            .held
            .iter()
            .filter(|(p, _)| **p != c.pod)
            .map(|(_, (n, _))| *n)
            .sum();
        let avail = (c.max as f64 - others as f64 - c.active as f64).floor();
        let kept = match st.held.get(&c.pod) {
            Some((held, _)) => c.unused.min(held.saturating_sub(c.active)),
            None => c.unused.min(avail.max(0.0) as u64),
        };
        let (granted, direct) =
            algo::grant(avail - kept as f64, c.want, c.need, c.allowance, c.replicas);
        let mine = c.active + kept + granted + direct;
        if mine > 0 {
            st.held
                .insert(c.pod.clone(), (mine, now + c.lease_ttl_ms as f64));
        } else {
            st.held.remove(&c.pod);
        }
        ConcReply {
            now_ms: now,
            kept,
            granted,
            direct,
            others,
        }
    }

    pub fn heartbeat(&self, h: &Heartbeat) -> HeartbeatReply {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = self.now(h.now_override_ms);
        let set = g.pods.entry(h.key.clone()).or_default();
        if h.leave {
            set.remove(&h.pod);
        } else {
            set.insert(h.pod.clone(), now);
        }
        let floor = now - h.live_window_ms as f64;
        set.retain(|_, seen| *seen > floor);
        HeartbeatReply {
            now_ms: now,
            replicas: set.len().max(1) as u32,
        }
    }

    pub fn release(&self, key: &Arc<str>, pod: &Arc<str>) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(k) = g.rate.get_mut(key) {
            k.leases.remove(pod);
        }
        if let Some(k) = g.conc.get_mut(key) {
            k.held.remove(pod);
        }
    }

    /// Run a whole batch synchronously (local-only and degraded modes call this directly).
    pub fn run(&self, b: &Batch) -> BatchReply {
        for (k, p) in &b.release {
            self.release(k, p);
        }
        BatchReply {
            rate: b.rate.iter().map(|r| self.sync_rate(r)).collect(),
            conc: b.conc.iter().map(|c| self.sync_conc(c)).collect(),
            heartbeat: b.heartbeat.as_ref().map(|h| self.heartbeat(h)),
        }
    }
}

impl Store for MemStore {
    fn round_trip<'a>(&'a self, batch: &'a Batch) -> BoxFuture<'a, Result<BatchReply, StoreError>> {
        Box::pin(async move {
            if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(StoreError("store unavailable (test)".into()));
            }
            Ok(self.run(batch))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> MemStore {
        MemStore::new(Arc::new(|| 0.0))
    }

    fn rs(pod: &str, hits: u64, unused: u64, want: u64, need: u64, now: f64) -> RateSync {
        RateSync {
            key: Arc::from("rl2:{t:k}:c"),
            alg: Algorithm::FixedWindow,
            windows: Arc::from(vec![WindowSpec {
                limit: 100,
                window_ms: 60_000,
                burst: 100,
            }]),
            pod: Arc::from(pod),
            hits,
            hits_old: 0,
            t_old_ms: 0.0,
            unused,
            want,
            need,
            allowance: 0.8,
            replicas: 4,
            lease_ttl_ms: 3_000,
            now_override_ms: Some(now),
        }
    }

    #[test]
    fn grants_are_reserved_against_other_replicas() {
        let s = store();
        let a = s.sync_rate(&rs("a", 0, 0, 1_000, 0, 60_000.0));
        assert_eq!(a.granted, 20); // ⌊0.8·100/4⌋
        let b = s.sync_rate(&rs("b", 0, 0, 1_000, 0, 60_000.0));
        assert_eq!(b.reserved_others, 20);
        assert_eq!(b.granted, 16); // ⌊0.8·80/4⌋
    }

    #[test]
    fn sum_of_grants_and_hits_never_exceeds_limit() {
        // A hot replica drains the budget through early syncs while three others hold leases:
        // the leases are what stops the cluster from overshooting.
        let s = store();
        let mut held = HashMap::new();
        for p in ["b", "c", "d"] {
            let r = s.sync_rate(&rs(p, 0, 0, 1_000, 0, 60_000.0));
            held.insert(p, r.granted);
        }
        let mut credit = 0u64;
        let mut admitted = 0u64;
        let mut t = 60_000.0;
        for _ in 0..200 {
            // spend all of the credit, push it, ask again
            admitted += credit;
            let r = s.sync_rate(&rs("a", credit, 0, 1_000, 1, t));
            admitted += r.direct;
            credit = r.granted;
            t += 1.0;
        }
        // the other replicas now spend what they hold
        let total = admitted + held.values().sum::<u64>();
        assert!(total <= 100, "cluster admitted {total} > 100");
        assert!(total >= 95, "cluster starved: {total}");
    }

    #[test]
    fn expired_leases_are_reclaimed() {
        let s = store();
        s.sync_rate(&rs("dead", 0, 0, 1_000, 0, 60_000.0));
        let r = s.sync_rate(&rs("a", 0, 0, 1_000, 0, 63_001.0));
        assert_eq!(r.reserved_others, 0);
    }

    #[test]
    fn last_mile_admits_waiters_directly() {
        let s = store();
        let mut r = rs("a", 0, 0, 10, 2, 60_000.0);
        r.windows = Arc::from(vec![WindowSpec {
            limit: 3,
            window_ms: 1_000,
            burst: 3,
        }]);
        let a = s.sync_rate(&r);
        assert_eq!((a.granted, a.direct), (0, 2));
        let b = s.sync_rate(&r);
        assert_eq!((b.granted, b.direct), (0, 1));
        let c = s.sync_rate(&r);
        assert_eq!((c.granted, c.direct), (0, 0));
    }

    #[test]
    fn concurrency_counts_are_absolute_and_expire() {
        let s = store();
        let mk = |pod: &str, active, need, now| ConcSync {
            key: Arc::from("cc2:{t:k}"),
            pod: Arc::from(pod),
            active,
            unused: 0,
            want: 0,
            need,
            max: 3,
            allowance: 0.8,
            replicas: 2,
            lease_ttl_ms: 5_000,
            now_override_ms: Some(now),
        };
        let a = s.sync_conc(&mk("a", 2, 0, 0.0));
        assert_eq!(a.others, 0);
        // idempotent: the same absolute report twice does not double count
        s.sync_conc(&mk("a", 2, 0, 1.0));
        let b = s.sync_conc(&mk("b", 0, 1, 2.0));
        assert_eq!(b.others, 2);
        assert_eq!(b.direct, 1);
        let b2 = s.sync_conc(&mk("b", 1, 1, 3.0));
        assert_eq!(b2.direct, 0, "3 of 3 slots taken");
        // a dies; after its TTL b gets its slots back
        let b3 = s.sync_conc(&mk("b", 1, 1, 5_001.0));
        assert_eq!(b3.others, 0);
        assert_eq!(b3.direct, 1);
    }

    #[test]
    fn heartbeat_counts_live_replicas() {
        let s = store();
        let hb = |pod: &str, now: f64, leave| Heartbeat {
            key: Arc::from("rl2:{t}:pods"),
            pod: Arc::from(pod),
            live_window_ms: 3_000,
            leave,
            now_override_ms: Some(now),
        };
        assert_eq!(s.heartbeat(&hb("a", 0.0, false)).replicas, 1);
        assert_eq!(s.heartbeat(&hb("b", 1.0, false)).replicas, 2);
        assert_eq!(s.heartbeat(&hb("a", 3_500.0, false)).replicas, 1);
        assert_eq!(s.heartbeat(&hb("a", 3_600.0, true)).replicas, 1); // floor of 1
    }
}
