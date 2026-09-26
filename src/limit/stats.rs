//! Decision counters. Incrementing a labelled `metrics` counter on every request costs a registry
//! lookup, so the hot path bumps a thread-sharded atomic instead and the sync task exports the
//! totals.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

/// Every way a request is decided. `as usize` indexes the counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Admitted from local credit: no I/O.
    AllowLocal,
    /// Admitted after waiting for a sync (credit refill or last-mile).
    AllowSync,
    /// Admitted from cold-start credit.
    AllowCold,
    /// Admitted on a stale view because the store did not answer in time.
    AllowOverrun,
    /// Admitted by the local fallback while the store is unavailable.
    AllowDegraded,
    /// Denied from local state: no I/O.
    DenyLocal,
    /// Denied after a sync said the budget is spent.
    DenySync,
    /// Denied by the local fallback while the store is unavailable.
    DenyDegraded,
}

impl Outcome {
    pub const ALL: [Outcome; 8] = [
        Outcome::AllowLocal,
        Outcome::AllowSync,
        Outcome::AllowCold,
        Outcome::AllowOverrun,
        Outcome::AllowDegraded,
        Outcome::DenyLocal,
        Outcome::DenySync,
        Outcome::DenyDegraded,
    ];

    pub fn labels(self) -> (&'static str, &'static str) {
        match self {
            Outcome::AllowLocal => ("allow", "local"),
            Outcome::AllowSync => ("allow", "sync"),
            Outcome::AllowCold => ("allow", "cold"),
            Outcome::AllowOverrun => ("allow", "overrun"),
            Outcome::AllowDegraded => ("allow", "degraded"),
            Outcome::DenyLocal => ("deny", "local"),
            Outcome::DenySync => ("deny", "sync"),
            Outcome::DenyDegraded => ("deny", "degraded"),
        }
    }

    pub fn is_allow(self) -> bool {
        matches!(
            self,
            Outcome::AllowLocal
                | Outcome::AllowSync
                | Outcome::AllowCold
                | Outcome::AllowOverrun
                | Outcome::AllowDegraded
        )
    }
}

const SHARDS: usize = 32;
const KINDS: usize = Outcome::ALL.len();

#[repr(align(128))]
#[derive(Default)]
struct Shard([AtomicU64; KINDS]);

pub struct Stats {
    shards: Box<[Shard]>,
}

impl Default for Stats {
    fn default() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Shard::default()).collect(),
        }
    }
}

thread_local! {
    static SHARD: Cell<usize> = const { Cell::new(usize::MAX) };
}

fn shard_index() -> usize {
    SHARD.with(|s| {
        let v = s.get();
        if v != usize::MAX {
            return v;
        }
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let i = (NEXT.fetch_add(1, Ordering::Relaxed) as usize) % SHARDS;
        s.set(i);
        i
    })
}

impl Stats {
    #[inline]
    pub fn record(&self, o: Outcome) {
        self.shards[shard_index()].0[o as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn total(&self, o: Outcome) -> u64 {
        self.shards
            .iter()
            .map(|s| s.0[o as usize].load(Ordering::Relaxed))
            .sum()
    }

    /// Push the totals to the `metrics` recorder (called from the sync task).
    pub fn export(&self, service: &'static str) {
        for o in Outcome::ALL {
            let (outcome, mode) = o.labels();
            metrics::counter!("resil_decisions_total", "svc" => service, "outcome" => outcome, "mode" => mode)
                .absolute(self.total(o));
        }
    }
}
