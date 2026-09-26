//! Pure algorithm math, shared by the hot path (local estimates), the in-process reference store
//! and — line for line — the Lua scripts in `lua/`. Times are store milliseconds (Redis `TIME`),
//! as `f64` so the token bucket keeps sub-millisecond periods.

/// The three admin algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Algorithm {
    FixedWindow,
    SlidingWindow,
    TokenBucket,
}

impl Algorithm {
    /// The integer code the Lua scripts receive.
    pub fn code(self) -> u8 {
        match self {
            Algorithm::FixedWindow => 0,
            Algorithm::SlidingWindow => 1,
            Algorithm::TokenBucket => 2,
        }
    }
}

/// One configured window: `limit` requests per `window_ms`; `burst` is the bucket depth for the
/// token bucket (equal to `limit` unless `burst_size` is set) and unused otherwise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WindowSpec {
    pub limit: u64,
    pub window_ms: u64,
    pub burst: u64,
}

impl WindowSpec {
    /// How many requests the window holds when empty.
    #[inline]
    pub fn capacity(&self, alg: Algorithm) -> f64 {
        match alg {
            Algorithm::TokenBucket => self.burst as f64,
            _ => self.limit as f64,
        }
    }

    /// GCRA emission interval `T = W / L`.
    #[inline]
    pub fn period_ms(&self) -> f64 {
        self.window_ms as f64 / self.limit as f64
    }
}

/// The store-side state of one window. Counter algorithms use `idx`/`cur`/`prev`; the token
/// bucket uses `tat` (theoretical arrival time).
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct WindowState {
    pub idx: u64,
    pub cur: u64,
    pub prev: u64,
    pub tat: f64,
}

#[inline]
fn window_index(t: f64, w: u64) -> u64 {
    if t <= 0.0 {
        0
    } else {
        (t / w as f64).floor() as u64
    }
}

/// Advance a counter state to the window containing `t`. Never moves backwards.
#[inline]
pub fn roll(alg: Algorithm, spec: &WindowSpec, s: WindowState, t: f64) -> WindowState {
    if alg == Algorithm::TokenBucket {
        return s;
    }
    let i = window_index(t, spec.window_ms);
    if i <= s.idx {
        s
    } else if i == s.idx + 1 {
        WindowState {
            idx: i,
            cur: 0,
            prev: s.cur,
            tat: s.tat,
        }
    } else {
        WindowState {
            idx: i,
            cur: 0,
            prev: 0,
            tat: s.tat,
        }
    }
}

/// Estimated usage at `t` (may exceed capacity: debt).
#[inline]
pub fn usage(alg: Algorithm, spec: &WindowSpec, s: WindowState, t: f64) -> f64 {
    let s = roll(alg, spec, s, t);
    match alg {
        Algorithm::FixedWindow => s.cur as f64,
        Algorithm::SlidingWindow => {
            let w = spec.window_ms as f64;
            let pos = (t - s.idx as f64 * w).clamp(0.0, w);
            s.prev as f64 * (1.0 - pos / w) + s.cur as f64
        }
        Algorithm::TokenBucket => (s.tat.max(t) - t) / spec.period_ms(),
    }
}

/// `capacity - usage(t)`: what the window can still take at `t`, before reservations.
#[inline]
pub fn free(alg: Algorithm, spec: &WindowSpec, s: WindowState, t: f64) -> f64 {
    spec.capacity(alg) - usage(alg, spec, s, t)
}

/// Record `n` admitted requests at `t`. Hits are **forced**: a replica has already admitted them,
/// so the state absorbs them even past the limit (debt that later denials pay back). Token-bucket
/// debt is capped at one window beyond an empty bucket.
#[inline]
pub fn apply(alg: Algorithm, spec: &WindowSpec, s: WindowState, t: f64, n: u64) -> WindowState {
    let mut s = roll(alg, spec, s, t);
    match alg {
        Algorithm::FixedWindow | Algorithm::SlidingWindow => {
            s.cur = s.cur.saturating_add(n);
        }
        Algorithm::TokenBucket => {
            let p = spec.period_ms();
            let cap = t + spec.burst as f64 * p + spec.window_ms as f64;
            s.tat = (s.tat.max(t) + n as f64 * p).min(cap);
        }
    }
    s
}

/// Record `n` hits admitted at an earlier time `t_hit` (pushed late, e.g. across a window
/// boundary). They are charged to the window they were admitted in: a finished fixed window is
/// over, so they are dropped; a sliding window carries them in `prev`; the token bucket debits at
/// `t_hit`.
#[inline]
pub fn apply_past(
    alg: Algorithm,
    spec: &WindowSpec,
    s: WindowState,
    t_hit: f64,
    n: u64,
) -> WindowState {
    if n == 0 {
        return s;
    }
    match alg {
        Algorithm::TokenBucket => apply(alg, spec, s, t_hit, n),
        _ => {
            let i = window_index(t_hit, spec.window_ms);
            if i >= s.idx {
                apply(alg, spec, s, t_hit, n)
            } else if i + 1 == s.idx && alg == Algorithm::SlidingWindow {
                WindowState {
                    prev: s.prev.saturating_add(n),
                    ..s
                }
            } else {
                s
            }
        }
    }
}

/// Round a wait up to whole milliseconds (at least 1), ignoring float noise below a nanosecond.
#[inline]
fn ceil_ms(d: f64) -> f64 {
    (d - 1e-6).ceil().max(1.0)
}

/// Milliseconds from `t` until the window can admit one more request with `reserved` units held
/// by others (leases, unpushed local hits). 0 when it can admit now. Never undershoots.
pub fn retry_after_ms(
    alg: Algorithm,
    spec: &WindowSpec,
    s: WindowState,
    t: f64,
    reserved: f64,
) -> f64 {
    let s = roll(alg, spec, s, t);
    let l = spec.capacity(alg);
    let w = spec.window_ms as f64;
    match alg {
        Algorithm::FixedWindow => {
            if s.cur as f64 + reserved + 1.0 <= l {
                0.0
            } else {
                ((s.idx + 1) as f64 * w - t).max(1.0)
            }
        }
        Algorithm::SlidingWindow => {
            // Absolute times: `t` may sit slightly before the state's window when the local
            // estimate of store time lags the store.
            let start = s.idx as f64 * w;
            let next = start + w;
            let room = l - 1.0 - reserved - s.cur as f64;
            if room >= 0.0 {
                if s.prev == 0 {
                    return 0.0;
                }
                // prev·(1 − f) ≤ room  ⇔  f ≥ 1 − room/prev
                let f = (1.0 - room / s.prev as f64).max(0.0);
                let at = start + f * w;
                if at <= t {
                    0.0
                } else {
                    ceil_ms(at - t)
                }
            } else {
                // Not in this window. Next window: prev' = cur, cur' = 0.
                let room_next = l - 1.0 - reserved;
                if room_next < 0.0 {
                    return ceil_ms(next + w - t);
                }
                if s.cur == 0 {
                    return ceil_ms(next - t);
                }
                let f = (1.0 - room_next / s.cur as f64).max(0.0);
                ceil_ms(next + f * w - t)
            }
        }
        Algorithm::TokenBucket => {
            let p = spec.period_ms();
            let tat = s.tat.max(t);
            let slack = (l - 1.0 - reserved) * p;
            if slack < 0.0 {
                // More reserved than the bucket holds: wait for it to be full.
                return (tat - t).max(1.0);
            }
            let d = tat - t - slack;
            if d <= 0.0 {
                0.0
            } else {
                ceil_ms(d)
            }
        }
    }
}

/// Milliseconds until the window "resets": the end of the current window for the counters, the
/// time until the bucket is full for the token bucket.
pub fn reset_ms(alg: Algorithm, spec: &WindowSpec, s: WindowState, t: f64) -> f64 {
    let s = roll(alg, spec, s, t);
    match alg {
        Algorithm::TokenBucket => (s.tat - t).max(0.0),
        _ => {
            let idx = s.idx.max(window_index(t, spec.window_ms));
            ((idx + 1) as f64 * spec.window_ms as f64 - t).max(0.0)
        }
    }
}

/// How much a sync may grant, and how many waiting requests it admits directly (last-mile), given
/// the unreserved budget `free` (already floored, across all windows). Mirrors the Lua exactly.
#[inline]
pub fn grant(free: f64, want: u64, need: u64, allowance: f64, replicas: u32) -> (u64, u64) {
    if free < 1.0 {
        return (0, 0);
    }
    let free = free.floor();
    let n = f64::from(replicas.max(1));
    let fair = (allowance.min(1.0) * free / n).floor();
    let granted = (want as f64).min(fair).max(0.0);
    let left = free - granted;
    let mut direct = 0.0;
    if (need as f64) > granted && left >= 1.0 {
        direct = (need as f64 - granted).min(left);
    }
    (granted as u64, direct as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FW: Algorithm = Algorithm::FixedWindow;
    const SW: Algorithm = Algorithm::SlidingWindow;
    const TB: Algorithm = Algorithm::TokenBucket;

    fn spec(limit: u64, window_ms: u64) -> WindowSpec {
        WindowSpec {
            limit,
            window_ms,
            burst: limit,
        }
    }

    #[test]
    fn fixed_window_counts_and_resets_on_boundary() {
        let sp = spec(10, 1000);
        let mut s = WindowState::default();
        s = apply(FW, &sp, s, 5_000.0, 10);
        assert_eq!(usage(FW, &sp, s, 5_999.0), 10.0);
        assert_eq!(free(FW, &sp, s, 5_999.0), 0.0);
        assert_eq!(retry_after_ms(FW, &sp, s, 5_500.0, 0.0), 500.0);
        assert_eq!(usage(FW, &sp, s, 6_000.0), 0.0);
        assert_eq!(retry_after_ms(FW, &sp, s, 6_000.0, 0.0), 0.0);
    }

    #[test]
    fn sliding_window_weights_previous_window() {
        let sp = spec(10, 1000);
        let mut s = apply(SW, &sp, WindowState::default(), 5_100.0, 10);
        // 25% into the next window: 10·0.75 = 7.5 used.
        assert!((usage(SW, &sp, s, 6_250.0) - 7.5).abs() < 1e-9);
        s = apply(SW, &sp, s, 6_250.0, 2);
        assert!((usage(SW, &sp, s, 6_250.0) - 9.5).abs() < 1e-9);
        // Needs prev·(1−f) + 2 + 1 ≤ 10 → f ≥ 0.3 → 50 ms after 6 250.
        assert_eq!(retry_after_ms(SW, &sp, s, 6_250.0, 0.0), 50.0);
        assert_eq!(retry_after_ms(SW, &sp, s, 6_300.0, 0.0), 0.0);
    }

    #[test]
    fn sliding_window_full_current_waits_into_next_window() {
        let sp = spec(10, 1000);
        let s = apply(SW, &sp, WindowState::default(), 5_100.0, 10);
        // Next window starts at 6 000 with prev' = 10: need 10·(1−f) + 1 ≤ 10 → f ≥ 0.1 → 6 100.
        assert_eq!(retry_after_ms(SW, &sp, s, 5_500.0, 0.0), 600.0);
    }

    #[test]
    fn sliding_window_two_windows_later_forgets() {
        let sp = spec(10, 1000);
        let s = apply(SW, &sp, WindowState::default(), 5_100.0, 10);
        assert_eq!(usage(SW, &sp, s, 7_000.0), 0.0);
    }

    #[test]
    fn token_bucket_burst_then_steady_rate() {
        let sp = WindowSpec {
            limit: 10,
            window_ms: 1000,
            burst: 5,
        };
        let t = 1_000_000.0;
        let s = apply(TB, &sp, WindowState::default(), t, 5);
        assert!((usage(TB, &sp, s, t) - 5.0).abs() < 1e-9);
        assert!(free(TB, &sp, s, t) < 1.0);
        // one token refills every 100 ms
        assert_eq!(retry_after_ms(TB, &sp, s, t, 0.0), 100.0);
        assert!(free(TB, &sp, s, t + 100.0) >= 1.0 - 1e-9);
        // full after 500 ms
        assert_eq!(reset_ms(TB, &sp, s, t), 500.0);
    }

    #[test]
    fn token_bucket_forced_debit_creates_capped_debt() {
        let sp = spec(10, 1000);
        let t = 1_000_000.0;
        let s = apply(TB, &sp, WindowState::default(), t, 1_000);
        // capped at burst (1 s) + one window (1 s)
        assert!((s.tat - (t + 2_000.0)).abs() < 1e-9);
        assert!(free(TB, &sp, s, t + 1_000.0) <= 0.0);
        assert!(free(TB, &sp, s, t + 1_100.0) >= 1.0 - 1e-9);
    }

    #[test]
    fn retry_after_counts_reservations() {
        let sp = spec(10, 1000);
        let s = apply(FW, &sp, WindowState::default(), 5_000.0, 5);
        assert_eq!(retry_after_ms(FW, &sp, s, 5_000.0, 4.0), 0.0);
        assert_eq!(retry_after_ms(FW, &sp, s, 5_000.0, 5.0), 1000.0);
    }

    #[test]
    fn retry_after_never_undershoots_randomised() {
        // For every algorithm and random state, waiting exactly retry_after must leave room.
        let mut rng = fastrand::Rng::with_seed(7);
        for alg in [FW, SW, TB] {
            for _ in 0..20_000 {
                let limit = rng.u64(1..200);
                let sp = WindowSpec {
                    limit,
                    window_ms: [1000, 60_000][rng.usize(0..2)],
                    burst: if alg == TB { rng.u64(1..=limit) } else { limit },
                };
                let t0 = 1.7e12 + rng.f64() * 1e6;
                let mut s = WindowState::default();
                for _ in 0..rng.usize(1..4) {
                    let dt = rng.f64() * sp.window_ms as f64;
                    s = apply(alg, &sp, s, t0 + dt, rng.u64(0..limit * 2));
                }
                let t = t0 + rng.f64() * 2.0 * sp.window_ms as f64;
                let reserved = rng.u64(0..3) as f64;
                let d = retry_after_ms(alg, &sp, s, t, reserved);
                let f = free(alg, &sp, s, t + d) - reserved;
                if reserved + 1.0 <= sp.capacity(alg) {
                    assert!(f >= 1.0 - 1e-6, "{alg:?} {sp:?} {s:?} t={t} d={d} free={f}");
                }
            }
        }
    }

    #[test]
    fn late_hits_are_charged_to_their_own_window() {
        let sp = spec(10, 1000);
        // someone already rolled the state into window 6
        let s = apply(FW, &sp, WindowState::default(), 6_010.0, 3);
        let f = apply_past(FW, &sp, s, 5_998.0, 4);
        assert_eq!(f.cur, 3, "a finished fixed window is over");
        let sw = apply(SW, &sp, WindowState::default(), 6_010.0, 3);
        let sw = apply_past(SW, &sp, sw, 5_998.0, 4);
        assert_eq!((sw.prev, sw.cur), (4, 3));
        // state still in the old window: counted there, then rolled
        let s = apply_past(FW, &sp, WindowState::default(), 5_998.0, 4);
        assert_eq!((s.idx, s.cur), (5, 4));
        let s = apply(SW, &sp, s, 6_001.0, 1);
        assert_eq!((s.prev, s.cur), (4, 1));
        // two windows back: gone
        let s = apply(SW, &sp, WindowState::default(), 7_010.0, 1);
        assert_eq!(apply_past(SW, &sp, s, 5_998.0, 4), s);
    }

    #[test]
    fn grant_splits_by_allowance_and_replicas() {
        assert_eq!(grant(100.0, 1_000, 0, 0.8, 4), (20, 0));
        assert_eq!(grant(100.0, 5, 0, 0.8, 4), (5, 0));
        // last-mile: the budget is too small to split, waiting requests get it directly
        assert_eq!(grant(3.0, 10, 2, 0.8, 4), (0, 2));
        assert_eq!(grant(3.0, 10, 5, 0.8, 4), (0, 3));
        assert_eq!(grant(0.9, 10, 5, 0.8, 4), (0, 0));
        // waiters beyond the grant take the rest directly
        assert_eq!(grant(10.0, 1, 3, 0.8, 1), (1, 2));
    }
}
