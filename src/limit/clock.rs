//! Time sources. The limiter reads a monotonic clock on the hot path and maps it onto the store's
//! clock with an offset measured at each sync, so replica wall clocks never matter.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync + 'static {
    /// Monotonic milliseconds since an arbitrary epoch.
    fn mono_ms(&self) -> f64;
    /// Best-effort Unix milliseconds (only used when there is no store to measure against).
    fn unix_ms(&self) -> f64;
}

/// Monotonic time from the TSC (`quanta`, as `governor` uses), a few ns per read.
pub struct SystemClock {
    clock: quanta::Clock,
    start: u64,
    unix_at_start: f64,
}

impl Default for SystemClock {
    fn default() -> Self {
        let unix_at_start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64() * 1000.0)
            .unwrap_or(0.0);
        let clock = quanta::Clock::new();
        let start = clock.raw();
        Self {
            clock,
            start,
            unix_at_start,
        }
    }
}

impl Clock for SystemClock {
    #[inline]
    fn mono_ms(&self) -> f64 {
        self.clock.delta_as_nanos(self.start, self.clock.raw()) as f64 / 1e6
    }
    fn unix_ms(&self) -> f64 {
        self.unix_at_start + self.mono_ms()
    }
}

/// The limiter's clock: the system clock inline, anything else behind a trait object.
pub(crate) enum ClockSrc {
    System(SystemClock),
    Custom(std::sync::Arc<dyn Clock>),
}

impl ClockSrc {
    #[inline]
    pub(crate) fn mono_ms(&self) -> f64 {
        match self {
            ClockSrc::System(c) => c.mono_ms(),
            ClockSrc::Custom(c) => c.mono_ms(),
        }
    }
    pub(crate) fn unix_ms(&self) -> f64 {
        match self {
            ClockSrc::System(c) => c.unix_ms(),
            ClockSrc::Custom(c) => c.unix_ms(),
        }
    }
}

/// A clock tests move by hand. Starts at a realistic Unix time so window maths looks like prod.
pub struct ManualClock {
    now: AtomicU64,
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new(1_750_000_000_000.0)
    }
}

impl ManualClock {
    pub fn new(start_ms: f64) -> Self {
        Self {
            now: AtomicU64::new(start_ms.to_bits()),
        }
    }
    pub fn now(&self) -> f64 {
        f64::from_bits(self.now.load(Ordering::SeqCst))
    }
    pub fn set(&self, ms: f64) {
        self.now.store(ms.to_bits(), Ordering::SeqCst);
    }
    pub fn advance(&self, ms: f64) {
        self.set(self.now() + ms);
    }
}

impl Clock for ManualClock {
    fn mono_ms(&self) -> f64 {
        self.now()
    }
    fn unix_ms(&self) -> f64 {
        self.now()
    }
}

/// An `f64` in an `AtomicU64`.
#[derive(Debug, Default)]
pub(crate) struct AtomicF64(AtomicU64);

impl AtomicF64 {
    pub(crate) fn new(v: f64) -> Self {
        Self(AtomicU64::new(v.to_bits()))
    }
    #[inline]
    pub(crate) fn load(&self) -> f64 {
        f64::from_bits(self.0.load(Ordering::Relaxed))
    }
    #[inline]
    pub(crate) fn store(&self, v: f64) {
        self.0.store(v.to_bits(), Ordering::Relaxed)
    }
}
