//! Retry with classification, vendor delay hints and a deadline (FRD §6.3).
//!
//! Two backoff profiles: budgateway's (1 s base, doubling, additive jitter — its historical
//! numbers) and WaaV's (250 ms base, full jitter — a voice turn cannot absorb a one-second floor).

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;

use crate::classify::Verdict;
use crate::policy::RetryConfig;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Jitter {
    None,
    /// `delay · U(0, 1)` (AWS "full jitter").
    Full,
    /// `delay + delay · U(0, 1)`, capped at `max` (backon's `with_jitter`).
    Additive,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Backoff {
    pub base: Duration,
    pub factor: f64,
    pub max: Duration,
    pub jitter: Jitter,
}

impl Backoff {
    /// budgateway: exponential from 1 s, doubling, jitter, capped at `max_delay_s`.
    pub fn budgateway(max_delay_s: f32) -> Self {
        Self {
            base: Duration::from_secs(1),
            factor: 2.0,
            max: secs(max_delay_s),
            jitter: Jitter::Additive,
        }
    }

    /// WaaV: exponential from `min(250 ms, max_delay_s)`, doubling, full jitter.
    pub fn waav(max_delay_s: f32) -> Self {
        let max = secs(max_delay_s);
        Self {
            base: Duration::from_millis(250).min(max),
            factor: 2.0,
            max,
            jitter: Jitter::Full,
        }
    }

    /// Delay before retry number `retry` (0-based).
    pub fn delay(&self, retry: u32) -> Duration {
        let raw = self.base.as_secs_f64() * self.factor.powi(retry.min(30) as i32);
        let capped = raw.min(self.max.as_secs_f64());
        let d = match self.jitter {
            Jitter::None => capped,
            Jitter::Full => capped * fastrand::f64(),
            Jitter::Additive => (capped + capped * fastrand::f64()).min(self.max.as_secs_f64()),
        };
        Duration::from_secs_f64(d.max(0.0))
    }
}

/// `max_delay_s` arrives as `f32`: round to whole microseconds so 0.1 s is exactly 100 ms.
fn secs(s: f32) -> Duration {
    if s.is_finite() && s > 0.0 {
        Duration::from_micros((f64::from(s.min(3_600.0)) * 1e6).round() as u64)
    } else {
        Duration::ZERO
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub backoff: Backoff,
}

impl RetryPolicy {
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            backoff: Backoff::waav(0.0),
        }
    }
    pub fn waav(c: &RetryConfig) -> Self {
        Self {
            max_retries: c.num_retries.min(10) as u32,
            backoff: Backoff::waav(c.max_delay_s),
        }
    }
    pub fn budgateway(c: &RetryConfig) -> Self {
        Self {
            max_retries: c.num_retries.min(10) as u32,
            backoff: Backoff::budgateway(c.max_delay_s),
        }
    }
}

/// The result of a retried operation.
#[derive(Debug)]
pub struct RetryOutcome<T, E> {
    pub result: Result<T, E>,
    /// Retries performed (attempts − 1).
    pub retries: u32,
    /// Classification of the final error, if it failed.
    pub verdict: Option<Verdict>,
}

/// Run `op` until it succeeds, fails with a non-retryable error, runs out of retries, or the next
/// wait would cross `deadline`. A vendor delay hint (`Verdict::retry_after`) replaces the backoff
/// (plus up to 10 % jitter) when it fits in the deadline; otherwise the chain ends.
pub async fn retry<T, E, Op, Fut, C>(
    policy: &RetryPolicy,
    deadline: Option<Instant>,
    mut op: Op,
    classify: C,
) -> RetryOutcome<T, E>
where
    Op: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    C: Fn(&E) -> Verdict,
{
    let mut retries = 0u32;
    loop {
        match op(retries).await {
            Ok(t) => {
                return RetryOutcome {
                    result: Ok(t),
                    retries,
                    verdict: None,
                }
            }
            Err(e) => {
                let v = classify(&e);
                let stop = |e| RetryOutcome {
                    result: Err(e),
                    retries,
                    verdict: Some(v),
                };
                if !v.retryable || retries >= policy.max_retries {
                    return stop(e);
                }
                let wait = match v.retry_after {
                    Some(hint) => hint + hint.mul_f64(0.1 * fastrand::f64()),
                    None => policy.backoff.delay(retries),
                };
                if let Some(d) = deadline {
                    if Instant::now() + wait >= d {
                        return stop(e);
                    }
                }
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                retries += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::classify::{classify, Failure};
    use std::sync::atomic::{AtomicU32, Ordering};

    fn verdict(code: &u16) -> Verdict {
        classify(&Failure::Status {
            code: *code,
            headers: None,
            body: None,
        })
    }

    #[test]
    fn budgateway_profile_keeps_its_numbers() {
        let b = Backoff::budgateway(10.0);
        for (i, lo) in [(0u32, 1.0), (1, 2.0), (2, 4.0)] {
            let d = b.delay(i).as_secs_f64();
            assert!(d >= lo && d <= (lo * 2.0).min(10.0), "retry {i}: {d}");
        }
        assert!(b.delay(10).as_secs_f64() <= 10.0);
    }

    #[test]
    fn waav_profile_starts_at_250ms_with_full_jitter() {
        let b = Backoff::waav(5.0);
        for _ in 0..100 {
            assert!(b.delay(0) <= Duration::from_millis(250));
            assert!(b.delay(20) <= Duration::from_secs(5));
        }
        assert_eq!(Backoff::waav(0.1).base, Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn retries_transient_errors_then_succeeds() {
        let calls = AtomicU32::new(0);
        let p = RetryPolicy::waav(&RetryConfig {
            num_retries: 2,
            max_delay_s: 5.0,
        });
        let out = retry(
            &p,
            None,
            |_| async {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(503u16)
                } else {
                    Ok("ok")
                }
            },
            verdict,
        )
        .await;
        assert_eq!(out.result, Ok("ok"));
        assert_eq!(out.retries, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn caller_errors_are_not_retried() {
        let calls = AtomicU32::new(0);
        let p = RetryPolicy::waav(&RetryConfig {
            num_retries: 5,
            max_delay_s: 5.0,
        });
        let out: RetryOutcome<(), u16> = retry(
            &p,
            None,
            |_| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(400u16)
            },
            verdict,
        )
        .await;
        assert_eq!(out.result, Err(400));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(out.verdict.unwrap().caller_error);
    }

    #[tokio::test(start_paused = true)]
    async fn honours_retry_after_within_the_deadline() {
        let p = RetryPolicy::waav(&RetryConfig {
            num_retries: 3,
            max_delay_s: 5.0,
        });
        let hinted = |_: &u16| Verdict {
            retry_after: Some(Duration::from_secs(2)),
            ..verdict(&429)
        };
        let start = Instant::now();
        let calls = AtomicU32::new(0);
        let out = retry(
            &p,
            Some(start + Duration::from_secs(10)),
            |_| async {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(429u16)
                } else {
                    Ok(())
                }
            },
            hinted,
        )
        .await;
        assert!(out.result.is_ok());
        let waited = start.elapsed();
        assert!(waited >= Duration::from_secs(2) && waited <= Duration::from_millis(2_200));

        // A hint beyond the deadline ends the chain instead of sleeping into it.
        let start = Instant::now();
        let long = |_: &u16| Verdict {
            retry_after: Some(Duration::from_secs(30)),
            ..verdict(&429)
        };
        let out: RetryOutcome<(), u16> = retry(
            &p,
            Some(start + Duration::from_secs(10)),
            |_| async { Err(429u16) },
            long,
        )
        .await;
        assert_eq!(out.retries, 0);
        assert!(start.elapsed() < Duration::from_millis(1));
        assert_eq!(
            out.verdict.unwrap().retry_after,
            Some(Duration::from_secs(30))
        );
    }
}
