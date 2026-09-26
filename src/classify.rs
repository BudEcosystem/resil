//! What a failed call means (FRD §6.3–6.5): retry it, fail over, count it against a breaker, or
//! surface it as the caller's own mistake.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use http::HeaderMap;

/// How a failure feeds the circuit breakers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BreakerSignal {
    /// Says nothing about the vendor (caller errors, vendor-concurrency 429s).
    Ignore,
    /// Counts against this deployment only (its credential, its quota).
    Deployment,
    /// Counts against the deployment and the vendor (5xx, timeouts, connect errors).
    Vendor,
    /// Open this deployment's breaker for exactly this long (429 with `Retry-After`).
    OpenFor(Duration),
}

/// The classification of one failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verdict {
    /// Worth another attempt on the same deployment.
    pub retryable: bool,
    /// Worth trying the next deployment in the fallback chain.
    pub failover: bool,
    pub breaker: BreakerSignal,
    /// The request itself is wrong: it would fail anywhere.
    pub caller_error: bool,
    /// The vendor said when to come back.
    pub retry_after: Option<Duration>,
    /// Vendor refused for concurrency (our `max_concurrent` is above the vendor plan).
    pub vendor_concurrency: bool,
}

/// A failed attempt as the gateway saw it.
#[derive(Debug, Clone, Copy)]
pub enum Failure<'a> {
    Status {
        code: u16,
        headers: Option<&'a HeaderMap>,
        body: Option<&'a [u8]>,
    },
    Timeout,
    Connect,
    /// A vendor WebSocket closed with this code.
    WsClose(u16),
    /// Anything else (decode errors, internal bugs): neither retried nor failed over.
    Other,
}

/// Short wait before retrying a vendor-concurrency 429 that carries no delay hint.
pub const CONCURRENCY_RETRY_WAIT: Duration = Duration::from_millis(250);

pub fn classify(f: &Failure<'_>) -> Verdict {
    let transient = Verdict {
        retryable: true,
        failover: true,
        breaker: BreakerSignal::Vendor,
        caller_error: false,
        retry_after: None,
        vendor_concurrency: false,
    };
    match *f {
        Failure::Timeout | Failure::Connect => transient,
        Failure::WsClose(code) => {
            if matches!(code, 1011 | 1013) {
                transient
            } else {
                Verdict {
                    retryable: false,
                    failover: code == 1008, // policy violation: usually our credential
                    breaker: if code == 1008 {
                        BreakerSignal::Deployment
                    } else {
                        BreakerSignal::Ignore
                    },
                    caller_error: false,
                    retry_after: None,
                    vendor_concurrency: false,
                }
            }
        }
        Failure::Other => Verdict {
            retryable: false,
            failover: false,
            breaker: BreakerSignal::Ignore,
            caller_error: false,
            retry_after: None,
            vendor_concurrency: false,
        },
        Failure::Status {
            code,
            headers,
            body,
        } => {
            let retry_after = headers.and_then(retry_after);
            match code {
                408 | 409 => Verdict {
                    retry_after,
                    breaker: BreakerSignal::Ignore,
                    ..transient
                },
                429 => {
                    if is_vendor_concurrency_429(headers, body) {
                        Verdict {
                            retryable: true,
                            failover: true,
                            breaker: BreakerSignal::Ignore,
                            caller_error: false,
                            retry_after: Some(retry_after.unwrap_or(CONCURRENCY_RETRY_WAIT)),
                            vendor_concurrency: true,
                        }
                    } else {
                        Verdict {
                            retryable: true,
                            failover: true,
                            breaker: match retry_after {
                                Some(d) => BreakerSignal::OpenFor(d),
                                None => BreakerSignal::Deployment,
                            },
                            caller_error: false,
                            retry_after,
                            vendor_concurrency: false,
                        }
                    }
                }
                500 | 502 | 503 | 504 => Verdict {
                    retry_after,
                    ..transient
                },
                401..=403 => Verdict {
                    retryable: false,
                    failover: true,
                    breaker: BreakerSignal::Deployment,
                    caller_error: false,
                    retry_after: None,
                    vendor_concurrency: false,
                },
                400 | 404 | 405 | 411 | 413 | 414 | 415 | 422 => Verdict {
                    retryable: false,
                    failover: false,
                    breaker: BreakerSignal::Ignore,
                    caller_error: true,
                    retry_after: None,
                    vendor_concurrency: false,
                },
                c if c >= 500 => Verdict {
                    retryable: false,
                    failover: true,
                    breaker: BreakerSignal::Vendor,
                    caller_error: false,
                    retry_after: None,
                    vendor_concurrency: false,
                },
                _ => Verdict {
                    retryable: false,
                    failover: false,
                    breaker: BreakerSignal::Ignore,
                    caller_error: code >= 400,
                    retry_after: None,
                    vendor_concurrency: false,
                },
            }
        }
    }
}

/// HTTP status codes worth retrying on the same target (the OpenAI SDK's set).
pub fn is_retryable_status(code: u16) -> bool {
    matches!(code, 408 | 409 | 429 | 500 | 502 | 503 | 504)
}

/// The vendor's delay hint: `retry-after-ms` (milliseconds), then `Retry-After` (seconds or an
/// HTTP date). Capped at one hour.
pub fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    const CAP: f64 = 3_600.0;
    if let Some(v) = headers.get("retry-after-ms").and_then(|v| v.to_str().ok()) {
        if let Ok(ms) = v.trim().parse::<f64>() {
            if ms.is_finite() && ms >= 0.0 {
                return Some(Duration::from_secs_f64((ms / 1000.0).min(CAP)));
            }
        }
    }
    let v = headers
        .get(http::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(secs) = v.parse::<f64>() {
        if secs.is_finite() && secs >= 0.0 {
            return Some(Duration::from_secs_f64(secs.min(CAP)));
        }
        return None;
    }
    let at = parse_http_date(v)?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
    Some(Duration::from_secs(at.saturating_sub(now).min(CAP as u64)))
}

/// IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) → Unix seconds.
fn parse_http_date(s: &str) -> Option<u64> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 6 || parts[5] != "GMT" {
        return None;
    }
    let day: u64 = parts[1].parse().ok()?;
    let month = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    .iter()
    .position(|m| *m == parts[2])? as u64
        + 1;
    let year: i64 = parts[3].parse().ok()?;
    let hms: Vec<u64> = parts[4]
        .split(':')
        .map(|p| p.parse().ok())
        .collect::<Option<_>>()?;
    if hms.len() != 3 || !(1..=31).contains(&day) || hms[0] > 23 || hms[1] > 59 || hms[2] > 60 {
        return None;
    }
    // days from civil (Howard Hinnant)
    let (y, m) = if month <= 2 {
        (year - 1, month + 9)
    } else {
        (year, month - 3)
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe as i64 - 719_468;
    if days < 0 {
        return None;
    }
    Some(days as u64 * 86_400 + hms[0] * 3_600 + hms[1] * 60 + hms[2])
}

/// A vendor's "too many concurrent requests" 429 (ElevenLabs `too_many_concurrent_requests` and
/// equivalents): our `max_concurrent` is set above the vendor plan.
pub fn is_vendor_concurrency_429(headers: Option<&HeaderMap>, body: Option<&[u8]>) -> bool {
    if let Some(b) = body {
        let s = String::from_utf8_lossy(&b[..b.len().min(4096)]).to_ascii_lowercase();
        if s.contains("too_many_concurrent_requests")
            || s.contains("concurrent_limit")
            || s.contains("concurrency_limit")
            || s.contains("too many concurrent")
            || s.contains("concurrency limit")
        {
            return true;
        }
    }
    if let Some(h) = headers {
        let cur = h
            .get("current-concurrent-requests")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok());
        let max = h
            .get("maximum-concurrent-requests")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok());
        if let (Some(c), Some(m)) = (cur, max) {
            return c >= m;
        }
    }
    false
}

/// The vendor's advertised concurrency cap (ElevenLabs `maximum-concurrent-requests`), for logs.
pub fn vendor_max_concurrency(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("maximum-concurrent-requests")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn status(code: u16) -> Verdict {
        classify(&Failure::Status {
            code,
            headers: None,
            body: None,
        })
    }

    #[test]
    fn retryable_set_matches_the_openai_sdk() {
        for c in [408, 409, 429, 500, 502, 503, 504] {
            assert!(status(c).retryable, "{c}");
            assert!(is_retryable_status(c));
        }
        for c in [400, 401, 403, 404, 413, 422] {
            assert!(!status(c).retryable, "{c}");
            assert!(!is_retryable_status(c));
        }
        assert!(classify(&Failure::Timeout).retryable);
        assert!(classify(&Failure::Connect).retryable);
        assert!(classify(&Failure::WsClose(1011)).retryable);
        assert!(classify(&Failure::WsClose(1013)).retryable);
        assert!(!classify(&Failure::WsClose(1000)).retryable);
    }

    #[test]
    fn failover_includes_our_credentials_but_not_caller_errors() {
        for c in [401, 402, 403, 429, 500, 503] {
            assert!(status(c).failover, "{c}");
        }
        for c in [400, 404, 413, 422] {
            let v = status(c);
            assert!(!v.failover, "{c}");
            assert!(v.caller_error, "{c}");
            assert_eq!(
                v.breaker,
                BreakerSignal::Ignore,
                "{c} must not trip a breaker"
            );
        }
    }

    #[test]
    fn breaker_tiers() {
        assert_eq!(status(401).breaker, BreakerSignal::Deployment);
        assert_eq!(status(402).breaker, BreakerSignal::Deployment);
        assert_eq!(status(503).breaker, BreakerSignal::Vendor);
        assert_eq!(classify(&Failure::Timeout).breaker, BreakerSignal::Vendor);
        assert_eq!(status(429).breaker, BreakerSignal::Deployment);
        let mut h = HeaderMap::new();
        h.insert("retry-after", HeaderValue::from_static("20"));
        let v = classify(&Failure::Status {
            code: 429,
            headers: Some(&h),
            body: None,
        });
        assert_eq!(v.breaker, BreakerSignal::OpenFor(Duration::from_secs(20)));
        assert_eq!(v.retry_after, Some(Duration::from_secs(20)));
    }

    #[test]
    fn vendor_concurrency_429_is_retried_but_not_counted() {
        let body = br#"{"detail":{"status":"too_many_concurrent_requests","message":"..."}}"#;
        let v = classify(&Failure::Status {
            code: 429,
            headers: None,
            body: Some(body),
        });
        assert!(v.vendor_concurrency);
        assert!(v.retryable);
        assert_eq!(v.breaker, BreakerSignal::Ignore);
        assert_eq!(v.retry_after, Some(CONCURRENCY_RETRY_WAIT));
        let mut h = HeaderMap::new();
        h.insert(
            "current-concurrent-requests",
            HeaderValue::from_static("10"),
        );
        h.insert(
            "maximum-concurrent-requests",
            HeaderValue::from_static("10"),
        );
        assert!(is_vendor_concurrency_429(Some(&h), None));
        assert_eq!(vendor_max_concurrency(&h), Some(10));
    }

    #[test]
    fn retry_after_prefers_ms_then_seconds_then_date() {
        let mut h = HeaderMap::new();
        h.insert("retry-after", HeaderValue::from_static("3"));
        assert_eq!(retry_after(&h), Some(Duration::from_secs(3)));
        h.insert("retry-after-ms", HeaderValue::from_static("1500"));
        assert_eq!(retry_after(&h), Some(Duration::from_millis(1500)));
        let mut d = HeaderMap::new();
        d.insert(
            "retry-after",
            HeaderValue::from_static("Sun, 06 Nov 1994 08:49:37 GMT"),
        );
        assert_eq!(
            retry_after(&d),
            Some(Duration::ZERO),
            "a past date means now"
        );
        assert_eq!(
            parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(784_111_777)
        );
        let mut bad = HeaderMap::new();
        bad.insert("retry-after", HeaderValue::from_static("soon"));
        assert_eq!(retry_after(&bad), None);
        let mut huge = HeaderMap::new();
        huge.insert("retry-after", HeaderValue::from_static("999999"));
        assert_eq!(retry_after(&huge), Some(Duration::from_secs(3600)));
    }
}
