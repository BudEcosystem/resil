//! Wire format shared by both gateways (FRD §5.9): the 429 bodies, the `X-RateLimit-*` headers and
//! the CORS expose list.

use http::{HeaderMap, HeaderName, HeaderValue, StatusCode};

use crate::limit::RateHeaders;

pub const X_RATELIMIT_LIMIT: HeaderName = HeaderName::from_static("x-ratelimit-limit");
pub const X_RATELIMIT_REMAINING: HeaderName = HeaderName::from_static("x-ratelimit-remaining");
pub const X_RATELIMIT_RESET: HeaderName = HeaderName::from_static("x-ratelimit-reset");

/// Headers a browser client must be allowed to read (CORS `Access-Control-Expose-Headers`).
pub const EXPOSE_HEADERS: [HeaderName; 4] = [
    http::header::RETRY_AFTER,
    X_RATELIMIT_LIMIT,
    X_RATELIMIT_REMAINING,
    X_RATELIMIT_RESET,
];

pub const RATE_LIMITED_BODY: &str = r#"{"error":{"message":"Rate limit exceeded","type":"rate_limit_error","code":"rate_limit_exceeded"}}"#;
pub const CONCURRENCY_LIMITED_BODY: &str = r#"{"error":{"message":"Too many concurrent requests for this deployment","type":"rate_limit_error","code":"concurrency_limit_exceeded"}}"#;

/// WebSocket close code for a session refused by a limit (RFC 6455 "Try Again Later").
pub const WS_CLOSE_TRY_AGAIN_LATER: u16 = 1013;

/// Write the `X-RateLimit-*` headers (and `Retry-After` on a denial).
pub fn write_headers(h: &RateHeaders, out: &mut HeaderMap) {
    out.insert(X_RATELIMIT_LIMIT, HeaderValue::from(h.limit));
    out.insert(X_RATELIMIT_REMAINING, HeaderValue::from(h.remaining));
    out.insert(X_RATELIMIT_RESET, HeaderValue::from(h.reset));
    if let Some(ra) = h.retry_after {
        out.insert(http::header::RETRY_AFTER, HeaderValue::from(ra));
    }
}

/// Status, headers and body of a rate-limit rejection.
pub fn rate_limited(h: &RateHeaders) -> (StatusCode, HeaderMap, &'static str) {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    write_headers(h, &mut headers);
    (StatusCode::TOO_MANY_REQUESTS, headers, RATE_LIMITED_BODY)
}

/// Status, headers and body of a `max_concurrent` rejection.
pub fn concurrency_limited(retry_after_s: u64) -> (StatusCode, HeaderMap, &'static str) {
    let mut headers = HeaderMap::new();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        http::header::RETRY_AFTER,
        HeaderValue::from(retry_after_s.max(1)),
    );
    (
        StatusCode::TOO_MANY_REQUESTS,
        headers,
        CONCURRENCY_LIMITED_BODY,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bodies_are_json() {
        for b in [RATE_LIMITED_BODY, CONCURRENCY_LIMITED_BODY] {
            let v: serde_json::Value = serde_json::from_str(b).unwrap();
            assert_eq!(v["error"]["type"], "rate_limit_error");
        }
    }

    #[test]
    fn rejection_carries_headers() {
        let (s, h, _) = rate_limited(&RateHeaders {
            limit: 60,
            remaining: 0,
            reset: 1_750_000_060,
            retry_after: Some(12),
        });
        assert_eq!(s, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(h["retry-after"], "12");
        assert_eq!(h["x-ratelimit-limit"], "60");
        assert_eq!(h["x-ratelimit-remaining"], "0");
        assert_eq!(h["content-type"], "application/json");
        let (_, h, b) = concurrency_limited(0);
        assert_eq!(h["retry-after"], "1");
        assert!(b.contains("concurrency_limit_exceeded"));
    }
}
