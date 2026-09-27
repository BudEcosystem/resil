//! Fallback chains: an ordered list of targets, tried until one serves the request, with cycle
//! detection, per-hop admission and one shared deadline.
//!
//! The trigger predicate belongs to the caller. An LLM gateway may fall back on **any** error (a
//! context-length 400 is exactly what a larger fallback model fixes); a service whose fallbacks
//! behave like the primary should fall back only on failover-eligible errors
//! ([`crate::classify::Verdict::failover`]).

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;

use crate::policy::MAX_FALLBACKS;

/// Expand `primary` and its fallbacks depth-first — a fallback's own fallbacks are tried before
/// the next sibling — skipping anything already visited (A → B → A stops at
/// B). At most `max_hops` entries, the primary included.
pub fn expand<F>(primary: &str, fallbacks_of: F, max_hops: usize) -> Vec<Arc<str>>
where
    F: Fn(&str) -> Vec<Arc<str>>,
{
    fn walk<F: Fn(&str) -> Vec<Arc<str>>>(
        node: Arc<str>,
        f: &F,
        seen: &mut HashSet<Arc<str>>,
        out: &mut Vec<Arc<str>>,
        max: usize,
    ) {
        if out.len() >= max || !seen.insert(node.clone()) {
            return;
        }
        out.push(node.clone());
        for next in f(&node).into_iter().take(MAX_FALLBACKS) {
            walk(next, f, seen, out, max);
        }
    }
    let mut out = Vec::new();
    walk(
        Arc::from(primary),
        &fallbacks_of,
        &mut HashSet::new(),
        &mut out,
        max_hops.max(1),
    );
    out
}

/// Why a hop was not attempted.
#[derive(Debug, Clone, PartialEq)]
pub enum Skip {
    /// Its circuit breaker is open.
    BreakerOpen { retry_in: Duration },
    /// Its own rate or concurrency limit said no.
    RateLimited { retry_after: Duration },
    /// It could not serve this request (unknown deployment, missing capability, format).
    Ineligible(String),
}

/// One hop's result, as the attempt function reports it.
#[derive(Debug)]
pub enum Hop<T, E> {
    Served(T),
    Skipped(Skip),
    Failed(E),
}

/// What happened on each hop, for headers, spans and logs.
#[derive(Debug, Clone, PartialEq)]
pub enum HopRecord {
    Served,
    Skipped(Skip),
    Failed,
}

#[derive(Debug)]
pub enum ChainError<E> {
    /// A hop failed with an error the trigger says must surface (e.g. the caller's own mistake).
    Surfaced { endpoint: Arc<str>, error: E },
    /// Every hop failed or was skipped.
    Exhausted {
        /// The first failure (normally the primary's), if any hop was attempted.
        first: Option<(Arc<str>, E)>,
        /// The last failure.
        last: Option<(Arc<str>, E)>,
        /// Every hop was skipped by a limit: the smallest wait.
        rate_limited: Option<Duration>,
    },
    /// The deadline passed before a hop could start.
    DeadlineExceeded,
}

#[derive(Debug)]
pub struct ChainOutcome<T, E> {
    pub result: Result<(Arc<str>, T), ChainError<E>>,
    pub hops: Vec<(Arc<str>, HopRecord)>,
}

impl<T, E> ChainOutcome<T, E> {
    /// Served by something other than the first entry.
    pub fn fell_back(&self) -> bool {
        match (&self.result, self.hops.first()) {
            (Ok((served, _)), Some((first, _))) => served != first,
            _ => false,
        }
    }
}

/// Try `candidates` in order. `attempt(endpoint, hop_index)` runs one hop (its own retries
/// included) and reports served / skipped / failed; `trigger(&error)` decides whether a failure
/// moves on to the next hop.
pub async fn run<T, E, A, Fut, P>(
    candidates: &[Arc<str>],
    deadline: Option<Instant>,
    mut attempt: A,
    trigger: P,
) -> ChainOutcome<T, E>
where
    A: FnMut(Arc<str>, usize) -> Fut,
    Fut: Future<Output = Hop<T, E>>,
    P: Fn(&E) -> bool,
{
    let mut hops = Vec::with_capacity(candidates.len());
    let mut first: Option<(Arc<str>, E)> = None;
    let mut last_failed: Option<Arc<str>> = None;
    let mut last: Option<(Arc<str>, E)> = None;
    let mut min_wait: Option<Duration> = None;
    let mut all_limited = true;
    for (i, c) in candidates.iter().enumerate() {
        if deadline.is_some_and(|d| Instant::now() >= d) {
            if hops.is_empty() {
                return ChainOutcome {
                    result: Err(ChainError::DeadlineExceeded),
                    hops,
                };
            }
            break;
        }
        match attempt(c.clone(), i).await {
            Hop::Served(t) => {
                hops.push((c.clone(), HopRecord::Served));
                return ChainOutcome {
                    result: Ok((c.clone(), t)),
                    hops,
                };
            }
            Hop::Skipped(s) => {
                if let Skip::RateLimited { retry_after } = &s {
                    min_wait = Some(min_wait.map_or(*retry_after, |m| m.min(*retry_after)));
                } else {
                    all_limited = false;
                }
                hops.push((c.clone(), HopRecord::Skipped(s)));
            }
            Hop::Failed(e) => {
                all_limited = false;
                hops.push((c.clone(), HopRecord::Failed));
                if !trigger(&e) {
                    return ChainOutcome {
                        result: Err(ChainError::Surfaced {
                            endpoint: c.clone(),
                            error: e,
                        }),
                        hops,
                    };
                }
                if first.is_none() {
                    first = Some((c.clone(), e));
                } else {
                    last = Some((c.clone(), e));
                }
                last_failed = Some(c.clone());
            }
        }
    }
    let _ = last_failed;
    ChainOutcome {
        result: Err(ChainError::Exhausted {
            first,
            last,
            rate_limited: if all_limited { min_wait } else { None },
        }),
        hops,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn graph(edges: &[(&str, &[&str])]) -> impl Fn(&str) -> Vec<Arc<str>> {
        let m: HashMap<String, Vec<Arc<str>>> = edges
            .iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(|s| Arc::from(*s)).collect()))
            .collect();
        move |k| m.get(k).cloned().unwrap_or_default()
    }

    fn names(v: &[Arc<str>]) -> Vec<&str> {
        v.iter().map(|s| &**s).collect()
    }

    #[test]
    fn expands_depth_first_and_cuts_cycles() {
        let g = graph(&[("A", &["B", "D"]), ("B", &["C", "A"]), ("C", &["A"])]);
        assert_eq!(names(&expand("A", &g, 10)), ["A", "B", "C", "D"]);
        assert_eq!(names(&expand("A", graph(&[("A", &["A"])]), 10)), ["A"]);
        assert_eq!(names(&expand("A", &g, 2)), ["A", "B"]);
    }

    #[tokio::test]
    async fn falls_back_until_served() {
        let c: Vec<Arc<str>> = ["a", "b", "c"].into_iter().map(Arc::from).collect();
        let out = run(
            &c,
            None,
            |e, _| async move {
                if &*e == "c" {
                    Hop::Served(e.to_string())
                } else {
                    Hop::Failed(503u16)
                }
            },
            |_| true,
        )
        .await;
        let (served, body) = out.result.as_ref().unwrap();
        assert_eq!(&**served, "c");
        assert_eq!(body, "c");
        assert!(out.fell_back());
        assert_eq!(out.hops.len(), 3);
    }

    #[tokio::test]
    async fn caller_error_surfaces_without_fallback() {
        // A 400 is not failover-eligible under the classifying trigger.
        let c: Vec<Arc<str>> = ["a", "b"].into_iter().map(Arc::from).collect();
        let tried = std::sync::atomic::AtomicUsize::new(0);
        let out: ChainOutcome<(), u16> = run(
            &c,
            None,
            |_, _| {
                tried.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                async { Hop::Failed(400u16) }
            },
            |e| *e != 400,
        )
        .await;
        assert!(matches!(
            out.result,
            Err(ChainError::Surfaced { error: 400, .. })
        ));
        assert_eq!(tried.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn all_rate_limited_reports_the_smallest_wait() {
        let c: Vec<Arc<str>> = ["a", "b"].into_iter().map(Arc::from).collect();
        let out: ChainOutcome<(), u16> = run(
            &c,
            None,
            |e, _| async move {
                Hop::Skipped(Skip::RateLimited {
                    retry_after: Duration::from_secs(if &*e == "a" { 7 } else { 3 }),
                })
            },
            |_| true,
        )
        .await;
        match out.result {
            Err(ChainError::Exhausted { rate_limited, .. }) => {
                assert_eq!(rate_limited, Some(Duration::from_secs(3)))
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn exhausted_keeps_first_and_last_errors() {
        let c: Vec<Arc<str>> = ["a", "b", "c"].into_iter().map(Arc::from).collect();
        let out: ChainOutcome<(), String> = run(
            &c,
            None,
            |e, _| async move { Hop::Failed(format!("{e} down")) },
            |_| true,
        )
        .await;
        match out.result {
            Err(ChainError::Exhausted {
                first: Some((f, fe)),
                last: Some((l, le)),
                rate_limited: None,
            }) => {
                assert_eq!((&*f, fe.as_str()), ("a", "a down"));
                assert_eq!((&*l, le.as_str()), ("c", "c down"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_stops_the_chain() {
        let c: Vec<Arc<str>> = ["a", "b"].into_iter().map(Arc::from).collect();
        let deadline = Instant::now() + Duration::from_secs(1);
        let out: ChainOutcome<(), u16> = run(
            &c,
            Some(deadline),
            |_, _| async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                Hop::Failed(503u16)
            },
            |_| true,
        )
        .await;
        assert_eq!(
            out.hops.len(),
            1,
            "second hop never starts past the deadline"
        );
    }
}
