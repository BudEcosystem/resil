//! Redis-backed [`Store`]: three Lua scripts, one pipelined round-trip per sync.
//!
//! The transport is the small [`RedisExec`] trait so each gateway plugs in the `redis` client it
//! already links. Features `redis-0-27` / `redis-0-31` provide ready adapters for
//! `redis::aio::ConnectionManager`.

use std::sync::Arc;

use arc_swap::ArcSwapOption;

use super::algo::WindowState;
use super::store::{
    Batch, BatchReply, BoxFuture, ConcReply, HeartbeatReply, RateReply, Store, StoreError,
};

pub const RATE_LUA: &str = include_str!("lua/rate.lua");
pub const CONC_LUA: &str = include_str!("lua/conc.lua");
pub const HEARTBEAT_LUA: &str = include_str!("lua/heartbeat.lua");

/// One command argument.
#[derive(Debug, Clone)]
pub enum Arg {
    Str(Arc<str>),
    Int(i64),
    Float(f64),
}

impl Arg {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Arg::Str(s) => s.as_bytes().to_vec(),
            Arg::Int(i) => i.to_string().into_bytes(),
            Arg::Float(f) => format!("{f:?}").into_bytes(),
        }
    }
}

/// One command of a pipeline.
#[derive(Debug, Clone)]
pub struct Cmd {
    pub name: &'static str,
    pub args: Vec<Arg>,
}

/// A reply value.
#[derive(Debug, Clone, PartialEq)]
pub enum RValue {
    Nil,
    Int(i64),
    Bulk(Vec<u8>),
    Status(String),
    Array(Vec<RValue>),
}

impl RValue {
    fn as_f64(&self) -> Option<f64> {
        match self {
            RValue::Int(i) => Some(*i as f64),
            RValue::Bulk(b) => std::str::from_utf8(b).ok()?.parse().ok(),
            RValue::Status(s) => s.parse().ok(),
            _ => None,
        }
    }
    fn as_i64(&self) -> Option<i64> {
        match self {
            RValue::Int(i) => Some(*i),
            other => other.as_f64().map(|f| f as i64),
        }
    }
    fn as_u64(&self) -> Option<u64> {
        self.as_i64().map(|i| i.max(0) as u64)
    }
}

/// Minimal Redis transport: run commands as one pipeline, in order.
pub trait RedisExec: Send + Sync + 'static {
    fn pipeline<'a>(&'a self, cmds: &'a [Cmd]) -> BoxFuture<'a, Result<Vec<RValue>, StoreError>>;
}

struct Shas {
    rate: Arc<str>,
    conc: Arc<str>,
    heartbeat: Arc<str>,
}

/// A [`Store`] on Redis (or Valkey). Keys are hash-tagged per limit key, so every script touches
/// one slot and runs on Redis Cluster; the replica registry is a separate key.
pub struct RedisStore<E: RedisExec> {
    exec: E,
    shas: ArcSwapOption<Shas>,
}

impl<E: RedisExec> RedisStore<E> {
    pub fn new(exec: E) -> Self {
        Self {
            exec,
            shas: ArcSwapOption::empty(),
        }
    }

    async fn load_scripts(&self) -> Result<Arc<Shas>, StoreError> {
        let cmds: Vec<Cmd> = [RATE_LUA, CONC_LUA, HEARTBEAT_LUA]
            .into_iter()
            .map(|src| Cmd {
                name: "SCRIPT",
                args: vec![Arg::Str(Arc::from("LOAD")), Arg::Str(Arc::from(src))],
            })
            .collect();
        let out = self.exec.pipeline(&cmds).await?;
        let sha = |v: Option<&RValue>| -> Result<Arc<str>, StoreError> {
            match v {
                Some(RValue::Bulk(b)) => Ok(Arc::from(String::from_utf8_lossy(b).as_ref())),
                Some(RValue::Status(s)) => Ok(Arc::from(s.as_str())),
                other => Err(StoreError(format!("SCRIPT LOAD returned {other:?}"))),
            }
        };
        let shas = Arc::new(Shas {
            rate: sha(out.first())?,
            conc: sha(out.get(1))?,
            heartbeat: sha(out.get(2))?,
        });
        self.shas.store(Some(shas.clone()));
        Ok(shas)
    }

    fn build(shas: &Shas, b: &Batch) -> Vec<Cmd> {
        let mut cmds = Vec::with_capacity(b.release.len() + b.rate.len() + b.conc.len() + 1);
        for (key, pod) in &b.release {
            cmds.push(Cmd {
                name: "HDEL",
                args: vec![
                    Arg::Str(key.clone()),
                    Arg::Str(Arc::from(format!("L:{pod}"))),
                    Arg::Str(pod.clone()),
                ],
            });
        }
        for r in &b.rate {
            let mut args = vec![
                Arg::Str(shas.rate.clone()),
                Arg::Int(1),
                Arg::Str(r.key.clone()),
                Arg::Int(i64::from(r.alg.code())),
                Arg::Int(r.windows.len() as i64),
                Arg::Str(r.pod.clone()),
                Arg::Int(r.hits as i64),
                Arg::Int(r.unused as i64),
                Arg::Int(r.want as i64),
                Arg::Int(r.need as i64),
                Arg::Float(r.allowance),
                Arg::Int(i64::from(r.replicas)),
                Arg::Int(r.lease_ttl_ms.min(i64::MAX as u64) as i64),
                Arg::Float(r.now_override_ms.unwrap_or(0.0)),
                Arg::Int(r.hits_old as i64),
                Arg::Float(r.t_old_ms),
            ];
            for w in r.windows.iter() {
                args.push(Arg::Int(w.limit as i64));
                args.push(Arg::Int(w.window_ms as i64));
                args.push(Arg::Int(w.burst as i64));
            }
            cmds.push(Cmd {
                name: "EVALSHA",
                args,
            });
        }
        for c in &b.conc {
            cmds.push(Cmd {
                name: "EVALSHA",
                args: vec![
                    Arg::Str(shas.conc.clone()),
                    Arg::Int(1),
                    Arg::Str(c.key.clone()),
                    Arg::Str(c.pod.clone()),
                    Arg::Int(c.active as i64),
                    Arg::Int(c.unused as i64),
                    Arg::Int(c.want as i64),
                    Arg::Int(c.need as i64),
                    Arg::Int(c.max as i64),
                    Arg::Float(c.allowance),
                    Arg::Int(i64::from(c.replicas)),
                    Arg::Int(c.lease_ttl_ms as i64),
                    Arg::Float(c.now_override_ms.unwrap_or(0.0)),
                ],
            });
        }
        if let Some(h) = &b.heartbeat {
            cmds.push(Cmd {
                name: "EVALSHA",
                args: vec![
                    Arg::Str(shas.heartbeat.clone()),
                    Arg::Int(1),
                    Arg::Str(h.key.clone()),
                    Arg::Str(h.pod.clone()),
                    Arg::Int(h.live_window_ms as i64),
                    Arg::Int(i64::from(h.leave)),
                    Arg::Float(h.now_override_ms.unwrap_or(0.0)),
                ],
            });
        }
        cmds
    }

    fn parse(b: &Batch, out: Vec<RValue>) -> Result<BatchReply, StoreError> {
        let bad = |what: &str| StoreError(format!("unexpected {what} reply"));
        let mut it = out.into_iter().skip(b.release.len());
        let mut reply = BatchReply::default();
        for r in &b.rate {
            let RValue::Array(v) = it.next().ok_or_else(|| bad("rate"))? else {
                return Err(bad("rate"));
            };
            let n = r.windows.len();
            if v.len() != 6 + 4 * n {
                return Err(bad("rate"));
            }
            let mut windows = Vec::with_capacity(n);
            for i in 0..n {
                let o = 6 + 4 * i;
                windows.push(WindowState {
                    idx: v[o].as_u64().ok_or_else(|| bad("rate idx"))?,
                    cur: v[o + 1].as_u64().ok_or_else(|| bad("rate cur"))?,
                    prev: v[o + 2].as_u64().ok_or_else(|| bad("rate prev"))?,
                    tat: v[o + 3].as_f64().ok_or_else(|| bad("rate tat"))?,
                });
            }
            reply.rate.push(RateReply {
                now_ms: v[0].as_f64().ok_or_else(|| bad("rate now"))?,
                kept: v[1].as_u64().ok_or_else(|| bad("rate kept"))?,
                avail: v[2].as_i64().ok_or_else(|| bad("rate avail"))?,
                granted: v[3].as_u64().ok_or_else(|| bad("rate granted"))?,
                direct: v[4].as_u64().ok_or_else(|| bad("rate direct"))?,
                reserved_others: v[5].as_u64().ok_or_else(|| bad("rate others"))?,
                windows,
            });
        }
        for _ in &b.conc {
            let RValue::Array(v) = it.next().ok_or_else(|| bad("conc"))? else {
                return Err(bad("conc"));
            };
            if v.len() != 5 {
                return Err(bad("conc"));
            }
            reply.conc.push(ConcReply {
                now_ms: v[0].as_f64().ok_or_else(|| bad("conc now"))?,
                kept: v[1].as_u64().ok_or_else(|| bad("conc kept"))?,
                granted: v[2].as_u64().ok_or_else(|| bad("conc granted"))?,
                direct: v[3].as_u64().ok_or_else(|| bad("conc direct"))?,
                others: v[4].as_u64().ok_or_else(|| bad("conc others"))?,
            });
        }
        if b.heartbeat.is_some() {
            let RValue::Array(v) = it.next().ok_or_else(|| bad("heartbeat"))? else {
                return Err(bad("heartbeat"));
            };
            if v.len() != 2 {
                return Err(bad("heartbeat"));
            }
            reply.heartbeat = Some(HeartbeatReply {
                now_ms: v[0].as_f64().ok_or_else(|| bad("heartbeat now"))?,
                replicas: v[1].as_u64().ok_or_else(|| bad("heartbeat n"))? as u32,
            });
        }
        Ok(reply)
    }

    async fn run(&self, b: &Batch) -> Result<BatchReply, StoreError> {
        let shas = match self.shas.load_full() {
            Some(s) => s,
            None => self.load_scripts().await?,
        };
        let cmds = Self::build(&shas, b);
        match self.exec.pipeline(&cmds).await {
            Ok(out) => Self::parse(b, out),
            // Redis restarted or failed over: the script cache is empty.
            Err(e) if e.0.to_ascii_lowercase().contains("noscript") => {
                let shas = self.load_scripts().await?;
                let cmds = Self::build(&shas, b);
                Self::parse(b, self.exec.pipeline(&cmds).await?)
            }
            Err(e) => Err(e),
        }
    }
}

impl<E: RedisExec> Store for RedisStore<E> {
    fn round_trip<'a>(&'a self, batch: &'a Batch) -> BoxFuture<'a, Result<BatchReply, StoreError>> {
        Box::pin(self.run(batch))
    }
}

macro_rules! redis_adapter {
    ($feature:literal, $krate:ident, $modname:ident) => {
        #[cfg(feature = $feature)]
        mod $modname {
            use super::{Cmd, RValue, RedisExec};
            use crate::limit::store::{BoxFuture, StoreError};
            use $krate as redis;

            fn convert(v: redis::Value) -> RValue {
                match v {
                    redis::Value::Nil => RValue::Nil,
                    redis::Value::Int(i) => RValue::Int(i),
                    redis::Value::BulkString(b) => RValue::Bulk(b),
                    redis::Value::SimpleString(s) => RValue::Status(s),
                    redis::Value::Okay => RValue::Status("OK".into()),
                    redis::Value::Array(items) => {
                        RValue::Array(items.into_iter().map(convert).collect())
                    }
                    redis::Value::Double(d) => RValue::Bulk(d.to_string().into_bytes()),
                    _ => RValue::Nil,
                }
            }

            impl RedisExec for redis::aio::ConnectionManager {
                fn pipeline<'a>(
                    &'a self,
                    cmds: &'a [Cmd],
                ) -> BoxFuture<'a, Result<Vec<RValue>, StoreError>> {
                    let mut conn = self.clone();
                    Box::pin(async move {
                        let mut p = redis::pipe();
                        for c in cmds {
                            let mut cmd = redis::cmd(c.name);
                            for a in &c.args {
                                cmd.arg(a.to_bytes());
                            }
                            p.add_command(cmd);
                        }
                        let out: Vec<redis::Value> = p
                            .query_async(&mut conn)
                            .await
                            .map_err(|e| StoreError(e.to_string()))?;
                        Ok(out.into_iter().map(convert).collect())
                    })
                }
            }
        }
    };
}

redis_adapter!("redis-0-27", redis027, adapter_0_27);
redis_adapter!("redis-0-31", redis031, adapter_0_31);
