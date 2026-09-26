-- resil replica registry v1. Mirrors MemStore::heartbeat (src/limit/store.rs).
-- KEYS[1]  zset: pod -> last heartbeat (store ms)
-- ARGV     pod, live_window_ms, leave (0/1), now_override
-- returns  {now, live replicas (>= 1)}
local key = KEYS[1]
local pod = ARGV[1]
local window = tonumber(ARGV[2])
local leave = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
if now <= 0 then
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
end
if leave == 1 then
  redis.call('ZREM', key, pod)
else
  redis.call('ZADD', key, now, pod)
end
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local n = redis.call('ZCARD', key)
if n < 1 then n = 1 end
redis.call('PEXPIRE', key, math.floor(window * 10))
return {string.format('%.17g', now), n}
