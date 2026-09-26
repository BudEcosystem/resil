-- resil concurrency sync v1. Mirrors MemStore::sync_conc (src/limit/store.rs).
-- KEYS[1]  hash: <pod> = "allowed:expiry_ms" (this replica's absolute count + reservation)
-- ARGV     pod, active, unused, want, need, max, allowance, replicas, lease_ttl_ms, now_override
-- returns  {now, kept, granted, direct, others}
local key = KEYS[1]
local pod = ARGV[1]
local active = tonumber(ARGV[2])
local unused = tonumber(ARGV[3])
local want = tonumber(ARGV[4])
local need = tonumber(ARGV[5])
local max = tonumber(ARGV[6])
local allowance = tonumber(ARGV[7])
local replicas = tonumber(ARGV[8])
local ttl = tonumber(ARGV[9])
local now = tonumber(ARGV[10])
if now <= 0 then
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
end

local raw = redis.call('HGETALL', key)
local others = 0
local mine_held = nil
local expired = {}
for i = 1, #raw, 2 do
  local f = raw[i]
  local c, e = string.match(raw[i + 1], '^([^:]+):(.+)$')
  c = tonumber(c)
  e = tonumber(e)
  if e <= now then
    expired[#expired + 1] = f
  elseif f == pod then
    mine_held = c
  else
    others = others + c
  end
end
if #expired > 0 then redis.call('HDEL', key, unpack(expired)) end

local avail = math.floor(max - others - active)
local kept
if mine_held ~= nil then
  local room = mine_held - active
  if room < 0 then room = 0 end
  kept = unused
  if room < kept then kept = room end
else
  local a = avail
  if a < 0 then a = 0 end
  kept = unused
  if a < kept then kept = a end
end

local free = avail - kept
local granted = 0
local direct = 0
if free >= 1 then
  free = math.floor(free)
  local n = replicas
  if n < 1 then n = 1 end
  local al = allowance
  if al > 1 then al = 1 end
  local fair = math.floor(al * free / n)
  granted = want
  if fair < granted then granted = fair end
  if granted < 0 then granted = 0 end
  local left = free - granted
  if need > granted and left >= 1 then
    direct = need - granted
    if left < direct then direct = left end
  end
end

local mine = active + kept + granted + direct
if mine > 0 then
  redis.call('HSET', key, pod, string.format('%d:%.17g', mine, now + ttl))
else
  redis.call('HDEL', key, pod)
end
redis.call('PEXPIRE', key, math.floor(2 * ttl))
return {string.format('%.17g', now), kept, granted, direct, others}
