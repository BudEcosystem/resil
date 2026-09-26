-- resil rate sync v1. Mirrors MemStore::sync_rate (src/limit/store.rs) line for line.
-- KEYS[1]  hash: w<i> = "idx:cur:prev:tat" per window, L:<pod> = "credit:expiry_ms"
-- ARGV     alg, nw, pod, hits, unused, want, need, allowance, replicas, lease_ttl_ms, now_override,
--          hits_old, t_old, then limit, window_ms, burst per window
-- returns  {now, kept, avail, granted, direct, others, (idx, cur, prev, tat) per window}
local key = KEYS[1]
local alg = tonumber(ARGV[1])
local nw = tonumber(ARGV[2])
local pod = ARGV[3]
local hits = tonumber(ARGV[4])
local unused = tonumber(ARGV[5])
local want = tonumber(ARGV[6])
local need = tonumber(ARGV[7])
local allowance = tonumber(ARGV[8])
local replicas = tonumber(ARGV[9])
local ttl = tonumber(ARGV[10])
local now = tonumber(ARGV[11])
if now <= 0 then
  local t = redis.call('TIME')
  now = tonumber(t[1]) * 1000 + tonumber(t[2]) / 1000
end

local hits_old = tonumber(ARGV[12])
local t_old = tonumber(ARGV[13])

local specs = {}
local maxw = 0
for i = 1, nw do
  local b = 13 + (i - 1) * 3
  local s = {l = tonumber(ARGV[b + 1]), w = tonumber(ARGV[b + 2]), b = tonumber(ARGV[b + 3])}
  specs[i] = s
  if s.w > maxw then maxw = s.w end
end

local function roll(s, sp, t)
  if alg == 2 then return s end
  local i = 0
  if t > 0 then i = math.floor(t / sp.w) end
  if i <= s.idx then return s end
  if i == s.idx + 1 then return {idx = i, cur = 0, prev = s.cur, tat = s.tat} end
  return {idx = i, cur = 0, prev = 0, tat = s.tat}
end

local function capacity(sp)
  if alg == 2 then return sp.b end
  return sp.l
end

local function usage(s, sp, t)
  s = roll(s, sp, t)
  if alg == 0 then return s.cur end
  if alg == 1 then
    local pos = t - s.idx * sp.w
    if pos < 0 then pos = 0 elseif pos > sp.w then pos = sp.w end
    return s.prev * (1.0 - pos / sp.w) + s.cur
  end
  local tat = s.tat
  if tat < t then tat = t end
  return (tat - t) / (sp.w / sp.l)
end

local function apply(s, sp, t, n)
  s = roll(s, sp, t)
  if alg == 2 then
    local p = sp.w / sp.l
    local cap = t + sp.b * p + sp.w
    local tat = s.tat
    if tat < t then tat = t end
    tat = tat + n * p
    if tat > cap then tat = cap end
    return {idx = s.idx, cur = s.cur, prev = s.prev, tat = tat}
  end
  return {idx = s.idx, cur = s.cur + n, prev = s.prev, tat = s.tat}
end

local function apply_past(s, sp, t, n)
  if n == 0 then return s end
  if alg == 2 then return apply(s, sp, t, n) end
  local i = 0
  if t > 0 then i = math.floor(t / sp.w) end
  if i >= s.idx then return apply(s, sp, t, n) end
  if i + 1 == s.idx and alg == 1 then
    return {idx = s.idx, cur = s.cur, prev = s.prev + n, tat = s.tat}
  end
  return s
end

local raw = redis.call('HGETALL', key)
local states = {}
local others = 0
local mine_held = nil
local expired = {}
for i = 1, #raw, 2 do
  local f = raw[i]
  local v = raw[i + 1]
  if string.sub(f, 1, 2) == 'L:' then
    local c, e = string.match(v, '^([^:]+):(.+)$')
    c = tonumber(c)
    e = tonumber(e)
    if e <= now then
      expired[#expired + 1] = f
    elseif string.sub(f, 3) == pod then
      mine_held = c
    else
      others = others + c
    end
  elseif string.sub(f, 1, 1) == 'w' then
    local idx, cur, prev, tat = string.match(v, '^([^:]+):([^:]+):([^:]+):(.+)$')
    states[tonumber(string.sub(f, 2))] =
      {idx = tonumber(idx), cur = tonumber(cur), prev = tonumber(prev), tat = tonumber(tat)}
  end
end
if #expired > 0 then redis.call('HDEL', key, unpack(expired)) end

for i = 1, nw do
  local s = states[i] or {idx = 0, cur = 0, prev = 0, tat = 0}
  s = apply_past(s, specs[i], t_old, hits_old)
  states[i] = apply(s, specs[i], now, hits)
end

local avail = nil
for i = 1, nw do
  local f = capacity(specs[i]) - usage(states[i], specs[i], now) - others
  if avail == nil or f < avail then avail = f end
end
avail = math.floor(avail)

local kept
if mine_held ~= nil then
  kept = unused
  if mine_held < kept then kept = mine_held end
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

if direct > 0 then
  for i = 1, nw do states[i] = apply(states[i], specs[i], now, direct) end
end

local fields = {}
for i = 1, nw do
  local s = states[i]
  fields[#fields + 1] = 'w' .. i
  fields[#fields + 1] = string.format('%d:%d:%d:%.17g', s.idx, s.cur, s.prev, s.tat)
end
redis.call('HSET', key, unpack(fields))
local mine = kept + granted
if mine > 0 then
  redis.call('HSET', key, 'L:' .. pod, string.format('%d:%.17g', mine, now + ttl))
else
  redis.call('HDEL', key, 'L:' .. pod)
end
redis.call('PEXPIRE', key, math.floor(2 * maxw + ttl + 1000))

local out = {string.format('%.17g', now), kept, avail, granted, direct, others}
for i = 1, nw do
  local s = states[i]
  out[#out + 1] = s.idx
  out[#out + 1] = s.cur
  out[#out + 1] = s.prev
  out[#out + 1] = string.format('%.17g', s.tat)
end
return out
