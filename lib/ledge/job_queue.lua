local tostring, tonumber, ipairs = tostring, tonumber, ipairs

local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_time = ngx.time
local ngx_null = ngx.null

local cjson_encode = require("cjson").encode
local cjson_decode = require("cjson").decode

local randomhex = require("ledge.util").string.randomhex


local _M = {
    _VERSION = "2.7.0",
}


local function job_key(jid)
    return "ledge:jobs:job:" .. jid
end
_M.job_key = job_key


local function ready_key(queue)
    return "ledge:jobs:" .. queue .. ":ready"
end
_M.ready_key = ready_key


local function delayed_key(queue)
    return "ledge:jobs:" .. queue .. ":delayed"
end
_M.delayed_key = delayed_key


local function running_key(queue)
    return "ledge:jobs:" .. queue .. ":running"
end
_M.running_key = running_key


-- A single set of jids that have permanently failed (given up after
-- max_retries), across all queues - for operator visibility, e.g.
-- `redis-cli -n <jobs_db> scard ledge:jobs:failed`.
local failed_key = "ledge:jobs:failed"
_M.failed_key = failed_key


-- Job records are never left without a TTL. A record and its queue entry
-- (the ready list / delayed zset / running zset) are separate keys, and
-- under memory pressure an evicting store (e.g. Dragonfly in cache_mode)
-- can drop one without the other. A record orphaned in "waiting" or
-- "running" state would otherwise live forever, and since put() de-dupes
-- on the record's state, every future put for that jid would be silently
-- swallowed - e.g. a stale-while-revalidate page that never refreshes.
-- Bounding every record's lifetime means an orphan clears itself and the
-- next put re-queues the job properly.

-- How long (sec) a job may sit waiting (queued, or after its delay) before
-- its record expires. Callers can override per job with options.ttl.
_M.default_waiting_ttl = 3600

-- Extra time (sec) a running job's record outlives its lease, so the
-- reaper has a chance to reclaim it before the record expires.
_M.running_ttl_grace = 60


local function record_ttl(redis, jid)
    local ttl = tonumber(redis:hget(job_key(jid), "ttl"))
    return ttl or _M.default_waiting_ttl
end


-- Fetches and decodes a job record hash. Returns nil if it doesn't exist.
local function get(redis, jid)
    local res, err = redis:hgetall(job_key(jid))
    if not res or res == ngx_null or #res == 0 then
        return nil, err
    end

    local record = redis:array_to_hash(res)
    record.jid = jid
    if record.data then record.data = cjson_decode(record.data) end
    if record.tags then record.tags = cjson_decode(record.tags) end
    record.priority = tonumber(record.priority)
    record.attempts = tonumber(record.attempts) or 0
    record.ttl = tonumber(record.ttl)

    return record
end
_M.get = get


-- Enqueues a job. If options.jid names a job already "waiting" (queued or
-- delayed, not yet reserved by a worker), the existing job's data is
-- overwritten in place rather than creating a duplicate queue entry -
-- concurrent duplicate purges/revalidations of the same resource coalesce
-- into a single pending job. If the existing job is "running", the put is
-- dropped. Returns { jid, klass, options } on success, or nil, err.
--
-- options.ttl bounds how long (sec) the job may wait to be picked up after
-- it becomes due before its record expires (default: default_waiting_ttl).
-- Coalescing into an existing job deliberately does NOT extend its TTL:
-- if that job was orphaned by eviction, extending it on every put would
-- keep the orphan (and the de-dupe that swallows new puts) alive forever.
local function put(redis, queue, klass, data, options)
    options = options or {}
    local jid = options.jid or randomhex(24)
    local ttl = tonumber(options.ttl) or _M.default_waiting_ttl

    if options.jid then
        local existing_state, err = redis:hget(job_key(jid), "state")
        if not existing_state or existing_state == ngx_null then
            existing_state = nil
        end

        -- Records written before TTLs were introduced may have none, and
        -- may be exactly the orphans this guards against. Give them one so
        -- they clear themselves too.
        if existing_state == "running" or existing_state == "waiting" then
            local key_ttl = redis:ttl(job_key(jid))
            if key_ttl == -1 then
                redis:expire(job_key(jid), ttl)
            end
        end

        if existing_state == "running" then
            return nil, "Job with the same jid is currently running"
        elseif existing_state == "waiting" then
            local ok, err = redis:hmset(job_key(jid),
                "data", cjson_encode(data),
                "tags", cjson_encode(options.tags or {}),
                "priority", options.priority or 0
            )
            if not ok or ok == ngx_null then
                return nil, err
            end

            return { jid = jid, klass = klass, options = options }
        end
    end

    local ok, err = redis:multi()
    if not ok then return nil, err end

    redis:hmset(job_key(jid),
        "queue", queue,
        "klass", klass,
        "data", cjson_encode(data),
        "tags", cjson_encode(options.tags or {}),
        "priority", options.priority or 0,
        "attempts", 0,
        "state", "waiting",
        "created", ngx_time(),
        "ttl", ttl
    )

    -- Replaces any TTL left over from a previous job that used this same
    -- jid (set by complete()/fail_or_retry()). A delayed job's record must
    -- also survive its delay.
    local delay = tonumber(options.delay)
    if delay and delay > 0 then
        redis:expire(job_key(jid), delay + ttl)
        redis:zadd(delayed_key(queue), ngx_time() + delay, jid)
    else
        redis:expire(job_key(jid), ttl)
        redis:rpush(ready_key(queue), jid)
    end

    local res, err = redis:exec()
    if not res or res == ngx_null then
        return nil, err
    end

    return { jid = jid, klass = klass, options = options }
end
_M.put = put


-- Pops the next ready jid from a queue (FIFO). Returns nil if empty.
local function pop_ready(redis, queue)
    local jid, err = redis:lpop(ready_key(queue))
    if not jid or jid == ngx_null then
        return nil, err
    end
    return jid
end
_M.pop_ready = pop_ready


-- Marks a job as running and creates its lease in the running zset, so the
-- reaper can reclaim it if this worker dies before completing it. The
-- record's TTL tracks the lease (plus grace), so if the lease entry is
-- evicted and the worker dies, the record doesn't stay "running" forever.
local function mark_running(redis, queue, jid, expires_at)
    local record_expiry = (expires_at - ngx_time()) + _M.running_ttl_grace
    redis:multi()
    redis:hset(job_key(jid), "state", "running")
    redis:expire(job_key(jid), record_expiry)
    redis:zadd(running_key(queue), expires_at, jid)
    return redis:exec()
end
_M.mark_running = mark_running


-- Extends a running job's record TTL alongside its lease. Called from
-- ledge.job's heartbeat() once the lease itself has been extended.
local function extend_running(redis, jid, expires_at)
    local record_expiry = (expires_at - ngx_time()) + _M.running_ttl_grace
    return redis:expire(job_key(jid), record_expiry)
end
_M.extend_running = extend_running


-- Marks a job complete. The record is kept briefly for post-hoc
-- visibility/debugging, then expires naturally.
local function complete(redis, queue, jid)
    redis:multi()
    redis:hset(job_key(jid), "state", "complete")
    redis:expire(job_key(jid), 60)
    redis:zrem(running_key(queue), jid)
    return redis:exec()
end
_M.complete = complete


-- Handles a job failure: retries (immediate re-queue) up to max_retries,
-- after which the job is marked permanently failed and logged.
local function fail_or_retry(redis, queue, jid, message, max_retries)
    local attempts, err = redis:hincrby(job_key(jid), "attempts", 1)
    if not attempts or attempts == ngx_null then
        ngx_log(ngx_ERR, "could not update job attempts: ", tostring(err))
        attempts = (max_retries or 0) + 1 -- give up rather than retry forever
    end

    local ttl = record_ttl(redis, jid)

    redis:multi()
    redis:zrem(running_key(queue), jid)

    if attempts <= (max_retries or 0) then
        redis:hset(job_key(jid), "state", "waiting")
        redis:expire(job_key(jid), ttl)
        redis:rpush(ready_key(queue), jid)
    else
        redis:hset(job_key(jid), "state", "failed")
        redis:expire(job_key(jid), 3600)
        redis:sadd(failed_key, jid)
    end

    local res, err = redis:exec()

    if attempts <= (max_retries or 0) then
        ngx_log(ngx_ERR,
            "job ", jid, " (", queue, ") failed, retrying (attempt ",
            attempts, "): ", tostring(message)
        )
    else
        ngx_log(ngx_ERR,
            "job ", jid, " (", queue, ") failed permanently: ", tostring(message)
        )
    end

    return res, err
end
_M.fail_or_retry = fail_or_retry


-- Atomically moves due delayed jobs onto the ready queue. Safe to call
-- concurrently from multiple worker processes without duplicating entries.
local promote_due_script = [[
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for i = 1, #due do
    if redis.call('ZREM', KEYS[1], due[i]) == 1 then
        redis.call('RPUSH', KEYS[2], due[i])
    end
end
return due
]]

local function promote_due(redis, queue, now, limit)
    return redis:eval(
        promote_due_script, 2,
        delayed_key(queue), ready_key(queue),
        now, limit or 100
    )
end
_M.promote_due = promote_due


-- Sweeps for jobs whose lease has expired (the worker holding them died
-- before completing them) and reclaims them onto the ready queue for
-- another worker to pick up. Returns the list of reclaimed jids. Safe to
-- call concurrently: only the reaper that actually removes a given jid from
-- the running zset reclaims it, so it's never requeued twice.
local reap_script = [[
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local reclaimed = {}
for i = 1, #expired do
    if redis.call('ZREM', KEYS[1], expired[i]) == 1 then
        redis.call('RPUSH', KEYS[2], expired[i])
        table.insert(reclaimed, expired[i])
    end
end
return reclaimed
]]

local function reap(redis, queue, now, limit)
    local reclaimed, err = redis:eval(
        reap_script, 2,
        running_key(queue), ready_key(queue),
        now, limit or 100
    )
    if not reclaimed or reclaimed == ngx_null then
        return nil, err
    end

    -- Reclaimed jobs are waiting again: reset their state and give their
    -- records a waiting TTL, otherwise the (shorter) running TTL could
    -- expire them before a worker gets to them. Done outside the script
    -- because Dragonfly rejects scripts touching undeclared keys by default.
    for _, jid in ipairs(reclaimed) do
        local key = job_key(jid)
        if redis:exists(key) == 1 then
            local ttl = record_ttl(redis, jid)
            redis:multi()
            redis:hset(key, "state", "waiting")
            redis:expire(key, ttl)
            redis:exec()
        end
    end

    return reclaimed
end
_M.reap = reap


return _M
