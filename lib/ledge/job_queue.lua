local tostring, tonumber = tostring, tonumber

local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_time = ngx.time
local ngx_null = ngx.null

local cjson_encode = require("cjson").encode
local cjson_decode = require("cjson").decode

local randomhex = require("ledge.util").string.randomhex


local _M = {
    _VERSION = "2.6.0",
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

    return record
end
_M.get = get


-- Enqueues a job. If options.jid names a job already "waiting" (queued or
-- delayed, not yet reserved by a worker), the existing job's data is
-- overwritten in place rather than creating a duplicate queue entry -
-- concurrent duplicate purges/revalidations of the same resource coalesce
-- into a single pending job. If the existing job is "running", the put is
-- dropped. Returns { jid, klass, options } on success, or nil, err.
local function put(redis, queue, klass, data, options)
    options = options or {}
    local jid = options.jid or randomhex(24)

    if options.jid then
        local existing_state, err = redis:hget(job_key(jid), "state")
        if not existing_state or existing_state == ngx_null then
            existing_state = nil
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

    -- Clear any TTL left over from a previous job that used this same jid
    -- (set by complete()/fail_or_retry()), otherwise this freshly (re)queued
    -- job can expire out from under itself before a worker gets to it.
    redis:persist(job_key(jid))

    redis:hmset(job_key(jid),
        "queue", queue,
        "klass", klass,
        "data", cjson_encode(data),
        "tags", cjson_encode(options.tags or {}),
        "priority", options.priority or 0,
        "attempts", 0,
        "state", "waiting",
        "created", ngx_time()
    )

    local delay = tonumber(options.delay)
    if delay and delay > 0 then
        redis:zadd(delayed_key(queue), ngx_time() + delay, jid)
    else
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
-- reaper can reclaim it if this worker dies before completing it.
local function mark_running(redis, queue, jid, expires_at)
    redis:multi()
    redis:hset(job_key(jid), "state", "running")
    redis:zadd(running_key(queue), expires_at, jid)
    return redis:exec()
end
_M.mark_running = mark_running


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

    redis:multi()
    redis:zrem(running_key(queue), jid)

    if attempts <= (max_retries or 0) then
        redis:hset(job_key(jid), "state", "waiting")
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
    return redis:eval(
        reap_script, 2,
        running_key(queue), ready_key(queue),
        now, limit or 100
    )
end
_M.reap = reap


return _M
