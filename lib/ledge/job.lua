local setmetatable = setmetatable

local ngx_time = ngx.time
local ngx_null = ngx.null

local job_queue = require("ledge.job_queue")


local _M = {
    _VERSION = "2.3.0",
}

local mt = { __index = _M }


-- redis is the job's own connection (selected to the cache DB) for the job
-- class to use for its work - this is what job classes see as `job.redis`.
-- jobs_redis is a separate connection (selected to jobs_db) used internally
-- to maintain this job's lease in the running set, via heartbeat().
local function new(jid, queue, klass, data, redis, jobs_redis, job_timeout, expires_at)
    return setmetatable({
        jid = jid,
        queue = queue,
        klass = klass,
        data = data,
        redis = redis,
        job_timeout = job_timeout,
        expires_at = expires_at,
        _jobs_redis = jobs_redis,
    }, mt)
end
_M.new = new


-- Seconds remaining before this job's lease is considered expired (and thus
-- eligible for the reaper to reclaim it for another worker).
local function ttl(self)
    return self.expires_at - ngx_time()
end
_M.ttl = ttl


-- Extends this job's lease. Long running jobs (e.g. ledge.jobs.purge, which
-- recurses over a SCAN cursor) must call this periodically to avoid being
-- reclaimed by the reaper while still legitimately in progress.
local function heartbeat(self)
    local new_expires_at = ngx_time() + self.job_timeout

    -- XX: only touch the lease if it's still ours (the reaper may have
    -- already reclaimed it and requeued it for someone else). CH makes ZADD
    -- report back whether it actually changed anything, so we can tell
    -- "still ours, extended" apart from "no longer ours, did nothing".
    local changed, err = self._jobs_redis:zadd(
        job_queue.running_key(self.queue),
        "XX", "CH", new_expires_at, self.jid
    )
    if not changed or changed == ngx_null then
        return nil, err
    elseif changed == 0 then
        return nil, "lease no longer held; job may have been reclaimed"
    end

    self.expires_at = new_expires_at
    return self.expires_at
end
_M.heartbeat = heartbeat


return _M
