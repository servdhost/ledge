local require = require
local math_ceil = math.ceil
local job_queue = require("ledge.job_queue")

local _M = {
    _VERSION = "2.5.0",
}

-- If options.jid is given (i.e. a non-random jid), putting this job will
-- overwrite any existing job with the same jid, unless that job is
-- currently running, in which case this put is silently dropped.
local function put_background_job(queue, klass, data, options)
    local ledge = require("ledge")

    local redis, err = ledge.create_jobs_connection()
    if not redis then return nil, err end

    local job, err = job_queue.put(redis, queue, klass, data, options or {})

    ledge.close_redis_connection(redis)

    return job, err
end
_M.put_background_job = put_background_job


-- Calculate when to GC an entity based on its size and the minimum download
-- rate setting, plus 1 second of arbitrary latency for good measure.
local function gc_wait(entity_size, minimum_download_rate)
    local dl_rate_Bps = minimum_download_rate * 128
    return math_ceil((entity_size / dl_rate_Bps)) + 1
end
_M.gc_wait = gc_wait


return _M
