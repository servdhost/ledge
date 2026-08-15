local setmetatable, require, tostring, ipairs, pcall =
    setmetatable, require, tostring, ipairs, pcall

local ngx_get_phase = ngx.get_phase
local ngx_timer_at = ngx.timer.at
local ngx_worker_exiting = ngx.worker.exiting
local ngx_sleep = ngx.sleep
local ngx_time = ngx.time
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR

local tbl_copy_merge_defaults = require("ledge.util").table.copy_merge_defaults
local fixed_field_metatable = require("ledge.util").mt.fixed_field_metatable

local job_queue = require("ledge.job_queue")


local _M = {
    _VERSION = "2.4.0",
}


local defaults = setmetatable({
    interval = 1,
    gc_queue_concurrency = 1,
    purge_queue_concurrency = 1,
    revalidate_queue_concurrency = 1,
    job_timeout = 60,
    max_retries = 5,
}, fixed_field_metatable)


local function new(config)
    assert(ngx_get_phase() == "init_worker",
        "attempt to create ledge worker outside of the init_worker phase")

    -- Take config by value and merge with defaults
    local config = tbl_copy_merge_defaults(config, defaults)
    return setmetatable({ config = config }, {
        __index = _M,
    })
end
_M.new = new


-- Runs a single popped job to completion (or failure) and reports the
-- outcome back to the queue. `jobs_redis` is the caller's jobs_db
-- connection; it is not closed here.
local function run_job(self, ledge, jobs_redis, queue, jid)
    local record, err = job_queue.get(jobs_redis, jid)
    if not record then
        ngx_log(ngx_ERR, "could not load job ", jid, ": ", tostring(err))
        return
    end

    local expires_at = ngx_time() + self.config.job_timeout
    local ok, err = job_queue.mark_running(jobs_redis, queue, jid, expires_at)
    if not ok then
        ngx_log(ngx_ERR, "could not mark job ", jid, " running: ", tostring(err))
        return
    end

    local job_redis, err = ledge.create_redis_connection()
    if not job_redis then
        job_queue.fail_or_retry(
            jobs_redis, queue, jid,
            "could not connect to redis: " .. tostring(err),
            self.config.max_retries
        )
        return
    end

    local job = require("ledge.job").new(
        jid, queue, record.klass, record.data,
        job_redis, jobs_redis, self.config.job_timeout, expires_at
    )

    -- Guard against a broken/missing job class, and against the job class
    -- raising a Lua-level error, so one bad job can't take down the worker
    -- lane.
    local req_ok, task = pcall(require, record.klass)

    local perform_ok, res, err_type, perform_err
    if req_ok then
        perform_ok, res, err_type, perform_err = pcall(task.perform, job)
    else
        perform_ok, res, err_type, perform_err = false, tostring(task), nil, nil
    end

    ledge.close_redis_connection(job_redis)

    if not perform_ok then
        job_queue.fail_or_retry(
            jobs_redis, queue, jid, tostring(res), self.config.max_retries
        )
    elseif res == nil and err_type then
        job_queue.fail_or_retry(
            jobs_redis, queue, jid, tostring(perform_err), self.config.max_retries
        )
    else
        job_queue.complete(jobs_redis, queue, jid)
    end
end


-- A single concurrency "lane" for a queue: pops and runs jobs one at a
-- time, sleeping `interval` seconds whenever the queue is empty. Runs for
-- the lifetime of the Nginx worker process.
local function runner_loop(premature, self, queue)
    if premature then return end

    local ledge = require("ledge")

    while not ngx_worker_exiting() do
        local jobs_redis, err = ledge.create_jobs_connection()
        if not jobs_redis then
            ngx_log(ngx_ERR, "worker could not connect to redis: ", tostring(err))
            ngx_sleep(self.config.interval)
        else
            local jid = job_queue.pop_ready(jobs_redis, queue)
            if jid then
                run_job(self, ledge, jobs_redis, queue, jid)
            end

            ledge.close_redis_connection(jobs_redis)

            if not jid then
                ngx_sleep(self.config.interval)
            end
        end
    end
end


-- One scheduler lane per queue: promotes due delayed jobs to ready, and
-- reclaims jobs whose lease expired because the worker running them died.
local function scheduler_loop(premature, self, queue)
    if premature then return end

    local ledge = require("ledge")

    while not ngx_worker_exiting() do
        local jobs_redis, err = ledge.create_jobs_connection()
        if not jobs_redis then
            ngx_log(ngx_ERR, "worker could not connect to redis: ", tostring(err))
        else
            job_queue.promote_due(jobs_redis, queue, ngx_time(), 100)
            job_queue.reap(jobs_redis, queue, ngx_time(), 100)
            ledge.close_redis_connection(jobs_redis)
        end

        ngx_sleep(self.config.interval)
    end
end


local function run(self)
    assert(ngx_get_phase() == "init_worker",
        "attempt to run ledge worker outside of the init_worker phase")

    local queues = {
        { name = "ledge_gc", concurrency = self.config.gc_queue_concurrency },
        { name = "ledge_purge", concurrency = self.config.purge_queue_concurrency },
        { name = "ledge_revalidate", concurrency = self.config.revalidate_queue_concurrency },
    }

    -- Lanes start after a short delay rather than immediately: this is the
    -- init_worker phase, so nothing is waiting on a job to be picked up yet,
    -- and it avoids a burst of simultaneous Redis connection attempts as
    -- every Nginx worker process starts up at once.
    for _, queue in ipairs(queues) do
        for _ = 1, queue.concurrency do
            local ok, err = ngx_timer_at(1, runner_loop, self, queue.name)
            if not ok then
                ngx_log(ngx_ERR, "failed to start worker: ", tostring(err))
            end
        end

        local ok, err = ngx_timer_at(1, scheduler_loop, self, queue.name)
        if not ok then
            ngx_log(ngx_ERR, "failed to start scheduler: ", tostring(err))
        end
    end

    return true
end
_M.run = run


return setmetatable(_M, fixed_field_metatable)
