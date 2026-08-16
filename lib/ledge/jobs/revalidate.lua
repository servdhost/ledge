local http = require "resty.http"
local http_headers = require "resty.http_headers"
local ngx_null = ngx.null
local ngx_time = ngx.time
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local math_min = math.min

local _M = {
    _VERSION = "2.5.0",
}


-- Utility to return all items in a Redis hash as a Lua table.
local function hgetall(redis, key)
    local res, err = redis:hgetall(key)
    if not res or res == ngx_null then
        return nil,
            "could not retrieve " .. tostring(key) .. " data:" .. tostring(err)
    end

    return redis:array_to_hash(res)
end


local function backoff_key(key_chain)
    return key_chain.reval_params .. ":backoff"
end
_M.backoff_key = backoff_key


-- Records a revalidation failure, extending the backoff window before the
-- next attempt (doubling per consecutive failure, capped at backoff_max).
-- This is what stops a struggling origin being hit by every subsequent
-- stale-serving request in the meantime.
local function record_failure(redis, key_chain, backoff_initial, backoff_max)
    backoff_initial = backoff_initial or 5
    backoff_max = backoff_max or 300

    local key = backoff_key(key_chain)

    local failures, err = redis:hincrby(key, "failures", 1)
    if not failures or failures == ngx_null then
        ngx_log(ngx_ERR,
            "could not record revalidation failure: ", tostring(err))
        return
    end

    local backoff = math_min(backoff_initial * (2 ^ (failures - 1)), backoff_max)
    local until_time = ngx_time() + backoff

    redis:hset(key, "until", until_time)
    -- Forget this run of failures if nothing has attempted (and thus hit
    -- this again) for a while - a good sign the situation resolved itself.
    redis:expire(key, backoff_max * 4)
end
_M.record_failure = record_failure


-- Clears any backoff state after a successful revalidation.
local function clear_backoff(redis, key_chain)
    redis:del(backoff_key(key_chain))
end
_M.clear_backoff = clear_backoff


function _M.perform(job)
    -- Normal background revalidation operates on stored metadata.
    -- A background fetch due to partial content from upstream however, uses the
    -- current request metadata for reval_headers / reval_params and passes it
    -- through as job data.
    local reval_params = job.data.reval_params
    local reval_headers = job.data.reval_headers
    local key_chain = job.data.key_chain

    -- If we don't have the metadata in job data, this is a background
    -- revalidation using stored metadata.
    if not reval_params and not reval_headers then
        local redis, err = job.redis, nil

        reval_params, err = hgetall(redis, key_chain.reval_params)
        if not reval_params or not next(reval_params) then
            return nil, "job-error",
                "Revalidation parameters are missing, presumed evicted. " ..
                tostring(err)
        end

        reval_headers, err = hgetall(redis, key_chain.reval_req_headers)
        if not reval_headers or not next(reval_headers) then
            return nil, "job-error",
                 "Revalidation headers are missing, presumed evicted." ..
                 tostring(err)
        end
    end

    -- Make outbound http request to revalidate
    local httpc = http.new()
    httpc:set_timeouts(
        reval_params.connect_timeout,
        reval_params.send_timeout,
        reval_params.read_timeout
    )

    local port = tonumber(reval_params.server_port)
    local ok, err
    if port then
        ok, err = httpc:connect(reval_params.server_addr, port)
    else
        ok, err = httpc:connect(reval_params.server_addr)
    end

    if not ok then
        return nil, "job-error",
            "could not connect to server: " .. tostring(err)
    end

    if reval_params.scheme == "https" then
        local ok, err = httpc:ssl_handshake(false, nil, false)
        if not ok then
            return nil, "job-error", "ssl handshake failed: " .. tostring(err)
        end
    end

    local headers = http_headers.new() -- Case-insensitive header table
    headers["Cache-Control"] = "max-stale=0, stale-if-error=0"
    headers["User-Agent"] =
        httpc._USER_AGENT .. " ledge_revalidate/" .. _M._VERSION

    -- Add additional headers from parent
    for k,v in pairs(reval_headers) do
        headers[k] = v
    end

    local res, err = httpc:request{
        method = "GET",
        path = reval_params.uri,
        headers = headers,
    }

    if not res then
        return nil, "job-error", "revalidate failed: " .. tostring(err)
    end

    local reader = res.body_reader
    -- Read and discard the body
    repeat
        local chunk, _ = reader()
    until not chunk

    httpc:set_keepalive(
        reval_params.keepalive_timeout,
        reval_params.keepalive_poolsize
    )

    -- A response object here only tells us the loopback round-trip
    -- completed - not that anything was actually refreshed. If the real
    -- origin is failing, Ledge's own error handling still returns a
    -- response (e.g. a 5xx, or a stale-if-error fallback) rather than a
    -- connection failure, and none of that updates the stored cache
    -- metadata. Treat 5xx as a failed revalidation so we notice and back
    -- off future attempts, rather than silently leaving stale content in
    -- place forever.
    --
    -- We deliberately don't return a job-error here (which would cause the
    -- job queue to retry immediately, several times, on its own schedule).
    -- record_failure() above already governs when the next attempt is
    -- allowed via revalidate_in_background()'s own backoff check, and each
    -- stale-serving request in the meantime will trigger a fresh attempt
    -- once that backoff expires anyway - a job-queue-level retry loop on
    -- top of that just repeats the same request several times in a row
    -- and inflates the backoff further for no benefit.
    if key_chain then
        if res.status and res.status >= 500 then
            record_failure(
                job.redis,
                key_chain,
                job.data.revalidate_backoff_initial,
                job.data.revalidate_backoff_max
            )
            ngx_log(ngx_ERR,
                "revalidate received upstream error status ",
                tostring(res.status), "; backing off further attempts"
            )
        else
            clear_backoff(job.redis, key_chain)
        end
    end
end


return _M
