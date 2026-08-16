use Test::Nginx::Socket 'no_plan';
use FindBin;
use lib "$FindBin::Bin/..";
use LedgeEnv;

# backoff_max needs to comfortably outlast this test's own wait chain
# (TEST 2's wait for 6 rapid job-queue retries to settle, plus TEST 3's
# own processing) - otherwise the backoff window could expire before TEST
# 3 gets a chance to verify it's still in effect.
our $HttpConfig = LedgeEnv::http_config(extra_lua_config => qq{
    require("ledge").set_handler_defaults({
        revalidate_backoff_initial = 5,
        revalidate_backoff_max = 30,
    })
}, run_worker => 1);

no_long_string();
no_diff();
run_tests();

__DATA__
=== TEST 1: Prime cache, immediately expired (stale-while-revalidate).
--- http_config eval: $::HttpConfig
--- config
location /revalback_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        local handler = require("ledge").create_handler()
        handler:bind("before_save", function(res)
            res.header["Cache-Control"] = "max-age=0, stale-while-revalidate=60"
        end)
        handler:run()
    }
}
location /revalback {
    content_by_lua_block {
        ngx.header["Cache-Control"] = "max-age=60, stale-while-revalidate=60"
        ngx.say("OK")
    }
}
--- request
GET /revalback_prx
--- response_body
OK
--- no_error_log
[error]


=== TEST 2: Origin now fails permanently. Trigger a stale-serving request
and let its background revalidation run to completion - it retries a
bounded number of times (the job queue's own immediate retry, unrelated
to the new cross-request backoff) and then gives up, entering backoff.
Origin hits are counted via Redis, so the count survives the nginx
restart between Test::Nginx blocks.
--- http_config eval: $::HttpConfig
--- config
location /revalback_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run()
    }
}
location /revalback {
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        redis:incr("test:revalback:hits")
        require("ledge").close_redis_connection(redis)
        return ngx.exit(500)
    }
}
--- request
GET /revalback_prx
--- response_body
OK
--- wait: 8
--- error_log
revalidate received upstream error status 500


=== TEST 3: Immediately after, a second stale-serving request must NOT
trigger another revalidation attempt while backing off. Does the
before/after comparison itself, via a real top-level loopback HTTP request
(like the background job's own loopback, not an ngx.location.capture
subrequest - Ledge's fetch_from_origin tries to read a client body reader,
which nginx disallows in subrequests, so a capture isn't a faithful
simulation of a second real client request here) - so it doesn't need to
know the exact hit count TEST 2 produced.
--- http_config eval: $::HttpConfig
--- config
location /revalback_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run()
    }
}
location /revalback {
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        redis:incr("test:revalback:hits")
        require("ledge").close_redis_connection(redis)
        return ngx.exit(500)
    }
}
location /diag {
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        local before = tonumber(redis:get("test:revalback:hits")) or 0

        local http = require("resty.http")
        local httpc = http.new()
        local ok, err = httpc:connect("127.0.0.1", ngx.var.server_port)
        if not ok then
            ngx.say("connect failed: ", err)
            return
        end
        local res, err = httpc:request({
            method = "GET",
            path = "/revalback_prx",
            headers = { ["Host"] = ngx.var.host },
        })
        if res then
            local reader = res.body_reader
            repeat local chunk = reader() until not chunk
        else
            ngx.say("request failed: ", err)
        end
        httpc:close()

        ngx.sleep(2) -- give the worker a moment, in case it does schedule one

        local after = tonumber(redis:get("test:revalback:hits")) or 0
        require("ledge").close_redis_connection(redis)

        ngx.say("before=", before, " after=", after,
            " backed_off=", tostring(before == after))
    }
}
--- request
GET /diag
--- response_body_like
before=(\d+) after=\1 backed_off=true
--- no_error_log
[error]


=== TEST 4: Clean up the test counter key.
--- http_config eval: $::HttpConfig
--- config
location /diag {
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        redis:del("test:revalback:hits")
        require("ledge").close_redis_connection(redis)
        ngx.say("ok")
    }
}
--- request
GET /diag
--- no_error_log
[error]
