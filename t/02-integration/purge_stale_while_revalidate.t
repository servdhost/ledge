use Test::Nginx::Socket 'no_plan';
use FindBin;
use lib "$FindBin::Bin/..";
use LedgeEnv;

our $HttpConfig = LedgeEnv::http_config(extra_lua_config => qq{
    package.loaded["state"] = {
        req = 1,
        req2 = 1,
        req3 = 1,
    }
}, run_worker => 1);

no_long_string();
no_diff();
run_tests();

__DATA__
=== TEST 1: PURGE (invalidate) a URL with a long stale-while-revalidate.
The immediate next request should be served the existing stale response
(fast, not a synchronous origin fetch) and a background revalidation
should be scheduled. All three requests share one nginx config/process
(matching real production, where config doesn't change between requests -
unlike separate Test::Nginx test blocks, which restart nginx and would
break the background job's loopback request mid-flight).
--- http_config eval: $::HttpConfig
--- config
location /swr_purge_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run()
    }
}
location /swr_purge {
    content_by_lua_block {
        local state = require("state")
        ngx.header["Cache-Control"] = "max-age=3600, stale-while-revalidate=10800"
        ngx.say("ORIGIN: ", state.req)
        state.req = state.req + 1
    }
}
--- more_headers eval
["Cache-Control: no-cache", "", ""]
--- request eval
["GET /swr_purge_prx", "PURGE /swr_purge_prx", "GET /swr_purge_prx"]
--- response_body eval
["ORIGIN: 1\n", qr/.*/, "ORIGIN: 1\n"]
--- response_headers_like eval
["X-Cache: MISS from .*", "", "X-Cache: HIT from .*"]
--- wait: 5
--- no_error_log
[error]


=== TEST 2: The background revalidation from TEST 1 should have completed
by now, so this now sees the fresh response.
--- http_config eval: $::HttpConfig
--- config
location /swr_purge_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run()
    }
}
--- request
GET /swr_purge_prx
--- response_body
ORIGIN: 2
--- response_headers_like
X-Cache: HIT from .*
--- no_error_log
[error]


=== TEST 3: Same, but purging via the JSON multi-URL purge_api (PURGE with
a Content-Type: application/json body naming the URI) - matching how the
PURGE API is actually invoked over HTTP (e.g. from an external client),
rather than a raw single-URL PURGE. This internally does its own loopback
PURGE per URI (lua-resty-http), so this exercises a materially different
code path (lib/ledge/purge.lua's purge_api()/send_purge_request()) to
TEST 1's direct PURGE. Cache-Control matches a real-world example.
--- http_config eval: $::HttpConfig
--- config
location /swr_api_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run()
    }
}
location /swr_api {
    content_by_lua_block {
        local state = require("state")
        ngx.header["Cache-Control"] =
            "max-age=1000, s-maxage=1000, stale-while-revalidate=2000, stale-if-error=4000"
        ngx.say("ORIGIN: ", state.req2)
        state.req2 = state.req2 + 1
    }
}
--- more_headers eval
["Cache-Control: no-cache", "Content-Type: application/json", ""]
--- request eval
[
    "GET /swr_api_prx",
    qq(PURGE /swr_api_prx
{"uris": ["http://localhost:$LedgeEnv::nginx_port/swr_api_prx"], "purge_mode": "invalidate"}),
    "GET /swr_api_prx",
]
--- response_body eval
["ORIGIN: 1\n", qr/.*/, "ORIGIN: 1\n"]
--- response_headers_like eval
["X-Cache: MISS from .*", "Content-Type: application/json", "X-Cache: HIT from .*"]
--- wait: 5
--- no_error_log
[error]


=== TEST 4: The background revalidation from TEST 3 should have completed
by now, so this now sees the fresh response.
--- http_config eval: $::HttpConfig
--- config
location /swr_api_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run()
    }
}
--- request
GET /swr_api_prx
--- response_body
ORIGIN: 2
--- response_headers_like
X-Cache: HIT from .*
--- no_error_log
[error]


=== TEST 5: Same again, but setting Cache-Control dynamically via an
after_upstream_request binding (rather than the origin sending it
directly) - reproducing the user's exact handler:bind() pattern, which
overrides whatever the origin sent, strips Pragma/Set-Cookie, and forces
no-cache for error/bodyless responses.
--- http_config eval: $::HttpConfig
--- config
location /swr_bind_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        local handler = require("ledge").create_handler()

        handler:bind("after_upstream_request", function(res)
            res.header["cache-control-backup"] = res.header["Cache-Control"]
            res.header["Pragma"] = ""

            if res.status >= 400 then
                res.header["Cache-Control"] = "no-cache"
            else
                res.header["Cache-Control"] =
                    "max-age=1000, s-maxage=1000, stale-while-revalidate=2000, stale-if-error=4000"
            end

            if res.length == 0 or not res.has_body then
                res.header["Cache-Control"] = "no-cache"
            end

            if res:is_cacheable() then
                res.header["Set-Cookie"] = ""
            end
        end)

        handler:run()
    }
}
location /swr_bind {
    content_by_lua_block {
        local state = require("state")
        -- Origin sends its own (different) Cache-Control and a real
        -- Content-Length, a Set-Cookie, and a Pragma - all of which the
        -- binding above should override/strip.
        ngx.header["Cache-Control"] = "private, max-age=30"
        ngx.header["Pragma"] = "no-cache"
        ngx.header["Set-Cookie"] = "sessionid=abc123"
        local body = "ORIGIN: " .. state.req3
        ngx.header["Content-Length"] = #body + 1
        ngx.say(body)
        state.req3 = state.req3 + 1
    }
}
--- more_headers eval
["Cache-Control: no-cache", "", ""]
--- request eval
["GET /swr_bind_prx", "PURGE /swr_bind_prx", "GET /swr_bind_prx"]
--- response_body eval
["ORIGIN: 1\n", qr/.*/, "ORIGIN: 1\n"]
--- response_headers_like eval
["X-Cache: MISS from .*", "", "X-Cache: HIT from .*"]
--- wait: 5
--- no_error_log
[error]


=== TEST 6: The background revalidation from TEST 5 should have completed
by now, so this now sees the fresh response.
--- http_config eval: $::HttpConfig
--- config
location /swr_bind_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        local handler = require("ledge").create_handler()
        handler:bind("after_upstream_request", function(res)
            res.header["Cache-Control"] =
                "max-age=1000, s-maxage=1000, stale-while-revalidate=2000, stale-if-error=4000"
        end)
        handler:run()
    }
}
--- request
GET /swr_bind_prx
--- response_body
ORIGIN: 2
--- response_headers_like
X-Cache: HIT from .*
--- no_error_log
[error]
