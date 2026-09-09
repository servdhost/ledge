use Test::Nginx::Socket 'no_plan';
use FindBin;
use lib "$FindBin::Bin/..";
use LedgeEnv;

our $HttpConfig = LedgeEnv::http_config(extra_lua_config => qq{
    -- Define storage backends here, and add requests to each test
    -- with backend=<backend> params.
    backends = {
        redis = {
            module = "ledge.storage.redis",
            params = {
                redis_connector_params = {
                    url = REDIS_URL,
                },
            },
            bad_params = {
                redis_connector_params = {
                    url = REDIS_URL,
                    foobar = "broken!"
                },
            }
        },
        redis_notransact = {
            module = "ledge.storage.redis",
            params = {
                redis_connector_params = {
                    url = REDIS_URL,
                    connection_is_proxied = true,
                },
                supports_transactions = false,
            },
            bad_params = {
                redis_connector_params = {
                    url = REDIS_URL,
                    foobar = "broken!"
                },
                supports_transactions = false,
            }
        },
    }

    function get_backend(backend)
        local config = backends[backend]

        if backend == "redis_notransact" then
            -- stub out transactional redis functions to error
            require("resty.redis").multi = function(...)
                error("called transactional function 'multi'")
            end

            require("resty.redis").exec = function(...)
                error("called transactional function 'exec'")
            end

            require("resty.redis").discard = function(...)
                error("called transactional function 'discard'")
            end
        end

        return config
    end


    -- Utility returning an iterator over given chunked data
    function get_source(data)
        local index = 0
        return function()
            index = index + 1
            if data[index] then
                return data[index][1], data[index][2], data[index][3]
            end
        end
    end


    -- Utility returning an iterator over given chunked data, but which
    -- fails (simulating storage connection failure) at fail_pos iteration.
    function get_and_fail_source(data, fail_pos, storage)
        local index = 0
        return function()
            index = index + 1

            if index == fail_pos then
                storage.redis:close()
            end

            if data[index] then
                return data[index][1], data[index][2], data[index][3]
            end
        end
    end


    -- Utility returning an iterator over given chunked data, but which
    -- fails (simulating upstream timeout) at fail_pos iteration.
    function get_and_fail_upstream_source(data, fail_pos)
        local index = 0
        return function()
            index = index + 1

            if index == fail_pos then return nil, "timeout" end

            if data[index] then
                return data[index][1], data[index][2], data[index][3]
            end
        end
    end


    -- Utility to read the body as is serving
    function sink(iterator)
        repeat
            local chunk, err, has_esi = iterator()
            if chunk then
                ngx.say(chunk, ":", err, ":", tostring(has_esi))
            end
        until not chunk
    end


    -- Utilitu to report success and the size written
    function success_handler(bytes_written)
        ngx.say("wrote ", bytes_written, " bytes")
    end


    -- Utility to report the onfailure event was called
    function failure_handler(reason)
        ngx.say(reason)
    end


    -- Response object stub
    _res = {}
    local _mt = { __index = _res }

    function _res.new(entity_id)
        return setmetatable({
            entity_id = entity_id,
            body_reader = function() return nil end,
        }, _mt)
    end
});

no_long_string();
no_diff();
run_tests();

__DATA__
=== TEST 1: Load connect and close without errors.
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local config = get_backend(ngx.req.get_uri_args()["backend"])
        local storage = require(config.module).new()

        assert(storage:connect(config.params),
            "storage:connect should return positively")
        assert(storage:close(),
            "storage:close() should return positively")

        ngx.print(ngx.req.get_uri_args()["backend"], " OK")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "redis OK",
    "redis_notransact OK",
]
--- no_error_log
[error]


=== TEST 2: Write entity, read it back
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()

        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00002-" .. backend)
        res.body_reader = get_source({
            { "CHUNK 1", nil, false },
            { "CHUNK 2", nil, true },
            { "CHUNK 3", nil, false },
        })

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        assert(storage:exists(res.entity_id),
            "entity should exist")

        -- Attach the reader, and run sink
        res.body_reader = storage:get_reader(res)
        sink(res.body_reader)

        assert(storage:close(),
            "storage:close should return positively")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "CHUNK 1:nil:false
CHUNK 2:nil:true
CHUNK 3:nil:false
wrote 21 bytes
CHUNK 1CHUNK 2CHUNK 3:nil:true
",
    "CHUNK 1:nil:false
CHUNK 2:nil:true
CHUNK 3:nil:false
wrote 21 bytes
CHUNK 1CHUNK 2CHUNK 3:nil:true
",
]
--- no_error_log
[error]


=== TEST 3: Fail to write entity larger than max_size
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()
        config.params.max_size = 8

        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00003-" .. backend)
        res.body_reader = get_source({
            { "123", nil, false },
            { "456", nil, true },
            { "789", nil, false },
        })

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        -- Prove entity wasn't written
        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        assert(storage:close(),
            "storage:close should return positively")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
456:nil:true
789:nil:false
body is larger than 8 bytes
",
    "123:nil:false
456:nil:true
789:nil:false
body is larger than 8 bytes
",
]
--- no_error_log
[error]


=== TEST 4: Test zero length bodies are not written
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()
        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00004-" .. backend)

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        -- Prove entity wasn't written
        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        assert(storage:close(),
            "storage:close should return positively")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "wrote 0 bytes
",
    "wrote 0 bytes
",
]

--- no_error_log
[error]


=== TEST 5: Test write fails and abort handler called if conn to storage is interrupted
--- http_config eval: $::HttpConfig
--- config
location /storage {
    lua_socket_log_errors off;
    content_by_lua_block {
        -- TODO find a way to check the error log, for no transact redis,
        -- where the optimistic call to redis:del() fails due to closed conn.

        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()
        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00005-" .. backend)
        -- Load source but fail on second chunk
        res.body_reader = get_and_fail_source({
            { "123", nil, false },
            { "456", nil, true },
            { "789", nil, true },
        }, 2, storage)

        assert(not storage:exists(res.entity_id),
            "entity should not yet exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        if backend ~= "redis_notransact" then
            -- Prove entity wasn't written (rolled back)
            assert(not storage:exists(res.entity_id),
                "entity should still not exist")
        end
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
456:nil:true
789:nil:true
error writing: closed
",
    "123:nil:false
456:nil:true
789:nil:true
error writing: closed
",
]


=== TEST 5b: Test write fails and abort handler called if upstream errors
--- http_config eval: $::HttpConfig
--- config
location /storage {
    lua_socket_log_errors off;
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()
        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00005b-" .. backend)
        -- Load source but fail on second chunk
        res.body_reader = get_and_fail_upstream_source({
            { "123", nil, false },
            { "456", nil, true },
            { "789", nil, true },
        }, 2)

        assert(not storage:exists(res.entity_id),
            "entity should not yet exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        if backend ~= "redis_notransact" then
            -- Prove entity wasn't written (rolled back)
            assert(not storage:exists(res.entity_id),
                "entity should still not exist")
        end
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
upstream error: timeout
",
    "123:nil:false
upstream error: timeout
",
]


=== TEST 6: Write entity with short exiry, test keys expire
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()

        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00006-" .. backend)
        res.body_reader = get_source({
            { "123", nil, false },
            { "456", nil, true },
            { "789", nil, false },
        })

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 1,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        assert(storage:exists(res.entity_id),
            "entity should exist")

        ngx.sleep(2)

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        assert(storage:close(),
            "storage:close should return positively")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
456:nil:true
789:nil:false
wrote 9 bytes
",
    "123:nil:false
456:nil:true
789:nil:false
wrote 9 bytes
",
]
--- no_error_log
[error]


=== TEST 7: Test maxmem keys are cleaned up when transactions are not available
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()

        config.params.max_size = 8

        -- Turn off atomicity
        config.params.supports_transactions = false
        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00007-" .. backend)
        -- Load source but fail on second chunk
        res.body_reader = get_source({
            { "123", nil, false },
            { "456", nil, true },
            { "789", nil, true },
        }, storage)

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        -- Prove entity wasn't written (rolled back)
        assert(not storage:exists(res.entity_id),
            "entity should not exist")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
456:nil:true
789:nil:true
body is larger than 8 bytes
",
    "123:nil:false
456:nil:true
789:nil:true
body is larger than 8 bytes
",
]
--- no_error_log
[error]


=== TEST 8: Keys will remain on failure when transactions are not available
--- http_config eval: $::HttpConfig
--- config
location /storage {
    lua_socket_log_errors off;
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()

        config.params.supports_transactions = false
        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00008-" .. backend)
        -- Load source but fail on second chunk
        res.body_reader = get_and_fail_source({
            { "123", nil, false },
            { "456", nil, true },
            { "789", nil, true },
        }, 2, storage)

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        -- Reconnect
        assert(storage:connect(config.params),
            "storage:connect should return positively")

        -- storage:exists() now correctly reports false here - body_esi
        -- is only ever written once, at a successful EOF, which this
        -- write never reached, so the entity is genuinely incomplete.
        -- But the orphaned body key itself (holding the one chunk that
        -- did write before the connection died) could not be cleaned up
        -- (the best-effort delete attempt also hit the dead connection),
        -- so it remains - prove that directly.
        assert(not storage:exists(res.entity_id),
            "entity should not report as existing (it's incomplete)")

        local exists, err = storage.redis:exists(
            "ledge:entity:{" .. res.entity_id .. "}:body"
        )
        assert(exists == 1, "orphaned body key should remain")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
456:nil:true
789:nil:true
error writing: closed
",
    "123:nil:false
456:nil:true
789:nil:true
error writing: closed
",
]
--- error_log eval
["closed"]


=== TEST 9: Close connection and then reconnect and re-read
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()

        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00009-" .. backend)
        res.body_reader = get_source({
            { "123", nil, false },
            { "456", nil, true },
            { "789", nil, false },
        })

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        assert(storage:exists(res.entity_id),
            "entity should exist")

        assert(storage:close(),
            "storage:close should return positively")

        assert(storage:connect(config.params),
            "storage:connect should return positively")

        assert(storage:exists(res.entity_id),
            "entity should exist")

        -- sink() drives the reader with no buffer_size, so get_reader()
        -- returns the whole body in a single call - with the aggregate
        -- has_esi flag for the entity (true, since chunk 2 had it set).
        res.body_reader = storage:get_reader(res)
        sink(res.body_reader)

        assert(storage:close(),
            "storage:close should return positively")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
456:nil:true
789:nil:false
wrote 9 bytes
123456789:nil:true
",
    "123:nil:false
456:nil:true
789:nil:false
wrote 9 bytes
123456789:nil:true
",
]
--- no_error_log
[error]


=== TEST 10: Entities can be deleted
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()

        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00010-" .. backend)
        res.body_reader = get_source({
            { "123", nil, false },
            { "456", nil, false },
            { "789", nil, false },
        })

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 99,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        assert(storage:exists(res.entity_id),
            "entity should exist")

        assert(storage:delete(res.entity_id),
            "entity should delete without error")

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        local ok, err = storage:delete("foo")
        assert(ok == false and err == nil,
            "deleting foo entity should return false without error")

        assert(storage:close(),
            "storage:close should return positively")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
456:nil:false
789:nil:false
wrote 9 bytes
",
    "123:nil:false
456:nil:false
789:nil:false
wrote 9 bytes
",
]
--- no_error_log
[error]


=== TEST 11: set_ttl / get_ttl
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()

        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00011-" .. backend)
        res.body_reader = get_source({
            { "123", nil, false },
            { "456", nil, false },
            { "789", nil, false },
        })

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        -- Attach the writer, and run sink
        res.body_reader = storage:get_writer(
            res, 99,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        assert(storage:exists(res.entity_id),
            "entity should exist")

        local ttl, err = storage:get_ttl("foo")
        assert(ttl == false and err == "entity does not exist",
            "getting ttl on foo entity should return false without error")

        local ttl = storage:get_ttl(res.entity_id)
        assert(ttl and ttl <= 99 and ttl >= 98,
            "entity ttl should be roughly 99")

        local ok, err = storage:set_ttl("foo", 1)
        assert(ok == false and err == "entity does not exist",
            "setting ttl on foo entity should return false without error")

        assert(storage:set_ttl(res.entity_id, 1),
            "setting ttl should return positively")

        ngx.sleep(2)

        assert(not storage:exists(res.entity_id),
            "entity should have expired")

        assert(storage:close(),
            "storage:close should return positively")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
456:nil:false
789:nil:false
wrote 9 bytes
",
    "123:nil:false
456:nil:false
789:nil:false
wrote 9 bytes
",
]
--- no_error_log
[error]

=== TEST 12: Bad params should return an error
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local config = get_backend(ngx.req.get_uri_args()["backend"])
        local storage = require(config.module).new()

        local ok, err = storage:connect(config.bad_params)

        assert(not ok,
            "storage:connect should not return positively")
        assert(type(err) == "string",
            "storage:connect should return an error string")

        ngx.log(ngx.INFO, err)
        ngx.print(ngx.req.get_uri_args()["backend"], " OK")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "redis OK",
    "redis_notransact OK",
]
--- no_error_log
[error]

=== TEST 13: Handler run with bad config should return an error
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local config = get_backend(ngx.req.get_uri_args()["backend"])
        local ok, err = require("ledge").create_handler({
            storage_driver = config.module,
            storage_driver_config = config.bad_params
        }):run()
        assert(ok == nil and err ~= nil,
            "run should return negatively with an error")

        ngx.print(ngx.req.get_uri_args()["backend"], " OK")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "redis OK",
    "redis_notransact OK",
]
--- no_error_log
[error]


=== TEST 14: get_reader on a missing / evicted entity returns nil, err
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()
        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00014-" .. backend)

        assert(not storage:exists(res.entity_id),
            "entity should not exist")

        local reader, err = storage:get_reader(res)
        assert(reader == nil,
            "get_reader should return nil for a missing entity")
        assert(err,
            "get_reader should return an error message")

        assert(storage:close(),
            "storage:close should return positively")

        ngx.print(ngx.req.get_uri_args()["backend"], " OK")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "redis OK",
    "redis_notransact OK",
]
--- no_error_log
[error]


=== TEST 15: get_reader refuses to start if the body key has been evicted
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()
        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00015-" .. backend)
        res.body_reader = get_source({
            { "123", nil, false },
            { "456", nil, true },
            { "789", nil, true },
        })

        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        assert(storage:exists(res.entity_id),
            "entity should exist")

        -- Simulate the whole body being evicted/tiered away. Unlike a
        -- per-chunk-key scheme, the body is a single string - one atomic
        -- Redis value - so this is the only way it can be lost: all or
        -- nothing, just like the original list design.
        local deleted, err = storage.redis:del(
            "ledge:entity:{" .. res.entity_id .. "}:body"
        )
        assert(deleted == 1, "should have deleted the body key")

        -- get_reader should catch this upfront and refuse to hand back
        -- an iterator, rather than yielding an empty/short body that
        -- looks like a complete, successful response. This is what lets
        -- the caller (handler.read_from_cache) treat it as a cache miss
        -- and fall back to fetching a fresh copy from the origin.
        assert(not storage:exists(res.entity_id),
            "entity should appear gone once the body is lost")

        local reader, err = storage:get_reader(res)
        assert(reader == nil,
            "get_reader should refuse to start when the body is missing")
        assert(err, "get_reader should return an error message")

        assert(storage:close(),
            "storage:close should return positively")

        ngx.print(ngx.req.get_uri_args()["backend"], " OK")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
456:nil:true
789:nil:true
wrote 9 bytes
redis OK",
    "123:nil:false
456:nil:true
789:nil:true
wrote 9 bytes
redis_notransact OK",
]
--- no_error_log
[error]


=== TEST 15b: get_reader errors (not silently truncates) if the body is lost mid-stream
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()
        assert(storage:connect(config.params),
            "storage:connect should return positively")

        -- No ESI markup here (has_esi=false throughout) - an entity
        -- with has_esi=true is always read back in a single call
        -- regardless of buffer_size (see get_reader()), which would
        -- short-circuit this test before it ever got to the deleted key.
        local res = _res.new("00015b-" .. backend)
        res.body_reader = get_source({
            { "123", nil, false },
            { "456", nil, false },
            { "789", nil, false },
        })

        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        -- Get a reader while everything is still intact (passes the
        -- upfront check), then lose the body afterwards - simulating the
        -- narrow window between that check and actually reading it,
        -- which streaming can't close off entirely.
        local reader, err = storage:get_reader(res)
        assert(reader, "get_reader should return an iterator")

        local chunk, err, has_esi = reader(3)
        ngx.say(chunk, ":", err, ":", tostring(has_esi))

        local deleted, err = storage.redis:del(
            "ledge:entity:{" .. res.entity_id .. "}:body"
        )
        assert(deleted == 1, "should have deleted the body key")

        -- This must come back as a distinguishable error, not as
        -- (nil, nil, nil) - which is indistinguishable from a clean,
        -- successful end of body.
        local chunk, err, has_esi = reader(3)
        assert(chunk == nil, "chunk should be nil")
        assert(err, "err should be set, not nil, to signal real failure")

        assert(storage:close(),
            "storage:close should return positively")

        ngx.print(ngx.req.get_uri_args()["backend"], " OK")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
456:nil:false
789:nil:false
wrote 9 bytes
123:nil:false
redis OK",
    "123:nil:false
456:nil:false
789:nil:false
wrote 9 bytes
123:nil:false
redis_notransact OK",
]
--- error_log eval
["entity removed during read"]


=== TEST 16: exists/get_reader degrade gracefully if only body_esi is evicted
--- http_config eval: $::HttpConfig
--- config
location /storage {
    content_by_lua_block {
        local backend = ngx.req.get_uri_args()["backend"]
        local config = get_backend(backend)

        local storage = require(config.module).new()
        assert(storage:connect(config.params),
            "storage:connect should return positively")

        local res = _res.new("00016-" .. backend)
        res.body_reader = get_source({
            { "123", nil, false },
        })

        res.body_reader = storage:get_writer(
            res, 60,
            success_handler,
            failure_handler
        )
        sink(res.body_reader)

        assert(storage:exists(res.entity_id),
            "entity should exist")

        -- Simulate only the small has_esi flag key being evicted, while
        -- the (larger) body key remains - body and body_esi are two
        -- independent Redis keys, so this residual risk is exactly the
        -- one the original two-list design always had.
        local deleted, err = storage.redis:del(
            "ledge:entity:{" .. res.entity_id .. "}:body_esi"
        )
        assert(deleted == 1, "should have deleted the body_esi key")

        -- exists() requires both keys, so this should now report false,
        -- and get_reader() should refuse to start - neither should error.
        assert(not storage:exists(res.entity_id),
            "entity should appear gone once body_esi is lost")

        local reader, err = storage:get_reader(res)
        assert(reader == nil and err,
            "get_reader should cleanly refuse, not error")

        assert(storage:close(),
            "storage:close should return positively")

        ngx.print(ngx.req.get_uri_args()["backend"], " OK")
    }
}
--- request eval
[
    "GET /storage?backend=redis",
    "GET /storage?backend=redis_notransact",
]
--- response_body eval
[
    "123:nil:false
wrote 3 bytes
redis OK",
    "123:nil:false
wrote 3 bytes
redis_notransact OK",
]
--- no_error_log
[error]
