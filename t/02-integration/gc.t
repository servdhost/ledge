use Test::Nginx::Socket 'no_plan';
use FindBin;
use lib "$FindBin::Bin/..";
use LedgeEnv;

our $HttpConfig = LedgeEnv::http_config(extra_nginx_config => qq{
    lua_check_client_abort on;
}, extra_lua_config => qq{
    require("ledge").set_handler_defaults({
        keep_cache_for = 0,
    })
}, run_worker => 1);

# Uses the default (long) keep_cache_for, unlike $HttpConfig above, to
# keep entity persistence timing well clear of this file's own GC-timing
# tests below (which rely on the file-wide keep_cache_for=0 to make
# metadata expire quickly). Tests that need a body to actually persist
# across requests use this.
our $HttpConfigNormalTTL = LedgeEnv::http_config(run_worker => 1);

no_long_string();
no_diff();
run_tests();

__DATA__
=== TEST 1: Prime cache
--- http_config eval: $::HttpConfig
--- config
location /gc_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run()
    }
}
location /gc {
    more_set_headers "Cache-Control: public, max-age=60";
    echo "OK";
}
--- request
GET /gc_prx
--- no_error_log
[error]
--- response_body
OK


=== TEST 2: Force revaldation (creates new entity)
--- http_config eval: $::HttpConfig
--- config
location /gc_prx {
    rewrite ^(.*)_prx$ $1 break;
    echo_location_async '/gc_a';
    echo_sleep 0.1;
    echo_location_async '/gc_b';
    echo_sleep 2.5;
}
location /gc_a {
    rewrite ^(.*)_a$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run();
    }
}
location /gc_b {
    rewrite ^(.*)_b$ $1 break;
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        local handler = require("ledge").create_handler()
        handler.redis = redis

        local key_chain = handler:cache_key_chain()
        local num_entities, err = redis:scard(key_chain.entities)
        ngx.say(num_entities)
    }
}
location /gc {
    more_set_headers "Cache-Control: public, max-age=5";
    content_by_lua_block {
        ngx.say("UPDATED")
    }
}
--- more_headers
Cache-Control: no-cache
--- request
GET /gc_prx
--- response_body
UPDATED
1
--- wait: 1


=== TEST 3: Check we now have just one entity
--- http_config eval: $::HttpConfig
--- config
location /gc {
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        local handler = require("ledge").create_handler()
        handler.redis = redis

        local key_chain = handler:cache_key_chain()
        local num_entities, err = redis:scard(key_chain.entities)
        ngx.say(num_entities)
    }
}
--- request
GET /gc
--- no_error_log
[error]
--- response_body
1
--- wait: 2


=== TEST 4: Entity will have expired, check Redis has cleaned up all keys.
--- http_config eval: $::HttpConfig
--- config
location /gc {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        local handler = require("ledge").create_handler()
        handler.redis = redis
        local key_chain = handler:cache_key_chain()
        local res, err = redis:keys(key_chain.full .. "*")
        assert(not next(res), "res should be empty")
    }
}
--- request
GET /gc
--- no_error_log
[error]


=== TEST 5: Prime cache
--- http_config eval: $::HttpConfig
--- config
location /gc_5_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run()
    }
}
location /gc_5 {
    more_set_headers "Cache-Control: public, max-age=60";
    echo "OK";
}
--- request
GET /gc_5_prx
--- no_error_log
[error]
--- response_body
OK


=== TEST 5b: Delete one part of the key chain
Simulate eviction under memory pressure. Will cause a MISS.
--- http_config eval: $::HttpConfig
--- config
location /gc_5_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        local handler = require("ledge").create_handler()
        handler.redis = redis
        local key_chain = handler:cache_key_chain()
        redis:del(key_chain.headers)
        handler:run()
    }
}
location /gc_5 {
    more_set_headers "Cache-Control: public, max-age=60";
    echo "OK 2";
}
--- request
GET /gc_5_prx
--- wait: 3
--- no_error_log
[error]
--- response_body
OK 2


=== TEST 5c: Missing keys should cause colleciton of the old entity.
--- http_config eval: $::HttpConfig
--- config
location /gc_5 {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        local handler = require("ledge").create_handler()
        handler.redis = redis
        local key_chain = handler:cache_key_chain()
        local res, err = redis:keys(key_chain.full .. "*")
        if res then
            ngx.say(#res)
        end
    }
}
--- request
GET /gc_5
--- no_error_log
[error]
--- response_body
5


=== TEST 6: Prime cache
--- http_config eval: $::HttpConfigNormalTTL
--- config
location /gc_6_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run()
    }
}
location /gc_6 {
    more_set_headers "Cache-Control: public, max-age=60";
    echo "OK";
}
--- request
GET /gc_6_prx
--- no_error_log
[error]
--- response_body
OK


=== TEST 6b: Entity evicted from storage but metadata survives (simulates
Redis evicting the body under memory pressure while the metadata hash stays
hot). Should MISS and re-fetch rather than silently serving an empty body.
--- http_config eval: $::HttpConfigNormalTTL
--- config
location /gc_6_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        local handler = require("ledge").create_handler()
        handler.redis = redis
        local key_chain = handler:cache_key_chain()
        local entity_id = handler:entity_id(key_chain)

        local storage = assert(require("ledge").create_storage_connection())
        assert(storage:exists(entity_id), "entity should exist before delete")
        assert(storage:delete(entity_id))
        assert(storage:close())

        handler:run()
    }
}
location /gc_6 {
    more_set_headers "Cache-Control: public, max-age=60";
    echo "OK 2";
}
--- request
GET /gc_6_prx
--- no_error_log
[error]
--- response_body
OK 2


=== TEST 6c: A fresh entity now exists in storage.
--- http_config eval: $::HttpConfigNormalTTL
--- config
location /gc_6 {
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        local handler = require("ledge").create_handler()
        handler.redis = redis
        local key_chain = handler:cache_key_chain()
        local entity_id = handler:entity_id(key_chain)

        local storage = assert(require("ledge").create_storage_connection())
        ngx.say(storage:exists(entity_id))
    }
}
--- request
GET /gc_6
--- no_error_log
[error]
--- response_body
true


=== TEST 6d: Prime cache for the race-window test below.
--- http_config eval: $::HttpConfigNormalTTL
--- config
location /gc_6d_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        require("ledge").create_handler():run()
    }
}
location /gc_6d {
    more_set_headers "Cache-Control: public, max-age=60";
    echo "OK 3";
}
--- request
GET /gc_6d_prx
--- no_error_log
[error]
--- response_body
OK 3


=== TEST 6e: Entity vanishes in the narrow window between the exists()
check and the read itself (the race storage:exists() alone can't catch,
since it's a separate round trip from get_reader()). Simulated by making
exists() lie (return true) while the entity is actually already gone -
this exercises the get_reader()-returns-nil fallback in read_from_cache()
directly, since reliably timing a genuine concurrent eviction into that
exact window isn't practical in a test.
--- http_config eval: $::HttpConfigNormalTTL
--- config
location /gc_6d_prx {
    rewrite ^(.*)_prx$ $1 break;
    content_by_lua_block {
        local redis = require("ledge").create_redis_connection()
        local handler = require("ledge").create_handler()
        handler.redis = redis
        local key_chain = handler:cache_key_chain()
        local entity_id = handler:entity_id(key_chain)

        local storage = assert(require("ledge").create_storage_connection())
        assert(storage:exists(entity_id), "entity should exist before delete")
        assert(storage:delete(entity_id))
        assert(storage:close())

        -- storage:exists() has already been called for real above and
        -- correctly confirmed presence; from here we lie so the handler's
        -- own (separate, later) exists() check believes the entity is
        -- still there, simulating eviction happening in between.
        local storage_mod = require("ledge.storage.redis")
        storage_mod.exists = function() return true end

        handler:run()
    }
}
location /gc_6d {
    more_set_headers "Cache-Control: public, max-age=60";
    echo "OK 4";
}
--- request
GET /gc_6d_prx
--- no_error_log
[error]
--- response_body
OK 4
