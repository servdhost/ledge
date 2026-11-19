use Test::Nginx::Socket 'no_plan';
use FindBin;
use lib "$FindBin::Bin/..";
use LedgeEnv;

our $HttpConfig = LedgeEnv::http_config();

our $HttpConfig_Test6 = LedgeEnv::http_config(extra_lua_config => qq{
    foo = 1
    package.loaded["ledge.job.test"] = {
        perform = function(job)
            foo = foo + 1
            return true
        end
    }
}, run_worker => 1);

no_long_string();
no_diff();
run_tests();

__DATA__
=== TEST 1: Load module without errors.
--- http_config eval: $::HttpConfig
--- config
location /worker_1 {
    echo "OK";
}
--- request
GET /worker_1
--- no_error_log
[error]


=== TEST 2: Create worker with default config
--- http_config eval: $::HttpConfig
--- config
location /worker_2 {
    echo "OK";
}
--- request
GET /worker_2
--- no_error_log
[error]


=== TEST 4: Create worker with bad config key
--- http_config eval
qq {
lua_package_path "./lib/?.lua;;";
init_by_lua_block {
    if $LedgeEnv::test_coverage == 1 then
        require("luacov.runner").init()
    end
}
init_worker_by_lua_block {
    require("ledge.worker").new({
        foo = "one",
    })
}
}
--- config
location /worker_4 {
    echo "OK";
}
--- request
GET /worker_4
--- error_log
field foo does not exist


=== TEST 5: Run workers without errors
--- http_config eval
qq {
lua_package_path "./lib/?.lua;;";
init_by_lua_block {
    if $LedgeEnv::test_coverage == 1 then
        require("luacov.runner").init()
    end
}
init_worker_by_lua_block {
    require("ledge.worker").new():run()
}
}
--- config
location /worker_5 {
    echo "OK";
}
--- request
GET /worker_5
--- no_error_log
[error]


=== TEST 6: Push a job and confirm it runs
--- http_config eval: $::HttpConfig_Test6
--- config
location /worker_6 {
    content_by_lua_block {
        local job_queue = require("ledge.job_queue")
        local redis = assert(require("ledge").create_jobs_connection())

        local job = assert(job_queue.put(redis, "ledge_gc", "ledge.job.test", {}, {}))

        ngx.sleep(2)
        ngx.say(foo)
        local record = job_queue.get(redis, job.jid)
        ngx.say(record.state)

        require("ledge").close_redis_connection(redis)
    }
}
--- request
GET /worker_6
--- response_body
2
complete
--- timeout: 5
--- no_error_log
[error]


=== TEST 7: Job records always carry a TTL, and orphaned waiting jobs self-heal
--- http_config eval: $::HttpConfig
--- config
location /worker_7 {
    content_by_lua_block {
        local job_queue = require("ledge.job_queue")
        local redis = assert(require("ledge").create_jobs_connection())
        redis:flushdb()

        local q = "ledge_revalidate"
        local jid = "orphan_test"
        local key = job_queue.job_key(jid)

        -- Waiting job gets the requested TTL
        assert(job_queue.put(redis, q, "ledge.job.test", {}, { jid = jid, ttl = 30 }))
        local ttl = redis:ttl(key)
        ngx.say("waiting ttl ok: ", ttl > 0 and ttl <= 30)

        -- Delayed job's TTL covers its delay
        assert(job_queue.put(redis, q, "ledge.job.test", {}, { ttl = 30, delay = 100 }))

        -- Simulate eviction of the ready list, leaving an orphaned record
        redis:del(job_queue.ready_key(q))

        -- Coalescing put must not extend the orphan's TTL
        redis:expire(key, 5)
        assert(job_queue.put(redis, q, "ledge.job.test", {}, { jid = jid, ttl = 30 }))
        ttl = redis:ttl(key)
        ngx.say("coalesce does not extend: ", ttl > 0 and ttl <= 5)

        -- Once the orphan expires, the next put re-queues the job
        redis:del(key)
        assert(job_queue.put(redis, q, "ledge.job.test", {}, { jid = jid, ttl = 30 }))
        ngx.say("requeued: ", job_queue.pop_ready(redis, q))

        -- Legacy record with no TTL gets one on the next put
        redis:persist(key)
        assert(job_queue.put(redis, q, "ledge.job.test", {}, { jid = jid, ttl = 30 }))
        ttl = redis:ttl(key)
        ngx.say("legacy gets ttl: ", ttl > 0 and ttl <= 30)

        -- Running record's TTL tracks its lease
        assert(job_queue.mark_running(redis, q, jid, ngx.time() + 10))
        ttl = redis:ttl(key)
        ngx.say("running ttl ok: ", ttl > 10 and ttl <= 10 + job_queue.running_ttl_grace)

        -- Reaped job goes back to waiting with its waiting TTL
        redis:zadd(job_queue.running_key(q), ngx.time() - 1, jid)
        local reclaimed = job_queue.reap(redis, q, ngx.time(), 100)
        ngx.say("reaped: ", reclaimed[1])
        ngx.say("state: ", (redis:hget(key, "state")))
        ttl = redis:ttl(key)
        ngx.say("reaped ttl ok: ", ttl > 0 and ttl <= 30)

        -- Don't leave queued test jobs for later tests' workers
        redis:flushdb()
        require("ledge").close_redis_connection(redis)
    }
}
--- request
GET /worker_7
--- response_body
waiting ttl ok: true
coalesce does not extend: true
requeued: orphan_test
legacy gets ttl: true
running ttl ok: true
reaped: orphan_test
state: waiting
reaped ttl ok: true
--- no_error_log
[error]
