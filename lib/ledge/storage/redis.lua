local redis_connector = require "resty.redis.connector"

local tostring, tonumber, pairs, next, unpack, setmetatable =
      tostring, tonumber, pairs, next, unpack, setmetatable

local ngx_null = ngx.null
local ngx_log = ngx.log
local ngx_ERR = ngx.ERR
local ngx_WARN = ngx.WARN

local tbl_insert = table.insert
local tbl_copy_merge_defaults = require("ledge.util").table.copy_merge_defaults
local fixed_field_metatable = require("ledge.util").mt.fixed_field_metatable
local get_fixed_field_metatable_proxy =
    require("ledge.util").mt.get_fixed_field_metatable_proxy


local _M = {
    _VERSION = "2.5.0",
}


-- Default parameters
local defaults = setmetatable({
    redis_connector_params = {},

    max_size = 1024 * 1024,  -- Max storable size, in bytes

    -- Optional atomicity
    -- e.g. for use with a Redis proxy which doesn't support transactions
    supports_transactions = true,
}, fixed_field_metatable)


-- Redis key namespace
local KEY_PREFIX = "ledge:entity:"


-- Returns the Redis keys for entity_id.
--
-- Body content is stored as one Redis string per chunk (body:0, body:1,
-- ...), rather than as a single list, so that each chunk is individually
-- eligible for tiering to disk under backends (e.g. DragonflyDB) which
-- only support this for string values. "count" tracks how many chunks
-- were written, since without a list there's no LLEN to rely on.
local function entity_keys(entity_id)
    if entity_id then
        return {
            count       = KEY_PREFIX .. "{" .. entity_id .. "}" .. ":count",
            body        = KEY_PREFIX .. "{" .. entity_id .. "}" .. ":body",
            body_esi    = KEY_PREFIX .. "{" .. entity_id .. "}" .. ":body_esi",
        }
    end
end


-- Returns the per-chunk key for a given body/body_esi prefix and index.
local function chunk_key(prefix, index)
    return prefix .. ":" .. index
end


-- Creates a new (disconnected) storage instance
--
-- @return  table   The module instance
function _M.new()
    return setmetatable({
        redis = {},
        params = {},

        _reader_cursor = 0,
        _keys_created = false,
    }, get_fixed_field_metatable_proxy(_M))
end


-- Connects to the Redis storage backend
--
-- @param   table   Module instance (self)
-- @param   table   Storage params
function _M.connect(self, user_params)
    -- take user_params by value and merge with defaults
    user_params = tbl_copy_merge_defaults(user_params, defaults)
    self.params = user_params

    local rc, err = redis_connector.new(
        user_params.redis_connector_params
    )
    if not rc then
        return nil, err
    end

    local redis, err = rc:connect()
    if not redis then
        return nil, err
    else
        self.redis = redis
        return self, nil
    end
end


-- Closes the Redis connection (placing back on the keepalive pool)
--
-- @param   table   Module instance (self)
function _M.close(self)
    self._reader_cursor = 0
    self._keys_created = false

    local redis = self.redis
    if redis then
        local rc, err = redis_connector.new(
            self.params.redis_connector_params
        )
        if not rc then
            return nil, err
        end
        return rc:set_keepalive(redis)
    end
end


-- Returns the maximum size this connection is prepared to store.
--
-- @param   table   Module instance (self)
-- @return  number  Size (bytes)
function _M.get_max_size(self)
    return self.params.max_size
end


-- Returns a boolean indicating if the entity exists.
--
-- @param   table   Module instance (self)
-- @param   string  The entity ID
-- @return  boolean (exists)
-- @return  string  err (or nil)
function _M.exists(self, entity_id)
    local keys = entity_keys(entity_id)
    if not keys then
        return nil, "no entity id"
    else
        local res, err = self.redis:exists(keys.count)
        if not res or res == ngx_null then
            return nil, err
        else
            return res == 1
        end
    end
end


-- Returns the number of chunks written for entity_id, or 0/nil, err.
local function get_chunk_count(redis, key_chain)
    local n, err = redis:get(key_chain.count)
    if not n or n == ngx_null then
        return nil, err
    end
    return tonumber(n) or 0
end


-- Deletes an entity
--
-- @param   table   Module instance (self)
-- @param   string  The entity ID
-- @return  boolean success
-- @return  string  err (or nil)
function _M.delete(self, entity_id)
    local key_chain = entity_keys(entity_id)
    if key_chain then
        local redis = self.redis
        local n, err = get_chunk_count(redis, key_chain)
        if not n then
            return false, err
        end

        local keys = { key_chain.count }
        for i = 0, n - 1 do
            tbl_insert(keys, chunk_key(key_chain.body, i))
            tbl_insert(keys, chunk_key(key_chain.body_esi, i))
        end

        local res, err = redis:del(unpack(keys))
        if res == 0 and not err then
            return false, nil
        else
            return res, err
        end
    end
end


-- Sets the time-to-live for an entity
--
-- @param   table   Module instance (self)
-- @param   string  The entity ID
-- @param   number  TTL (seconds)
-- @return  boolean success
-- @return  string  err (or nil)
function _M.set_ttl(self, entity_id, ttl)
    local key_chain = entity_keys(entity_id)
    if key_chain then
        local redis = self.redis
        local n, err = get_chunk_count(redis, key_chain)
        if not n then
            return false, "entity does not exist"
        end

        redis:init_pipeline(1 + n * 2)
        redis:expire(key_chain.count, ttl)
        for i = 0, n - 1 do
            redis:expire(chunk_key(key_chain.body, i), ttl)
            redis:expire(chunk_key(key_chain.body_esi, i), ttl)
        end
        local res, err = redis:commit_pipeline()
        if not res or res == ngx_null then
            return false, err
        end

        -- Only the count key's own result (the first in the pipeline)
        -- determines success. Individual chunks may already have been
        -- evicted independently of the rest of the entity - that's not
        -- something a fresh TTL can fix, and shouldn't be reported as a
        -- total failure when the update did succeed for everything that
        -- still exists.
        if res[1] == 0 then
            return false, "entity does not exist"
        end

        return true, nil
    end
end


-- Gets the time-to-live for an entity
--
-- @param   table   Module instance (self)
-- @param   string  The entity ID
-- @return  number  ttl
-- @return  string  err (or nil)
function _M.get_ttl(self, entity_id)
    local key_chain = entity_keys(entity_id)
    if next(key_chain) then
        local res, err = self.redis:ttl(key_chain.count)
        if not res then
            return res, err
        elseif res == -2 then
            return false, "entity does not exist"
        elseif res == -1 then
            return false, "entity does not have a ttl"
        else
            return res, nil
        end
    end
end


-- Returns an iterator for reading the body chunks.
--
-- The caller is expected to have already confirmed the entity exists (e.g.
-- via exists()), but that check and this call are two separate round trips
-- to Redis - under eviction pressure the entity can still vanish in
-- between. So we re-check here too: if there's nothing to read despite the
-- caller believing there should be, we return nil, err rather than an
-- iterator which would silently yield zero chunks (i.e. an empty body).
--
-- Because each chunk is now its own key, it's also possible for just one
-- chunk out of many to be evicted/tiered away independently, whereas a
-- single list value was always all-or-nothing. A gap like that discovered
-- only once streaming has started can't be undone - some bytes may
-- already be with the client - so we confirm every chunk is present
-- upfront, in one pipelined round trip, and fail the whole read before
-- any of it begins. That lets the caller (see handler.read_from_cache)
-- treat it exactly like a wholly-missing entity and fall back to
-- fetching a fresh copy from the origin, rather than serving a
-- truncated body as if it were a complete, successful response.
--
-- @param   table       Module instance (self)
-- @param   table       Response object
-- @return  function    Iterator, returning chunk, err, has_esi for each call
-- @return  string      err (only set if a reader could not be returned)
function _M.get_reader(self, res)
    local redis = self.redis
    local entity_id = res.entity_id
    local entity_keys = entity_keys(entity_id)
    local num_chunks = get_chunk_count(redis, entity_keys) or 0

    if num_chunks == 0 then
        return nil, "entity has no body chunks in storage"
    end

    redis:init_pipeline(num_chunks * 2)
    for i = 0, num_chunks - 1 do
        redis:exists(chunk_key(entity_keys.body, i))
        redis:exists(chunk_key(entity_keys.body_esi, i))
    end
    local exists_res, err = redis:commit_pipeline()
    if not exists_res or exists_res == ngx_null then
        return nil, err
    end
    for _, r in ipairs(exists_res) do
        if r ~= 1 then
            return nil, "entity is missing one or more chunks in storage"
        end
    end

    return function()
        local cursor = self._reader_cursor
        self._reader_cursor = cursor + 1

        if cursor < num_chunks then
            local chunk, err = redis:get(chunk_key(entity_keys.body, cursor))
            if not chunk then return nil, err, nil end

            local has_esi, err =
                redis:get(chunk_key(entity_keys.body_esi, cursor))
            if not has_esi then return nil, err, nil end

            if chunk == ngx_null or has_esi == ngx_null then
                -- Lost the race against eviction between the upfront
                -- check above and this read. Distinct from a clean EOF
                -- (nil, nil, nil) - the caller must not treat this as
                -- the body having ended successfully.
                ngx_log(ngx_WARN,
                    "entity removed during read, ",
                    entity_keys.body
                )
                return nil, "entity removed during read", nil
            end

            return chunk, nil, has_esi == "true"
        end
    end
end


-- Writes a given chunk.
--
-- Each chunk is a freshly created string key, so (unlike a list, whose TTL
-- is set once for the whole key regardless of how many elements it holds)
-- its expiry has to be set at the point of creation. That's 3 commands per
-- chunk (4 on the first, since the count key's own TTL is also set once
-- there) - pipelined into a single round trip rather than sent one at a
-- time, to keep write latency in line with the previous two-list design.
local function write_chunk(self, entity_keys, index, chunk, has_esi, ttl)
    local redis = self.redis

    local first_write = not self._keys_created
    local expected = first_write and 4 or 3

    redis:init_pipeline(expected)
    redis:set(chunk_key(entity_keys.body, index), chunk, "EX", ttl)
    redis:set(chunk_key(entity_keys.body_esi, index), tostring(has_esi), "EX", ttl)

    -- Track how many chunks exist, so reads/deletes/ttl updates know the
    -- range of per-chunk keys to address.
    redis:incr(entity_keys.count)

    -- The count key itself only needs its TTL set once, on creation.
    if first_write then
        redis:expire(entity_keys.count, ttl)
    end

    local res, e = redis:commit_pipeline()
    if not res or res == ngx_null or #res < expected then
        return nil, e or "pipelined chunk write failed"
    end

    self._keys_created = true

    return true, nil
end


-- Returns an iterator which writes chunks to cache as they are read from
-- reader belonging to the repsonse object.
-- If we cross the maxsize boundary, or error for any reason, we just
-- keep yielding chunks to be served, after having removed the cache entry.
--
-- @param   table       Module instance (self)
-- @param   table       Response object
-- @param   number      time-to-live
-- @param   function    onsuccess callback
-- @param   function    onfailure callback
-- @return  function    Iterator, returning chunk, err, has_esi for each call
function _M.get_writer(self, res, ttl, onsuccess, onfailure)
    local redis = self.redis
    local max_size = self.params.max_size
    local supports_transactions = self.params.supports_transactions

    local entity_id = res.entity_id
    local entity_keys = entity_keys(entity_id)

    local failed = false
    local failed_reason = ""
    local transaction_open = false

    local size = 0
    local chunk_index = 0
    local reader = res.body_reader

    return function(buffer_size)
        if not transaction_open and supports_transactions then
            redis:multi()
            transaction_open = true
        end

        local chunk, err, has_esi = reader(buffer_size)
        if not chunk and err then
            failed = true
            failed_reason = "upstream error: " .. err
        end

        if chunk and not failed then  -- We have something to write
            size = size + #chunk

            if max_size and size > max_size then
                failed = true
                failed_reason = "body is larger than " .. max_size .. " bytes"
            else
                local ok, e = write_chunk(self,
                    entity_keys,
                    chunk_index,
                    chunk,
                    has_esi,
                    ttl
                )
                if ok then
                    chunk_index = chunk_index + 1
                else
                    failed = true
                    failed_reason = "error writing: " .. tostring(e)
                end
            end

        elseif not chunk and not failed then  -- We're finished
            if supports_transactions then
                local ok, e = redis:exec() -- Commit

                if not ok or ok == ngx_null then
                    -- Transaction failed
                    ok, e = pcall(onfailure, e)
                    if not ok then ngx_log(ngx_ERR, e) end
                end
            end

            -- All good, report success
            local ok, e = pcall(onsuccess, size)
            if not ok then ngx_log(ngx_ERR, e) end

        elseif not chunk and failed then  -- We're finished, but failed
            if supports_transactions then
                redis:discard() -- Rollback
            else
                -- Attempt to clean up manually (connection could be dead)
                local keys = { entity_keys.count }
                for i = 0, chunk_index - 1 do
                    tbl_insert(keys, chunk_key(entity_keys.body, i))
                    tbl_insert(keys, chunk_key(entity_keys.body_esi, i))
                end

                local ok, e = redis:del(unpack(keys))
                if not ok or ok == ngx_null then ngx_log(ngx_ERR, e) end
            end

            local ok, e = pcall(onfailure, failed_reason)
            if not ok then ngx_log(ngx_ERR, e) end
        end

        -- Always bubble up
        return chunk, err, has_esi
    end
end


return _M
