local redis_connector = require "resty.redis.connector"

local tostring, pairs, next, unpack, setmetatable =
      tostring, pairs, next, unpack, setmetatable

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
-- The body is stored as a single Redis string, built up with APPEND as
-- chunks arrive and read back with GETRANGE, rather than as a list - so
-- the whole entity is one plain string value, eligible for tiering to
-- disk under backends (e.g. DragonflyDB) which only support this for
-- strings, not lists/hashes/etc.
--
-- has_esi records whether *any* chunk contained ESI markup when the body
-- was originally scanned, for the entity as a whole rather than per
-- chunk. Pages that don't use ESI (most) are unaffected; pages that do
-- simply run the whole body through the ESI filter on serve, rather than
-- skipping the parts of it already known to be ESI-free.
local function entity_keys(entity_id)
    if entity_id then
        return {
            body        = KEY_PREFIX .. "{" .. entity_id .. "}" .. ":body",
            body_esi    = KEY_PREFIX .. "{" .. entity_id .. "}" .. ":body_esi",
        }
    end
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
        local redis = self.redis

        redis:init_pipeline(2)
        redis:exists(keys.body)
        redis:exists(keys.body_esi)
        local res, err = redis:commit_pipeline()

        if not res and err then
            return nil, err
        elseif res == ngx_null or #res < 2 then
            return nil, "expected 2 pipelined command results"
        else
            return res[1] == 1 and res[2] == 1
        end
    end
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
        local keys = {}
        for _, v in pairs(key_chain) do
            tbl_insert(keys, v)
        end
        local res, err = self.redis:del(unpack(keys))
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
        local res, err
        for _,key in pairs(key_chain) do
            res, err = self.redis:expire(key, ttl)
        end
        if not res then
            return res, err
        elseif res == 0 then
            return false, "entity does not exist"
        else
            return true, nil
        end
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
        local res, err = self.redis:ttl(key_chain.body)
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


-- Returns an iterator for reading the body in buffer_size windows, via
-- GETRANGE over the single body string.
--
-- The caller is expected to have already confirmed the entity exists (e.g.
-- via exists()), but that check and this call are two separate round trips
-- to Redis - under eviction pressure the entity can still vanish in
-- between. So we re-check here too: if there's nothing to read despite the
-- caller believing there should be, we return nil, err rather than an
-- iterator which would silently yield zero chunks (i.e. an empty body).
--
-- Because the body is one key, loss is all-or-nothing (unlike a per-chunk
-- key scheme, where one chunk out of many could be evicted independently)
-- - STRLEN alone is enough to catch a vanished entity upfront, before any
-- of it is read.
--
-- @param   table       Module instance (self)
-- @param   table       Response object
-- @return  function    Iterator, returning chunk, err, has_esi for each call
-- @return  string      err (only set if a reader could not be returned)
function _M.get_reader(self, res)
    local redis = self.redis
    local entity_id = res.entity_id
    local entity_keys = entity_keys(entity_id)

    local body_len, err = redis:strlen(entity_keys.body)
    if not body_len or body_len == ngx_null then
        return nil, err
    elseif body_len == 0 then
        return nil, "entity has no body in storage"
    end

    local has_esi, err = redis:get(entity_keys.body_esi)
    if not has_esi or has_esi == ngx_null then
        return nil, err or "entity has no body_esi flag in storage"
    end
    has_esi = has_esi == "true"

    return function(buffer_size)
        local cursor = self._reader_cursor
        if cursor >= body_len then return nil end

        -- No buffer_size means "give me everything remaining" in one go
        -- (e.g. a caller not doing incremental/range-based streaming),
        -- rather than requiring every caller to know/care about chunking.
        --
        -- Likewise, if the entity has ESI markup anywhere, don't chunk on
        -- read at all: a tag could otherwise be split across an
        -- arbitrary buffer_size-sized boundary that has nothing to do
        -- with where the tag actually is in the body (unlike the
        -- original per-network-chunk scan, which buffers and reassembles
        -- tags split across upstream reads). The ESI process filter
        -- expects to find a complete tag within a single chunk it's
        -- handed; splitting one across two GETRANGE windows means
        -- neither half matches, and it's served unprocessed. Bodies are
        -- bounded by max_size regardless, and ESI-using pages are a
        -- minority, so reading the whole thing in one go here is an
        -- acceptable one-off memory cost for those pages specifically.
        local last
        if not buffer_size or has_esi then
            last = body_len - 1
        else
            last = cursor + buffer_size - 1
            if last > body_len - 1 then last = body_len - 1 end
        end

        local chunk, err = redis:getrange(entity_keys.body, cursor, last)
        if not chunk then return nil, err, nil end

        -- GETRANGE on a missing key returns an empty string (not ngx.null
        -- like GET), so that's our signal the entity vanished mid-read.
        -- Distinct from a clean EOF (nil, nil, nil) - the caller must not
        -- treat this as the body having ended successfully.
        if chunk == ngx_null or chunk == "" then
            ngx_log(ngx_WARN,
                "entity removed during read, ",
                entity_keys.body
            )
            return nil, "entity removed during read", nil
        end

        self._reader_cursor = cursor + #chunk

        return chunk, nil, has_esi
    end
end


-- Writes a given chunk onto the body string.
local function write_chunk(self, entity_keys, chunk, ttl)
    local redis = self.redis

    local ok, e = redis:append(entity_keys.body, chunk)
    if not ok then return nil, e end

    -- If this is the first write, set expiration too (a string's TTL, once
    -- set, applies regardless of how many further APPENDs extend it).
    if not self._keys_created then
        self._keys_created = true

        ok, e = redis:expire(entity_keys.body, ttl)
        if not ok then return nil, e end
    end

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
    -- Whether any chunk was found (at fetch time) to contain ESI markup,
    -- for the entity as a whole - see entity_keys() above.
    local entity_has_esi = false
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
            if has_esi then entity_has_esi = true end

            if max_size and size > max_size then
                failed = true
                failed_reason = "body is larger than " .. max_size .. " bytes"
            else
                local ok, e = write_chunk(self, entity_keys, chunk, ttl)
                if not ok then
                    failed = true
                    failed_reason = "error writing: " .. tostring(e)
                end
            end

        elseif not chunk and not failed then  -- We're finished
            -- Only record the (now fully known) has_esi flag if we
            -- actually wrote something - a zero-length body has no
            -- entity at all, matching the pre-write state.
            if self._keys_created then
                local ok, e = redis:set(
                    entity_keys.body_esi, tostring(entity_has_esi)
                )
                if not ok then ngx_log(ngx_ERR, e) end

                ok, e = redis:expire(entity_keys.body_esi, ttl)
                if not ok then ngx_log(ngx_ERR, e) end
            end

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
                local ok, e = redis:del(
                    entity_keys.body,
                    entity_keys.body_esi
                )
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
