# Local development/test image: OpenResty + this project's Lua dependencies
# + the Perl test harness (Test::Nginx::Socket) used by t/*.t.
#
# Source is bind-mounted at runtime (see docker/docker-compose.yml), so this
# image only needs to provide the toolchain, not a copy of the code.

FROM openresty/openresty:alpine-fat

RUN apk add --no-cache \
        lua5.1 \
        git \
        make \
        redis \
        perl-dev \
        perl-app-cpanminus \
        zlib-dev

# Test::Nginx::Socket drives the t/*.t suite over a real nginx/openresty
# instance.
RUN cpanm --notest Test::Nginx && rm -rf /root/.cpanm

# Lua module dependencies - versions pinned to what this codebase is
# actually tested against (dist.ini's `requires` line only states minimum
# floors; newer majors of lua-resty-http and lua-resty-cookie in particular
# are NOT drop-in compatible with the current lib/ code, e.g. lua-resty-http
# >= 0.17 rejects a POST with a nil body where this codebase relies on the
# older permissive behaviour). Bump deliberately, not by omission.
RUN luarocks install lua-resty-http 0.15 && \
    luarocks install lua-resty-redis-connector 0.10 && \
    luarocks install lua-resty-cookie 0.1.0 && \
    luarocks install lua-ffi-zlib 0.5

# Dev/test-only tooling used by `make check` (luacheck) and `make coverage`
# (luacov).
RUN luarocks install luacheck && \
    luarocks install luacov

WORKDIR /code

CMD ["make", "test"]
