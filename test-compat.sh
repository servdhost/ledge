#!/usr/bin/env bash
#
# Runs Ledge's test suite against both Redis and DragonflyDB, to catch
# compatibility regressions between the two backends. Requires Docker.
#
# Usage:
#   ./test-compat.sh
#   TEST_FILE="t/02-integration/gc.t" ./test-compat.sh   # targeted run

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required to run this script" >&2
    exit 1
fi

BASE="docker/docker-compose.yml"
DRAGONFLY="docker/docker-compose.dragonfly.yml"

# Runs the suite against one backend. Args after $1 (a label) are the
# `docker compose` invocation to use (e.g. `docker compose -f "$BASE"`).
run_suite() {
    local name=$1
    shift

    echo
    echo "=============================================="
    echo " Running tests against ${name}"
    echo "=============================================="
    echo

    "$@" down -v >/dev/null 2>&1

    "$@" run --rm test
    local status=$?

    "$@" down -v >/dev/null 2>&1

    return $status
}

redis_status=0
dragonfly_status=0

run_suite "Redis" docker compose -f "$BASE" || redis_status=$?
run_suite "DragonflyDB" docker compose -f "$BASE" -f "$DRAGONFLY" || dragonfly_status=$?

echo
echo "=============================================="
echo " Summary"
echo "=============================================="
[ "$redis_status" -eq 0 ] && echo "  Redis:       PASS" || echo "  Redis:       FAIL"
[ "$dragonfly_status" -eq 0 ] && echo "  DragonflyDB: PASS" || echo "  DragonflyDB: FAIL"
echo

if [ "$redis_status" -ne 0 ] || [ "$dragonfly_status" -ne 0 ]; then
    exit 1
fi
