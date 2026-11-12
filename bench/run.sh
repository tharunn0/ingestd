#!/usr/bin/env bash
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

BENCH_PATTERN="${1:-.}"
shift || true

exec ./bench.test -test.bench="$BENCH_PATTERN" -test.benchmem -test.run='^$' "$@"
