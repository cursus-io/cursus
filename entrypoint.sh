#!/bin/bash
set -eu

if [ "${1:-}" = "./broker" ] || [ "${1:-}" = "broker" ]; then
  shift
fi

exec /app/broker "$@"
