#!/usr/bin/env bash
# Starts a pail process and runs the AWS CLI and boto3 smoke scripts against it.
# Inputs: PAIL_BIN (default ./pail), SMOKE_ADDR (default 127.0.0.1:9000),
# PYTHON (default python3; it must import boto3).
set -euo pipefail

cd "$(dirname "$0")/.."

PAIL_BIN=${PAIL_BIN:-./pail}
SMOKE_ADDR=${SMOKE_ADDR:-127.0.0.1:9000}
PYTHON=${PYTHON:-python3}

work=$(mktemp -d)
pail_pid=
status=1

# shellcheck disable=SC2329
cleanup() {
  [ -n "$pail_pid" ] && kill "$pail_pid" 2>/dev/null || true
  [ -n "$pail_pid" ] && wait "$pail_pid" 2>/dev/null || true
  if [ "$status" -ne 0 ] && [ -s "$work/pail.log" ]; then
    echo "--- pail log (tail) ---" >&2
    tail -n 40 "$work/pail.log" >&2
  fi
  rm -rf "$work"
}
trap cleanup EXIT

# Keep smoke traffic on loopback.
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy

export SMOKE_ENDPOINT=http://$SMOKE_ADDR
export SMOKE_ACCESS_KEY=AKIAPAILSMOKETEST000
export SMOKE_SECRET_KEY=pail-smoke-secret

PAIL_SECRET_ACCESS_KEY=$SMOKE_SECRET_KEY "$PAIL_BIN" \
  --addr "$SMOKE_ADDR" --data "$work/data" --access-key "$SMOKE_ACCESS_KEY" \
  >"$work/pail.log" 2>&1 &
pail_pid=$!

ready=
for _ in $(seq 60); do
  if curl -fsS -o /dev/null "$SMOKE_ENDPOINT/_pail/health" 2>/dev/null; then
    ready=1
    break
  fi
  kill -0 "$pail_pid" 2>/dev/null || break
  sleep 0.5
done
[ -n "$ready" ] || { echo "FAIL: pail did not become healthy at $SMOKE_ENDPOINT" >&2; exit 1; }

# Run each client even when the first fails, so one log shows both.
failed=0
for client in awscli boto3; do
  echo "=== $client ==="
  if [ "$client" = awscli ]; then
    scripts/smoke-awscli.sh || rc=$?
  else
    $PYTHON scripts/smoke-boto3.py || rc=$?
  fi
  if [ "${rc:-0}" -eq 0 ]; then
    echo "PASS: $client"
  else
    echo "FAIL: $client"
    failed=1
  fi
  rc=0
done

status=$failed
exit "$failed"
