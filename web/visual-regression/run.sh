#!/usr/bin/env bash
# Run the tau web console visual regression check end to end:
#   1. build the static export (unless SKIP_BUILD=1)
#   2. start a two-node demo (alice serves the console on :8787, bob
#      is the peer) and seed the two-message state
#   3. screenshot mobile (390px) + desktop (1280px) and assert the
#      layout invariants in screenshot.mjs
#
# Usage:
#   web/visual-regression/run.sh [out-dir] [--update-baselines]
#
# out-dir defaults to web/visual-regression/out. Pass
# --update-baselines to refresh the committed baselines with the
# freshly captured (good) screenshots — only after eyeballing them.
#
# Requires: node, a headless Chromium (CHROME_BIN or on PATH; the
# Hatch CVM build at /opt/meta-chromium/chrome is auto-detected),
# and the repo venv (created by `make venv` / `make web-build`).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$ROOT/web/visual-regression/out"
if [ $# -gt 0 ] && [ "${1#--}" = "$1" ]; then
  OUT="$1"
  shift
fi

cd "$ROOT"
if [ "${SKIP_BUILD:-0}" != "1" ]; then
  make web-build
fi

# Serve the export this checkout just built. Without this, `tau serve`
# resolves web/out through the editable install (whatever worktree the
# venv points at) and the check screenshots a stale build.
export TAU_WEB_DIR="$ROOT/web/out"

TMP="$(mktemp -d /tmp/tau-visual-regression.XXXXXX)"
ALICE_PID=""
BOB_PID=""
cleanup() {
  [ -n "$ALICE_PID" ] && kill "$ALICE_PID" 2>/dev/null || true
  [ -n "$BOB_PID" ] && kill "$BOB_PID" 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

# Fresh node identities each run; explicit TCP loopback so the pair finds
# each other deterministically (alice listens on :4343, bob connects).
"$ROOT/.venv/bin/tau" init --data-dir "$TMP/alice" --name alice --tcp-listen 127.0.0.1:4343 --no-auto-interface --yes >/dev/null 2>&1 || true
"$ROOT/.venv/bin/tau" init --data-dir "$TMP/bob" --name bob --tcp-connect 127.0.0.1:4343 --no-auto-interface --yes >/dev/null 2>&1 || true
"$ROOT/.venv/bin/tau" serve --data-dir "$TMP/alice" --http --announce-interval 5 >"$TMP/alice.log" 2>&1 &
ALICE_PID=$!
"$ROOT/.venv/bin/tau" serve --data-dir "$TMP/bob" --announce-interval 5 >"$TMP/bob.log" 2>&1 &
BOB_PID=$!

"$ROOT/.venv/bin/python" "$ROOT/web/visual-regression/seed.py" "$TMP"
node "$ROOT/web/visual-regression/screenshot.mjs" "http://127.0.0.1:8787/" "$OUT" "$@"
