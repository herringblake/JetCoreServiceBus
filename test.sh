#!/usr/bin/env bash
# Runs the test suite the way Defects.md Defect 3 requires: with the six
# adapter APPLICATION containers stopped (nats/mysql stay up — pytest
# needs real infra, just not a co-running production fleet). Without
# this, a test's own trigger/result subject races against a real,
# permanently-deployed adapter also listening on the exact same
# "placeholder" subject (Design.md Decision #14) — Defect 3's own root
# cause, not a hypothetical.
#
# Restarts the adapters afterward regardless of whether pytest passed
# (via `trap ... EXIT`), so the stack is left in its normal running state
# for manual poking or a Step K3-style smoke test — this is a workflow
# fix, not a "leave the stack half down" one. Any arguments given to this
# script are forwarded to pytest as-is (e.g. `./test.sh -k some_test`).
#
# CI (Design.md §10 Phase 4, not yet built) should go a step further:
# never bring the adapter containers up at all before running pytest,
# rather than bringing them up and stopping them again — see Defects.md
# Defect 1's own "Complementary practice" note.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NATS_BOX_IMAGE="natsio/nats-box:0.19.7-nonroot"
COMPOSE_NETWORK="jetcore_default"   # matches docker-compose.yml's `name: jetcore`
CREDS_DIR="$SCRIPT_DIR/infra/nats/operator/creds"

ADAPTER_SERVICES=(
  file-storage-adapter
  webhook-listener
  webhook-sender
  http-adapter
  rest-api-service
  db-adapter-mysql
)

echo "==> Stopping adapter containers (nats/mysql stay up)..."
docker compose stop "${ADAPTER_SERVICES[@]}"

restart_adapters() {
  echo "==> Restarting adapter containers..."
  docker compose up -d "${ADAPTER_SERVICES[@]}"
}
trap restart_adapters EXIT

# A stopped container's own service-directory registration doesn't clear
# itself — it only expires on its own 60s TTL (Design.md §11 parameter
# table), confirmed by testing: without this, a test asserting "nobody is
# registered for this subject yet" (libs/jetcore/tests/test_bus_client.py
# ::test_publish_with_no_recipients_does_not_crash) can still see a real
# adapter's now-stale-but-not-yet-expired entry if it runs early in the
# suite. Clearing it explicitly, right after stopping, is faster and
# deterministic rather than waiting out the TTL on every run.
echo "==> Clearing service-directory (stopped adapters' entries would otherwise linger up to 60s)..."
for key in $(docker run --rm --network "$COMPOSE_NETWORK" \
  -v "$CREDS_DIR:/creds:ro" \
  "$NATS_BOX_IMAGE" \
  nats --server nats://nats:4222 --creds /creds/jetcore-admin.creds \
  kv ls service-directory 2>/dev/null); do
  docker run --rm --network "$COMPOSE_NETWORK" \
    -v "$CREDS_DIR:/creds:ro" \
    "$NATS_BOX_IMAGE" \
    nats --server nats://nats:4222 --creds /creds/jetcore-admin.creds \
    kv del service-directory "$key" -f >/dev/null 2>&1 || true
done

echo "==> Running test suite..."
uv run --all-packages pytest "$@"
