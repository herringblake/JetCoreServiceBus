#!/usr/bin/env bash
set -euo pipefail

# infra/nats/up.sh
# Phase 1, Step A6 (Design.md §11 Track A)
#
# Orchestrates bringing the infra-only bus up in the order Compose alone
# can't express (see the comment at the top of docker-compose.yml for why
# this isn't wired in as Compose services with depends_on):
#   1. bootstrap_auth.sh (A3)       — must run BEFORE `nats` starts, since
#      nats-server.conf `include`s the resolver.conf this generates.
#   2. docker compose up -d nats
#   3. wait for the server to actually accept authenticated connections —
#      the nats:2.14.5 image has no shell at all (confirmed: `sh` isn't
#      found), so a Compose-native HEALTHCHECK isn't viable; checked
#      externally instead, with the same nats-box tooling used throughout
#      Phase 1 testing.
#   4. bootstrap_jetstream.sh (A5)  — needs a live, authenticated server.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
NATS_BOX_IMAGE="natsio/nats-box:0.19.7-nonroot"
COMPOSE_NETWORK="gsb_default"   # matches docker-compose.yml's `name: gsb`
CREDS_DIR="$SCRIPT_DIR/operator/creds"

echo "== Step A3: auth bootstrap =="
bash "$SCRIPT_DIR/bootstrap_auth.sh"

echo "== Starting nats service =="
(cd "$REPO_ROOT" && docker compose up -d nats)

echo "== Waiting for nats to accept authenticated connections =="
ready=0
for _ in $(seq 1 30); do
  if docker run --rm --network "$COMPOSE_NETWORK" \
    -v "$CREDS_DIR:/creds:ro" \
    "$NATS_BOX_IMAGE" \
    nats --server nats://nats:4222 --creds /creds/gsb-admin.creds \
    server check connection >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "nats did not become ready within 30s — check: docker compose logs nats" >&2
  exit 1
fi
echo "nats is ready"

echo "== Step A5: JetStream objects =="
NATS_URL="nats://nats:4222" DOCKER_NETWORK="$COMPOSE_NETWORK" bash "$SCRIPT_DIR/bootstrap_jetstream.sh"

echo "Done — infra is up. Try: docker compose logs -f nats"
