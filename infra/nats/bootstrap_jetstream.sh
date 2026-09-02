#!/usr/bin/env bash
set -euo pipefail

# infra/nats/bootstrap_jetstream.sh
# Phase 1, Step A5 — JetStream objects (Design.md §11 Track A)
#
# Idempotently creates, using the jetcore-admin identity from Step A3 (a
# dedicated infra-provisioning user, not any particular adapter's creds):
#   - the EVENTS stream (events.>, file storage, limits retention, 7-day
#     max-age, 1G max-bytes backstop) — Design.md §6.
#   - the service-directory KV bucket (60s per-key TTL, uniform across all
#     entries — Design.md §4.5, §11 parameter table).
#   - the service-identity KV bucket (Design.md §9 item #4 / §11 Step B6
#     follow-up). Unlike service-directory (which tracks "is this adapter
#     currently alive to receive"), an identity's signing key doesn't need
#     to expire on a heartbeat; a decommissioned adapter's stale identity
#     entry is harmless (worst case: a claim to be that old serviceId
#     still needs that old service's real private key), whereas a
#     *missing* entry for a still-live service would wrongly reject
#     legitimate messages. Registered once at BusClient.connect(), not on
#     a repeating heartbeat — so unlike service-directory, this bucket
#     does NOT use a uniform bucket-wide --ttl (every key would expire on
#     the same fixed schedule regardless of when it was actually
#     registered, wrong for a "register once at connect time" bucket).
#     Design.md §16 Step N3 (§9 item #11) instead enables PER-KEY TTL
#     (--marker-ttl, distinct from --ttl — confirmed via `nats kv add
#     --help` and a real bucket's own reported config, not assumed: this
#     flag sets AllowMsgTTL=true plus how long a key's own delete-marker/
#     tombstone is retained after it expires, NOT a default TTL applied
#     to every key). The actual per-key expiry is supplied by the client
#     on each write instead (registry.py's register_identity(), a fixed
#     90-day value) — a long, set-once expiry so a genuinely decommissioned
#     identity ages out on its own, refreshed automatically on the
#     adapter's next real connect/register_identity() call, with no
#     ongoing heartbeat needed.
#
# Design.md §16 Step N4 (§9 item #12): both KV buckets keep --history 5
# (was 1) — cheap, no application code change, lets a suspected Decision
# #20 collision (whichever registration lands last silently overwrites
# the previous one) actually be investigated after the fact via `nats kv
# history`, instead of the prior value already being gone by the time
# anyone looks.
#
# Requires a running nats-server reachable at $NATS_URL, already bootstrapped
# per Steps A3/A4 — in particular, the JETCORE account needs JetStream storage
# granted (bootstrap_auth.sh does this; it's disabled per-account by default
# under decentralized JWT auth, independent of the server's global
# jetstream{} block — see that script's comments for how this was found).
#
# Everything runs containerized (natsio/nats-box), no native install.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CREDS_DIR="$SCRIPT_DIR/operator/creds"
CREDS_FILE="$CREDS_DIR/jetcore-admin.creds"
NATS_BOX_IMAGE="natsio/nats-box:0.19.7-nonroot"

# Overridable for local testing; defaults to the service name Step A6's
# docker-compose uses. For that hostname to resolve, this script's
# containerized `nats` calls need to join the compose network — set
# DOCKER_NETWORK (up.sh does this automatically; e.g. `jetcore_default`).
NATS_URL="${NATS_URL:-nats://nats:4222}"
DOCKER_NETWORK="${DOCKER_NETWORK:-}"

STREAM_NAME="EVENTS"
STREAM_SUBJECTS="events.>"
STREAM_MAX_AGE="7d"      # Design.md §6, Decision #9
STREAM_MAX_BYTES="1G"    # dev-sized backstop; comfortably under the JETCORE
                          # account's 2G js-disk-storage cap (bootstrap_auth.sh)
                          # since the KV bucket below shares that same budget

KV_HISTORY=5               # Design.md §16 Step N4 (§9 item #12), both buckets

KV_BUCKET="service-directory"
KV_TTL="60s"              # Design.md §11 parameter table

IDENTITY_KV_BUCKET="service-identity"
# Tombstone-retention window for an expired per-key TTL entry — but ALSO,
# confirmed empirically (not just from docs, which only describe the
# tombstone-retention half): a floor on any individual key's own
# requested TTL. A `js.publish(..., msg_ttl=X)` with X below this value
# gets silently raised to match it, not rejected or left as X (probed
# directly: requesting 3s/10s/100s against a real 1h marker-ttl bucket
# all produced a real `Nats-TTL: 3600` header on the written message;
# requesting registry.py's real 90-day value passed through unclamped,
# confirming this is a floor, not a ceiling). Harmless here since
# IDENTITY_TTL_SECONDS (registry.py) is 90 days, far above this — but
# worth knowing before ever using a shorter per-key TTL against this
# same bucket.
IDENTITY_KV_MARKER_TTL="1h"

if [ ! -f "$CREDS_FILE" ]; then
  echo "Missing $CREDS_FILE — run bootstrap_auth.sh (Step A3) first." >&2
  exit 1
fi

nats() {
  local net_args=()
  [ -n "$DOCKER_NETWORK" ] && net_args=(--network "$DOCKER_NETWORK")
  docker run --rm "${net_args[@]}" \
    -v "$CREDS_DIR:/creds:ro" \
    "$NATS_BOX_IMAGE" \
    nats --server "$NATS_URL" --creds /creds/jetcore-admin.creds "$@"
}

echo "== Stream: $STREAM_NAME =="
if nats stream ls -n 2>/dev/null | grep -qx "$STREAM_NAME"; then
  echo "already exists, skipping add"
else
  nats stream add "$STREAM_NAME" \
    --subjects "$STREAM_SUBJECTS" \
    --storage file \
    --retention limits \
    --discard old \
    --max-age "$STREAM_MAX_AGE" \
    --max-bytes "$STREAM_MAX_BYTES" \
    --replicas 1 \
    --defaults
fi

echo "== KV bucket: $KV_BUCKET =="
if nats kv ls -n 2>/dev/null | grep -qx "$KV_BUCKET"; then
  echo "already exists, skipping add"
else
  nats kv add "$KV_BUCKET" \
    --storage file \
    --ttl "$KV_TTL" \
    --replicas 1 \
    --history "$KV_HISTORY"
fi

echo "== KV bucket: $IDENTITY_KV_BUCKET =="
if nats kv ls -n 2>/dev/null | grep -qx "$IDENTITY_KV_BUCKET"; then
  echo "already exists, skipping add"
else
  nats kv add "$IDENTITY_KV_BUCKET" \
    --storage file \
    --replicas 1 \
    --history "$KV_HISTORY" \
    --marker-ttl "$IDENTITY_KV_MARKER_TTL"
fi

echo "Done. Next: Step A6 wires everything into docker-compose."
