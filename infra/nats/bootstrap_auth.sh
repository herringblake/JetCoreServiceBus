#!/usr/bin/env bash
set -euo pipefail

# infra/nats/bootstrap_auth.sh
# Phase 1, Step A3 — Auth bootstrap (Design.md §11 Track A)
#
# Idempotently, via nsc (containerized — natsio/nats-box, no native install
# per Design.md §11 parameters):
#   1. Creates the Operator representing this bus, if it doesn't exist.
#   2. Creates the JETCORE Account, if it doesn't exist, and grants it JetStream
#      storage (disabled per-account by default under decentralized JWT
#      auth — independent of nats-server.conf's global jetstream{} block).
#   3. For each entry in adapter_identities.yaml (Step A2): creates the User
#      (nkey + JWT) if it doesn't exist, then (re)applies its permissions —
#      the manifest's publish/subscribe subjects, plus KV-write on its own
#      recipient-registration keys (derived per the manifest header), plus a
#      v1 baseline ($JS.API.>, _INBOX.>, KV-read-all) every JetStream client
#      needs. That baseline isn't scoped down further in v1 — a candidate
#      for tightening later, not a Phase 1 requirement.
#   4. Generates a .creds file per user (JWT+nkey bundled — the idiomatic
#      NATS client connection artifact; see Design.md §7.6, Decision #17-adjacent
#      update) under operator/creds/.
#   4b. Creates a dedicated jetcore-admin identity (not an adapter) for
#       provisioning shared JetStream infrastructure — used by Step A5.
#   5. Generates the memory-resolver server config block (resolver.conf) —
#      consumed by nats-server.conf in Step A4.
#
# ============================================================================
# STATUS: executed and verified end-to-end against real Docker + nsc — 3
# consecutive runs confirmed idempotency (no duplicate permission entries
# accumulate on repeat `edit user` calls). See infra/nats/README.md's "Step
# A3" section for the 3 real bugs that surfaced only by running this, not by
# reading docs (stderr-vs-stdout on `--json`, ANSI codes breaking `grep -w`
# on table output, `jq` assumed-but-absent on the host) — all fixed below.
# ============================================================================
#
# Requires on the host: docker, bash. Everything else (nsc, nats CLI, YAML/
# JSON parsing) runs containerized — an earlier draft assumed `jq` would be
# present on the host; it wasn't, so this uses `yq` alone for both YAML and
# JSON querying instead of adding another native dependency.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$SCRIPT_DIR/adapter_identities.yaml"
STORE_DIR="$SCRIPT_DIR/operator/nsc"        # git-ignored — nsc's JWT+nkey store
CREDS_DIR="$SCRIPT_DIR/operator/creds"      # git-ignored — generated .creds files
RESOLVER_CONF_NAME="resolver.conf"          # generated; consumed by Step A4

NATS_BOX_IMAGE="natsio/nats-box:0.19.7-nonroot"
YQ_IMAGE="mikefarah/yq:4.52.4"

OPERATOR_NAME="JetCoreServiceBus"
ACCOUNT_NAME="JETCORE"
ADMIN_ID="jetcore-admin"   # infra-provisioning identity (Step A5), not an adapter

# JetStream storage the JETCORE account is allowed to use — kept in step with
# nats-server.conf's own jetstream{} caps (256M mem / 2G file), since a
# single-account dev setup has no reason for the account limit to be lower
# than the server-wide limit.
JS_MEM_STORAGE="256M"
JS_DISK_STORAGE="2G"

# v1 baseline permissions, granted to every adapter identity in addition to
# its manifest-declared subjects — needed for the JetStream client library
# and NATS request-reply plumbing to function. See header note above.
BASELINE_PUB='$JS.API.>'
# $KV.service-identity.> read: every adapter needs to look up *any* other
# adapter's registered signing key to verify a received message's sender
# (Design.md §9 item #4 resolution) — read access is necessarily broad,
# same as service-directory's; only the per-adapter *write* scope (own
# namespace only, derived above) is what actually enforces least privilege.
BASELINE_SUB='$JS.API.>,_INBOX.>,$KV.service-directory.>,$KV.service-identity.>'
# --allow-pub-response (applied below, not a subject in the lists above):
# JetStream's msg.ack() publishes to the delivered message's own reply-to
# subject (nats-py's Msg.ack(), confirmed by reading its source), which is
# a per-message subject the server generates dynamically
# ($JS.ACK.<stream>.<consumer>...) — a static $JS.ACK.> wildcard grant
# would work but is broader than needed. nsc's --allow-pub-response instead
# dynamically permits exactly one reply per message actually received, the
# purpose-built mechanism for this. **Found missing entirely** while
# building Design.md §12 Step C4: every ack() call in every adapter
# (including Phase 1's own B6/B7 tests) was silently failing server-side
# — nats-py's ack() doesn't await a response, so the "permissions
# violation" surfaced only via the connection's async error callback/log,
# never as a raised exception on the ack() call itself. Nothing in Phase 1
# ever tested "does a second fetch confirm the message was actually
# consumed" specifically enough to catch it. Fixed here, retroactively
# covering every existing identity too, not just the ones Step C4 added.

mkdir -p "$STORE_DIR" "$CREDS_DIR"

# Run nsc in nats-box, with the store persisted to $STORE_DIR and this
# directory writable at /workspace for file-producing commands (generate
# config, generate creds). -u matches the container's file ownership to the
# host user, avoiding the UID-1000 permission mismatch the nats-box README
# otherwise works around with a manual `chown`.
nsc() {
  docker run --rm \
    -v "$STORE_DIR:/nsc" \
    -v "$SCRIPT_DIR:/workspace" \
    -e NSC_HOME=/nsc \
    -e NKEYS_PATH=/nsc/keys \
    -u "$(id -u):$(id -g)" \
    -w /workspace \
    "$NATS_BOX_IMAGE" nsc "$@"
}

yq() {
  docker run --rm -i "$YQ_IMAGE" "$@"
}

# Query a single field out of a JSON blob passed on stdin, via yq (-p=json).
yqjson() {
  local expr="$1"
  yq -p=json "$expr" -
}

# True if `nsc <list-subcommand...> --json` includes an entry whose .name
# equals $1. Two nsc quirks made this fiddlier than expected, both confirmed
# by actually running it (not guessed):
#   - Table output carries ANSI color codes even without a TTY, sitting
#     flush against the name with no word boundary, so a plain `grep -w` on
#     table output silently fails to match.
#   - --json output goes to STDERR, not stdout (confirmed for `list
#     operators`) — so this deliberately merges 2>&1 rather than discarding
#     stderr, which is what broke idempotency on the first version of this
#     script (every entity looked "not found" on every run).
# (. // []) guards against nsc printing bare `null` for an empty list
# (confirmed behavior of `nsc list users` on a freshly created account).
nsc_has() {
  local name="$1"; shift
  # Materialize the full name list into a variable before matching, rather
  # than a live 3-stage pipe (nsc | yq | grep -qx) straight through — that
  # form is racy: `grep -qx` exits the instant it finds a match, closing
  # its stdin early, which can SIGPIPE nsc/yq mid-write ("write /dev/stdout:
  # broken pipe") once the identity list is big enough to not fit a single
  # pipe buffer. Under `pipefail`'s "rightmost nonzero" rule that SIGPIPE
  # can make the *pipeline* report failure even though the name was
  # genuinely present — Defects.md Defect 4, found via real, reproducible
  # failures once the manifest grew past ~15 identities (fine at the
  # smaller counts this was originally written and tested against). A
  # command substitution here runs nsc+yq to completion before grep ever
  # starts, so there's no live downstream consumer left to close early.
  local names
  names="$(nsc "$@" --json 2>&1 | yqjson '(. // []) | .[].name')"
  grep -qx "$name" <<<"$names"
}

echo "== Operator: $OPERATOR_NAME =="
if nsc_has "$OPERATOR_NAME" list operators; then
  echo "already exists, skipping add"
else
  # --sys: generates a System Account alongside the operator. Confirmed via
  # a real run (not just the docs) that this isn't optional — nats-server
  # refuses to start JetStream under decentralized JWT auth without one
  # ("[FTL] Can't start JetStream: ... system account not setup").
  nsc add operator --name "$OPERATOR_NAME" --sys
fi

echo "== Account: $ACCOUNT_NAME =="
if nsc_has "$ACCOUNT_NAME" list accounts; then
  echo "already exists, skipping add"
else
  nsc add account --name "$ACCOUNT_NAME"
fi

# JetStream is disabled per-account by default under decentralized JWT auth
# — entirely independent of nats-server.conf's global jetstream{} block.
# Confirmed via a real run: `nsc describe account --json` showed no
# .nats.limits.jetstream at all until this was added, and stream creation
# would fail against a freshly-added account without it. Re-run (idempotent
# edit) every time so a manifest/limit change takes effect on rerun.
nsc edit account --name "$ACCOUNT_NAME" \
  --js-mem-storage "$JS_MEM_STORAGE" \
  --js-disk-storage "$JS_DISK_STORAGE"

echo "== Users (from $MANIFEST) =="

# One compact JSON line per manifest entry, read via process substitution so
# the loop body's `nsc`/variable assignments run in this shell, not a
# subshell (a plain `| while read` would lose them after the loop).
while IFS= read -r adapter; do
  service_id="$(echo "$adapter" | yqjson '.serviceId')"
  adapter_type="$(echo "$adapter" | yqjson '.adapterType')"
  pub_subjects="$(echo "$adapter" | yqjson '(.permissions.publish // []) | join(",")')"
  sub_subjects="$(echo "$adapter" | yqjson '(.permissions.subscribe // []) | join(",")')"

  # Derived: KV write on this identity's own recipient-registration keys,
  # one per subscribed subject (Design.md §4.5 / adapter_identities.yaml
  # header comment). Built in bash, not a nested yq expression — keeps each
  # containerized yq call trivially simple to verify individually.
  kv_write=""
  if [ -n "$sub_subjects" ]; then
    IFS=',' read -ra _subs <<<"$sub_subjects"
    for s in "${_subs[@]}"; do
      kv_write="${kv_write:+$kv_write,}\$KV.service-directory.$s.$service_id"
    done
  fi
  # Every adapter — publisher or subscriber — registers its own signing
  # identity once at connect time (Design.md §9 item #4 resolution / §11
  # Step B6 follow-up), unlike service-directory registration, which only
  # happens for subjects actually subscribed to.
  kv_write="${kv_write:+$kv_write,}\$KV.service-identity.$service_id"

  all_pub="$(printf '%s,%s,%s' "$pub_subjects" "$BASELINE_PUB" "$kv_write" | sed 's/,,*/,/g; s/^,//; s/,$//')"
  all_sub="$(printf '%s,%s' "$sub_subjects" "$BASELINE_SUB" | sed 's/,,*/,/g; s/^,//; s/,$//')"

  echo "-- $service_id ($adapter_type) --"
  if nsc_has "$service_id" list users --account "$ACCOUNT_NAME"; then
    echo "already exists, skipping add"
  else
    nsc add user --account "$ACCOUNT_NAME" --name "$service_id"
  fi

  nsc edit user --account "$ACCOUNT_NAME" --name "$service_id" \
    --allow-pub "$all_pub" \
    --allow-sub "$all_sub" \
    --allow-pub-response

  nsc generate creds --account "$ACCOUNT_NAME" --name "$service_id" \
    >"$CREDS_DIR/$service_id.creds"
  echo "wrote $CREDS_DIR/$service_id.creds"
done < <(yq -o=json -I=0 '.adapters[]' <"$MANIFEST")

echo "== Admin identity: $ADMIN_ID =="
# Creating/managing the EVENTS stream and service-directory KV bucket
# (Step A5) isn't any particular adapter's job — a dedicated identity keeps
# "who can provision shared infrastructure" separate from "who can act as
# adapter X", rather than borrowing an arbitrary adapter's creds for it.
if nsc_has "$ADMIN_ID" list users --account "$ACCOUNT_NAME"; then
  echo "already exists, skipping add"
else
  nsc add user --account "$ACCOUNT_NAME" --name "$ADMIN_ID"
fi

nsc edit user --account "$ACCOUNT_NAME" --name "$ADMIN_ID" \
  --allow-pubsub '$JS.API.>,$KV.service-directory.>,$KV.service-identity.>,_INBOX.>' \
  --allow-pub-response

nsc generate creds --account "$ACCOUNT_NAME" --name "$ADMIN_ID" \
  >"$CREDS_DIR/$ADMIN_ID.creds"
echo "wrote $CREDS_DIR/$ADMIN_ID.creds"

echo "== Memory resolver config =="
nsc generate config --mem-resolver --force \
  --config-file "$RESOLVER_CONF_NAME"
echo "wrote $SCRIPT_DIR/$RESOLVER_CONF_NAME"

echo "Done. Next: Step A4 wires $RESOLVER_CONF_NAME into nats-server.conf."
