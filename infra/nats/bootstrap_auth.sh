#!/usr/bin/env bash
set -euo pipefail

# infra/nats/bootstrap_auth.sh
# Phase 1, Step A3 — Auth bootstrap (Design.md §11 Track A)
#
# Idempotently, via nsc (containerized — natsio/nats-box, no native install
# per Design.md §11 parameters):
#   1. Creates the Operator representing this bus, if it doesn't exist.
#   2. Creates the GSB Account, if it doesn't exist.
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

OPERATOR_NAME="GregorsServiceBus"
ACCOUNT_NAME="GSB"

# v1 baseline permissions, granted to every adapter identity in addition to
# its manifest-declared subjects — needed for the JetStream client library
# and NATS request-reply plumbing to function. See header note above.
BASELINE_PUB='$JS.API.>'
BASELINE_SUB='$JS.API.>,_INBOX.>,$KV.service-directory.>'

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
  nsc "$@" --json 2>&1 | yqjson '(. // []) | .[].name' | grep -qx "$name"
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
    --allow-sub "$all_sub"

  nsc generate creds --account "$ACCOUNT_NAME" --name "$service_id" \
    >"$CREDS_DIR/$service_id.creds"
  echo "wrote $CREDS_DIR/$service_id.creds"
done < <(yq -o=json -I=0 '.adapters[]' <"$MANIFEST")

echo "== Memory resolver config =="
nsc generate config --mem-resolver --force \
  --config-file "$RESOLVER_CONF_NAME"
echo "wrote $SCRIPT_DIR/$RESOLVER_CONF_NAME"

echo "Done. Next: Step A4 wires $RESOLVER_CONF_NAME into nats-server.conf."
