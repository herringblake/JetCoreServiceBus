# NATS Toolchain — Phase 1, Step A1

Companion to [Design.md §11](../../Design.md#11-phase-1--detailed-breakdown) (Track A) and [Dependencies.md](../../Dependencies.md), which is the source of truth for these pins going forward.

**Nothing here is installed natively on the host.** This dev machine has Docker, so every NATS tool — the server itself, `nsc`, and the `nats` CLI — runs as a container. Anything that needs to persist (the `nsc`-managed Operator/Account/User store) is a volume mapped to a local path, not host-installed state.

## Pinned versions

| Tool | Image | Purpose |
|---|---|---|
| `nats-server` | `nats:2.14.5` (released 2026-08-12) | The message bus. Run via `docker-compose` (§11 Track A6). |
| `nsc` + `nats` CLI | `natsio/nats-box:0.19.7-nonroot` | Bundles `nsc` (JWT/account management, drives bootstrap step A3) and the `nats` CLI (pub/sub, stream/KV inspection, used in the smoke test A7) in one official NATS utilities image. `-nonroot` variant preferred — least-privilege container by default. |

Both are pinned to specific tags rather than `latest`, for the same reproducibility reason as everything else in [Dependencies.md](../../Dependencies.md). The `nsc`/`nats` CLI *tool* versions bundled inside a given `nats-box` release aren't independently pinned by us — if that precision matters later, `docker run natsio/nats-box:0.19.7-nonroot nsc --version` (and `nats --version`) confirms exactly what's inside. Re-verify all tags against their release pages before wiring `docker-compose.yml` (A6) or the bootstrap script (A3) — NATS ships frequently.

- `nats-server` releases: https://github.com/nats-io/nats-server/releases
- `nats-box` releases / tags: https://github.com/nats-io/nats-box/releases · https://hub.docker.com/r/natsio/nats-box/tags

## Running `nsc` / `nats` CLI via Docker

No local binary to install. General pattern (confirmed against `nsc`'s documented env vars and `nats-box`'s own README — see Sources below):

```bash
docker run --rm -it \
  -v "$(pwd)/infra/nats/operator/nsc:/nsc" \
  -e NSC_HOME=/nsc \
  -e NKEYS_PATH=/nsc/keys \
  natsio/nats-box:0.19.7-nonroot \
  nsc --version
```

- `NSC_HOME` (JWT/account store) and `NKEYS_PATH` (private keys) are `nsc`'s own documented env vars — pointing both under the one mounted volume persists everything to `infra/nats/operator/nsc/` on the host. That directory is git-ignored (see [.gitignore](../../.gitignore)) — nothing containing key material gets committed.
- Once A6 stands up the `docker-compose` network, run this container with `--network <compose network name>` so it can reach the `nats` service by its compose service name for the A7 smoke test, instead of a host port.

**Sources:** [`nsc` env vars](https://docs.nats.io/using-nats/nats-tools/nsc/basics) · [`nats-box` README](https://github.com/nats-io/nats-box/blob/main/README.md)

## Additional pinned tools (Step A3)

| Tool | Image/version | Purpose |
|---|---|---|
| `yq` (mikefarah) | `mikefarah/yq:4.52.4` (Docker only) | Parses `adapter_identities.yaml` to JSON, and does all JSON field extraction, inside [bootstrap_auth.sh](bootstrap_auth.sh). Originally paired with a host-installed `jq` for the JSON side — `jq` turned out not to actually be present on this host, so the script does everything through `yq` alone instead. |

## Step A3 — Auth bootstrap

[`bootstrap_auth.sh`](bootstrap_auth.sh) reads [adapter_identities.yaml](adapter_identities.yaml) (Step A2) and idempotently creates the Operator, the `JETCORE` Account, and one `nsc` User per manifest entry — applying that entry's subject permissions plus a derived KV-write grant and a v1 baseline (JetStream API + `_INBOX` + KV-read). It then generates a `.creds` file per adapter (JWT+nkey bundled — the standard NATS client connection artifact) and the memory-resolver config block consumed by `nats-server.conf` in Step A4.

**Executed and verified end-to-end** against real Docker + `nsc` — 3 consecutive runs confirmed the script is genuinely idempotent (no duplicate permission entries accumulate on repeat `edit user` calls). Three real bugs only surfaced by running it, not by reading docs, all now fixed in the script:

1. **`nsc list ... --json` writes its output to stderr, not stdout.** The first version of `nsc_has()` (the idempotency check) discarded stderr — so every entity looked "not found" on every run, and re-running the script failed with `operator ... exists already`.
2. **`nsc`'s table output carries ANSI color codes even without a TTY**, and they sit flush against the name with no word boundary — a plain `grep -w` on that output silently never matches. Fixed by switching every existence check to `--json` output instead of parsing tables.
3. **`jq` wasn't actually installed on the host**, despite being assumed as a "standard, ubiquitous" utility. Rather than ask for another native install, the script was rewritten to do all JSON extraction through the already-containerized `yq` instead.

**Updated during Phase 2 (Design.md §12 Step C4)** with a 6th real finding, this one much more consequential than the first three — a genuine, project-wide permission gap that had been silently present since Phase 1: every identity was missing publish permission for **JetStream ack replies**. `msg.ack()` (nats-py) publishes to the delivered message's own dynamically-generated reply-to subject (`$JS.ACK.<stream>.<consumer>...`) — the baseline permission set never granted that. Because `ack()` doesn't await a server response, the resulting "permissions violation" surfaced only via the connection's async error log, never as an exception on the `ack()` call itself, so nothing in Phase 1's tests (which never specifically checked "does a second fetch confirm no redelivery") ever noticed. Fixed with `nsc`'s purpose-built `--allow-pub-response` flag (dynamically permits exactly one reply per message actually received — more precise than a blanket `$JS.ACK.>` grant), applied to every identity including `jetcore-admin`. Re-applied idempotently against the live stack, same as every other permission change so far — no server restart needed.

Also added a new **`test-observer-01`** identity (read-only, subscribes to `events.files.FileCreateCompleted`/`FileWriteCompleted`/`FileWriteRequested`, publishes nothing) — no real adapter is allowed to watch its own published result events (same reasoning as the DB Adapter's write/CDC subject split), so integration tests that need to confirm what actually lands on the bus had no permitted identity to do it with until this one existed.

## Step A4 — `nats-server.conf`

[`nats-server.conf`](nats-server.conf) enables the client listener (`4222`), monitoring (`8222`), and a dev-sized `jetstream {}` block, then `include`s the generated `resolver.conf` for auth.

**Executed and verified** — ran the pinned `nats:2.14.5` image against this config directly (not just checked syntax): clean startup, JetStream initialized, no warnings. That test caught a 4th real bug, this one in **Step A3**: JetStream refuses to start under decentralized JWT auth without a **System Account** (`[FTL] Can't start JetStream: ... system account not setup`) — the `[WRN] Trusted Operators should utilize a System Account` line on the first attempt looked like a nicety, not a hard requirement. Fixed by adding `--sys` to `bootstrap_auth.sh`'s operator-creation call (now in the script); `nsc generate config --mem-resolver` then emits the needed `system_account:` directive into `resolver.conf` automatically — no changes needed to `nats-server.conf` itself.

Further confirmed with real traffic, using the actual `.creds` files A3 generates: published successfully as `webhook-listener-01` on its allowed subject, and confirmed `webhook-sender-01` is hard-rejected ("Permissions Violation for Publish") publishing outside its allow-list — the per-adapter subject restrictions from A2/A3 are genuinely enforced by the server, not just declared in the manifest.

## Step A5 — JetStream objects

[`bootstrap_jetstream.sh`](bootstrap_jetstream.sh) creates the `EVENTS` stream and `service-directory` KV bucket against a running server, using the `jetcore-admin` identity (added to `bootstrap_auth.sh` in this step — see below).

**Executed and verified**, including two things that only running it (not reading docs) turned up:

1. **JetStream is disabled per-account by default** under decentralized JWT auth — a *separate* gap from Step A4's system-account fix, and independent of the server's global `jetstream{}` block. Stream creation would otherwise fail against the `JETCORE` account as originally bootstrapped. Fixed by adding `nsc edit account --js-mem-storage 256M --js-disk-storage 2G` to `bootstrap_auth.sh`, kept in step with `nats-server.conf`'s own caps.
2. **Provisioning shared infrastructure isn't any adapter's job.** Added a dedicated `jetcore-admin` identity to `bootstrap_auth.sh` (`$JS.API.>` + `$KV.service-directory.>` + `_INBOX.>`) rather than reusing one adapter's `.creds` file for stream/bucket management.

Beyond "did it create the objects," the actual **TTL/heartbeat mechanism** the registry design depends on (Design.md §4.5) was verified directly, not assumed: wrote two keys to the real bucket, refreshed only one at t=30s, checked both at t=65s — the refreshed key was still there (revision 3, confirming the write-refreshes-the-clock behavior), the unrefreshed one had genuinely expired (`key not found`, confirming keys really do age out on schedule). Test keys and containers were cleaned up afterward.

One terminology note worth keeping in mind: `nats kv add --ttl` sets a bucket-wide max-age applied per key's own last-write time — not NATS's separate opt-in "Per-Key TTL" feature (`--marker-ttl`), which is for giving *different* keys *different* TTLs. Our design only ever needs one uniform TTL value, so the simpler mechanism is sufficient and is what's used.

**Updated after Step B6:** `bootstrap_jetstream.sh` now also creates a second bucket, `service-identity` — deliberately **no** `--ttl` this time (Design.md §4.6, Decision #20). It closes a real gap Step B6's integration testing found: a message's embedded `sourcePublicKey` alone can't prove who really sent it, only that the signature is self-consistent with *some* key. `bootstrap_auth.sh` was re-run (idempotently) to grant every identity access to the new bucket; confirmed this took effect on fresh connections with no `nats-server` restart needed — permission grants live in each User's own JWT, not the server's static config, so a freshly-regenerated `.creds` file is all a client needs.

## Step A6 — docker-compose

[`docker-compose.yml`](../../docker-compose.yml) (repo root) runs just the `nats` service for now — adapters get added in Phase 3. [`up.sh`](up.sh) is the orchestration wrapper: `bootstrap_auth.sh` → `docker compose up -d nats` → wait for the server to actually accept authenticated connections → `bootstrap_jetstream.sh`.

**Deviated from the original plan on purpose, flagged rather than silently changed:** Design.md originally called for a "one-shot init container running A3+A5 on startup." Both bootstrap scripts already shell out to `docker run` themselves — running them as Compose services too would mean mounting the host's `docker.sock` into a container just to spawn sibling containers (Docker-outside-of-Docker), real added fragility for scripts that already work well as host-level tooling. `up.sh` sequences them from the host instead.

Two things confirmed only by running this, not by reading Compose docs:

1. **The `nats:2.14.5` image has no shell at all** (`sh` isn't found) — a Compose-native `HEALTHCHECK` command can't run inside it. `up.sh` checks readiness externally instead, using the same nats-box tooling as the rest of Phase 1.
2. **`bootstrap_jetstream.sh`'s default `nats://nats:4222` only resolves inside the Compose network** — its containerized `nats` calls need to join that network explicitly. Added a `DOCKER_NETWORK` override (`up.sh` sets it to `jetcore_default`, pinned via `name: jetcore` in the compose file so the network name doesn't depend on the checkout directory's name).

**Executed and verified end-to-end, twice, from a fully clean state** — no operator, account, users, stream, bucket, or containers existing beforehand. First run: auth bootstrap → `nats` starts clean → readiness confirmed → `EVENTS` stream and `service-directory` bucket created. Second run: every step correctly reports "already exists, skipping add" while still confirming the server is up — genuine idempotency of the *entire* pipeline end to end, not just individual scripts tested in isolation. Left running afterward (this is the real deployment now, not a throwaway test container) — `docker compose down` to stop it, `docker compose down -v` to also drop the JetStream data volume.

## Step A7 — Manual smoke test

Ran the full sequence from Design.md §11 against the live A6 stack:

1. **Allowed publish** — `webhook-listener-01` → `events.files.FileWriteRequested`; confirmed it landed in the `EVENTS` stream (`messages: 0 → 1`).
2. **Durable pull consumer** — created one as `file-storage-01` (`--filter events.files.FileWriteRequested --pull --ack explicit`), the actual delivery mechanism adapters use (Design.md §6), not exercised by any earlier step. Pulled the message through it — payload intact, acknowledged.
3. **Denied publish** — `webhook-sender-01` still hard-rejected outside its allow-list (re-confirmed from A4).
4. **KV write scoping** — `file-storage-01` can write its own registration key, denied writing another adapter's.
5. **Confirmed (4) is real enforcement, not a typo** — the identical key write succeeds for `jetcore-admin` (which has broad KV access).

**New finding, worth remembering for `jetcore`'s `registry.py` (Track B, Step B5):** a KV write permission violation manifests as a **timeout** (`context deadline exceeded`), not an explicit rejection the way plain pub/sub denials are (`Permissions Violation for Publish`, immediate). A hang on a KV write is more likely a permissions problem than a network one — don't assume otherwise when that code gets built.

All test messages/consumers/keys were cleaned up afterward (stream purged back to 0 messages, test KV keys deleted); the running stack itself stayed up.

### Reproducing this by hand

Requires the stack up (`docker compose ps` shows `jetcore-nats-1` running — `bash up.sh` if not) and a shell with real `docker` group access (no `sg docker -c` needed there). Run from the repo root:

```bash
CREDS="infra/nats/operator/creds"
as() {
  local who="$1"; shift
  docker run --rm --network jetcore_default \
    -v "$(pwd)/$CREDS:/creds:ro" \
    natsio/nats-box:0.19.7-nonroot \
    nats --server nats://nats:4222 --creds "/creds/$who.creds" "$@"
}

# 1. Allowed publish
as webhook-listener-01 pub events.files.FileWriteRequested '{"path":"manual-test.txt"}'

# 2. Confirm the stream captured it
as jetcore-admin stream info EVENTS -j | grep '"messages"'

# 3. Durable pull consumer — the real delivery mechanism adapters use
as file-storage-01 consumer add EVENTS file-storage-01-consumer \
  --filter events.files.FileWriteRequested \
  --pull --ack explicit --deliver all --replay instant --defaults
as file-storage-01 consumer next EVENTS file-storage-01-consumer --ack

# 4. Denied publish (expect: Permissions Violation for Publish)
as webhook-sender-01 pub events.files.FileWriteRequested should-be-denied

# 5. KV write scoping: allowed on your own key, denied on someone else's
#    (the second one hangs a few seconds then times out — KV denials don't
#    fail fast the way pub/sub denials do)
as file-storage-01 kv put service-directory events.files.FileWriteRequested.file-storage-01 '{"encryptionPublicKey":"placeholder"}'
as file-storage-01 kv put service-directory events.files.FileWriteRequested.some-other-adapter should-be-denied

# 6. Confirm #5 was a real permission denial, not a typo — same key, admin creds
as jetcore-admin kv put service-directory events.files.FileWriteRequested.some-other-adapter should-succeed

# Cleanup
as jetcore-admin consumer rm EVENTS file-storage-01-consumer -f
as jetcore-admin stream purge EVENTS -f
as jetcore-admin kv del service-directory events.files.FileWriteRequested.file-storage-01 -f
as jetcore-admin kv del service-directory events.files.FileWriteRequested.some-other-adapter -f
```

## Track A: complete

Steps A1–A7 are done and empirically verified, not just written — versions pinned, auth bootstrapped, server configured, JetStream objects provisioned, everything wired into `docker-compose`, and the whole pipeline smoke-tested end to end including the actual durable-consumer delivery mechanism. See [Design.md §11](../../Design.md#11-phase-1--detailed-breakdown) for Track B (`jetcore` library — B1–B7), which can now build against this real infrastructure instead of a hypothetical one.


