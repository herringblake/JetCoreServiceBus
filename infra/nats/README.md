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

[`bootstrap_auth.sh`](bootstrap_auth.sh) reads [adapter_identities.yaml](adapter_identities.yaml) (Step A2) and idempotently creates the Operator, the `GSB` Account, and one `nsc` User per manifest entry — applying that entry's subject permissions plus a derived KV-write grant and a v1 baseline (JetStream API + `_INBOX` + KV-read). It then generates a `.creds` file per adapter (JWT+nkey bundled — the standard NATS client connection artifact) and the memory-resolver config block consumed by `nats-server.conf` in Step A4.

**Executed and verified end-to-end** against real Docker + `nsc` — 3 consecutive runs confirmed the script is genuinely idempotent (no duplicate permission entries accumulate on repeat `edit user` calls). Three real bugs only surfaced by running it, not by reading docs, all now fixed in the script:

1. **`nsc list ... --json` writes its output to stderr, not stdout.** The first version of `nsc_has()` (the idempotency check) discarded stderr — so every entity looked "not found" on every run, and re-running the script failed with `operator ... exists already`.
2. **`nsc`'s table output carries ANSI color codes even without a TTY**, and they sit flush against the name with no word boundary — a plain `grep -w` on that output silently never matches. Fixed by switching every existence check to `--json` output instead of parsing tables.
3. **`jq` wasn't actually installed on the host**, despite being assumed as a "standard, ubiquitous" utility. Rather than ask for another native install, the script was rewritten to do all JSON extraction through the already-containerized `yq` instead.

## Not yet done

Steps A1–A3 pin versions, document how they're run, and bootstrap auth identities. Still outstanding: writing `nats-server.conf` (A4), bootstrapping the `EVENTS` stream and `service-directory` KV bucket (A5), wiring everything into `docker-compose` (A6), and the manual smoke test (A7) — see [Design.md §11](../../Design.md#11-phase-1--detailed-breakdown).
