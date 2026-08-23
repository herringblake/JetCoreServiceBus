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

No local binary to install. General pattern:

```bash
docker run --rm -it \
  -v "$(pwd)/infra/nats/operator:/nsc" \
  natsio/nats-box:0.19.7-nonroot \
  nsc --version
```

- `-v "$(pwd)/infra/nats/operator:/nsc"` — persists `nsc`'s Operator/Account/User store (nkeys + JWTs) to a local path instead of leaving it inside the ephemeral container. This is the same `infra/nats/operator/` directory called out as git-ignored in [Design.md §11](../../Design.md#11-phase-1--detailed-breakdown)'s parameter table — nothing containing key material gets committed. **Confirm the exact in-container path `nats-box` expects (`NSC_HOME`/`HOME`-derived) against its docs/entrypoint when actually running `nsc init` in step A3** — `/nsc` above is a placeholder until that's verified, not yet confirmed correct.
- Once A6 stands up the `docker-compose` network, run this container with `--network <compose network name>` so it can reach the `nats` service by its compose service name for the A7 smoke test, instead of a host port.

## Not yet done

This step only pins versions and documents how they're run. It does **not** install/run anything persistent, create the Operator/Account (A3), write `nats-server.conf` (A4), or start any container (A6) — those are separate steps in [Design.md §11](../../Design.md#11-phase-1--detailed-breakdown).
