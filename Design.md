# Title: Gregor's Service Bus — Design Document

Status: Draft v0.2 — companion to [Design_Notes.md](Design_Notes.md)
Date: 2026-08-22
Scope: **Design only.** No implementation yet.

This document expands the original design notes into a concrete architecture, resolves the ambiguous points through the decisions recorded below, and proposes an implementation plan. Sections marked **Open Question** are still unresolved and need a decision before that part of the system can be built with confidence. This document also aims to satisfy [DevelopmentGuidelines.md](DevelopmentGuidelines.md)'s documentation standards as the project proceeds; per-dependency version/provenance/security-vetting detail lives in the companion [Dependencies.md](Dependencies.md) ledger rather than here.

---

## 1. Goals & Non-Goals

**Goals**
- A message bus, built on NATS JetStream, that any authorized service ("adapter") can connect to.
- Connection-level identity via public-key authentication (SSH-like).
- Payload confidentiality via public-key encryption, scoped to the specific recipients entitled to read a given event — independent of who else can technically subscribe to the subject.
- A small, well-documented set of baseline adapters that make the bus useful immediately (HTTP, webhooks, MySQL, file storage).
- A Python codebase organized like a normal, idiomatic Python project, deployable locally via `docker-compose`.

**Non-Goals (for this phase)**
- Multi-region / clustered NATS deployment.
- A UI/admin console (CLI + config files are sufficient for v1).
- Horizontal auto-scaling of adapters.
- Anything resembling a general-purpose ESB (business rules, orchestration, BPM). This is transport + adapters, not a workflow engine.

---

## 2. Key Decisions (resolved during design discussion)

| # | Question | Decision |
|---|----------|----------|
| 1 | How does the bus authenticate a service's identity? | **NATS nkeys** (built-in Ed25519 challenge/response — functionally identical to SSH pubkey auth). No custom auth service for v1. |
| 2 | Where does the "who is allowed to decrypt subject X" directory live? | **JetStream KV bucket** (`service-directory`) — reuses NATS infrastructure, no new datastore. |
| 3 | Is adapter-level "PII encryption" a separate mechanism from payload encryption? | **No — same mechanism.** Whole-payload public-key encryption covers PII; no separate field-level scheme in v1. |
| 4 | Should NATS subscribe permissions be wide open ("any connected adapter sees everything") or restricted per adapter? | **Restricted per adapter.** Each adapter gets an explicit subject allow-list. Encryption remains the confidentiality boundary regardless of what an adapter is permitted to subscribe to; permissions add defense-in-depth and reduce blast radius / noise. This is a deliberate refinement of the original notes' "open bus" framing — see §4.4. |
| 5 | Should events be signed for authenticity independent of transport? | **Yes** — the sender signs a digest of the plaintext with its nkey (Ed25519); the signature travels in the envelope (§4.1, §5). |
| 6 | Static NATS config or decentralized JWT for adapter onboarding? | **Decentralized JWT (`nsc`)** — adapters are provisioned/deprovisioned by issuing/revoking a User JWT, no server config edit or reload needed (§4.2, §4.4). |
| 7 | Registry entry lifecycle? | **TTL/heartbeat** — adapters refresh their own KV entry on a heartbeat; a dead adapter's key ages out automatically (§4.5). |
| 8 | Target Python version? | **3.12** — using the `uuid6` package for UUIDv7 (stdlib support arrives in 3.14) (§7.2). |
| 9 | `EVENTS` stream retention? | **7 days** age limit, plus a size cap as backstop (§6). |
| 10 | Database Adapter directionality? | **Bidirectional in v1** — write path (bus → MySQL) plus a CDC read path (MySQL → bus) via binlog streaming (§8). |
| 11 | File Storage Adapter shape? | **Command-driven CRUD** (list/read/write/delete triggered by bus commands), not a passive archival sink; filesystem-change → bus events deferred (§8). |
| 12 | Webhook Sender delivery guarantee? | **Best-effort** — single attempt, no retry/backoff (§8). |
| 13 | CI platform? | **GitHub Actions** (§7.4, §10). |
| 14 | Subject naming / bounded contexts? | **Placeholder pattern retained** — `events.<context>.<EventType>` with `orders` as a stand-in; real domain names to be supplied later (§5). |
| 15 | CDC approach for the Database Adapter? | **Lightweight** — `python-mysql-replication` tails the binlog in-process; no Debezium/Kafka Connect for now (§8, §9). |
| 16 | Python packaging/dependency-management tool? | **`uv`** — workspace mode handles `gsb-core` as a shared local dependency across all 6 adapter packages cleanly (§7.5). |
| 17 | Where does per-dependency version/release-date/source/security-vetting documentation live (per [DevelopmentGuidelines.md](DevelopmentGuidelines.md))? | **New [Dependencies.md](Dependencies.md) ledger** — keeps Design.md focused on architecture; the ledger is updated whenever a dependency is added, pinned, or reviewed (§7.2). |
| 18 | Is the adapter identity manifest (§11 Step A2) keyed per adapter *type* or per *instance*? | **Per instance.** The service-directory KV's `<subject>.<serviceId>` key shape (§4.5) already implies instance-level identity (e.g. `file-storage-01`), so a deployment can run multiple independent instances of the same adapter type — e.g. two HTTP Adapter instances, each wired to a different external API, each with its own NATS identity and permissions. See [infra/nats/adapter_identities.yaml](infra/nats/adapter_identities.yaml). |
| 19 | Does `FileWriteRequested` need a separate `FileCreateRequested` command, and does the File Storage Adapter watch for externally-created files? | **No separate command** — `FileWriteRequested` auto-creates a missing file. It emits `FileCreateCompleted` if the file was new, `FileWriteCompleted` if an existing file was updated. `FileCreateCompleted` is **also** published outside the command flow when the adapter notices a file created by an outside process (e.g. a shared/watched directory) — no preceding command, no `correlationId`. This is a deliberately narrow pull-forward of the folder-watch capability otherwise deferred (§8) — creation-detection only, not update-detection (§9). |

---

## 3. High-Level Architecture

```mermaid
flowchart LR
  subgraph bus["NATS JetStream"]
    STREAM[["EVENTS stream<br/>subjects: events.&gt;"]]
    KV[("service-directory<br/>KV bucket")]
  end

  subgraph adapters["Adapters"]
    HTTP["HTTP Adapter"]
    RESTAPI["REST API Service"]
    WHSEND["Webhook Sender"]
    WHLISTEN["Webhook Listener"]
    DB["Database Adapter (MySQL)"]
    FILE["File Storage Adapter"]
  end

  ExtREST[("External REST APIs")]
  ExtClient["External Clients"]
  ExtWebhookSrc["External Webhook Sources"]
  ExtWebhookDst["External Webhook Consumers"]
  MySQL[("MySQL")]
  FS[("Local Filesystem")]

  ExtClient <--> RESTAPI
  HTTP <--> ExtREST
  ExtWebhookSrc --> WHLISTEN
  WHSEND --> ExtWebhookDst
  DB <--> MySQL
  FILE <--> FS

  HTTP <--> STREAM
  RESTAPI <--> STREAM
  WHSEND <--> STREAM
  WHLISTEN <--> STREAM
  DB <--> STREAM
  FILE <--> STREAM

  adapters -. "register (subject, pubkey)" .-> KV
  adapters -. "look up recipients" .-> KV
```

All adapters share a common library (`gsb-core`, §7.1) that implements the bus client, event envelope, crypto, and registry lookups — so each adapter is a thin wrapper around its specific integration logic.

---

## 4. Security Model

### 4.1 Two separate keypairs per service

A service participating in the bus holds **two distinct keypairs**, used for two distinct purposes:

| Keypair | Algorithm | Purpose | Where it's used |
|---|---|---|---|
| **nkey** | Ed25519 (signing) | Prove identity to NATS when connecting | NATS auth handshake only |
| **encryption keypair** | X25519 (encryption) | Encrypt/decrypt event payloads | Application-level crypto, never touches NATS auth |

This mirrors "public key for access similar to SSH" (nkey) plus "payload encrypted for all public keys allowed to receive data" (encryption keypair) as two independent mechanisms — connecting to the bus and being able to *read a given message* are not the same permission.

**Decision:** Yes — every event is signed. The publisher signs a digest of the plaintext payload with its nkey before encrypting; the signature travels in `eventDetails.signature` (§5), so any recipient can verify authenticity/integrity independent of transport trust, even though the payload itself is only readable by entitled recipients.

### 4.2 Connection authentication (nkeys + decentralized JWT)

- Each adapter is provisioned an nkey seed (Ed25519 keypair) at deploy time (analogous to an SSH key pair).
- Rather than listing keys statically in `nats-server.conf`, the bus uses NATS's **decentralized JWT auth**, managed via the `nsc` CLI:
  - An **Operator** identity represents the bus itself.
  - An **Account** groups related adapters (a single `GSB` account is enough for v1 — subject permissions are still applied per-user within it).
  - Each adapter gets its own **User JWT**, signed by the account, embedding its nkey identity *and* its subject permissions (publish/subscribe allow-list, per Decision #4).
  - The NATS server is configured with a JWT **resolver** (a directory of issued JWTs, or a small resolver service) instead of a static authorization block.
- On connect, NATS sends a nonce; the adapter signs it with its private nkey and presents its User JWT; the server verifies the signature and reads permissions straight out of the JWT claims. No secret ever crosses the wire.
- Provisioning/deprovisioning an adapter is now an `nsc` operation (issue or revoke a User JWT) — **no server config edit or reload needed**, which is the point of choosing this over static config.
- Still 100% built into NATS tooling — no custom auth service to build.

### 4.3 Payload confidentiality (hybrid / envelope encryption)

Standard hybrid-encryption pattern, so we don't hand-roll multi-recipient crypto from scratch:

1. Publisher generates a random **Content Encryption Key (CEK)** per message.
2. Payload is encrypted with the CEK using an AEAD cipher (e.g. XChaCha20-Poly1305).
3. The CEK itself is encrypted ("wrapped") once per recipient, using each recipient's X25519 public key.
4. The envelope carries the ciphertext plus one wrapped-CEK entry per recipient. Any recipient with the matching private key unwraps the CEK and decrypts the payload; nobody else can.

This is exactly what the **[age](https://age-encryption.org)** format does natively (it's designed for "encrypt to multiple recipients"), so we don't need to implement the envelope construction ourselves — see §7.2.

```mermaid
sequenceDiagram
  participant Pub as Publishing Adapter
  participant KV as service-directory (KV)
  participant JS as JetStream (EVENTS stream)
  participant Sub as Subscribing Adapter(s)

  Pub->>KV: Get/Watch recipients for subject events.orders.OrderCreated
  KV-->>Pub: [pubkey_A, pubkey_B, ...]
  Pub->>Pub: Generate random CEK, encrypt payload (AEAD)
  Pub->>Pub: Wrap CEK for each recipient X25519 pubkey
  Pub->>JS: Publish envelope (metadata + ciphertext + wrapped keys)
  JS-->>Sub: Deliver to durable consumers permitted on this subject
  Sub->>Sub: Locate own wrapped-key entry, unwrap CEK w/ private key
  Sub->>Sub: Decrypt payload with CEK
```

### 4.4 Subject-level authorization (NATS permissions)

Per Decision #4, each adapter's NATS identity is granted an explicit `subscribe`/`publish` subject allow-list rather than a blanket wildcard. Two independent layers now exist:

- **Transport layer (NATS permissions):** can this adapter even receive traffic on this subject at all?
- **Application layer (encryption):** even if it receives the message, can it actually read the payload?

A message an adapter is permitted to subscribe to but not entitled to decrypt is still meaningless noise to it — metadata only. This satisfies the spirit of the original "open bus, encrypted payload" framing while adding least-privilege at the transport layer too.

**Decision:** Permission assignment happens via the decentralized JWT mechanism in §4.2 — each adapter's User JWT embeds its subject allow-list directly, so onboarding and permission changes don't require a NATS server restart or config reload.

### 4.5 Registry / recipient directory (JetStream KV)

- Bucket: `service-directory`.
- Key shape: `<subject>.<serviceId>` — e.g. `events.orders.OrderCreated.file-storage-01`.
- Value: JSON — `{ "serviceId", "adapterType", "encryptionPublicKey", "registeredAt" }`.
- **Write access:** an adapter may only `PUT` keys under its own `serviceId` namespace (enforced via NATS permission on the underlying `$KV.service-directory.*.<serviceId>` subject).
- **Read access:** all adapters can read/watch the bucket to discover current recipients for a subject.
- Publishers cache the recipient list in memory and keep it fresh via a JetStream KV **watch** (not a `GET` per publish) for performance.

```mermaid
sequenceDiagram
  participant Adm as Deploy/Admin
  participant Ad as Adapter (e.g. File Storage)
  participant NS as NATS Server
  participant KV as service-directory (KV)

  Adm->>Ad: Provision nkey seed + X25519 keypair
  Ad->>NS: Connect (nkey signs server-issued nonce, presents User JWT)
  NS-->>Ad: Authenticated, permissions applied from JWT claims
  Ad->>KV: PUT events.orders.OrderCreated.file-storage-01 = {encryptionPublicKey, adapterType, ...}
  KV-->>Ad: Ack
  Note over Ad,KV: Other adapters watch this bucket to discover recipients
```

**Decision:** Registry entries use JetStream KV's native per-key TTL. Each adapter refreshes its own entry on a heartbeat interval (shorter than the TTL); if the adapter stops heartbeating, its entry expires and it silently drops out of the recipient list for new messages — no manual deregistration step needed for the common "adapter died" case. Explicit deregistration via the `tools/` CLI remains available for deliberate decommissioning.

---

## 5. Event Envelope

Baseline schema (extends the shape from the design notes):

```json
{
  "event": {
    "eventDetails": {
      "eventId": "uuid7",
      "eventCreated": "2026-08-22T18:04:00Z",
      "eventType": "OrderCreated",
      "eventSchemaVersion": "1.0.0",
      "sourceServiceId": "rest-api-service-01",
      "correlationId": "uuid7 (optional, for request/reply chains)",
      "signature": "base64 — Ed25519 signature of the plaintext digest, signed with the sender's nkey (see §4.1)"
    },
    "encryption": {
      "algorithm": "age-v1 (X25519 + XChaCha20-Poly1305)",
      "recipients": [
        { "keyId": "<recipient pubkey fingerprint>", "wrappedKey": "<base64>" }
      ]
    },
    "eventPayload": "<base64 ciphertext>"
  }
}
```

Notes / assumptions baked into this:
- **`subject` = one unique subject per `eventType`** (not per message instance) — e.g. `events.orders.OrderCreated` is the permanent home for that event type, and its payload schema is documented once, referenced by that subject.
- Subject naming convention: `events.<boundedContext>.<EventType>` — kept as a **placeholder pattern** for now (`orders` is a stand-in bounded context). Real bounded-context names will replace the placeholder once they're known; nothing else in the design depends on the specific names chosen.
- Schemas for `eventPayload` (pre-encryption, logical shape) are documented as versioned JSON Schema files, one per subject, checked into `schemas/` in the repo (see §7). `eventSchemaVersion` lets consumers detect breaking changes.

---

## 6. NATS JetStream Layout

- **Stream:** single stream `EVENTS`, subject filter `events.>` (one stream is simplest to operate; can be split by bounded context later if retention/ops needs diverge).
- **Retention:** durable, limits-based retention — **7 days** age limit, plus a size cap as a backstop — not work-queue, since multiple adapters need independent delivery of the same message. Long enough to recover from adapter downtime/redeploys without replaying stale data; revisit if audit requirements later call for a longer window.
- **Consumers:** one durable, filtered consumer per adapter (`filter_subject` scoped to what that adapter is permitted/configured to handle), so each adapter tracks its own delivery cursor independently and can restart without replaying everything.
- **KV bucket:** `service-directory`, as above.

---

## 7. Python Project

### 7.1 Repository layout

```
GregorsServiceBus/
  Design.md
  Design_Notes.md
  Dependencies.md               # dependency ledger: version, release date, source, security vetting
  DevelopmentGuidelines.md
  pyproject.toml                 # uv workspace root — members: libs/*, adapters/*
  .python-version                # pins 3.12 for uv
  docker-compose.yml            # infra + all adapters (built later)
  schemas/                      # versioned JSON Schema per subject/eventType
  libs/
    gsb-core/                   # shared library used by every adapter
      pyproject.toml
      gsb_core/
        envelope.py             # EventEnvelope pydantic model, (de)serialization
        crypto.py               # encrypt_for_recipients(), decrypt(), signing helpers
        bus_client.py           # thin wrapper over nats-py + JetStream
        registry.py             # KV read/write + in-memory watch cache
        config.py               # pydantic-settings based adapter config loader
      tests/
  adapters/
    http_adapter/
    rest_api_service/
    webhook_sender/
    webhook_listener/
    db_adapter_mysql/
    file_storage_adapter/
      # each: pyproject.toml, app/, tests/, Dockerfile
  infra/
    nats/
      nats-server.conf        # JetStream + JWT resolver config (no static authorization block)
      operator/                # nsc-managed Operator/Account/User JWTs + nkeys (secrets git-ignored)
    mysql/
      init.sql
      # my.cnf snippet enabling binlog (log_bin, binlog_format=ROW, server-id) for CDC
  tools/                         # key-generation CLI, registry admin CLI
```

Each adapter is its own installable Python package with its own `Dockerfile`, depending on `gsb-core` as a local/editable dependency (or a private index later). This keeps adapters independently deployable while sharing the crypto/envelope/bus logic in one audited place.

### 7.2 Libraries (proposed)

Architectural rationale for each choice is below; **version pins, release dates, sources, and security-vetting notes live in [Dependencies.md](Dependencies.md)**, updated as each dependency is actually added via `uv add`.

| Concern | Library | Notes |
|---|---|---|
| NATS/JetStream client | `nats-py` | Official async client; supports JetStream + KV. |
| Data modeling / validation | `pydantic` v2 | Event envelope, config models, adapter-specific payload schemas. |
| Config | `pydantic-settings` | Env-var/`.env`-driven adapter configuration. |
| Payload encryption | `pyrage` (age bindings) | Native multi-recipient encryption — matches §4.3 exactly. Fallback: `PyNaCl` (libsodium) if `age` integration proves awkward, at the cost of hand-rolling the envelope. |
| Signing (nkey / Ed25519) | `nats-py`'s built-in nkey support (`nkeys` lib) | Reused for the payload signing that's part of every event (§4.1). |
| REST API Service / Webhook Listener | `fastapi` + `uvicorn` | ASGI HTTP surface for inbound adapters. |
| Outbound HTTP (HTTP Adapter, Webhook Sender) | `httpx` (async) | |
| Retry/backoff | `tenacity` | Database Adapter writes (not the Webhook Sender, which is best-effort per §8). |
| MySQL adapter (write path) | `SQLAlchemy` 2.0 (async) + `asyncmy` | |
| MySQL adapter (CDC read path) | `python-mysql-replication` | Tails the MySQL binlog directly (row-based) and turns row changes into bus events — avoids standing up a full Debezium/Kafka Connect stack for a single-adapter use case. Confirmed choice for v1 (Decision #15). |
| File storage adapter | `aiofiles` | Backs the list/read/write/delete command handlers (§8). |
| File storage adapter — external-creation detection | `watchfiles` | Watches the managed directory to detect files created by outside processes, so `FileCreateCompleted` can be emitted without a preceding command (Decision #19). Async-native, fits the existing async stack; same author/ecosystem as `pydantic`. |
| UUIDv7 | `uuid6` (PyPI) | Stdlib `uuid` doesn't have `uuid7()` until Python 3.14; project targets Python 3.12, so this stays a dependency. |
| Structured logging | `structlog` | JSON logs correlated by `eventId`/`correlationId`. |
| CLI tooling | `typer` | Key generation, registry admin, local dev utilities. |

### 7.3 Required services (for `docker-compose`, described — not written yet)

- `nats` — NATS server with JetStream + KV enabled.
- `mysql` — for the Database Adapter; configured with binlog enabled (`log_bin`, `binlog_format=ROW`, unique `server-id`) to support the CDC read path (§8).
- One container per adapter (6 total for v1).
- Optional: `adminer` (DB inspection, dev convenience).

### 7.4 Development tooling

| Purpose | Tool |
|---|---|
| Testing | `pytest`, `pytest-asyncio`, `pytest-cov` |
| Integration testing | `testcontainers-python` (spin up real NATS+JetStream and MySQL for tests) |
| Lint/format | `ruff` |
| Type checking | `mypy` |
| Security static analysis | `bandit` |
| Dependency vulnerability scanning | `pip-audit` |
| Git hook orchestration | `pre-commit` (ties the above together) |
| Containerization | `docker`, `docker-compose` |

**Decision:** GitHub Actions runs the above (lint, type-check, test, security scans) on every push/PR.

### 7.5 Development environment setup

- **Packaging/dependency management:** [`uv`](https://docs.astral.sh/uv/) (Decision #16). The repo root `pyproject.toml` declares a **uv workspace** with members `libs/*` and `adapters/*` — `gsb-core` is consumed by every adapter as an in-workspace path dependency (no publishing to an index needed), and `uv sync` at the root resolves/installs the whole project's dependency graph in one lockfile (`uv.lock`).
- **Python version:** pinned via a root `.python-version` file (3.12, Decision #8); `uv` provisions/uses that interpreter automatically.
- **Per-package env:** `uv` manages a single shared virtual environment for the workspace by default (`.venv/` at the repo root) — no per-adapter venv juggling.
- **Local paths / system paths:** nothing outside the repo tree is required beyond `uv` itself and Docker; `nsc`'s store is repo-local and git-ignored (§11, Track A parameters).

### 7.6 Environment properties (docker-compose)

Full documentation of each service's ports/volumes/env vars happens when `docker-compose.yml` is actually authored (§10 Phase 5), but the anticipated shape, per [DevelopmentGuidelines.md](DevelopmentGuidelines.md)'s "document environment properties" requirement:

| Service | Anticipated env vars | Ports | Volumes |
|---|---|---|---|
| `nats` | — (config-file driven) | `4222` (client), `8222` (monitoring) | JetStream store dir; `infra/nats/` config + resolver |
| `mysql` | `MYSQL_ROOT_PASSWORD`, `MYSQL_DATABASE`, binlog settings via `my.cnf` mount | `3306` | data dir; `infra/mysql/init.sql` |
| each adapter | `GSB_SERVICE_ID`, `GSB_NATS_CREDS_PATH`, `GSB_NATS_URL`, `GSB_SUBJECTS`, `GSB_LOG_LEVEL` (via `config.py`, §7.1) | adapter-specific (e.g. `8080` for REST API Service / Webhook Listener) | none by default; File Storage Adapter additionally mounts a data volume |

This table is a preview, not a commitment — it will be corrected/expanded against the real compose file once written.

---

## 8. Initial Adapters

| Adapter | Direction | Role | Assumption to confirm |
|---|---|---|---|
| **HTTP Adapter** | Bus → external, external → Bus | On event, calls a configured external REST API; may publish the response back as a correlated event. | "Emit and receive" read as: outbound call triggered by a bus event, response optionally re-published. |
| **REST API Service** | External → Bus (+ optional sync reply) | Exposes a REST API for external clients; translates calls into bus events. Can do request/reply by publishing then waiting on a correlated response subject. | This is the "front door" for external HTTP clients — confirm that's the intent vs. something else. |
| **Webhook Sender** | Bus → external | Subscribes to configured subjects; POSTs decrypted payload (or a projection) to a registered external webhook URL — **single attempt, best-effort, no retry/backoff**. | A downstream outage can silently drop a delivery at this boundary; acceptable per current requirements. If this changes later, add `tenacity`-based retry + `eventId` dedupe guidance for the receiver. |
| **Webhook Listener** | External → Bus | Exposes an HTTP endpoint for inbound third-party webhooks; verifies source, converts to the event envelope, encrypts, publishes. | Source verification mechanism (shared secret, HMAC header) is source-specific — TBD per integration. |
| **Database Adapter (MySQL)** | Bidirectional | **Write path:** subscribes to subjects and persists decrypted event data into MySQL. **Read path (CDC):** tails the MySQL binlog via `python-mysql-replication` and publishes row-level changes as bus events. Both in scope for v1. | Bidirectional scope makes this adapter meaningfully bigger than originally sketched, but the CDC mechanism itself is settled (Decision #15). **Write-path input and CDC-path output must use different subjects** (e.g. `events.orders.OrderCreated` in vs. `events.db.orders.RowChanged` out) — otherwise the adapter could subscribe to its own CDC output as a write-path trigger. Reflected in [infra/nats/adapter_identities.yaml](infra/nats/adapter_identities.yaml). |
| **File Storage Adapter (local)** | Command-driven (bus → adapter → bus), plus narrow watch → bus | Not a passive archival sink — a **file operations service** invoked via bus commands: `list`, `read`, `write`, `delete` against local files. `FileWriteRequested` auto-creates a missing file; the adapter reports `FileCreateCompleted` (new file) or `FileWriteCompleted` (existing file updated) accordingly (Decision #19). `FileCreateCompleted` is **also** emitted when the adapter notices a file created by an outside process (no preceding command). Full folder-watch (arbitrary external changes, including updates) remains **deferred**. | Exact reply/result-subject scoping (e.g. per-requester) still TBD when this adapter is built (Phase 3); externally-triggered *update* detection is an open question (§9). |

All adapters are built on `gsb-core`, so "build a new adapter" mostly means writing the integration-specific glue (HTTP call, file write, SQL write) around a shared `BusClient`.

---

## 9. Open Questions Summary

All architectural questions have been resolved and folded into §2–§8 above (Decisions #1–#15). What's left is deliberately deferred, not blocking:

1. **File Storage Adapter command/response schema** — exact subject names and payload shape for the `list`/`read`/`write`/`delete` commands are TBD, to be nailed down when that adapter is actually built (Phase 3).
2. **Subject naming** — real bounded-context names are deferred by choice; `events.<context>.<EventType>` stays a placeholder pattern (`orders` as stand-in) until real domains are known. Nothing else in the design depends on the specific names chosen.
3. **Externally-triggered file *updates*** — Decision #19 has the File Storage Adapter detect externally-created files (→ `FileCreateCompleted`), but not externally-triggered updates to existing files. Is that asymmetry intentional, or should update-detection be added too when this adapter is built?

---

## 10. Proposed Implementation Plan (high-level, no code yet)

1. **Phase 0** — Resolve remaining §9 items as they come up; finalize this document.
2. **Phase 1** — Core infra: `nsc`-issued Operator/Account/User JWTs, NATS JetStream config (JWT resolver, `EVENTS` stream, `service-directory` KV bucket); `gsb-core` library (envelope, signing, crypto, bus client, registry client) with unit tests, no adapters yet.
3. **Phase 2** — One vertical slice end-to-end to prove the pattern: **Webhook Listener → File Storage Adapter** (inbound HTTP → encrypted+signed publish of a `FileWriteRequested` command → File Storage Adapter decrypts, verifies signature, writes the file, publishes a result event). Wire into `docker-compose`.
4. **Phase 3** — Remaining adapters (HTTP Adapter, REST API Service, Webhook Sender, Database Adapter — including its CDC read path per Decision #10; nail down the File Storage Adapter's full command/response schema).
5. **Phase 4** — Hardening: GitHub Actions CI (ruff, mypy, pytest, bandit, pip-audit), populate `schemas/` for all defined event types, integration test suite via `testcontainers-python`.
6. **Phase 5** — Finalize `docker-compose.yml` as the complete local demo environment.

---

## 11. Phase 1 — Detailed Breakdown

Phase 1 (§10) splits into two tracks that can largely proceed in parallel, converging at the end for an integration proof. Nothing here is built yet — this is the step-by-step scope for when implementation starts.

**A few parameters this breakdown assumes — flag now if any should change, otherwise treated as settled for Phase 1:**

| Parameter | Proposed default | Rationale |
|---|---|---|
| `service-directory` KV entry TTL | 60s | Short enough that a dead adapter drops out quickly; long enough to tolerate a missed heartbeat or two. |
| Heartbeat interval (re-`PUT` own entry) | 20s | 3x safety margin under the TTL. |
| JWT resolver type | NATS **memory resolver** (JWTs embedded directly in `nats-server.conf`, generated by the bootstrap step) | Simplest option for a handful of adapters; swappable for a directory/`full` resolver later without touching anything else if the adapter count grows. |
| nsc store / key material | **Not committed.** `infra/nats/operator/` is git-ignored; a bootstrap step regenerates the Operator/Account/User JWTs and nkeys locally (and in CI) on demand. | Standard practice — private key material never belongs in git, and regeneration is cheap since nothing depends on the JWTs' specific content, only their claims. |

### Track A — NATS / JetStream Infrastructure

- [x] **A1. Toolchain pin** — `nats-server` 2.14.5 and `nats-box` 0.19.7-nonroot (bundles `nsc` + `nats` CLI), both Docker images only — nothing installed natively on the host; persistent `nsc` state is a volume-mapped local path. Documented in [infra/nats/README.md](infra/nats/README.md); pins also recorded in [Dependencies.md](Dependencies.md).
- [x] **A2. Adapter identity manifest** — [infra/nats/adapter_identities.yaml](infra/nats/adapter_identities.yaml): one entry per adapter *instance* (Decision #18), with `serviceId`, `adapterType`, and a publish/subscribe subject allow-list. Populated with the Phase 2 pair (`webhook-listener-01`, `file-storage-01`) plus one placeholder instance for each of the other 4 adapter types. KV read/write permissions are derived from the subscribe list by the bootstrap script (A3), not hand-authored per entry.
- [x] **A3. Bootstrap script (auth)** — [infra/nats/bootstrap_auth.sh](infra/nats/bootstrap_auth.sh): idempotent, containerized (`nsc` via `natsio/nats-box`, manifest parsed via `mikefarah/yq`), creates the Operator, `GSB` Account, and one User per A2 manifest entry, applies its permissions plus a derived KV-write grant and a v1 baseline (`$JS.API.>`, `_INBOX.>`, KV-read-all — not scoped down further in v1, a candidate for later tightening), generates a `.creds` file per adapter, and emits the memory-resolver config block. **Executed and verified end-to-end** (Docker access resolved — host user added to the `docker` group) across 3 consecutive runs, confirming true idempotency (no duplicate permission entries on repeat `edit user` calls). Four real bugs surfaced only by running it, not by reading docs — worth remembering: (1) `nsc list ... --json` writes to **stderr**, not stdout — a `2>/dev/null` silently broke every existence check; (2) `nsc`'s table output carries ANSI color codes even without a TTY, breaking `grep -w` matching — switched existence checks to `--json` throughout; (3) `jq` wasn't actually present on the host despite being assumed — dropped entirely in favor of `yq` alone; (4) JetStream refuses to start under decentralized JWT auth without a **System Account** — only surfaced when Step A4 actually started the server, fixed by adding `--sys` to the operator-creation call, after which `nsc generate config --mem-resolver` emits the needed `system_account:` directive automatically; (5) **JetStream is separately disabled per-account by default**, independent of both the system-account fix and the server's global `jetstream{}` block — surfaced while testing Step A5, fixed by adding `nsc edit account --js-mem-storage 256M --js-disk-storage 2G` (kept in step with `nats-server.conf`'s own caps). Also added a dedicated **`gsb-admin`** identity (`$JS.API.>` + `$KV.service-directory.>` + `_INBOX.>`, full pub+sub) — provisioning shared JetStream infrastructure isn't any particular adapter's job, so Step A5 uses this rather than repurposing one adapter's creds. Also settled: client connection uses a single `.creds` file (JWT+nkey bundled, `nsc generate creds`), not separate nkey-seed/JWT paths — §7.6 and B4 updated accordingly.
- [x] **A5. Bootstrap script (JetStream objects)** — [infra/nats/bootstrap_jetstream.sh](infra/nats/bootstrap_jetstream.sh): idempotent, containerized, using the `gsb-admin` identity from A3. Creates the `EVENTS` stream (`events.>`, file storage, limits retention, discard-old, 7-day max-age, 1G max-bytes backstop) and the `service-directory` KV bucket (file storage, 60s max-age applied uniformly per key's last-write time — see terminology note below). **Executed and verified end-to-end**: both objects created successfully against the live A4 server; then empirically verified the actual TTL/heartbeat *mechanism* itself (not just that the bucket was created with some TTL value) by writing two keys, refreshing only one at t=30s, and checking both at t=65s — the refreshed key was still present (revision 3), the unrefreshed one had genuinely expired (`key not found`). This is the concrete behavior Decision #7's heartbeat/TTL design depends on, now confirmed rather than assumed. **Terminology note**: NATS's `nats kv add --ttl` sets a bucket-wide max-age applied per key's last-write timestamp — not the same mechanism as NATS's separate opt-in "Per-Key TTL" feature (`--marker-ttl`, for *different* TTL values per key). Since every registry entry uses the *same* fixed TTL in our design, the simpler bucket-wide setting is sufficient and was used; "per-key TTL" language elsewhere in this doc (§4.5) refers to the functional behavior (each key's own expiry clock resets on its own writes), not literally NATS's `--marker-ttl` feature.
- [x] **A4. `nats-server.conf`** — [infra/nats/nats-server.conf](infra/nats/nats-server.conf): client listener (`0.0.0.0:4222`), monitoring (`8222`), `jetstream {}` block (dev-sized: 256M mem / 2G file, store dir matching Step A6's planned volume mount), and `include resolver.conf` for auth. **Executed and verified**: started the pinned `nats:2.14.5` image against this config — clean startup ("Server is ready", JetStream initialized, no warnings) after the A3 system-account fix. Further confirmed with real traffic: published successfully using `webhook-listener-01`'s `.creds` on its allowed subject, and confirmed `webhook-sender-01` is hard-rejected ("Permissions Violation for Publish") publishing outside its allow-list — the per-adapter subject restriction from Decision #4 is genuinely enforced by the server, not just declared.
- [x] **A6. `docker-compose` (infra-only)** — [docker-compose.yml](docker-compose.yml): just the `nats` service (adapters come in Phase 3). **Deviated from the original "one-shot init container" framing**, and flagging it rather than letting it slide: the two bootstrap scripts already shell out to `docker run` themselves (Design.md's "no native install" decision), so running them *as Compose services too* would mean Docker-outside-of-Docker — mounting the host's `docker.sock` into a container just to spawn sibling containers — for scripts that already work well as host-level tooling. Used [infra/nats/up.sh](infra/nats/up.sh) instead: a host-level wrapper sequencing A3 (must run *before* `nats` starts — `nats-server.conf` `include`s the resolver.conf it generates) → `docker compose up -d nats` → an external readiness check (the `nats:2.14.5` image has **no shell at all**, confirmed by testing — `sh` isn't found — so a Compose-native `HEALTHCHECK` wasn't viable) → A5 (needs the live server). Also fixed a real dependency gap this surfaced: `bootstrap_jetstream.sh`'s default `nats://nats:4222` only resolves inside the Compose network, so it now accepts a `DOCKER_NETWORK` override, which `up.sh` sets to the project's fixed network name (`gsb_default`, pinned via `name: gsb` in the compose file so it doesn't depend on the checkout directory's name). **Executed and verified end-to-end, twice**, from a fully clean state (no operator/account/users/stream/bucket/containers): full run succeeds (auth → server up → stream/KV created), and a second full run correctly no-ops everything that already exists while still confirming server readiness — genuine idempotency of the *entire* Phase 1 Track A pipeline, not just individual scripts in isolation.
- [x] **A7. Manual smoke test** — ran the full sequence against the live A6 stack: (1) published as `webhook-listener-01` to `events.files.FileWriteRequested`, confirmed it landed in the `EVENTS` stream; (2) created a **durable pull consumer** as `file-storage-01` (`--filter events.files.FileWriteRequested --ack explicit`) — the actual mechanism adapters use per §6, not yet exercised by any earlier step — and pulled the message through it, payload intact, acknowledged; (3) confirmed `webhook-sender-01` still hard-denied publishing outside its allow-list; (4) confirmed `file-storage-01` can KV-write its own registration key but is denied writing another adapter's; (5) confirmed the denial in (4) is real permissions enforcement, not a syntax slip, by successfully writing the identical key as `gsb-admin`. **New finding from this step**: KV write permission violations manifest as a **timeout** (`context deadline exceeded`), not an explicit rejection message the way plain pub/sub denials are — worth remembering when `gsb-core`'s `registry.py` (Track B, B5) handles KV writes, since a hang there is more likely a permissions problem than a network issue. All test messages/consumers/keys cleaned up afterward (stream purged back to 0 messages, test KV keys deleted) — the running stack itself was left up. **Track A (A1–A7) is now fully complete and empirically verified**, not just written.

### Track B — `gsb-core` Library

- [x] **B1. Package scaffold** — root [pyproject.toml](pyproject.toml) (virtual `uv` workspace, `members = ["libs/*", "adapters/*"]`, shared dev dependency group, ruff/mypy/pytest config), [.python-version](.python-version) (3.12), [libs/gsb-core/pyproject.toml](libs/gsb-core/pyproject.toml) + minimal `gsb_core` package + a scaffold test, [.pre-commit-config.yaml](.pre-commit-config.yaml), and a root [README.md](README.md) covering dev setup. **Executed and verified**: `uv sync`, `pytest`, `ruff check`/`format`, `mypy`, `bandit`, `pip-audit`, and `pre-commit run --all-files` all actually run clean against the scaffold — not just configured. `uv` itself installed and pinned (0.12.5) via its official no-sudo user-space installer. Two real gaps found only by running this, both documented in [README.md](README.md) and [Dependencies.md](Dependencies.md): (1) plain `uv sync` in a virtual-root workspace **silently installs nothing** from `libs/`/`adapters/` when nothing depends on them — no error, just a misleadingly successful-looking resolve of the dev group alone; `--all-packages` is required, now the documented standard command. (2) `mypy` hard-errors on a missing path argument (unlike `pytest`, which tolerates an absent `testpaths` entry) — broke the pre-commit config until `adapters/` (which doesn't exist until Phase 3) was dropped from its invocation. No NATS/pydantic/crypto dependencies added yet, as scoped — those arrive with B2 onward.
- [x] **B2. `envelope.py`** — [libs/gsb-core/gsb_core/envelope.py](libs/gsb-core/gsb_core/envelope.py): pydantic v2 models matching §5's wire schema exactly (`Event` → `EventEnvelope` → `EventDetails`/`EncryptionMetadata`/`RecipientKey`), frozen, with `to_wire()`/`from_wire()` (de)serialization and a `new_event_id()` (UUIDv7) + `utc_now()` default-factory pair so a caller doesn't have to remember to fill in `eventId`/`eventCreated` by hand. No NATS/crypto dependency — `signature` is `Optional[str]` on purpose, since B3 is what actually signs. **Executed and verified**: 10 tests (round-trip, exact wire-shape assertion against §5's key names, UUIDv7 defaulting/ordering, frozen-model immutability, `populate_by_name` construction) all pass, plus clean `ruff`/`mypy`/`bandit`/`pip-audit`/`pre-commit`. Two real findings, not just guesses: (1) empirically confirmed pydantic serializes an aware UTC `datetime` with a `Z` suffix (`2026-08-24T22:09:17.515177Z`), matching §5's example format rather than diverging into `+00:00`; (2) pydantic's mypy plugin **does not understand `populate_by_name=True`** — it still expects camelCase aliases in the synthesized `__init__`, a known plugin limitation confirmed by actually enabling the plugin and watching it still fail, not assumed — documented with a `type: ignore[call-arg]` at the one call site that exercises snake_case construction, rather than silently suppressing it project-wide.
- [ ] **B3. `crypto.py`** — hybrid encryption (`pyrage`: encrypt-for-recipients / decrypt) and Ed25519 signing/verification (`nkeys`), unit tested against fixed test keys. Still no live bus needed.
- [ ] **B4. `config.py`** — `pydantic-settings` loader for adapter config (NATS creds file path — JWT+nkey bundled, per Step A3's `nsc generate creds` output — subject list, service ID).
- [ ] **B5. `registry.py`** — KV client wrapper (get/watch/put-with-TTL, heartbeat loop). First piece that needs a live NATS — build against the Track A `docker-compose` stack (A6).
- [ ] **B6. `bus_client.py`** — the `BusClient` façade tying together JetStream publish/subscribe + envelope + crypto + registry lookup. Integration-tested against the same running stack.
- [ ] **B7. End-to-end proof** — a test with two in-process fake adapters using `BusClient`: one publishes a signed+encrypted event, the other receives, verifies the signature, and decrypts it. This is the concrete "Phase 1 is done" checkpoint — everything downstream (real adapters) builds on this working.

### Suggested sequencing

B1–B4 have no infrastructure dependency and can start immediately / in parallel with Track A. B5–B7 need A6 (a running, bootstrapped NATS) to exist first. A7 and B7 both serve as the exit criteria for Phase 1 — once both pass, Phase 2's vertical slice (§10) can begin.

---

*This document is a living draft — update it as open questions are resolved and the design evolves, rather than letting decisions drift into code undocumented.*
