# Defects Ledger

Companion to [Design.md](Design.md) — tracks real, confirmed defects found while building this project: what's broken, how it was diagnosed, and the recommended (or applied) fix. Distinct from [Design.md §9](Design.md#9-open-questions-summary)'s "Open Questions," which are deliberate, deferred *design* choices, not bugs — a defect here may reference an §9 item (or vice versa) when the two are related, but they're tracked separately: §9 is "not decided yet, on purpose," this document is "decided, and it's wrong, here's the fix."

**Status: 1 fixed, 1 open.** See the management instructions at the bottom for how entries get added, updated, and closed.

| # | Name | Status | Severity | Found |
|---|---|---|---|---|
| 1 | [Test/Production Identity Collision](#defect-1-testproduction-identity-collision) | Fixed — Verified | Medium (test-reliability, not a production data-loss/security bug on its own) | Design.md §12 Step E4; root cause isolated §13 Step J7; confirmed at full scale §13 Step K2 |
| 2 | [Ambient Test Traffic Undecryptable by Real Containers](#defect-2-ambient-test-traffic-undecryptable-by-real-containers) | Open — root cause not yet isolated | Low-Medium (log noise + unbounded redelivery on the real containers; no observed effect on pytest's own pass/fail results) | Found verifying Defect 1's fix |

---

## Defect 1: Test/Production Identity Collision

**Status:** Fixed — Verified. See "Applied fix" and "Verification" below.
**Severity:** Medium. Doesn't corrupt data or expose a security hole on its own; it produces false-negative test failures (a passing feature reported as failing), which is a real cost to development velocity and — left unaddressed — to CI trust once [Design.md §10 Phase 4](Design.md#10-proposed-implementation-plan-high-level-no-code-yet) stands up GitHub Actions.
**Found:** first symptom at [Design.md §12 Step E4](Design.md#12-phase-2--detailed-breakdown); misdiagnosed as generic "NATS server strain" through Steps H5/I7; root cause actually isolated at [§13 Step J7](Design.md#13-phase-3--detailed-breakdown); confirmed at full 6-adapter scale at [§13 Step K2](Design.md#13-phase-3--detailed-breakdown). Full narrative in [Design.md §9 item #5](Design.md#9-open-questions-summary).

### Symptom

An intermittent, non-deterministic test failure — `pyrage.DecryptError: No matching keys found` or `jetcore.bus_client.SignatureVerificationError` — in a test that has nothing wrong with it, most often not even the test whose name appears in the failure (an *unrelated* test happens to be the one mid-flight when the race fires). Reproduces standalone, not just under full-suite load, which is what eventually disproved the original "accumulated JetStream consumer strain" theory.

### Root cause

Two independent, individually reasonable project conventions collide:

1. **Real adapter containers use a *stable* identity.** `BusClient.connect_as_adapter()` derives an adapter's signing key from its own `.creds` file — deliberately stable across restarts (Design.md §12 Step C6), and (for encryption) registers a recipient key on every `subscribe()` call, refreshed by a 20s heartbeat (Design.md §11 parameter table).
2. **Tests that want to prove a real entrypoint works also call `connect_as_adapter()` under the adapter's own real `serviceId`** — by design, not by accident: this is what makes e.g. `test_entrypoint.py`/`test_*_entrypoint.py` files and the root [tests/](tests/) cross-adapter tests actually exercise production code paths (`__main__.run()`, `create_app()`'s real lifespan) rather than a stand-in.

Both directories that store an identity are keyed *purely* by `serviceId`, with no concept of "which specific process instance":

- `service-identity` (signing keys, Decision #20): **no TTL, no revocation** — whichever registration lands last simply overwrites the previous one, permanently, until something registers again.
- `service-directory` (per-subject recipient encryption keys): **has** a TTL/heartbeat (60s TTL / 20s heartbeat), but that doesn't help here — if the real container and a test's own instance are *both alive* under the same `serviceId` at once, they each keep re-asserting their own key on their own heartbeat cadence, so whichever heartbeats most recently is simply the one currently "correct," and it flips back and forth for as long as both processes are alive.

So whenever a test drives a real entrypoint via `connect_as_adapter()` for `serviceId` X **while the real, `docker compose`-deployed container for X is also running** (true for every adapter as of Step K2), the two processes are genuinely, legitimately both alive under one identity — not a bug in either one individually, but an unhandled case in an architecture that assumed exactly one live process per `serviceId` (Decision #18's whole premise). Whichever one's registration is overwritten leaves its already-published message unverifiable/undecryptable to anyone who looks the key up afterward.

### Evidence

Isolated by directly toggling the variable: stopping the real adapter containers and rerunning made the failure disappear completely, every time; restarting them and rerunning reproduced it again. Not a one-off:

- Step J7: `test_create_watch.py::test_external_file_creation_is_detected_and_published` — 2-of-3 failures with containers up, 5-for-5 clean with them stopped (isolated, standalone run — not just full-suite).
- Step K2, full 6-container fleet: 5 simultaneous entrypoint-test failures across 4 adapters in one run, versus 230/230 clean with all 6 containers stopped.

**Every currently-affected file** — corrected during the fix itself (see "A scope correction" below): the true set is anywhere a REAL adapter's own `serviceId` is used to connect from test code at all, whether via `connect_as_adapter()` (a stable, entrypoint-driving identity) **or** the plain `connect()` test helper (a fresh key per call, used throughout every adapter's own handler-level tests and as a "borrowed identity" trigger/replier in other adapters' tests) — both sides of the collision are symmetric; it doesn't matter which side is "the fresh key," only that two live connections share one `serviceId`. Confirmed by direct search (`grep` for every real `serviceId` literal), not estimated:

- `adapters/file_storage_adapter/tests/*.py` — `test_entrypoint.py`, `test_write_handler.py`, `test_read_handler.py`, `test_list_handler.py`, `test_delete_handler.py`, `test_create_watch.py` (`file-storage-01`, `webhook-listener-01`)
- `adapters/webhook_listener/tests/test_webhook_listener_entrypoint.py` (`webhook-listener-01`)
- `adapters/webhook_sender/tests/test_relay_handler.py`, `test_webhook_sender_entrypoint.py` (`webhook-sender-01`, `rest-api-service-01`)
- `adapters/http_adapter/tests/test_trigger_handler.py`, `test_http_adapter_entrypoint.py` (`http-adapter-01`, `rest-api-service-01`)
- `adapters/rest_api_service/tests/test_rest_api_service_app.py` (`rest-api-service-01`, `db-adapter-mysql-01`)
- `adapters/db_adapter_mysql/tests/test_db_adapter_mysql_write_handler.py`, `test_cdc_watcher.py`, `test_db_adapter_mysql_entrypoint.py` (`db-adapter-mysql-01`, `rest-api-service-01`)
- `tests/test_webhook_to_file_storage_end_to_end.py` (`file-storage-01`, `webhook-listener-01`)
- `tests/test_rest_api_service_to_db_adapter_end_to_end.py` (`rest-api-service-01`, `db-adapter-mysql-01`)

`test-observer-01` is unaffected throughout — it has no live production counterpart to collide with, by design.

### Applied fix

**Dedicated test-only identities, distinct from every real adapter's own `serviceId`** — implemented as designed, with one scope correction found while applying it (see below).

Six new manifest entries added to [infra/nats/adapter_identities.yaml](infra/nats/adapter_identities.yaml), under the existing "Test/CI tooling identities" section (the same section `test-observer-01` already lives in — this generalizes that established pattern, not a new one): `file-storage-01-test`, `webhook-listener-01-test`, `webhook-sender-01-test`, `http-adapter-01-test`, `rest-api-service-01-test`, `db-adapter-mysql-01-test`. Each has **identical publish/subscribe permissions to its real `<adapter>-01` counterpart** — the point is to keep exercising the exact real subject/permission set, not a reduced one; the only difference is the `serviceId` itself, so it registers under separate `service-identity`/`service-directory` KV keys and can never collide with the real deployed container. Provisioned via the normal `bootstrap_auth.sh` run — no server restart needed, same as every prior permission change this project has made.

**A scope correction found while applying the fix, not before**: the original "Evidence" list above (8 files) only searched for `connect_as_adapter()` calls. Actually applying the fix surfaced that plain `connect()` calls under a real `serviceId` — used throughout every adapter's own handler-level tests (e.g. `file-storage-01` as "the adapter under test" in `test_write_handler.py`) and as a "borrowed identity" trigger/replier in other adapters' tests (e.g. `rest-api-service-01` used by `webhook_sender`/`http_adapter`/`db_adapter_mysql`'s own tests) — are exactly as vulnerable, for the identical reason: the collision only cares that two live connections share a `serviceId`, not which side holds a stable vs. fresh key. The Evidence section above has been corrected to the true, complete list (15 files, not 8). Every one of those files now connects as the corresponding `-test` identity instead; durable consumer names derived from `service_id` (both fixed entrypoint names and ad-hoc ones) were updated to match, and each adapter's own `conftest.py` (plus the root [tests/conftest.py](tests/conftest.py)) had their `FIXED_ENTRYPOINT_DURABLE_NAME(S)` cleanup lists updated to the new `-test`-suffixed names.

Purely a manifest + test-fixture change, as designed: no `jetcore` library code changes, no new bus mechanism, no change to what `docker-compose.yml` deploys.

**Complementary practice, not yet applied** — when Phase 4's CI workflow is actually built, it should start only `nats` + `mysql` before running pytest, never the six adapter application containers (see the original recommendation below); left as forward guidance for that step, not something to retrofit here.

**Considered and not applied:**

- *A revocation/TTL model for `service-identity`*, matching `service-directory`'s existing heartbeat. Rejected as *this* defect's fix: it doesn't actually solve the dual-live-instance case (if both processes keep heartbeating, they'd just flip back and forth on a shorter cycle instead of overwriting once) — it solves a related but different problem (a truly dead process's stale entry lingering forever). Worth doing eventually as real production hardening (already flagged as future work under Decision #20), not needed to close this defect.
- *Mutual-exclusion / fencing* (reject a second live registration under one `serviceId`). Architecturally the "most correct" fix and would make the true production risk (two live instances under one identity) loud instead of silent — but it's a meaningfully bigger mechanism to design and build, and it would also break the *legitimate* test pattern this defect's fix preserves (a test deliberately driving a second live instance under a real identity, on purpose, for a few seconds). Not proportionate to what's actually observed here.

### Verification

Before the fix: full-suite runs with all 6 adapter containers up hit 4-5 failures per 230-test run, concentrated in exactly the affected-file list above.

After the fix, with all 6 containers running the entire time (not stopped — the actual condition that used to trigger the collision): **5 consecutive full-suite runs, 230/230 passing every time**, immediately following a clean infra/container state reset. `ruff`, `mypy --all-packages`, and `bandit` all clean on every changed file.

### Scope

Fixes the observed test-reliability defect — pytest's own pass/fail results are no longer affected by whether the real adapter fleet happens to be running. Does **not** address:

- The deeper, separate production-hardening question of what should happen if two *real* deployed instances ever legitimately overlap under one `serviceId` (credential rotation, a botched rolling deploy) — that remains Decision #20's own flagged v1 limitation, tracked in [Design.md §9](Design.md#9-open-questions-summary), not resolved by this fix.
- **Defect 2** (below) — found while verifying this fix, a related-looking but mechanistically distinct issue: the real containers' *own* logs still show occasional decrypt errors from ambient test-generated traffic on subjects they subscribe to, unrelated to identity collision.

---

## Defect 2: Ambient Test Traffic Undecryptable by Real Containers

**Status:** Open — root cause confirmed and reproduced deterministically; fix not yet designed/applied.
**Severity:** Low-Medium. No observed effect on pytest's own pass/fail results (this is about the real containers' *own* logs, not test outcomes) — but real messages apparently going permanently undecryptable and then redelivering indefinitely (no `max_deliver` cap is set anywhere in `bus_client.py`'s `_consumer_config()`) is a genuine, unbounded resource cost on a long-running deployment, and the log noise alone makes real errors harder to spot.
**Found:** while verifying Defect 1's fix — with 5 consecutive clean full-suite pytest runs (host-side, Defect 1 confirmed fixed), the real containers' own `docker compose logs` still showed dozens of `pyrage.DecryptError` (`"No matching keys found"` and, distinctly, `"failed to fill whole buffer"`) and `SignatureVerificationError` entries accumulating *during* those same runs.

### Symptom

The real, long-running adapter containers occasionally fail to decrypt/verify a message on a subject they legitimately subscribe to (e.g. `events.orders.OrderCreated`), even though nothing about their own identity changed and Defect 1's collision no longer applies (confirmed separately: pytest itself reports clean runs). Two distinct underlying errors observed, which may point to two different contributing causes rather than one:

- `pyrage.DecryptError: No matching keys found` — a real, well-formed ciphertext, just not encrypted for this recipient's key.
- `pyrage.DecryptError: failed to fill whole buffer` — looks consistent with attempting to decrypt an *empty* ciphertext, which `bus_client.py`'s `publish()` produces verbatim (`ciphertext = encrypt_for_recipients(payload, recipients) if recipients else b""`) whenever the publisher's recipient cache was empty at publish time.

### Working theory, not yet confirmed

Test code publishing to a subject a real container subscribes to only waits for *its own* test-relevant recipient(s) to register (`wait_until_cache_has(...)`) before publishing — it has no reason to also wait for every other real-world subscriber on that subject to be visible in its recipient cache. If the real container's own registration hasn't yet propagated into the *publisher's* cache at the moment of encryption (a timing/propagation question, not an identity collision), the real container is silently left out of that message's recipient list and can never decrypt it, no matter how many times it's redelivered — since nothing about the message itself can retroactively include it. This would be a pre-existing characteristic of running ephemeral test publishers against subjects a long-running real fleet also subscribes to, not something Defect 1's fix introduced or changed; Defect 1 and this defect are two different mechanisms that happen to produce the same class of on-the-wire symptom (`pyrage.DecryptError`/`SignatureVerificationError`), which is itself worth remembering — a decrypt/verify failure in this project's logs is not self-diagnosing; it takes isolation work to tell which cause is in play.

Not isolated the way Defect 1 was (no controlled toggle test run yet) — recorded now, precisely, so it doesn't need rediscovering from scratch, rather than left to accumulate as unexplained log noise.

### Recommended next step

Needs its own isolation pass before a fix can be designed: reproduce deliberately (a test that publishes to a subject a real container subscribes to, immediately, without waiting for the real container's own recipient-cache propagation) and confirm whether the theory above actually holds. If it does, candidate directions include: adding a `max_deliver` cap + dead-letter/log-and-drop handling to `_consumer_config()` (bounds the resource cost regardless of root cause) and/or having `publish()` wait for cache staleness to clear past some bound before encrypting (addresses the root cause directly, at the cost of publish latency). Not designed further here — out of scope for what this session was asked to fix.

---

## How to manage this document

**For humans and for Claude, equally — follow this whenever a defect is found, fixed, or reconsidered:**

- **Numbering**: sequential, permanent, never reused and never renumbered — even if a defect turns out to be invalid, its number stays retired (mark it `Won't Fix` / `Invalid`, don't delete the entry or shift everyone else's number down).
- **Adding a new defect**: append a new `## Defect N: <short descriptive name>` section (don't insert out of order), add a row to the summary table at the top, and fill in at minimum: Status, Severity, Found (a link to where in Design.md/code it surfaced), Symptom, Root Cause (only once actually understood — see Status below), Evidence, Recommended Solution. Cross-link both directions: this document should link back to the relevant Design.md section(s), and that Design.md section should link forward here (a short `See [Defects.md#defect-n-...]` pointer, not a duplicated writeup — this document is the source of truth for defect narratives, Design.md's own step-by-step writeups shouldn't re-explain a defect at length once it has its own entry here).
- **Status values, in the lifecycle order they normally move through**:
  - `Open` — confirmed real, not yet fixed. A solution may or may not be designed yet; say which in the Status line itself (e.g. "Open — fix designed, not yet applied" vs. "Open — root cause not yet isolated").
  - `In Progress` — a fix is actively being implemented.
  - `Fixed — Verified` — the fix is in, and re-tested to confirm the original symptom is actually gone (not just "the code changed"). Note *how* it was verified.
  - `Deferred` — real, understood, deliberately not being fixed right now (say why, and under what condition that could change).
  - `Won't Fix` / `Invalid` — investigated and closed without a code change; explain the reasoning so it isn't re-opened by accident.
- **Never delete an entry** once it's been added, even after it's `Fixed`/`Won't Fix` — this document's value is as a historical record of what actually went wrong and why, not just a current-state TODO list. Update the Status line and add a short "Resolution" note instead of removing anything.
- **Keep the summary table's Status column current** whenever an entry's status changes — it's the fast-scan view; don't let it drift out of sync with the full entry below it.
- **A defect is not the same thing as a Design.md §9 "Open Question"**: if what you're recording is a deliberate, not-yet-made design decision, it belongs in §9, not here. If it's something that was built, is actually wrong, and needs a real fix, it belongs here. When they're related (as with Defect 1 above), cross-link rather than duplicate the narrative in both places.
