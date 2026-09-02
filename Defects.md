# Defects Ledger

Companion to [Design.md](Design.md) — tracks real, confirmed defects found while building this project: what's broken, how it was diagnosed, and the recommended (or applied) fix. Distinct from [Design.md §9](Design.md#9-open-questions-summary)'s "Open Questions," which are deliberate, deferred *design* choices, not bugs — a defect here may reference an §9 item (or vice versa) when the two are related, but they're tracked separately: §9 is "not decided yet, on purpose," this document is "decided, and it's wrong, here's the fix."

**Status: 5 fixed, 0 open.** See the management instructions at the bottom for how entries get added, updated, and closed. Running the test suite locally should always go through [test.sh](test.sh) (`./test.sh` in place of `uv run --all-packages pytest`) — see Defect 3.

| # | Name | Status | Severity | Found |
|---|---|---|---|---|
| 1 | [Test/Production Identity Collision](#defect-1-testproduction-identity-collision) | Fixed — Verified | Medium (test-reliability, not a production data-loss/security bug on its own) | Design.md §12 Step E4; root cause isolated §13 Step J7; confirmed at full scale §13 Step K2 |
| 2 | [Ambient Test Traffic Undecryptable by Real Containers](#defect-2-ambient-test-traffic-undecryptable-by-real-containers) | Fixed — Verified | Low-Medium (log noise + unbounded redelivery on the real containers; no observed effect on pytest's own pass/fail results) | Found and reproduced verifying Defect 1's fix |
| 3 | [Real Adapters React to Shared-Subject Test Traffic](#defect-3-real-adapters-react-to-shared-subject-test-traffic) | Fixed — Verified | Medium (8 real pytest failures per run, a regression from Defects 1/2 fully working) | Found immediately after applying Defect 2's fix |
| 4 | [`bootstrap_auth.sh` Idempotency Check Races Against Its Own Pipeline](#defect-4-bootstrap_authsh-idempotency-check-races-against-its-own-pipeline) | Fixed — Verified | Medium (script aborts, or hangs, on a genuinely idempotent rerun — blocks local dev and would intermittently break CI's own `infra/nats/up.sh` step, Design.md §14 Step L2) | Design.md §15 Step M3, a fresh-teardown `docker compose down -v` rebuild |
| 5 | [Un-acked Test Message Masked by the Old Flat AckWait](#defect-5-un-acked-test-message-masked-by-the-old-flat-ackwait) | Fixed — Verified | Low (a single test's own false-negative risk, pre-existing but latent — never actually flaked before, since nothing had reduced AckWait below the test's own observation window until now) | Design.md §16 Step N1, immediately after implementing the real per-attempt backoff delay |

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

**A second gap found later, while verifying Defect 2's fix, not at the time**: `libs/jetcore/tests/test_bus_client.py` — Track B's own foundational `BusClient` tests, predating every adapter tracked above — was missed by the original file search entirely (scoped to `adapters/` and root `tests/`, never `libs/jetcore/tests/`). It connects as bare `webhook-listener-01`/`file-storage-01` in ten places (`connect()`, `connect_as_adapter()`, and forged-event `sourceServiceId` literals alike). Fixed the same way, connecting as `webhook-listener-01-test`/`file-storage-01-test` instead. Its earlier absence from this fix is why a `SignatureVerificationError` referencing bare `webhook-listener-01` turned up during Defect 2's own verification — not a new defect, just this one's rollout finishing late.

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

**Status:** Fixed — Verified. See "Applied fix" and "Verification" below. (Fixing this surfaced a third, structural issue — see [Defect 3](#defect-3-real-adapters-react-to-shared-subject-test-traffic).)
**Severity:** Low-Medium. No observed effect on pytest's own pass/fail results (this is about the real containers' *own* logs, not test outcomes) — but real messages apparently going permanently undecryptable and then redelivering indefinitely (no `max_deliver` cap was set anywhere in `bus_client.py`'s `_consumer_config()`) was a genuine, unbounded resource cost on a long-running deployment, and the log noise alone made real errors harder to spot.
**Found:** while verifying Defect 1's fix — with 5 consecutive clean full-suite pytest runs (host-side, Defect 1 confirmed fixed), the real containers' own `docker compose logs` still showed dozens of `pyrage.DecryptError` (`"No matching keys found"` and, distinctly, `"failed to fill whole buffer"`) and `SignatureVerificationError` entries accumulating *during* those same runs.

### Symptom

The real, long-running adapter containers occasionally fail to decrypt/verify a message on a subject they legitimately subscribe to (e.g. `events.orders.OrderCreated`), even though nothing about their own identity changed and Defect 1's collision no longer applies (confirmed separately: pytest itself reports clean runs). Two distinct underlying errors observed, which may point to two different contributing causes rather than one:

- `pyrage.DecryptError: No matching keys found` — a real, well-formed ciphertext, just not encrypted for this recipient's key.
- `pyrage.DecryptError: failed to fill whole buffer` — looks consistent with attempting to decrypt an *empty* ciphertext, which `bus_client.py`'s `publish()` produces verbatim (`ciphertext = encrypt_for_recipients(payload, recipients) if recipients else b""`) whenever the publisher's recipient cache was empty at publish time.

### Root cause (confirmed)

Not a cache-propagation timing issue (the first theory, now ruled out — see "Theory considered and ruled out" below). The actual mechanism is direct, active deletion:

**Five different `conftest.py` files' `_clean_state` autouse fixtures blanket-delete *every* `service-directory` KV entry under a shared subject prefix (e.g. `events.orders.OrderCreated.*`) before every single test — including the real, live adapter containers' own registrations, not just test identities.** `adapters/db_adapter_mysql/tests/conftest.py`, `adapters/rest_api_service/tests/conftest.py`, `adapters/webhook_sender/tests/conftest.py`, `adapters/http_adapter/tests/conftest.py`, and the root `tests/conftest.py` all do the same thing: `for key in await kv.keys(filters=[f"{subject}."]): await kv.delete(key)`, unconditionally, for `ORDER_CREATED_SUBJECT` (and other shared subjects). This was always safe when no real adapter was also registered under that subject — it stopped being safe the moment Step K2 made `http-adapter-01`, `webhook-sender-01`, and `db-adapter-mysql-01` real, permanently-subscribed, always-registered containers on that exact subject.

Once deleted, a real container's entry doesn't come back until its *next* scheduled 20s heartbeat re-`PUT`s it — and since dozens of tests across five files each re-trigger the same delete, often faster than 20s apart, a real container's registration is realistically **absent for a large fraction of any full-suite run**, not a rare edge case. Any message published during one of those gaps (by a test, or by the real REST API Service container itself) is encrypted only for whichever recipients happen to be currently registered — permanently excluding whichever real containers were mid-gap, with no way for a later heartbeat to retroactively fix an already-published ciphertext. With no `max_deliver` cap on `bus_client.py`'s `_consumer_config()`, that message then redelivers forever, logging the same decrypt/verify failure every time.

**Reproduced and confirmed directly, not inferred**: polled `db-adapter-mysql-01`'s and `http-adapter-01`'s real `service-directory` entries once per second (`nats kv get`, bypassing any client-side cache entirely) while running a normal full pytest suite. Both entries showed real, direct evidence of the mechanism:

- `http-adapter-01`: present continuously, then **absent for ~8 consecutive seconds** (01:57:24–01:57:31), then present again — one delete-then-heartbeat-recovery cycle.
- `db-adapter-mysql-01`: **absent for 59 consecutive seconds** (01:57:32–01:58:31) — `db_adapter_mysql`'s own 26-test suite alone re-triggers the delete faster than any single heartbeat can catch up.

Cross-checked against `docker compose logs` for that exact same window: `db-adapter-mysql-01` logged 20 `DecryptError`/`SignatureVerificationError` entries and `http-adapter-01` logged 13, both concentrated in that window — not spread evenly across the run.

### Theory considered and ruled out

The first hypothesis (recorded in this entry's earlier draft) was that a fresh test publisher's own `RecipientCache` might not have "caught up" to a real container's registration yet at publish time — a client-side propagation-lag bug. Reading `registry.py`'s own implementation argues against this: `RecipientCache.start()` genuinely blocks on the KV watch's initial "caught up" marker before returning, and nats-py's watch semantics deliver a real, complete snapshot before that marker (confirmed behavior, not assumed — see `registry.py`'s own module docstring on this exact point). A freshly-started cache should therefore already be accurate as of the moment it starts, which it is — the actual problem isn't a stale read, it's that the entry it's accurately reading has genuinely, currently, been deleted.

### Applied fix

Both candidate directions from the original recommendation were applied together:

1. **Scoped every `_clean_state` fixture's KV-delete to test-only identities only.** All eight `conftest.py` files with this pattern (`libs/jetcore/tests`, `adapters/{webhook_listener,file_storage_adapter,webhook_sender,http_adapter,rest_api_service,db_adapter_mysql}/tests`, and the root `tests/conftest.py`) now check `service_id.endswith("-test") or service_id.startswith("test-")` before deleting a `service-directory` key — a real adapter's own registration (no matching suffix/prefix) is now never touched, no matter which subject it shares with test traffic.
2. **Added a `max_deliver` cap** (`MAX_DELIVER_ATTEMPTS = 5` in `bus_client.py`) to `_consumer_config()` — completing what the module's own docstring already described as the intended design ("eventually the consumer's own max-deliver handling") but had never actually been set, leaving NATS's own unlimited default in effect. Bounds any *remaining* undecryptable-message case (this defect's now-fixed one, or any other) to a finite number of redelivery attempts instead of forever. Required rebuilding all 6 adapter images and clearing existing durable consumers for the new config to take effect (an existing durable *attaches to*, not replaces, its prior config on `pull_subscribe` — the same finding already documented elsewhere in this project).

### Verification

Direct, targeted re-check of the exact mechanism found under Evidence: polled `http-adapter-01`'s real `service-directory` entry once a second (bypassing any cache) while running an entire 26-test `db_adapter_mysql` suite — a suite that, before the fix, reliably produced a 59-second continuous gap. **The entry's `registeredAt` timestamp never changed across the whole run** — it was never deleted, so it never needed a heartbeat to recover. `ruff`, `mypy --all-packages` clean on every changed file.

### A consequence of fixing this, not a failure of the fix itself

Applying this fix (plus completing Defect 1's own rollout — see its "Applied fix" section) made the real adapter containers *reliably* live and registered for the first time in this project's test-suite history. That, in turn, fully exposed a third, previously partially-masked issue: several tests assume they have exclusive control over a shared "placeholder" subject (`events.files.FileWriteRequested`, `events.orders.OrderCreated`) that a real, permanently-deployed adapter also legitimately subscribes to. With the real container now reliably present and responsive, it races the test's own assertions — logged as **[Defect 3](#defect-3-real-adapters-react-to-shared-subject-test-traffic)**, not fixed here.

---

## Defect 3: Real Adapters React to Shared-Subject Test Traffic

**Status:** Fixed — Verified. See "Applied fix" and "Verification" below.
**Severity:** Medium. Directly caused real, reproducible pytest failures (8 in the run that surfaced it) — a regression in observed test-suite reliability versus before Defects 1/2 were fixed, even though neither of those fixes is wrong on its own terms (see each entry's own Verification section).
**Found:** immediately after applying Defect 2's fix (plus completing Defect 1's rollout) — the very next full-suite run went from 0 failures to 8, all newly, not present in the same form before either fix.

### Symptom

Tests that publish to a "placeholder" subject a real adapter also subscribes to (`events.files.FileWriteRequested`, `events.orders.OrderCreated`) get an extra, unwanted reaction from that real container — which now reliably receives, decrypts, and acts on the *same* message the test published, via its own independent durable consumer (JetStream fans out to every interested durable, regardless of which identity created it). Two ways this manifests, both confirmed directly:

- **A shared *result* subject gets two competing publishers.** `test_success_response_publishes_request_completed` (`http_adapter`) publishes `OrderCreated`; the real `http-adapter-01` container reacts to it independently (confirmed: its own logs show a real `POST http://webhook-listener:8080/healthz` for every single test-published `OrderCreated`, at the exact cadence of the test run) and publishes its *own* `RequestCompleted` — racing the test's own handler's `RequestCompleted` for whichever one `test-observer-01`'s `[result] = await observer.fetch(...)` happens to see first. Same mechanism produced `SignatureVerificationError` noise on `file-storage-01` earlier, before Defect 1's `libs/jetcore/tests` gap was closed — a message from one of *its* tests, landing on the same shared `FileWriteRequested` subject the real container also processes.
- **A test's own precondition becomes false.** `test_publish_with_no_recipients_does_not_crash` asserts a `"no registered recipients yet"` warning fires — true only when *nothing* is registered for that subject. The real `file-storage-01` container is now always registered for `events.files.FileWriteRequested` (that's Defect 2's fix working correctly), so the precondition the test depends on no longer holds, full stop, regardless of timing.

### Root cause (confirmed)

Structural, not a bug in either Defect 1 or Defect 2's own fix: `-test` identities were deliberately given **identical** publish/subscribe permissions to their real counterparts (Defect 1's own design choice, to keep exercising the real subject/permission set). That means a real adapter and its `-test` twin are *both* legitimate, simultaneous subscribers to the same subject whenever that subject is one the real adapter also owns — which is every "placeholder" subject in this project's manifest (Decision #14), since there's no test-only subject namespace distinct from the real one. Before Defects 1/2 were fixed, the real container was often *unable* to actually complete this reaction (killed by the identity collision, or missing its own registration from the KV-delete bug) — partially masking this exact race. Fixing both made the real container a reliable, fast responder, which is what turned a rare, easy-to-miss flake into an 8-failure regression in a single run.

### Options considered

- *Give up subscribe-permission parity between `-test` identities and their real counterparts.* Not viable — most affected tests use the `-test` identity as the adapter under test; it fundamentally needs that permission to prove anything.
- *Route test traffic through entirely separate, test-only subjects.* Architecturally the cleanest option, but the highest cost (new manifest entries, new subject constants throughout the suite) and raises a real question for K3/K4, which deliberately want to prove the *real* subject against the real deployed system.
- *Make assertions robust to a second, unrelated publisher* (marker-based filtering, the pattern `test_cdc_watcher.py`/K3 already use). Doesn't fix `test_publish_with_no_recipients_does_not_crash`'s precondition problem regardless — that test's premise ("nobody is registered") is structurally false with a real fleet running, no filtering can restore it.
- **Chosen: stop the real adapter containers before running the local test suite** (`nats`/`mysql` stay up), matching the complementary CI practice already recommended under Defect 1. Removes the entire *class* of problem, not just the 8 tests that happened to surface it, with zero test-code changes. Confirmed not to cost K3/K4 anything: K4's own sync-reply correlation is already keyed strictly by `correlationId` (marker-filtered by construction, not by luck), and K3 is a deliberate, manual "prove it against the real deployed thing" step that's supposed to run with the fleet up.

### Applied fix

[test.sh](test.sh) — stops the six adapter application containers, explicitly clears `service-directory` (a stopped container's own registration doesn't clear itself; it only expires on its own 60s TTL, found empirically — a test running early enough could still see a real adapter's stale-but-not-yet-expired entry), runs `uv run --all-packages pytest "$@"`, then restarts the six containers via a `trap ... EXIT` — so they come back up whether or not pytest passed, and the stack is left in its normal running state afterward. `nats`/`mysql` are never touched. Arguments are forwarded to pytest as-is.

Documented in the script's own header that CI (Design.md §10 Phase 4, not yet built) should go a step further and never bring the adapter containers up in the first place, rather than bringing them up and stopping them again.

### Verification

`./test.sh -q`, three consecutive runs, all 6 containers left running from a prior session (the actual condition that produces the race): **230/230 passing every time**, including both symptoms from this entry (the `RequestCompleted` race and `test_publish_with_no_recipients_does_not_crash`'s precondition test). Confirmed the six containers are healthy and running again after each run (`docker compose ps`).

---

## Defect 4: `bootstrap_auth.sh` Idempotency Check Races Against Its Own Pipeline

**Status:** Fixed — Verified. See "Applied fix" and "Verification" below.
**Severity:** Medium. Doesn't corrupt any NATS state — every failure mode here is the script refusing to finish a rerun that should be a no-op. But `infra/nats/up.sh` (which calls this) is exactly what [Design.md §14 Step L6](Design.md#14-phase-4--detailed-breakdown)'s fresh-clone verification and Step L2's CI `test` job both depend on — an intermittent failure here would show up as flaky CI, not a one-off local annoyance.
**Found:** [Design.md §15 Step M3](Design.md#15-phase-5--detailed-breakdown), while rebuilding the stack from a genuine `docker compose down -v` teardown as part of writing the Phase 5 demo walkthrough — not something Step L6's own fresh-*clone* verification happened to hit, since a fresh clone's `operator/nsc` store starts empty rather than already populated with all 14 identities (the count that made this race reliably reproducible; see Root cause).

### Symptom

Re-running `infra/nats/bootstrap_auth.sh` against an *already-provisioned* `operator/nsc` store — the exact case the whole script is written to make a safe no-op — intermittently either:

- Aborts with `write /dev/stdout: broken pipe` immediately followed by `Error: the user "<some-identity>" already exists`, at a different identity on different runs (not deterministically the same one — direct evidence this is a live race, not a fixed bug tied to one specific manifest entry), or
- Hangs indefinitely partway through the `== Users ==` loop, with no error printed at all.

### Root cause

`nsc_has()` (the idempotency check every `if nsc_has ...; then skip; else add; fi` guard in the script depends on) piped three live processes straight through each other:

```bash
nsc "$@" --json 2>&1 | yqjson '(. // []) | .[].name' | grep -qx "$name"
```

`grep -qx` exits the instant it finds a matching line — which, for a store with 14 identities, is usually well before `nsc`'s `docker run` has finished writing its full JSON listing. Closing `grep`'s stdin early sends `SIGPIPE` back up the pipe to `yq`, and potentially to `nsc` itself if `yq` was still mid-read when it died — surfacing as `nsc`'s own `write /dev/stdout: broken pipe` message (part of `nsc`'s `2>&1`-merged output, so it can itself get mistaken for part of the JSON `yq` is parsing). Under `set -o pipefail`, bash reports a pipeline's exit status as the *rightmost* command with a non-zero exit — so even though `grep` genuinely found the name (a true "yes, it exists"), a `SIGPIPE`-killed `yq` sitting to its left can make the whole `nsc_has` call report failure anyway. The `if nsc_has ...` guard then takes the `else` branch and calls `nsc add user` on an identity that already exists → the observed `Error: the user "..." already exists` abort. The hang (observed once, not reliably reproduced on demand) is consistent with the same race leaving one of the `docker run` processes in a state where it's not exiting cleanly on `SIGPIPE`, though the exact mechanism there wasn't isolated further — the fix removes the race entirely rather than needing to.

This didn't surface earlier because it's a genuine race that gets *more* likely to fire as the JSON payload `nsc list users --json` produces grows — with only a handful of identities (Phase 1, when this script was written and "3 consecutive idempotent reruns" was verified per its own header comment), the whole listing fits well within a single pipe buffer and `nsc`/`yq` typically finish writing before `grep` ever reads far enough to match and exit. By Phase 3/4 the manifest has grown to 13 adapter identities (6 real + 6 `-test` twins, Defect 1's fix, + `test-observer-01`) plus `jetcore-admin` — 14 users total, per [adapter_identities.yaml](infra/nats/adapter_identities.yaml) — large enough to make the race a real, frequent occurrence rather than a theoretical one.

### Evidence

Reproduced directly, not inferred: after a `docker compose down -v` + fresh `infra/nats/up.sh` (Step M3), a second, purely idempotent rerun of `bootstrap_auth.sh` failed with `Error: the user "rest-api-service-01" already exists`. A third rerun succeeded cleanly. A tight loop of 5 further reruns hit the same failure mode again at a *different* identity (`http-adapter-01-test`) on attempt 1, and hung with no error on attempt 4 (verified no zombie `docker run`/container processes were left behind — `docker ps -a` showed nothing beyond the already-known, long-stopped unrelated container). Different failing identity each time — the signature of a live scheduling race, not a deterministic bug in one manifest entry's handling.

### Applied fix

Rewrote `nsc_has()` to fully materialize `nsc`'s (piped through `yq`) output into a shell variable via command substitution *before* running `grep` against it, rather than streaming all three through one live pipe:

```bash
nsc_has() {
  local name="$1"; shift
  local names
  names="$(nsc "$@" --json 2>&1 | yqjson '(. // []) | .[].name')"
  grep -qx "$name" <<<"$names"
}
```

Command substitution (`$(...)`) waits for `nsc | yq` to run to completion and exit before assigning `$names` — there is no longer a live downstream reader that can exit early and `SIGPIPE` anything upstream. `grep` then runs alone, against a static string, with nothing left to race.

### Verification

Before the fix: 2 failures in a tight loop of 5 reruns against the fully-populated (21-identity) store, at two different identities, plus one hang — matching the intermittent, non-deterministic signature described above. After the fix: **7 consecutive reruns, 0 failures, 0 hangs** (2 individually observed clean, then 5 more in a single batch, all exit 0) — confirmed via each run's own captured log showing zero `broken pipe`/`Error:` lines, not just a clean process exit code. Every identity ended up in its correct, expected `already exists, skipping add` state on every rerun.

### Scope

Fixes the idempotency-check race in `bootstrap_auth.sh` specifically. Does **not** change anything about the identities/permissions the script provisions, and doesn't touch `bootstrap_jetstream.sh` or `up.sh`'s own orchestration — neither showed the same live-pipe-with-early-exit pattern on inspection.

---

## Defect 5: Un-acked Test Message Masked by the Old Flat AckWait

**Status:** Fixed — Verified. See "Applied fix" and "Verification" below.
**Severity:** Low. Never actually caused a flaky test in this project's own history — it's a latent gap that only became observable once something else changed the timing it was implicitly relying on. No production/adapter-code impact at all; purely a test-suite correctness issue.
**Found:** [Design.md §16 Step N1](Design.md#16-phase-6--detailed-breakdown), immediately after implementing `ReceivedEvent.nak()`'s new per-attempt backoff delay — a full-suite run that had been clean moments earlier started failing `adapters/file_storage_adapter/tests/test_create_watch.py::test_command_triggered_creation_is_not_also_reported_by_watch` consistently (3/3 reruns), with no other tests affected.

### Symptom

`test_command_triggered_creation_is_not_also_reported_by_watch` fetches a correlated `FileCreateCompleted` event (`first`), asserts it looks right, then fetches again with a 4-second timeout expecting nothing else (`second == []`) — proving `recent_writes.py`'s suppression of the filesystem watch's own uncorrelated republish actually works. After Step N1's change, that second fetch started returning one extra item every time.

### Root cause

`first` was never acked (or nakked) by the test — a violation of this project's own stated invariant (`ReceivedEvent`'s own docstring: "Ack/nak explicitly — nothing here is auto-acked") that had been silently harmless up to this point. An un-acked message becomes eligible for ordinary JetStream redelivery, timed by the consumer's own AckWait/backoff — before Step N1, that was a flat, unset 30s default, comfortably longer than this test's own 4-second observation window, so the redelivery never actually happened *during* the test. Step N1 made `nak()`'d messages redeliver fast (2s first attempt) — but this message was never nakked *or* acked, so it was already riding the passive/silent path Step N1 also confirmed the consumer-level `backoff` genuinely governs (see that step's own verification). With `backoff[0] = 2s`, well inside this test's 4-second window, the same un-acked `first` message redelivered as a spurious "extra" item — not a real second publish from the watch at all, which is what the assertion was actually trying to detect.

Not a bug in Step N1's own change — the change just removed a large, previously-generous safety margin (30s) this one test had been unknowingly depending on, exposing a real, pre-existing gap in the test itself.

### Evidence

Reproduced directly: 3 consecutive isolated reruns of the single test, 3/3 failures, `assert second == []` failing with exactly one extra `ReceivedEvent` each time — not a flake (deterministic given the new fast backoff). After adding the missing `await first[0].ack()`, 3 consecutive isolated reruns, 3/3 clean.

### Applied fix

Added the missing `await first[0].ack()` immediately after `first`'s assertions, in [test_create_watch.py](adapters/file_storage_adapter/tests/test_create_watch.py) — every message this project's own code fetches now gets ack()'d or nak()'d exactly once, this test included, closing the gap rather than working around Step N1's own (correct) timing change.

### Verification

3 consecutive isolated reruns clean post-fix (see Evidence). Full suite via `./test.sh -q`: 236/236 passing immediately after, with Step N1's real backoff change still in place — confirms this was the only test in the whole suite relying on the old flat AckWait as an implicit safety margin (a broader search for the same "fetch, don't ack, fetch-again-expecting-empty" shape across every test file turned up no other instances).

### Scope

Fixes this one test's own gap. Does not change anything about `ReceivedEvent.nak()`'s new behavior (Design.md §16 Step N1) or suggest other tests need auditing beyond the search already done above.

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
