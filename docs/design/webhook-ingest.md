# relayfile-control: integration webhook ingest

Status: design, 2026-11-08. Author: layered-control. Reviewers: layered-relayfile
(Relayfile contracts), the implementing agent on dogpatch-mini, Khaliq (merge).
Decision source: Khaliq, 2026-11-08 (task relay-245068478901772288): integration
webhook ingest stays out of relay-control; all of it lives here.

This document is the implementation plan. It ports the *logic* of Cloud's
webhook path (webhook-worker + cloud-web nango router + core record-writer at
cloud `1a59dec`, plus PRs #6176 and #6177) into one Node + Postgres service.
It is not a line-for-line port. Where Cloud behaviour is a hard requirement it
is marked **HARD**; where it is a known Cloud defect it is marked **FIX**.

Name clash: `openapi/relayfile-control-plane-v1.openapi.yaml` in the OSS
relayfile repo is the *local daemon* contract (`relayfile control-plane serve`
over a Unix socket, API version 3). It has nothing to do with this service.
Do not model anything here on it.

## 0. Summary

```
provider / Nango / Hookdeck ──POST──▶ ingress (verify, bound, envelope, claim)
                                          │ 202 accepted | 401 | 400 | 413 | 429/503
                                          ▼
                               Postgres: webhook_envelopes (+ payload blob)
                                          │ SKIP LOCKED, per-workspace lanes
                                          ▼
                               consumer: resolve workspace → handler
                                          │
                   ┌──────────────────────┴──────────────────────┐
                   ▼                                             ▼
      Relayfile /v1/internal/webhook-envelopes       Relayfile /v1/internal/provider-writes
      (OSS + cloud)                                  (cloud only; capability-gated)
                   │                                             │
                   └───────── #6175 backpressure policy ─────────┘
                                          │
                     ack | retry(delay) | DLQ (preservation cycles, archive)
```

One process (`relayfile-control`) runs both the HTTP ingress and the consumer;
they can also be run as separate roles (`ROLE=ingress|consumer|all`) so the
hosted shape can scale them independently. Postgres is the only state.
No Cloudflare Queues, R2, KV or Durable Objects.

## 1. Stack and repo layout

Mirror relaycontrol (`@cloud/selfhost-api`) so the two services share operator
and contributor conventions:

- Node 22, TypeScript, ESM, `pg`, `zod`, no web framework beyond `node:http`
  (same as relaycontrol `src/app.ts`); tests with `node --test` + `tsx`;
  PGlite for unit tests, real Postgres (opt-in env `RELAYFILE_CONTROL_PG_TEST_URL`)
  for concurrency tests.
- Layout:
  ```
  package.json            @relayfile/control (private)
  src/server.ts           entry: config, migrate-on-start (opt), roles
  src/config.ts           every knob below, all finite, all env-driven
  src/db/                 pool, migrations runner (same shape as relaycontrol)
  src/ingress/            http routes + verification per ingress
  src/envelope/           envelope model, delivery-id rules, dedupe keys
  src/queue/              claim/lease/retry/DLQ on Postgres
  src/consumer/           lanes, concurrency, fairness, cool-down
  src/relayfile/          HMAC signer, envelopes client, provider-writes client,
                          backpressure policy (port of relayfile-backpressure.ts)
  src/providers/<p>/      github, slack, jira, linear, notion, google-mail, nango-sync
  src/recordwriter/       port of record-writer semantics (coalescing, dependsOn)
  src/entitlement/        licence/capability check against relaycontrol
  src/observability/      metrics, structured logs, reason codes
  migrations/0001_*.sql   …
  openapi.yaml            ingress + admin API
  test/                   one file per module, plus test/e2e-fake-relayfile.test.ts
  docs/design/            this file; later ADRs
  ```
- Dockerfile identical in shape to relaycontrol's (build stage → `npm pack` →
  runtime stage, non-root `node`).
- Commits end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

## 2. Ingress

### 2.1 Routes

| Route | Ingress | Who sends |
|---|---|---|
| `POST /api/v1/webhooks/github` | `github` | GitHub App directly (`x-hub-signature-256`) |
| `POST /api/v1/webhooks/nango` | `nango` | Nango forward/sync/auth events |
| `POST /api/v1/webhooks/hookdeck` + `GET ?challenge=` | `hookdeck` | Hookdeck (Standard Webhooks or legacy), Dropbox challenge |
| `POST /api/v1/webhooks/slack` | `slack` | Slack Events API directly (`x-slack-signature`), incl. `url_verification` |
| `POST /api/v1/webhooks/linear` | `linear` | Linear directly (`linear-signature`) |
| `GET /healthz`, `GET /readyz` | | liveness / DB + entitlement readiness |
| `GET /metrics` | | Prometheus text, bound to the internal listener |
| `POST /api/v1/admin/dlq/{replay,list}` etc. | | operator, bearer-authenticated (see §9) |

`/cloud/api/v1/webhooks/*` aliases are mounted too so existing Cloud webhook
registrations can be repointed by host only (§12).

Jira and Notion have no direct routes: Cloud receives them only through Nango
and does no provider-native verification (not in the code, do not invent one).
Google Mail arrives as a Nango forward of a Pub/Sub notification.

### 2.2 Verification (per ingress, raw bytes before parse) **HARD**

All comparisons use `crypto.timingSafeEqual` on equal-length buffers. Any
failure is `401 {"error":"invalid_signature"}` with no further detail. A
missing configured secret for an ingress is `503 {"error":"ingress_not_configured"}`
and is counted (`ingress_unconfigured_total{ingress}`), never a 401.

- **GitHub**: `x-hub-signature-256` must match `/^sha256=[a-f0-9]{64}$/i`;
  expected = `sha256=` + HMAC-SHA256(secret, raw body) hex. Secret
  `GITHUB_WEBHOOK_SECRET`. Multiple secrets allowed (comma list) for rotation.
- **Nango**: `x-nango-hmac-sha256` = HMAC-SHA256(secret, raw body) hex, else
  legacy `x-nango-signature` = sha256(secret + raw body) hex, else
  sha256(secret + JSON.stringify(JSON.parse(raw))). No timestamp window (Cloud
  has none). Secret `NANGO_WEBHOOK_SECRET`.
- **Hookdeck**: Standard Webhooks (`webhook-id`, `webhook-timestamp`,
  `webhook-signature` = `v1,<base64 HMAC(key, "${id}.${ts}.${body}")>`, key
  from `whsec_<base64>`, ±300 s) **or** legacy `x-hookdeck-signature` /
  `x-hookdeck-signature-2` = base64 HMAC-SHA256(secret, raw body). Skipped for
  provider ∈ {gitlab, bitbucket, granola, shortcut} (Cloud rule), then the
  Nango signature is required unless the request is one of the raw-provider
  branches. `GET ?challenge=` echoes text/plain 200; plain `GET` is 405
  `Allow: POST`. `x-dropbox-signature` branch: HMAC-SHA256(DROPBOX_APP_SECRET,
  raw body) hex.
- **Slack**: `x-slack-signature` = `v0=` + HMAC-SHA256(signing secret,
  `v0:${ts}:${body}`) hex; `x-slack-request-timestamp` within ±300 s.
  `url_verification` answered `200 {"challenge"}` before any queueing.
- **Linear**: `linear-signature` = HMAC-SHA256(secret, raw body) hex, and body
  `webhookTimestamp` within ±60 s (Linear's documented contract; Cloud's
  implementation is in the external `@relayfile/adapter-linear`, not visible,
  so this is a documented decision, not a port).
- **Body bounds** (streamed, not just Content-Length): `INGRESS_MAX_BODY_BYTES`
  default 25 MiB for github, 1 MiB for every other ingress (Cloud had no limit
  on nango/hookdeck; **FIX**). Over limit → `413 {"error":"payload_too_large"}`
  after the signature check fails or succeeds (read is capped, never buffered
  past the limit).
- Content-Type is not trusted for parsing; JSON parse failure after a valid
  signature → `400 {"error":"invalid_json"}`.

### 2.3 Envelope and delivery id

Every accepted request becomes one `webhook_envelopes` row (the Cloud "envelope
v2" union collapses to one shape; `kind` discriminates):

```
kind            nango-forward | nango-sync | nango-auth | provider-webhook | hookdeck-raw
ingress         github | nango | hookdeck | slack | linear
provider        github | slack | jira | linear | notion | google-mail | gitlab | … (free text ≤64)
request_id      ULID minted at ingress (also returned to the sender)
delivery_id     see precedence
dedupe_surface  see §3
dedupe_id       see §3
headers         jsonb, lower-cased names, allow-listed (never Authorization)
payload         inline jsonb when ≤ 96 KiB, else payload_blob_id → webhook_payload_blobs (bytea + sha256)
payload_sha256  always
received_at     timestamptz
workspace_id    nullable until the consumer resolves it (see §5)
```

Delivery id precedence (Cloud's, kept): github `github:<x-github-delivery>`;
nango `x-nango-delivery-id` → `x-nango-webhook-id` → `webhook-id`; gitlab
`x-gitlab-event-uuid`; slack `event_id`; linear `linear-delivery` header →
`payload._connection.deliveryId`; jira `x-atlassian-webhook-identifier`;
notion `payload.id`; generic `x-github-delivery` → `webhook-id` →
`x-hookdeck-eventid`; otherwise the request id; the final fallback is
`sha256(normalized payload)` (only in the consumer, where the provider shape is
known).

### 2.4 Ingress response contract

- `202 {"accepted":true,"requestId","queue":"webhook-events"}` once the row is
  committed (durable). Cloud returned a queue messageId; `requestId` is the
  equivalent and is the correlation id end-to-end.
- `202 {"accepted":true,"dropped":true,"reason":"missing_nango_provider_identifier"}`
  for Nango bodies without a provider identifier (Cloud drop rule, kept).
- `200 {"accepted":true,"duplicate":true}` when the ingress-level dedupe key
  already exists (§3) — the sender's retry is satisfied, nothing is re-queued.
- `429 {"error":"ingest_backpressure","retryAfterSeconds"}` + `Retry-After` when
  the ingress admission gate (§6.4) refuses. Providers (GitHub, Slack, Nango,
  Hookdeck) all retry on non-2xx; this is deliberate shedding with a reason.
- `503 {"error":"storage_unavailable"}` when Postgres is unreachable.

Every non-2xx carries `x-relayfile-control-request-id` and is logged once with
`reason` (§10).

## 3. Idempotency and dedupe **HARD**

Table `webhook_delivery_claims(surface text, dedupe_id text, status text,
lease_until timestamptz, envelope_id, attempts int, created_at, completed_at,
PRIMARY KEY (surface, dedupe_id))`, status ∈ `processing | completed | failed`.

Two levels, both in Postgres (Cloud's Notion claim was in-memory — **FIX**):

1. **Ingress dedupe** (surface = `ingress:<ingress>`), key = `len:value|…`
   length-prefixed tuple of `(provider, delivery_id)` when a real provider
   delivery id exists. Insert is `INSERT … ON CONFLICT DO NOTHING RETURNING`
   inside the same transaction as the envelope row. Conflict → `200 duplicate`.
   No TTL on completed rows except the retention sweep (§8). Requests whose
   only id is the minted request id are never deduped here.
2. **Handler claims** (surfaces exactly as Cloud: `nango-forward` keyed by
   request id, `slack` keyed by `workspace:slack:<event_id>` or the fallback
   tuple, `google-mail-forward-sync` / `google-mail-forward-trailing-sync` with
   TTL coalescing, `notion` keyed by `did:<id>` or the entity tuple, `jira`,
   `linear`). Claim = atomic
   `INSERT … ON CONFLICT (surface, dedupe_id) DO UPDATE SET status='processing', lease_until=now()+lease WHERE claims.status='failed' OR claims.lease_until < now() RETURNING`;
   no row returned ⇒ already claimed/completed ⇒ handler returns `duplicate`
   (acked). Lease 15 min (`CLAIM_LEASE_SECONDS`). `completed` only on handler
   success; `failed` on handler failure (release); a crashed worker's lease
   expires. Google-mail TTL coalescing: a `completed` row younger than
   `GMAIL_SYNC_COALESCE_SECONDS` is treated as a duplicate (Cloud behaviour).

Both levels share one implementation (`src/queue/claims.ts`) with tests for:
concurrent claim race (real PG), expired-lease takeover, failed→reclaim,
completed→duplicate, TTL coalescing.

## 4. Queue and consumer

### 4.1 Storage

`webhook_envelopes` doubles as the queue (no second table to keep consistent):

```
state            queued | leased | done | dead
attempts         int, 0..MAX_ATTEMPTS (default 5, Cloud maxRetries)
visible_at       timestamptz   (retry delay lands here)
leased_by        text, lease_until timestamptz (consumer heartbeat extends)
lane             text          (= workspace_id when resolved, else 'unresolved:<provider>:<connection>')
last_outcome     jsonb         {disposition, status, reason, retryDelaySeconds, …} (§10 fields)
preservation_cycle int         (DLQ cycles, §8)
```

Indexes: `(state, visible_at)` partial on `state='queued'`; `(lane, state)`;
`(received_at)` for the sweep.

### 4.2 Claiming with bounded concurrency and per-workspace fairness **HARD**

The consumer runs `CONSUMER_CONCURRENCY` (default 10, Cloud batch concurrency)
worker slots per process. Each slot claims one envelope with:

```sql
WITH pick AS (
  SELECT id FROM webhook_envelopes
   WHERE state='queued' AND visible_at <= now()
     AND lane NOT IN (SELECT lane FROM lane_cooldowns WHERE until > now())   -- §6.3
     AND lane NOT IN (SELECT lane FROM webhook_envelopes
                       WHERE state='leased' GROUP BY lane
                      HAVING count(*) >= $per_lane_max)                     -- fairness
   ORDER BY visible_at, received_at
   FOR UPDATE SKIP LOCKED LIMIT 1)
UPDATE webhook_envelopes e SET state='leased', leased_by=$worker, lease_until=now()+$lease
  FROM pick WHERE e.id = pick.id RETURNING e.*;
```

- `PER_LANE_MAX_INFLIGHT` default 2: one workspace in a storm cannot occupy
  more than two of the ten slots per process, so other workspaces keep
  flowing. Cloud had no such isolation (lanes were keyed by dedupe id only,
  so one workspace could take every slot) — **FIX**, and the direct cause of
  the Nov 7 cross-tenant stall.
- Lane assignment happens at ingress when the workspace can be resolved
  cheaply (GitHub installation id, Slack team id, Nango connection id → cached
  binding lookup, §5); otherwise the consumer resolves and re-lanes the row
  before handling. Unresolved rows get `lane='unresolved:<provider>:<cid>'`
  so a flood of unroutable deliveries also cannot starve others.
- Lease heartbeat every `LEASE_SECONDS/3`; a handler that exceeds its
  deadline is aborted (AbortSignal) and the row retried with the normal delay.
- Multiple consumer processes are safe (SKIP LOCKED); fairness is per process,
  global fairness is approximated by the cool-down table and the per-lane cap
  counted across all leased rows (the query above counts globally).

### 4.3 Deadlines **HARD**

`handlerDeadlineMs(provider)` = 120 000 for provider ∈ {github, google-mail,
linear, notion} (prefix match on `-`), else 15 000 (Slack, Jira, everything
else). Same table drives `inPlaceRetryPolicy` (§6.2). Payload blob read is
bounded by `PAYLOAD_READ_TIMEOUT_MS` 15 000.

### 4.4 Dispositions **HARD**

| Handler outcome | Disposition |
|---|---|
| success / duplicate / unmatched (202 ignored) | `ack` → `done` |
| `TerminalHandlerError` (bad contract, 400/422-class, `retryable:false`) | `ack_terminal` → `done` with reason; counted |
| `RelayfileBackpressureError` (final 429/503 from Relayfile) | `retry` with `delay = resolveForwardRetryDelaySeconds({attempts, retryAfterSeconds})`, arm cool-down (§6.3) |
| any other error / timeout / network | `retry` with the schedule delay |
| attempts ≥ `MAX_ATTEMPTS` | `dead` → DLQ (§8); **never silently dropped** |

Retry schedule (port of `queue-retry-delay.ts`): `min(900, max(60,
60·2^(attempts−1)·(1+U(0,0.25))))`; a server hint can only lengthen:
`ceil(max(hint·(1+U(0,0.25)) capped 86 400, schedule))`. Property tests:
monotone in attempts, never below hint, never above 86 400.

## 5. Workspace resolution and bindings

relayfile-control owns the *integration binding* table, ported from Cloud's
`workspace_integrations` lookups:

```
integration_bindings(id, workspace_id (rw_… relayfile workspace), provider,
  connection_id (Nango), provider_config_key, native_ids jsonb
  (installation_id | team_id/enterprise_id | organization_id | cloud_id | email),
  relayfile_base_url, status, created_at, updated_at)
UNIQUE (provider, connection_id); GIN on native_ids
```

Resolution order **HARD**: connection id → provider-native id (GitHub
installation fans out to *every* matching workspace for the direct route;
Nango forward takes first match and logs `ambiguous_binding`) → `unmatched`
(ack with reason `unmatched_workspace`; direct GitHub returns 202
`{ignored:true}` at ingress when the installation is unknown and
`INGRESS_REJECT_UNKNOWN_INSTALLATION` is off). Workspace ids must start with
`rw_` or the binding is refused at write time (`invalid_workspace_id`,
terminal).

Bindings are written by relaycontrol (or an operator CLI) through
`POST /api/v1/admin/bindings` (§9); this service never talks to Nango's API to
discover them. Nango `auth` / `connection.created` events upsert a *pending*
binding keyed by connection id so forwards arriving before the control-plane
write can still lane correctly.

## 6. Writing to Relayfile

### 6.1 Client and signing **HARD**

`src/relayfile/client.ts` targets one Relayfile base URL per binding
(`relayfile_base_url`, default `RELAYFILE_BASE_URL`). Every request:

- `X-Relay-Timestamp` = RFC3339 UTC now, `X-Relay-Signature` = lowercase hex
  HMAC-SHA256(`RELAYFILE_INTERNAL_HMAC_SECRET`, `${timestamp}\n${rawBody}`),
  `X-Correlation-Id` = envelope request id (always present), `Content-Type:
  application/json`, explicit `Content-Length`, `redirect: "manual"` (a
  redirect is a terminal failure, never followed).
- **Fresh timestamp and signature on every attempt** (OSS replay guard
  rejects identical bytes+headers). The signer is called inside `post()`.
- Secret is one per installation, read from `RELAYFILE_INTERNAL_HMAC_SECRET`
  (same env name as the OSS server so the chart wires one Secret ref). Never
  logged.

Envelopes: `POST /v1/internal/webhook-envelopes` body limited to the OSS
required set (OSS is `additionalProperties:false`): `envelopeId, workspaceId,
provider, deliveryId, receivedAt, correlationId, payload, headers?`.
`envelopeId = provider + "_" + sha256(JSON.stringify([workspaceId, deliveryId,
path, providerEventType, eventType]))` (stable across retries). `202 queued`,
`202 repaired` and `409 duplicate_envelope` are all acks.

Provider-writes: `POST /v1/internal/provider-writes` body
`{workspaceId, jobId?, files[], event:{path, providerEventType, resourceRef?}}`
with the validated invariants: single provider per batch, every path under
`/<provider>/` (github|jira|linear|slack), `/discovery/<provider>/` or
`/LAYOUT.md`; exactly one file equals `event.path`; `providerEventType`
matches `^[a-z_]{1,60}(\.[a-z_]{1,60})?$`; terminal provider states
(closed/merged/archived) are written as data, never deletes. Slack canonical
files carry `contentIdentity {kind:"provider_event", key: sha256([workspaceId,
path, providerEventType, deliveryId]), ttlSeconds: 86400}`. 413 and 400/401/403
are terminal. **Provider-writes exists only in relayfile-cloud**; OSS has no
such route. See §11 capability.

### 6.2 Backpressure policy (port of `relayfile-backpressure.ts`, cloud#6175) **HARD**

Port the module nearly verbatim as `src/relayfile/backpressure.ts`, including
its tests:

- In-place retry only on 429, exactly as Cloud final (`retryRelayfileBackpressure`
  loops while `response.status === 429`). No branching on body `reason`.
  Retrying 503 in place (layered-relayfile's ask for cloud `storage_full`/
  `root_event_outbox_full` and OSS secret-unset) is an explicit option
  `RELAYFILE_BACKPRESSURE_RETRY_503` default **off**, so the default is a
  behaviour-identical port; a 503 without the option is a `retry` disposition
  (§4.4) with the schedule delay, never an ack.
- One wall-clock budget bounds when a retry may *start*; per-attempt caller
  timeout; strictly sequential; a rejected `post()` propagates at once.
- Delay precedence: `Retry-After` (delta-seconds or IMF-fixdate) → body
  `details.retryAfterSeconds`/`retryAfterSeconds` (clone, ≤4096 bytes, ≤1 s
  read) → jittered exponential fallback (base 1 s doubling cap 8 s ±20 %).
  The server hint is a hard minimum lengthened by uniform jitter up to 25 %;
  zero/elapsed hints use the fallback schedule.
- Exhausted ⇒ throw `RelayfileBackpressureError {status, retryAfterMs,
  attempts, surface}` with `surface ∈ webhook-envelopes | provider-writes`.
- `inPlaceRetryPolicy(provider)`: `{maxRetries:1, budgetMs:30000,
  attemptTimeoutMs:15000}` for long-deadline providers, `{maxRetries:0,
  budgetMs:0}` otherwise (Slack/Jira under the 15 s deadline). Envelope
  admission: `ADMISSION_MAX_RETRIES 1`, `ADMISSION_RETRY_BUDGET_MS 30 000`,
  `ADMISSION_ATTEMPT_TIMEOUT_MS 15 000`. Provider-writes:
  `PROVIDER_WRITE_MAX_RETRIES 1`, `PROVIDER_WRITE_RETRY_BUDGET_MS 30 000`
  (Cloud sets no separate per-attempt timeout there; the handler deadline
  bounds it).
- **Jira native comment path** (`jira:comment_created`, `objectType ==
  "comment"`, Cloud router ~8355-8385): the per-item writes run under the same
  policy; the FIRST `RelayfileBackpressureError` is kept (`backpressure ??=`)
  and rethrown as the typed error after the loop, before the generic
  "normalization or admission failed" error, so the consumer's `retry`
  honours Retry-After instead of the fixed schedule. Port
  `nango-webhook-router-jira-forward-backpressure.test.ts`.
- Record-writer bulk path: `BULK_WRITE_BUSY_RETRY_MAX_ATTEMPTS 6`, base 500 ms,
  max 30 s, deadline 240 s, retrying only 429 `workspace_busy`/`queue_full`;
  a final 429 surfaces as `backpressureError` on `BatchWriteResult` and is
  rethrown as `RelayfilePrimaryWriteError {cause, retryAfterMs}`.

End-to-end **HARD**: a final backpressure error must become `retry` with
`retryAfterSeconds = max(1, ceil(retryAfterMs/1000))` or default 60
(`RELAYFILE_BACKPRESSURE_DEFAULT_RETRY_AFTER_SECONDS`), never an ack, never a
drop. This is Cloud's route-handler "typed `RelayfileBackpressureError` →
503 + Retry-After" contract collapsed into one process: there is no HTTP hop
between route and consumer here, so the 503 becomes the `retry` disposition
with the same `retryAfterSeconds`; the error is detected by `instanceof` OR
`error.name === "RelayfileBackpressureError"` OR the same on `error.cause`
(`detectRelayfileBackpressure`), exactly as Cloud. Never an ack, never a drop, and the envelope's `last_outcome` records
`{disposition:"retry", code:"relayfile_backpressure", status, surface,
relayfileReason, retryAfterSeconds, retryDelaySeconds, attempts}`. Test:
fake Relayfile returning 429 forever ⇒ exactly `maxRetries+1` posts per
attempt, N queue attempts with lengthening delays, then DLQ with full reason
chain; a 429 followed by 202 ⇒ ack.

### 6.3 Workspace cool-down (cloud#6177 Option D, optional, **default off**)

Cloud's version was isolate-local and keyed by Nango connection because the
worker did not know the workspace. Here the consumer knows the lane, so:

- Table `lane_cooldowns(lane PK, until timestamptz, armed_by, reason)`; armed
  on a final backpressure error with window = Retry-After delta > 0 → body
  `retryAfterSeconds` > 0 → 60, clamped [1, 86 400]; lengthen-only; expire
  on read. `COOLDOWN_ENABLED` default **false** (Khaliq, 2026-11-08: ship as
  a default-off option), `COOLDOWN_MAX_KEYS` 1000 (LRU prune by oldest
  `until`). Arming contract identical to #6177 @bda4057: arm only on a final
  backpressure error (never on 429/502/other 5xx/timeouts/network); window =
  Retry-After header → body `retryAfterSeconds` → 60; clamp [1, 86 400];
  longer extends, shorter never shortens; skip delay = the §4.4 schedule with
  `retryAfterSeconds = ceil(remaining)`; one `forward_cooldown_skip` warn per
  skip with the 16-hex key hash, `cooldownArmedSeconds` on the rejected line;
  no payload, raw key, connection id or secret logged. Port the 12 tests from
  `workspace-cooldown.test.ts`. Known differences from Cloud, both deliberate:
  key = lane (workspace is known here) instead of `providerConfigKey +
  connectionId`; store = table shared by all consumers instead of
  isolate-local memory; a skip does NOT consume a queue attempt (rows are
  simply not claimed), whereas Cloud's skip costs one of the 6 deliveries.
- Effect: the claim query skips cooled lanes (§4.2) — no work is dequeued for
  a workspace that just told us it is busy, so the budget goes to other
  workspaces. It is advisory: rows stay `queued` with their own `visible_at`
  and are never acked or dropped; attempts are not consumed by a skip.
  Metric `cooldown_skips_total{provider}`, log `forward_cooldown_skip` with
  `laneHash` (sha256 16 hex), `cooldownRemainingSeconds`.

### 6.4 Ingress admission gate (end-to-end backpressure)

Backpressure must reach the sender, not just the queue. The ingress refuses
with `429 ingest_backpressure` + `Retry-After` when any of:

- queued rows for the target lane ≥ `LANE_MAX_QUEUED` (default 5000), or
- total queued ≥ `GLOBAL_MAX_QUEUED` (default 100 000), or
- oldest queued age ≥ `MAX_QUEUE_AGE_SECONDS` (default 3600) and the lane is
  cooled down.

Counts come from a `queue_depth` materialised counter maintained by triggers
(not `count(*)` per request). GitHub, Slack, Nango and Hookdeck all redeliver
on 429 with their own backoff, which is the desired behaviour: the storm backs
up to the provider's redelivery queue instead of filling Postgres. Every
refusal logs `{reason: lane_queue_full | global_queue_full | lane_stale_cooldown,
laneHash, queued, retryAfterSeconds}`. Cloud's admission gate was a
fail-closed run-id switch; this replaces it with a measured gate (**FIX**).
The switch form is kept as `INGRESS_PAUSED=1` for operators.

## 7. Record writer (port of `record-writer.ts` semantics + cloud#6176)

Scope: GitHub issues and pull requests, Linear and Jira normalised batches,
Slack messages/reactions, Notion pages, Google-mail incremental sync. Behaviour
to keep:

- `RelayfileWriteClient` interface (`readFile`, `listTree`, `writeFile{baseRevision, dependsOn?}`,
  `bulkWrite{files, correlationId}` → `{written, errorCount?, errors[], dedupedFiles?}`,
  `deleteFile{baseRevision}`), namespacing client prefixing `/{provider}/{connectionId}/`,
  managed writes `baseRevision:"*"`, 404-tolerant deletes, deletes stamped
  `_nango_metadata.last_action = deleted`, `WRITE_CONCURRENCY 10`,
  `BULK_WRITE_MAX_REQUEST_BYTES 8 MiB`, under-commit error when
  `written + deduped + errors < sent` (keep 5 error samples).
- Stale check: skip when the incoming provider timestamp is older; unknown or
  equal timestamps are not stale. Reads of canonical and by-id alias happen
  once and are passed as `preloadedPrimary`.
- **PR coalescing (cloud#6176)**: for GitHub `pull_request` only, equal
  `updated_at` + identical content (key-order-insensitive, arrays ordered,
  `_nango_metadata` ignored) ⇒ `skipped`, no write, no watch event, log
  `dedupKind: equal-timestamp-identical`. Both canonical and by-id alias must
  match. **Repair on identical retry** (the two review follow-ups): the skip
  additionally requires `githubPullRequestAuxiliaryEffectsComplete` (dry-run
  the auxiliary emitter against a recording client; skip only if every planned
  write is already stored with equivalent content and no delete is pending),
  and the stale-alias-delete ledger guard (`githubNumberedObjectKey`: once a
  delete for an object fails, its remaining writes are rejected so the by-id
  alias keeps describing the old state; in staged bulk mode each numbered write
  carries `dependsOn` on its deletes and Relayfile fails it with
  `dependency_failed`). Source = #6176 head 5158f41 (802b0f4 coalescing,
  fe700c9 auxiliary-effects repair, 5158f41 alias-cleanup intent). Port the
  21 tests from #6176 (`record-writer.test.ts`) plus the +33 lines #6175 added
  to the same file (bulk-write busy retry).
- Auxiliary files staged after canonical with `dependsOn`; dependents of
  failed paths skipped.

Dispatch to agents (Cloud's `enqueueIntegrationWatchEvent`) is **not** ported
here: relayfile-control writes files; agent fan-out is Relayfile's watch
pipeline. The ordering quirks Cloud had (GitHub enqueue before write, Linear
after) therefore disappear. If Khaliq wants the shared integration-watch
dispatch in this service, it is a separate design.

## 8. DLQ, preservation and replay

- `dead` rows stay in `webhook_envelopes` with `preservation_cycle`. A sweeper
  (every 60 s) re-queues dead rows whose cycle < `DLQ_MAX_PRESERVATION_CYCLES`
  (default 3, range 0–10) with delay `min(300, 60·2^min(cycle,3))` and
  `attempts=0, cycle+1` — Cloud's preservation loop, now a DB job.
- Exhausted rows move to `webhook_dlq_archive` (full envelope, payload blob
  retained, outcome chain) and raise alert `webhook_dlq_preservation_exhausted`
  (severity 9, metric + structured log). Archive retention
  `DLQ_ARCHIVE_RETENTION_SECONDS` default 1 209 600 (14 d).
- Operator replay: `POST /api/v1/admin/dlq/replay {ids|lane|provider,
  since}` re-queues archive rows with `attempts=0`, `cycle=0`,
  `replayed_by`; `GET /api/v1/admin/dlq?lane=&provider=&limit=` lists.
- Retention sweep: `done` envelopes and completed claims deleted after
  `ENVELOPE_RETENTION_SECONDS` default 7 d; payload blobs cascade.

## 9. Admin and auth

Admin routes require `Authorization: Bearer <RELAYFILE_CONTROL_ADMIN_TOKEN>`
(constant-time compare; a comma list allows rotation) and are only mounted on
the internal listener (`ADMIN_LISTEN`, default `127.0.0.1:3101`) so they are
never on the ingress hostname. Routes: bindings CRUD, DLQ list/replay, lane
cool-down list/clear, queue stats, `POST /api/v1/admin/ingress/pause`.

## 10. Observability **HARD** (the Nov 7 storm must be diagnosable)

Structured JSON logs (one line per event, `area`, `requestId`, `laneHash`,
`provider`, `ingress`, never payloads, never secrets). Mandatory events:

| event | when | required fields |
|---|---|---|
| `ingress_accepted` | 202 | kind, provider, bytes, inline/blob |
| `ingress_rejected` | any non-2xx | status, **reason** (`invalid_signature`, `payload_too_large`, `ingest_backpressure:<sub-reason>`, `invalid_json`, `ingress_not_configured`, `storage_unavailable`) |
| `consume_outcome` | each attempt | disposition, attempts, status, code, surface, **relayfileReason** (body `reason`/`error` ≤120 chars), retryAfterSeconds, retryDelaySeconds, durationMs, payloadReadMs, relayfileMs |
| `relayfile_backpressure` | every 429/503 from Relayfile, including ones retried in place | surface, status, relayfileReason, retryAfterMs, attempt, budgetRemainingMs |
| `forward_cooldown_skip` / `cooldown_armed` | §6.3 | laneHash, remaining/armed seconds |
| `dlq_preserve` / `dlq_exhausted` | §8 | cycle, delay |

Metrics (Prometheus, `relayfile_control_` prefix): `ingress_requests_total{ingress,status,reason}`,
`envelopes_queued`, `queue_oldest_age_seconds`, `lane_queued{lane_hash}` (top-N
only, bounded cardinality), `consume_total{provider,disposition,code}`,
`relayfile_backpressure_total{surface,status,reason}` (Cloud had **no**
429-specific metric — **FIX**), `relayfile_request_seconds{surface}` histogram,
`cooldown_lanes`, `dlq_rows{state}`, `claims_duplicates_total{surface}`.

Alerts (as rules shipped in the chart): backlog ≥1000 or oldest ≥15 min
(`webhook_queue_backlog_stale`), DLQ backlog stale, preservation exhausted,
`relayfile_backpressure_total` rate > threshold per surface (new).

`GET /readyz` reports DB, migration version, entitlement state and the ingress
pause flag.

## 11. Entitlement (relayfile is a licence add-on)

relayfile-control refuses to run its ingress unless the installation is
entitled to `relayfile`:

- At start and every `ENTITLEMENT_REFRESH_SECONDS` (default 300) it reads
  `GET <RELAYCONTROL_BASE_URL>/api/v1/license` (relaycontrol feature-switch
  DTO, PR #37) with a service bearer `RELAYCONTROL_SERVICE_TOKEN`, and requires
  `capabilities.relayfile === true`.
- Offline: it also accepts the licence document directly
  (`RELAYCONTROL_LICENSE_DOC` or a file) and verifies it locally: Ed25519 over
  `"relaycontrol-license-v1\n" + payload` with the public key published by
  relaycontrol (`RELAYCONTROL_LICENSE_PUBLIC_KEY`), checking `expires_at` and
  `features ∋ relayfile`. The two sources are OR-ed; the last good result is
  cached with `ENTITLEMENT_GRACE_SECONDS` (default 86 400) so a relaycontrol
  outage does not stop ingest.
- Not entitled: ingress returns `403 {"error":"feature_not_licensed"}` and the
  consumer pauses (rows stay queued); `/readyz` says why.
- Capability `provider_writes`: decided 2026-11-08 (Khaliq): OSS Relayfile
  gets a provider-writes route too (a relayfile PR, coordinated with
  layered-relayfile), so self-host reaches canonical-record parity. The
  capability is therefore NOT flavour-gated. It is on when the installation
  sets `RELAYFILE_PROVIDER_WRITES=true` (the Relayfile target serves
  `POST /api/v1/workspaces/{ws}/provider-writes`); until the OSS route lands
  the OSS default is `false` and the record-writer stage is skipped with
  reason `provider_writes_unsupported` (logged once per lane per hour).
  `RELAYFILE_FLAVOR` stays only to select response-code nuances (§6.2).
- `/readyz` stays 200 and reports entitlement in its body; only database or
  migration failure makes the pod unready (otherwise the 403 is never seen
  behind a load balancer).

## 12. Deployment shapes and migration from Cloud

- **Self-host**: Helm chart `relayfile-control` (helm-charts repo, owned by the
  Helm lane; or a sub-chart of the relayfile add-on in infra #79). One
  Deployment (`ROLE=all`, 1 replica is safe; >1 is safe for the consumer,
  ingress is stateless), one Service + Ingress for `/api/v1/webhooks/*`, an
  internal ClusterIP for admin/metrics, the shared RDS/Postgres (own database
  `relayfile_control`), Secret refs: `RELAYFILE_INTERNAL_HMAC_SECRET` (same
  Secret as the relayfile server), per-ingress webhook secrets,
  `RELAYCONTROL_SERVICE_TOKEN`, `RELAYFILE_CONTROL_ADMIN_TOKEN`. Secret
  creation is a human gate. No egress except to Relayfile and relaycontrol.
- **Hosted**: same image, `ROLE=ingress` ×N behind the load balancer and
  `ROLE=consumer` ×M; one Postgres. Ingress hostname replaces the Cloudflare
  webhook-worker hostname.
- **Migration**: Cloud's routes `/api/v1/webhooks/{github,nango,hookdeck}` and
  the `/cloud/api/...` aliases keep their paths, so cutover is a DNS/host
  change in GitHub App, Nango and Hookdeck settings plus the Slack/Linear
  direct registrations. Steps: (1) deploy with `INGRESS_PAUSED=0` and bindings
  imported from Cloud `workspace_integrations` via the admin API; (2) dual-run
  (providers redeliver on failure, both sides dedupe by delivery id, Relayfile
  dedupes by envelopeId); (3) repoint providers; (4) drain the Cloudflare
  queue; (5) remove the worker. No step touches production without Khaliq's
  own go.

## 13. Test plan and acceptance

Unit (PGlite): every signature scheme with known vectors (good, bad, missing
secret, rotation); body bounds; delivery-id precedence; dedupe key encoding;
claim state machine; retry-delay properties; backpressure module (port
Cloud's tests); route failure classification; cool-down store; entitlement
verifier (good/expired/wrong-feature/bad-signature); record-writer incl. the
#6176 21 tests.

cloud#6175 regression tests, ported one-to-one (Cloud path → here), all
**HARD** for PR 1 unless marked PR 2:

| Cloud @ 1a59dec | relayfile-control |
|---|---|
| `web/lib/integrations/relayfile-backpressure.test.ts` (455 lines) | `test/relayfile/backpressure.test.ts` |
| `web/lib/integrations/github-webhook-admission-retry.test.ts` | `test/relayfile/admission-retry.test.ts` |
| `web/lib/integrations/github-webhook-ingest.admission-retry.test.ts` | `test/consumer/admission-retry.test.ts` |
| `web/lib/integrations/github-webhook-ingest.provider-writes-backpressure.test.ts` | `test/providers/github/provider-writes-backpressure.test.ts` (PR 2) |
| `web/lib/integrations/nango-webhook-route-handler.test.ts` (+69, backpressure classification) | `test/consumer/dispositions.test.ts` |
| `web/lib/integrations/nango-webhook-router-jira-forward-backpressure.test.ts` | `test/providers/jira/forward-backpressure.test.ts` (PR 2) |
| `webhook-worker/src/queue-retry-delay.test.ts` (113 lines) | `test/consumer/retry-delay.test.ts` |
| `webhook-worker/test/queue.test.ts` (+148, Retry-After honoured, never ack) | `test/consumer/queue.test.ts` |
| `core/tests/record-writer.test.ts` (+33, bulk busy retry) | `test/recordwriter/record-writer.test.ts` (PR 2) |
| `webhook-worker/test/workspace-cooldown.test.ts` (#6177, 12 tests) | `test/consumer/cooldown.test.ts` |

Each ported file keeps Cloud's test names so a reviewer can diff them.

### 13.1 cloud#6175 backfill checklist (Khaliq, 2026-11-08, message 245080494008729600)

| Final behaviour (cloud trunk 1a59dec / main e1ae98ef) | Lands in |
|---|---|
| single in-place retry for admission + provider-writes | §6.2 policy constants |
| lengthen-only positive jitter, server Retry-After as a hard floor | §6.2 (hint ×(1+U[0,0.25))), §4.4 (queue schedule, 86 400 cap) |
| typed `RelayfileBackpressureError` → 503 + Retry-After, incl. Jira native-comment path | §6.2 end-to-end paragraph + Jira bullet, §4.4 `retry` disposition |
| Slack/Jira 0 in-place retries under the 15 s forward deadline | §4.3, §6.2 `inPlaceRetryPolicy` |
| queue retry delay honouring Retry-After, 86 400 s cap | §4.4 |
| never ack/drop on retryable | §4.4, §6.2 **HARD** |
| same regression tests ported | §13 table |
| #6177 per-workspace cool-down, default-off option | §6.3 |
| #6176 identical-retry coalescing with the repair fix | §7 (head 5158f41) |

Integration (real PG, opt-in): two consumers racing SKIP LOCKED claims;
per-lane fairness under a 10 000-row single-lane flood with a second lane
still draining within N seconds; lease expiry takeover; DLQ preservation
cycles and archive; replay.

End-to-end (fake Relayfile in-process, both `oss` and `cloud` flavours): the
scripted Nov 7 storm — one workspace returns 429 `workspace_busy` for 2 min
then recovers — must show: ingress 429s with reasons once the lane cap is hit,
other lanes unaffected, cool-down armed and released, every attempt logged
with `relayfileReason`, zero acks of unwritten envelopes, zero drops, DLQ
empty after recovery. The run writes a report the PR body cites.

Acceptance for PR 1 (foundation): ingress (github, nango, hookdeck, slack,
linear), envelope/claims/queue/consumer/backpressure/cool-down/DLQ, Relayfile
envelope client, entitlement, observability, Dockerfile, migrations, OpenAPI,
README, the test suites above green in CI. PR 2: record-writer + provider
writes + GitHub/Slack/Jira/Linear/Notion/Gmail handlers. PR 3: chart + migration
runbook. Each PR is a draft until Khaliq un-drafts; Khaliq merges.

## 14. Open questions

1. ANSWERED (Khaliq, 2026-11-08 via relay-desktop-7): YES, OSS Relayfile gets
   a provider-writes route; self-host reaches canonical-record parity, not
   envelope-only. The route is an OSS relayfile PR coordinated with
   layered-relayfile; relayfile-control gates on `RELAYFILE_PROVIDER_WRITES`
   (§11).
2. ANSWERED (Khaliq, 2026-11-08): integration-watch agent dispatch lives in
   Relayfile. relayfile-control ingests and writes only.
3. ANSWERED (layered-relayfile, 2026-11-08): the envelope `payload` is NOT
   free-form on either flavour. Both accept (202) any JSON object, but a
   file is materialised only for the filesystem-event shape. Contract for
   every provider on both flavours, top level of `payload`:
   `{event_type: file.created|file.updated|file.deleted, path, content,
   contentType?, providerEventType?, resourceRef?}`. Never put provider
   raw objects under `payload.data` (cloud reads `content` from `data`,
   OSS from the top level); use `raw` or omit them. OSS: other event types
   are ignored (202, no file, no retry); missing path is a parse error
   (retried then dead-lettered); Slack paths under channels/ dms/ teams/
   users/; coalesce key providerObjectId → objectId → path. Cloud: unknown
   event type WITH a path throws (retry then DLQ), no path is silently
   ignored; cloud-only extras `provider_event_type`, `resource_ref`,
   `resource_refs` are honoured there and ignored by OSS.
   Dedupe key is `(workspaceId, provider, deliveryId)`, NOT envelopeId;
   HARD: envelopeId is always derived WITH deliveryId (OSS main does not
   detect envelopeId collisions between deliveries):
   retries MUST keep deliveryId stable (§6.1). Headers are a string→string
   map. OSS 413 is on the whole envelope body (RELAYFILE_MAX_BODY_BYTES,
   default 1 MiB), terminal. OSS 500 (generic store error, no Retry-After)
   gets the same bounded retry as 503. The path-less envelopeId derivation
   (`path=""`, event header as both event types) is still awaiting its
   one-line confirmation.
4. (helm lane) Chart home: standalone `relayfile-control` or sub-chart of the
   relayfile add-on.
5. (relaycontrol, me) Service token for `/api/v1/license` and the licence
   public key env name: `RELAYCONTROL_SERVICE_TOKEN` and
   `RELAYCONTROL_LICENSE_PUBLIC_KEY` (PEM or base64 raw 32 bytes); the
   relaycontrol side is not implemented yet.

### 14.1 Implementation rulings (PR 1, 2026-11-08)

- 413: streamed cap, returned as soon as the cap is crossed, before any
  signature check (the HMAC needs the whole body); nothing past the cap is
  buffered.
- §4.2 per-lane cap is made strict with `pg_try_advisory_xact_lock(
  hashtextextended(lane, 0))` plus a re-check inside the claim transaction;
  a real-Postgres race test covers it.
- §6.4 queue depth: per-lane counter rows maintained by trigger; the global
  total is their sum, cached 1 s in-process; oldest age from the
  `(state, visible_at)` index.
- GitHub installation fan-out: the consumer splits a parent envelope into one
  child envelope per workspace (own lane, parent request id as correlation)
  and acks the parent in the same transaction.
- Hookdeck verification follows Cloud's exact order: Dropbox signature first,
  then Hookdeck signature (skipped for gitlab/bitbucket/granola/shortcut),
  then Nango signature required; raw providers without handlers fail closed
  (401) in PR 1.
- PR 1 also carries `integration_bindings`, the minimal admin API (bindings
  upsert/list, DLQ list/replay, ingress pause) and the generic claims module.
- Commits by the implementer carry its own model trailer (Opus 5.5).

## 15. Source map (Cloud @ 1a59dec unless noted)

`packages/webhook-worker/src/{index.ts,handlers/*.ts,utils.ts,queue-consumer.ts,queue-retry-delay.ts}`;
`packages/web/lib/integrations/{nango-webhook-route-handler.ts,nango-webhook-router.ts,github-webhook-ingest.ts,github-webhook-admission-retry.ts,relayfile-backpressure.ts,relayfile-writeback-auth.ts,workspace-cooldown.ts (bda4057, #6177)}`;
`packages/core/src/sync/record-writer.ts` (+ 5158f41, #6176);
relayfile-cloud 433e6d2 `middleware/auth.ts`, `routes/webhooks.ts`, `handlers/provider-writes.ts`, `middleware/workspace-write-admission.ts`, `durable-objects/request-admission.ts`;
OSS relayfile cf9d40dd `internal/httpapi/server.go`, `internal/relayfile/store.go`, `openapi/relayfile-v1.openapi.yaml`.
