# relayfile-control: per-run Relayfile credential for self-host Flows workers

Status: draft for coordination, 2026-11-09. Author: layered-relayfile
(relayfile-control side). Counterpart: layered-control (relay-control side,
the credential itself). Assigned by lead-desktop (task relay-245580040030216192).
Decision owner for anything marked **DECISION**: Khaliq.

## 0. Problem

Self-host Flows workers receive the assignment payload with

```
relayfileUrl:   <SELFHOST_FLOWS_WORKER_RELAYFILE_URL>
relayfileToken: ""
```

at `src/flows/store.ts:1166` (relay-control has the same literal at its
`src/flows/store.ts:1126`). Cloud filled this field with a RelayAuth
`relay_pa_` path-scoped token; self-host has no RelayAuth, so the field was
left empty when the Flows API was ported. The worker then mounts Relayfile
with an empty bearer and fails late, inside the run, with an auth error that
names neither the cause nor the owner.

Two things are wrong and each has one owner:

| Gap | Owner |
|-----|-------|
| No per-run Relayfile credential exists in self-host. | layered-control (relay-control mints it). |
| relayfile-control hands out whatever it has, including `""`, unverified. | layered-relayfile (this document). |

Relayfile itself needs no change: since relayfile#568 (merged 7d50e49e) it
verifies relaycontrol tokens through `POST /internal/v1/auth/introspect`
(`RELAYFILE_AUTH_MODE=introspection|dual`, `RELAYCONTROL_INTERNAL_URL`).

## 1. The credential (contract layered-control produces)

Reuse the token class relaycontrol already has for Relayfile, so nothing
new is needed in relayfile's verifier:

| Field | Value |
|-------|-------|
| Token class | workspace API token, prefix `sh_wk_`, purpose `relayfile-agent` (relay-control `src/admin/api-tokens.ts`). |
| `agent_name` | `flows-run:<runId>` (fits `NODE_ID_PATTERN`: letters, digits, `.`, `_`, `:`, `-`). Relayfile attributes writes to this agent name. |
| `aud` | the Relayfile public origin the worker will call, i.e. relayfile-control's `SELFHOST_FLOWS_WORKER_RELAYFILE_URL` normalized as an origin. Must be in relay-control's `SELFHOST_INTROSPECT_AUDIENCES`, otherwise introspection answers `audience`. |
| `workspace` | the run's relaycontrol workspace id. |
| `scopes` | the purpose's fixed set `fs:read fs:write sync:read sync:trigger` for phase 1 (see **DECISION D2** for path scoping). |
| `exp` | issued TTL, see §2.3. Never "no expiry". |
| Issuer | relay-control, through the existing Flows broker surface (bearer `RELAYFILE_CONTROL_BROKER_SECRET`), not the admin API-token routes: no user session is involved. |

Broker contract (additions to `BROKER_PATHS` in `src/flows/broker.ts`;
layered-control owns the server half on `feat/flows-broker-api`):

```
POST /internal/v1/flows/credentials/relayfile-run-token
  { workspaceId, runId, aud, ttlSeconds }
  200 { token, tokenId, agentName, aud, scopes[], expiresAt }
  409 relayfile_run_token_exists      (runId already has a live token; see §2.2)
  422 relayfile_aud_not_accepted      (aud not in SELFHOST_INTROSPECT_AUDIENCES)
  404 workspace_not_found
POST /internal/v1/flows/credentials/revoke   { runId }    (existing; MUST now
  also revoke the run's relayfile-run token; stays idempotent)
```

Requirements on the issuer:

- Store only the token hash (as today for `sh_wk_`); the raw token crosses
  the wire once, in the 200 body.
- Bind the token to `runId` so `revoke { runId }` finds it. One live token
  per run; re-issue is a 409, not a silent rotation (a redelivered
  assignment must carry the same token, §2.2).
- Introspection of this token behaves exactly like any `relayfile-agent`
  token: `principal_type: "agent"`, `agent_name`, `scopes`, `aud`, `exp`,
  `token_id`; inactive answers `expired | revoked | audience | workspace |
  wrong-kind | unknown`.
- Never log the token, its hash or the broker secret (same rule as
  `introspect.ts`).

## 2. relayfile-control side (this repository)

### 2.1 Invariant

```
relayfileUrl !== ""  ⇒  relayfileToken is non-empty, issued for this run,
                        and verified by introspection before dispatch
relayfileUrl === ""  ⇒  relayfileToken === "" and the run carries one
                        run.relayfile_unavailable event
```

The literal `relayfileToken: ""` at `store.ts:1166` goes away. An empty
token is never sent next to a non-empty URL.

### 2.2 Issue once per run, reuse on redelivery

`workflowPayload(runId)` runs at claim (`store.ts:1261`) and again on
redelivery of an unacked assignment (`store.ts:1206`). The token is therefore
minted at most once per run and stored encrypted, like `env_secrets`:

```
ALTER TABLE selfhost_workflow_runs
  ADD COLUMN relayfile_token_ciphertext text,
  ADD COLUMN relayfile_token_id         text,
  ADD COLUMN relayfile_token_expires_at timestamptz;
```

- First `workflowPayload` call with the column empty: call the broker,
  verify (§2.4), store ciphertext + id + expiry in the same transaction that
  marks the assignment delivered, put the token in the payload.
- Later calls: decrypt and reuse; re-verify only if `expires_at` is within
  the verification lead (§2.4), never re-mint.
- `finishRun` (completed, failed, cancelled, worker-revoked, queue timeout)
  already enqueues `enqueueCredentialRevocation(tx, runId)`; the same outbox
  row now also revokes the Relayfile token because relay-control's
  `revoke { runId }` covers both. The ciphertext column is nulled in the
  same transaction. No new outbox.

### 2.3 TTL

New config `SELFHOST_FLOWS_RELAYFILE_TOKEN_TTL_MS`, default 24 h, bounds
[10 min, 7 d], parsed with the existing `duration()` helper in
`src/flows/config.ts`. Revocation at terminal state is the real bound; the
TTL caps the damage if the outbox never delivers. The payload gains
`relayfileTokenExpiresAt` (ISO 8601), the field name the relayfile SDK and
`relayfile-mount --creds-file` already read
(`internal/delegatedauth/delegatedauth.go:43`).

### 2.4 Verification before dispatch

relayfile-control verifies what it hands out with the introspection client
it already has (`src/flows/introspection.ts`), with one change: the
audience is a parameter instead of the fixed `RELAYFILE_CONTROL_PUBLIC_ORIGIN`,
because this token's `aud` is Relayfile's origin, not ours. The call sends
`{ token, aud: <relayfile origin>, workspace: <run workspace> }` and the
answer must satisfy all of:

| Check | Else |
|-------|------|
| `active === true` | refused, reason = introspection `reason` |
| `principal_type === "agent"` and `agent_name === "flows-run:<runId>"` | refused `principal` |
| `token_id === tokenId` from the 200 body | refused `token_id` |
| `workspace` equals the run's workspace | refused `workspace` |
| `aud` normalizes to the Relayfile origin | refused `audience` |
| `exp` is at least `queueWaitMs + 60 s` in the future | refused `expires_too_soon` |
| `scopes` ⊇ `fs:read` (phase 1 required set) | refused `scopes` |

Raw tokens are never logged or cached beyond the existing sha256 cache key.
A refused token is revoked straight away through the normal outbox row.

### 2.5 "Refuse clearly"

Two failure classes, mapped onto the holds that already exist in
`claimNext` (`store.ts:1275-1299`):

| Situation | Error class | Effect |
|-----------|-------------|--------|
| Broker 4xx (`relayfile_aud_not_accepted`, `workspace_not_found`, 409, unknown code) or verification failed (§2.4) | `RelayfileCredentialRefused { status, code, reason }` (new, next to `RelaycastBindingRefused` in `broker.ts`) | claim released; workspace held `relaycastRefusalHoldMs`; log event `flows_relayfile_credential_refused { workspaceId, runId, status, code, reason, retryInMs }`; run event `run.relayfile_credential_refused { code, reason }` appended once per hold, so the run owner sees it in the run's event list. Nothing from the token in either. |
| Broker or introspection transport failure / 5xx | existing `BrokerUnavailable` / `IntrospectionUnavailable` | existing exponential outage hold; no new logging. |

Startup: when `SELFHOST_FLOWS_WORKER_RELAYFILE_URL` is set, the broker seam
must be configured. `src/flows/wiring.ts:75-79` already fails startup
without `RELAYFILE_CONTROL_BROKER_SECRET`, so no new check is needed; the
doc states the dependency.

Unconfigured install (`relayfileUrl === ""`): the payload carries both
empty strings (worker compatibility: today's worker reads both keys) and the
run gets one `run.relayfile_unavailable {}` event at first claim. See
**DECISION D1**.

### 2.6 Worker-facing payload after this change

```
relayfileUrl:            "https://relayfile.example"
relayfileToken:          "sh_wk_…"
relayfileTokenExpiresAt: "2026-11-10T12:00:00.000Z"
```

or, when Relayfile is not deployed for the install:

```
relayfileUrl: ""   relayfileToken: ""   (no relayfileTokenExpiresAt)
```

### 2.7 Tests (table-driven, `node:test`)

- payload builder: configured + broker 200 + introspection active ⇒ token in
  payload, ciphertext stored, redelivery reuses the same token without a
  second broker call; each §2.4 row refused ⇒ `RelayfileCredentialRefused`
  with the named reason, claim back to `queued`, hold set, both events
  emitted, no token substring in any event or log line.
- broker client: request/response shapes, 409/422/404 mapping, 5xx ⇒
  `BrokerUnavailable`.
- finishRun: ciphertext nulled, one outbox row.
- unconfigured: both empty strings, `run.relayfile_unavailable` once.
- openapi: `relayfileTokenExpiresAt` added to the assignment payload schema;
  `scripts`/contract check as the repo does today.

## 3. Relayfile side (no code change)

The self-host Relayfile must run `RELAYFILE_AUTH_MODE=introspection` (or
`dual` while RelayAuth JWKS is still around) with `RELAYCONTROL_INTERNAL_URL`,
`RELAYCONTROL_INTROSPECT_SECRET` and its audience set to the same origin the
token carries. The infra chart does not yet expose these slots
(flagged to infra-kind-stack after relayfile#568). Positive introspection
answers are cached for at most 60 s, so a revoked run token can be honoured
for up to 60 s after `finishRun`; acceptable, documented.

## 4. Worker side (not mine, flagged)

The Cloud launcher refuses any scoped workflow token that does not start
with `relay_pa_` (cloud `packages/core/src/bootstrap/launcher.ts:1271`).
If the self-host Flows worker shares that code path it will reject the
`sh_wk_` token before mounting. lead-desktop: please confirm which worker
build consumes `payload.relayfileToken` on self-host and whether that
prefix check applies there.

## 5. Decisions needed

- **D1** Unconfigured install: dispatch with both fields empty plus a
  `run.relayfile_unavailable` event (recommended; Relayfile is an optional
  paid add-on per Khaliq 2026-11-07), or refuse every run until Relayfile is
  configured.
- **D2** Scope width: phase 1 workspace-wide `relayfile-agent` scopes.
  Path-scoped (`relayfile:fs:read:<path>`) needs the run to declare its mount
  paths; relayfile-control does not parse the workflow today. Follow-up.
- **D3** TTL default 24 h and the `queueWaitMs + 60 s` verification lead.

## 6. Sequence

```
worker ──claim──▶ relayfile-control
                   ├─ workflowPayload(run)
                   │   ├─ (first time) POST broker relayfile-run-token ──▶ relay-control
                   │   │              ◀── { token, tokenId, expiresAt }
                   │   ├─ POST introspect { token, aud: relayfile origin, workspace }
                   │   │              ◀── { active, agent_name, token_id, exp, scopes… }
                   │   ├─ verify §2.4, store ciphertext, or refuse §2.5
                   │   └─ payload { relayfileUrl, relayfileToken, relayfileTokenExpiresAt, … }
                   ◀── assignment
worker ──Bearer sh_wk_…──▶ relayfile ──introspect──▶ relay-control (568 path)
…
reportStatus(terminal) ─▶ finishRun ─▶ outbox ─▶ POST broker revoke { runId } ─▶ relay-control
```
