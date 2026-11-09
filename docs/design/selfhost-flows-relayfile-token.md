# relayfile-control: per-run Relayfile credential for self-host Flows workers

Status: draft for coordination, 2026-11-09 (rev 2, reconciled with
layered-control's issuer contract, relay message 245581962694701056).
Author: layered-relayfile (relayfile-control side). Counterpart:
layered-control (relay-control issuer; its doc lands as relay-control
`docs/flows-relayfile-credential.md`). Assigned by lead-desktop (task
relay-245580040030216192). Anything marked **DECISION** is Khaliq's.

## 0. Problem

Self-host Flows workers receive the assignment payload with

```
relayfileUrl:   <SELFHOST_FLOWS_WORKER_RELAYFILE_URL>
relayfileToken: ""
```

at `src/flows/store.ts:1166` (relay-control has the same literal at its
`src/flows/store.ts:1126`). Cloud filled this field with a RelayAuth
`relay_pa_` path-scoped token; self-host has no RelayAuth, so the field was
left empty when the Flows API was ported. The worker mounts Relayfile with an
empty bearer and fails late, inside the run, with an auth error that names
neither the cause nor the owner.

| Gap | Owner |
|-----|-------|
| No per-run Relayfile credential exists in self-host. | layered-control (relay-control issues it). |
| relayfile-control hands out whatever it has, including `""`, unverified. | layered-relayfile (this document). |
| Worker launcher accepts only `relay_pa_` tokens. | asked of lead-desktop (§4). |

Relayfile's verifier needs no change in `introspection` mode: since
relayfile#568 (merged 7d50e49e) it sends every bearer to relay-control's
`POST /internal/v1/auth/introspect`. `dual` mode routes by prefix and must
learn the new prefix (§3, a relayfile PR owned by layered-relayfile).

## 1. The credential (relay-control, layered-control's contract)

Restated here so both sides review one text; relay-control's doc is the
source if they ever differ.

| Field | Value |
|-------|-------|
| Token | opaque bearer `sh_fr_<random>`, sha256 at rest, no refresh token, exactly one active per run (re-issue revokes the previous). |
| Introspection | `principal_type: "agent"`, `agent_name: "flows-run-<runId>"`, `sub` = `token_id` = credential id, `scopes`, `aud`, `workspace`, `org`, `iat`, `exp`. Inactive: `unknown | revoked | expired | audience | workspace`. |
| Scopes | per mount root `p`: `relayfile:fs:read:p`, `relayfile:fs:read:p/*`, and unless `readOnly` the same two `fs:write` scopes. Never `fs:*`, `sync:*`, `manage`, `*`. Matches relayfile's `scopePathMatches`: `p` is the exact path, `p/*` every descendant. |
| `aud` | the worker-facing Relayfile origin, from relay-control's own `SELFHOST_FLOWS_WORKER_RELAYFILE_URL`; must be in `SELFHOST_INTROSPECT_AUDIENCES`. |
| TTL | hard horizon `SELFHOST_FLOWS_RELAYFILE_CREDENTIAL_TTL_MS` (relay-control config, default 12 h, 5 min..24 h); the real end is revocation. |

Issue route, behind the existing `RELAYFILE_CONTROL_BROKER_SECRET` bearer:

```
POST /internal/v1/flows/credentials/relayfile
  { workspaceId, runId, mountPaths: string[], readOnly?: boolean }
  200 { relayfileToken, relayfileTokenExpiresAt, relayfileMountPaths, scopes, aud }
  400 invalid_request | invalid_mount_paths
  404 relayfile_not_configured          (issuer has no Relayfile URL or no introspection secret)
  409 relayfile_audience_not_accepted   (Relayfile origin not an accepted introspection audience)
POST /internal/v1/flows/credentials/revoke { runId }
  existing route; now also revokes the run's Relayfile credential; idempotent
```

Mount-path rules (400 `invalid_mount_paths`): 1..16 paths, absolute,
normalized, no `.`/`..`, no trailing slash, no `*`, at most 512 chars; the
first path must be `/workflows/runs/<runId>` for this run (or
`/workflows/schedules/<id>`).

Asks from this side, to settle before the relay-control PR goes READY:

- **A1** Add `credentialId` to the 200 body. relayfile-control verifies
  `token_id === credentialId` (§2.4) and logs the id, never the token.
- **A2** Accept an optional `aud` in the request body and answer 409
  `relayfile_audience_not_accepted` when it differs from the issuer's own
  origin. Both services carry `SELFHOST_FLOWS_WORKER_RELAYFILE_URL`; a drift
  between the two configs should fail at issue time, not at the worker.
- **A3** Confirm that `revoke { runId }` for a run with no Relayfile
  credential stays a 2xx (it must; relayfile-control calls it for every run).

## 2. relayfile-control side (this repository)

### 2.1 Invariant

```
relayfileToken present  ⇒  non-empty, issued for this run, verified (§2.4),
                           with relayfileTokenExpiresAt and relayfileMountPaths
relayfileToken absent   ⇒  the run carries one run.relayfile_credential_unavailable
                           event naming the code; the worker refuses Flows-to-File
                           with that reason
```

The literal `relayfileToken: ""` at `store.ts:1166` goes away. An empty
string is never sent; absence is the only "no credential" signal.
`relayfileUrl` is sent whenever it is configured, with or without a token,
so the worker can name the service it could not get a credential for.

### 2.2 Mount paths (Cloud semantics, computed here)

`relayfileMountPaths = [runRoot, ...providerRoots]`, as Cloud's
`workflowRelayfileMountConfig` (cloud `packages/web/lib/workflows/relayfile-mount.ts`):

- `runRoot = /workflows/runs/<runId>`.
- `providerRoots`: for YAML workflows, the keys of the top-level
  `integrations:` map minus the reserved keys `relayfile` and
  `subscriptions`, each resolved through the mountable allowlist
  `github gitlab jira linear shortcut slack` to the adapter root
  `/<provider>`, deduped and sorted. An unknown provider is a refusal
  (code `invalid_mount_paths`, reason `unmountable_provider:<name>`),
  not a silent drop, exactly as Cloud. TS workflows declare no providers.
- Phase 1 does not narrow GitHub or GitLab to one repository: the run record
  has no repository grant. See **DECISION D2**.
- `readOnly` is `false` in phase 1; the run record has no read-only flag.

The `yaml` package is already a dependency.

### 2.3 Issue once per run, reuse on redelivery

`workflowPayload(runId)` runs at claim (`store.ts:1261`) and again on
redelivery of an unacked assignment (`store.ts:1206`). The credential is
therefore issued at most once per run and stored encrypted with the store
keyring, like `env_secrets_ciphertext`:

```
ALTER TABLE selfhost_workflow_runs
  ADD COLUMN relayfile_credential_ciphertext text,     -- the 200 body, encrypted
  ADD COLUMN relayfile_credential_id         text,
  ADD COLUMN relayfile_credential_expires_at timestamptz;
```

- First call with the column empty: compute mount paths (§2.2), call the
  issuer, verify (§2.4), store, place the three fields in the payload.
- Later calls: decrypt and reuse. Never re-issue for a live run; the
  issuer's "re-issue revokes the previous" rule only matters if this
  service crashed between issue and store, which then rotates harmlessly.
- `finishRun` already enqueues `enqueueCredentialRevocation(tx, runId)`
  (`src/flows/revoke-outbox.ts`); the same outbox row now also revokes the
  Relayfile credential because relay-control's `revoke { runId }` covers
  both. The ciphertext columns are nulled in the same transaction. No new
  outbox, no new config.

### 2.4 Verification before dispatch

relayfile-control verifies what it hands out with the introspection client
it already has (`src/flows/introspection.ts`), with one change: the
audience becomes a per-call parameter, because this credential's `aud` is
Relayfile's origin, not `RELAYFILE_CONTROL_PUBLIC_ORIGIN`. One call per run,
`{ token, aud: <Relayfile origin>, workspace: <run workspace> }`; the answer
must satisfy every row:

| Check | Else (reason) |
|-------|---------------|
| `active === true` | the introspection `reason` |
| `principal_type === "agent"` and `agent_name === "flows-run-<runId>"` | `principal` |
| `token_id === credentialId` (A1) | `credential_id` |
| `workspace` equals the run's relaycontrol workspace | `workspace` |
| `aud` normalizes to this service's `SELFHOST_FLOWS_WORKER_RELAYFILE_URL` origin | `audience` |
| `exp` at least 60 s in the future and equal to `relayfileTokenExpiresAt` within 1 s | `expiry` |
| `scopes` equals the issuer's `scopes` and every scope is `relayfile:fs:(read|write):<p>` or `<p>/*` for `p` in `relayfileMountPaths` | `scopes` |

Raw tokens are never logged or cached beyond the existing sha256 cache key.
A credential that fails verification is treated as unavailable (§2.5) and
revoked at once through a normal outbox row.

### 2.5 "Refuse clearly"

The run proceeds without a Relayfile credential; the refusal is recorded
where the run owner looks and where the operator looks, and nothing from
the token appears in either.

| Situation | Code in the run event | Operator log event |
|-----------|----------------------|--------------------|
| Relayfile not configured here (`SELFHOST_FLOWS_WORKER_RELAYFILE_URL` empty) | `relayfile_not_configured` | none (expected state for installs without the add-on) |
| Issuer 404 `relayfile_not_configured`, 409 `relayfile_audience_not_accepted`, 400 `invalid_mount_paths`, unknown 4xx | the issuer's code (`unknown_refusal:<status>` for unknown) | `flows_relayfile_credential_refused { workspaceId, runId, status, code }` |
| Unmountable provider (§2.2) | `invalid_mount_paths` with `reason` | same, with `reason` |
| Verification failed (§2.4) | `relayfile_credential_invalid` with `reason` | same, with `reason`, plus the outbox revocation |
| Issuer or introspection transport failure / 5xx | not a refusal | existing exponential outage hold in `claimNext` (`store.ts:1275-1285`): claim released, run retried |

Run event: `run.relayfile_credential_unavailable { code, reason? }`,
appended once per run (the redelivery path reuses the stored "no
credential" decision, so a run is not spammed). The worker is expected to
refuse Flows-to-File with the same code (§4).

Startup: `src/flows/wiring.ts:75-79` already refuses to start Flows without
`RELAYFILE_CONTROL_BROKER_SECRET` and `RELAYCONTROL_INTROSPECT_SECRET`, so
no new check is needed.

### 2.6 Worker-facing payload after this change

```
relayfileUrl:            "https://relayfile.example"
relayfileToken:          "sh_fr_…"
relayfileTokenExpiresAt: "2026-11-10T00:00:00.000Z"
relayfileMountPaths:     ["/workflows/runs/<runId>", "/github", "/slack"]
```

or, when no credential could be issued:

```
relayfileUrl: "https://relayfile.example"      (or absent when unconfigured)
(no relayfileToken, relayfileTokenExpiresAt, relayfileMountPaths)
```

No refresh fields: the credential lives as long as the run.

### 2.7 Tests (table-driven, `node:test`)

- mount paths: run root only; YAML with each mountable provider; reserved
  keys dropped; unknown provider refused; TS workflow has no providers;
  dedupe and sort.
- payload builder: issuer 200 + introspection active ⇒ three fields
  present, ciphertext stored, redelivery reuses without a second issuer
  call; each §2.4 row fails ⇒ fields absent, run event with the named
  reason, revocation row enqueued; each §2.5 refusal row ⇒ fields absent,
  run event with the code, log event, no hold; issuer 5xx ⇒ claim released
  and outage hold as today. No test fixture token substring may appear in
  any event payload or log line (asserted).
- issuer client: request/response shapes, 400/404/409 mapping,
  5xx ⇒ `BrokerUnavailable`.
- finishRun: ciphertext nulled, exactly one outbox row.
- openapi: the three payload fields added to the assignment payload schema.

## 3. Relayfile side (layered-relayfile, separate PR)

Relayfile must run `RELAYFILE_AUTH_MODE=introspection` or `dual` with
`RELAYCONTROL_INTERNAL_URL`, `RELAYCONTROL_INTROSPECT_SECRET` and its
audience equal to the credential's `aud`. `dual` mode routes bearers by
prefix (`internal/httpapi/auth_introspection.go`,
`relaycontrolTokenPrefixes`), so a small relayfile PR adds `sh_fr_` there
with a test; `introspection` mode already works. The infra chart does not
yet expose the introspection env slots (flagged to infra-kind-stack after
relayfile#568). Positive introspection answers are cached for at most 60 s,
so a revoked credential can be honoured for up to 60 s after `finishRun`.

## 4. Worker side (not mine, asked of lead-desktop)

The Cloud launcher refuses any scoped workflow token that does not start
with `relay_pa_` (cloud `packages/core/src/bootstrap/launcher.ts:1271`).
The self-host worker must accept `sh_fr_` when the control plane is
relaycontrol, and must treat an absent `relayfileToken` as a clear refusal
of Flows-to-File that names the run event's code. layered-control has asked
lead-desktop for the owner; this document only depends on the field names.

## 5. Decisions needed

- **D1** Unconfigured or refused credential: run proceeds without
  Relayfile plus the run event (recommended; Relayfile is an optional paid
  add-on per Khaliq 2026-11-07), or the claim is refused until the add-on
  is configured.
- **D2** Repository narrowing for GitHub/GitLab mounts needs a repository
  grant on the run record. Follow-up after phase 1.
- **D3** TTL default 12 h on the issuer side; relayfile-control adds no
  knob.

## 6. Sequence

```
worker ──claim──▶ relayfile-control
                   ├─ workflowPayload(run)
                   │   ├─ mountPaths = [/workflows/runs/<runId>, /<provider>…]
                   │   ├─ (first time) POST broker credentials/relayfile ──▶ relay-control
                   │   │              ◀── { relayfileToken, …ExpiresAt, …MountPaths, scopes, aud, credentialId }
                   │   ├─ POST introspect { token, aud: Relayfile origin, workspace }
                   │   │              ◀── { active, agent_name, token_id, exp, scopes, aud, workspace }
                   │   ├─ verify §2.4 and store, or record §2.5 and omit
                   │   └─ payload { relayfileUrl, relayfileToken?, relayfileTokenExpiresAt?, relayfileMountPaths?, … }
                   ◀── assignment
worker ──Bearer sh_fr_…──▶ relayfile ──introspect──▶ relay-control (568 path)
…
reportStatus(terminal) ─▶ finishRun ─▶ outbox ─▶ POST broker credentials/revoke { runId } ─▶ relay-control
```
