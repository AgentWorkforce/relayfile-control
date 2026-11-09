# relayfile-control

Integration webhook ingest for Relayfile. It covers provider ingress (GitHub,
Nango, Hookdeck, Slack, Linear), durable envelope intake with idempotency, a
Postgres queue with per-workspace fairness and end-to-end backpressure,
dead-letter preservation and replay, and the client that delivers envelopes
into Relayfile. Node 22 + Postgres, same conventions as
`AgentWorkforce/relay-control`.

Design: `docs/design/webhook-ingest.md` (branch `design/webhook-ingest`, PR #1).

Status: PR 1 ("foundation", plan §13). Provider-writes, the record writer and
per-provider handlers are PR 2. The Helm chart and the migration runbook are
PR 3. Nothing is deployed.

## Shape

```
provider ──POST──▶ ingress: entitlement → secret configured → capped body read
                   → signature (raw bytes) → parse → classify → lane
                   → admission gate → envelope + ingress dedupe (one txn)
                   ──▶ 202 / 200 duplicate / 4xx / 429 / 503
Postgres webhook_envelopes (the queue)
                   ──▶ consumer: strict per-lane claim (SKIP LOCKED + advisory lock)
                   → resolve workspace (connection → native id; GitHub fan-out)
                   → POST /v1/internal/webhook-envelopes (signed, bounded retry)
                   → ack | ack_terminal | retry(delay) + cool-down | dead → DLQ
```

`ROLE=ingress|consumer|all` (default `all`). The public listener (`LISTEN`,
default `0.0.0.0:3100`) serves `/api/v1/webhooks/*`, their `/cloud/...`
aliases, `/healthz` and `/readyz`. The internal listener (`ADMIN_LISTEN`,
default `127.0.0.1:3101`) serves `/metrics` (unauthenticated: protected only by
the listener binding) and `/api/v1/admin/*` (bearer token). See `openapi.yaml`.

## Configuration

Every setting is an environment variable with a finite default; see
`src/config.ts`. The ones you must set:

| Variable | Purpose |
|---|---|
| `DATABASE_URL` | Postgres (own database, e.g. `relayfile_control`) |
| `MIGRATE_ON_START=1` | apply `migrations/` under an advisory lock (or run them out of band) |
| `GITHUB_WEBHOOK_SECRET`, `NANGO_WEBHOOK_SECRET`, `HOOKDECK_WEBHOOK_SECRET`, `DROPBOX_APP_SECRET`, `SLACK_SIGNING_SECRET`, `LINEAR_WEBHOOK_SECRET` | per-ingress secrets; comma lists allow rotation. An unset secret makes its ingress answer `503 ingress_not_configured` |
| `RELAYFILE_BASE_URL`, `RELAYFILE_INTERNAL_HMAC_SECRET` | Relayfile target and the internal HMAC secret (same Secret as the Relayfile server) |
| `RELAYFILE_CONTROL_ADMIN_TOKEN` | admin bearer (comma list for rotation) |
| `RELAYFILE_CONTROL_PUBLIC_ORIGIN` | this service's absolute http(s) origin (no path, query or credentials); the audience it asks relay-control to check tokens against |
| `RELAYCONTROL_BASE_URL` + `RELAYCONTROL_SERVICE_TOKEN`, or `RELAYCONTROL_LICENSE_DOC` / `RELAYCONTROL_LICENSE_FILE` + `RELAYCONTROL_LICENSE_PUBLIC_KEY` | entitlement. Without the `relayfile` capability, ingress answers `403 feature_not_licensed` and the consumer pauses |

Backpressure is cloud#6175 final by default. Relayfile 429 gets one in-place
retry for long-deadline providers and none for Slack/Jira; a final 429 becomes
a queue retry that honours Retry-After (cap 86 400 s). Opt-in switches:

| Variable | Default | Effect |
|---|---|---|
| `RELAYFILE_BACKPRESSURE_RETRY_503` | off | also retry 503 (as backpressure) and OSS 500 in place |
| `COOLDOWN_ENABLED` | off | #6177 per-workspace cool-down after a final backpressure error |

Secrets are never logged: the startup line reports each one only as `set` or `unset`.

## Flows API

relayfile-control serves the Flows API that used to live in relay-control:
flow definitions, worker enrollment and queues, runs, steps, logs, storage
and artifacts. The route contracts are unchanged. Paths, methods, request and
response schemas, status codes, error codes and the SSE queue behaviour are
the same as before, and `openapi.yaml` carries the same operations and
components. Ingress must route `/api/v1/flows*`, `/api/v1/workers*` and
`/api/v1/workflows*` (and their `/cloud/api/v1/...` aliases) to
relayfile-control instead of relay-control.

Flows are off unless `FLOWS_ENABLED=true`. When it is on, these settings are
required:

| Variable | Purpose |
|---|---|
| `RELAYFILE_CONTROL_PUBLIC_ORIGIN` | audience (`aud`) sent with every introspection request |
| `RELAYCONTROL_BASE_URL` | relay-control, for introspection and the credential broker |
| `RELAYCONTROL_INTROSPECT_SECRET` | bearer for relay-control's introspection endpoint |
| `RELAYFILE_CONTROL_BROKER_SECRET` | sent as `Authorization: Bearer` on every credential-broker call to relay-control |
| `RELAYFILE_CONTROL_ENCRYPTION_KEY_ID`, `RELAYFILE_CONTROL_ENCRYPTION_KEY` | key for run secrets and worker tokens at rest (32 bytes, hex or base64); `_PREVIOUS_KEY_ID` / `_PREVIOUS_KEY` keep the old key readable during rotation |
| `SELFHOST_FLOWS_WORKER_RELAYCAST_URL` or `RELAYCAST_PUBLIC_URL` | the Relaycast URL workers are told to use |

Optional:

| Variable | Default | Effect |
|---|---|---|
| `RELAYFILE_CONTROL_TELEMETRY_SECRET` | unset | at least 32 bytes; enables `GET /internal/v1/telemetry/flows` (unset: 404) |
| `FLOWS_PUBLIC_BASE_URL` | the public origin | base URL put into links the API returns |
| `FLOWS_INTROSPECTION_CACHE_SECONDS` | 30 | introspection cache lifetime, capped at 30 |
| `FLOWS_REVOKE_OUTBOX_INTERVAL_MS` | 5000 | how often the revocation outbox is drained |
| `SELFHOST_FLOWS_STORAGE`, `SELFHOST_FLOWS_GCS_BUCKET`, `SELFHOST_FLOWS_GCS_PREFIX` | `postgres` | where run storage and artifacts live |
| `SELFHOST_FLOWS_WORKER_RELAYFILE_URL`, `SELFHOST_DASHBOARD_URL`, `SELFHOST_FLOWS_QUEUE_WAIT_MS`, `SELFHOST_FLOWS_ENROLLMENT_TTL_MS`, `SELFHOST_FLOWS_QUEUE_PING_MS` | as before | same meaning as in relay-control |

Every secret above can also be given as a file path in `<NAME>_FILE`.
Setting both the value and `_FILE` is a configuration error.

### Seams

- **Introspection (user routes).** Bearer tokens, plus the
  `agent_relay_session` cookie under `/cloud`, are sent to relay-control's
  `POST /internal/v1/auth/introspect` with
  `aud = RELAYFILE_CONTROL_PUBLIC_ORIGIN`. Only active `desktop-user` answers
  authenticate. User, workspace and organization ids are relay-control ids;
  admin routes require the `owner` or `admin` role. Answers are cached for
  at most 30 s (and never past `exp`), keyed by a hash of the token. Raw
  tokens are never stored.
- **CliCredentialBroker (worker CLI logins).** The operations are
  `issueRunReference`, `readForRun`, `writeBack` and `revokeForRun`, plus the
  workspace Relaycast binding. All of them are HTTP calls to relay-control
  carrying `RELAYFILE_CONTROL_BROKER_SECRET`. The paths are kept in one
  object (`BROKER_PATHS` in `src/flows/broker.ts`) under
  `/internal/v1/flows/credentials/...` until relay-control publishes the
  contract. Each call sends both the run id and the reference; relay-control
  enforces the binding between them. Finishing a run writes a row to the
  `selfhost_cli_credential_revocations` outbox in the same transaction. A
  worker delivers it with `revokeForRun` and retries with capped exponential
  backoff until relay-control accepts it.
- **Telemetry.** relay-control calls `GET /internal/v1/telemetry/flows` with
  `Authorization: Bearer <RELAYFILE_CONTROL_TELEMETRY_SECRET>`. The secret
  is compared in constant time. The response is
  `{runs_by_status, workers_by_status, flow_definitions}`.

### Migration

`migrations/0002_flows.sql` creates the Flows tables and the revocation
outbox. The user, workspace, organization and credential-reference columns
hold relay-control ids that were validated through introspection, so they
have no foreign keys. Deleting a workspace in relay-control does not cascade
here.

## Development

```sh
npm ci
npm run typecheck
npm test                                    # PGlite unit tests, e2e against an in-process fake Relayfile
RELAYFILE_CONTROL_PG_TEST_URL=postgres://… npm test   # adds the real-Postgres concurrency suite
npm run build && DATABASE_URL=… MIGRATE_ON_START=1 npm start
```

The e2e storm test writes `$TMPDIR/e2e-storm-report-{oss,cloud}-cooldown-{on,off}.md`.
Cloud test files ported verbatim are typechecked by `tsconfig.cloud-port.json`.
Tests never contact a real Relayfile, Nango, Hookdeck or GitHub. All secrets
in tests are random fixtures generated per run.
