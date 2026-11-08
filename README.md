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
default `127.0.0.1:3101`) serves `/metrics` and `/api/v1/admin/*`, which need
a bearer token. See `openapi.yaml`.

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
| `RELAYCONTROL_BASE_URL` + `RELAYCONTROL_SERVICE_TOKEN`, or `RELAYCONTROL_LICENSE_DOC` / `RELAYCONTROL_LICENSE_FILE` + `RELAYCONTROL_LICENSE_PUBLIC_KEY` | entitlement. Without the `relayfile` capability, ingress answers `403 feature_not_licensed` and the consumer pauses |

Backpressure is cloud#6175 final by default. Relayfile 429 gets one in-place
retry for long-deadline providers and none for Slack/Jira; a final 429 becomes
a queue retry that honours Retry-After (cap 86 400 s). Opt-in switches:

| Variable | Default | Effect |
|---|---|---|
| `RELAYFILE_BACKPRESSURE_RETRY_503` | off | also retry 503 (as backpressure) and OSS 500 in place |
| `COOLDOWN_ENABLED` | off | #6177 per-workspace cool-down after a final backpressure error |

Secrets are never logged: the startup line reports each one only as `set` or `unset`.

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
