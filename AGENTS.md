# Agent instructions — relayfile-control

## Service ownership

`relayfile-control` owns Relayfile integration ingestion and the Flows service.
Read the matching [relay-control boundary](https://github.com/AgentWorkforce/relay-control/blob/main/AGENTS.md)
(local checkout: `../relay-control/AGENTS.md`) before
changing authentication, workspace policy or credential handling.

| Responsibility | Canonical owner |
| --- | --- |
| Provider webhook ingress, integration bindings, durable envelope intake, deduplication, queues, retries, backpressure, dead letters and replay | `relayfile-control` |
| Flow definitions and source validation; runs, steps, events, logs, artifacts and storage; Flows worker enrollment, assignment and lifecycle | `relayfile-control` |
| People, organizations, workspaces, membership, device/browser login, user sessions, workspace API credentials and introspection | `relay-control` |
| Session-history access/handoff, Desktop/MCP session control, messaging and remote-agent connections, workspace configuration and canonical credential storage | `relay-control` |
| Credential-broker server, run-bound subscription/Relayfile credential issuance and revocation, workspace Relaycast binding | `relay-control` |
| Hosted dashboard/UI, integration setup and the provider-readiness aggregation currently implemented in Cloud | `cloud` (UI ports in `relay-webapp` where implemented) |

Implement Flows service fixes here, including source-validation and stored
run/definition lifecycle changes. Do not send those fixes back to the legacy
`relay-control/src/flows` copy automatically. Provider-specific adapter
contracts stay in their owning packages; use existing exports rather than
creating a second hand-maintained declaration or implementation.

## The HTTP boundary

- This repo owns the public `/api/v1/flows`, `/api/v1/workers` and
  `/api/v1/workflows` route families and their `/cloud/api/v1/...` aliases.
- User authentication is delegated to `relay-control` through
  `POST /internal/v1/auth/introspect`; this repo validates the returned
  workspace, role, scope, audience and lifetime as the existing contract requires.
  Do not create a parallel user/organization/session authority here.
- `src/flows/broker.ts` is the client of relay-control's
  `/internal/v1/flows/credentials/*` server. The issuer, canonical provider
  logins and workspace Relaycast binding remain in `relay-control`.
  Preserve workspace/run/reference binding, least-privilege scopes, audiences,
  revocation and refusal-versus-outage behavior.
- Flow worker/enrollment credentials and encrypted, scoped per-run credential
  caches belong here. Receiving a run credential does not transfer authority
  over the underlying user or workspace credential store.
- Use documented HTTP seams. Do not import sibling application source,
  directly query/write the identity database or copy its migrations. Each
  service owns its database and migrations. Update producers, consumers and
  seam tests together when a contract changes; see [README — Seams](README.md#seams),
  `test/flows-seams.test.ts` and `test/flows-relayfile-credential.test.ts`.

Service/queue health here is distinct from the hosted provider-readiness
aggregation in Cloud. Do not copy Cloud's dashboard or GitLab readiness code
into this service when no equivalent component exists. A later extraction
needs an explicit owner and contract, not an incidental copy.

## Legacy code and cutover

Older `relay-control` builds may still serve Flows. That is deployment state,
not a change to canonical ownership. Any temporary legacy compatibility patch
must have a verified active deployment and tracked cutover/retirement reason.
Do not mirror every fix, reintroduce public Flows ownership in relay-control,
or delete live legacy handlers automatically.

This document does not change routing, migrate data, enable Flows or authorize
a deployment. Treat those as separate cutover work under the existing release
policy, preserving active callers and persisted runs. Keep both repos' boundary
instructions consistent when an explicitly agreed service move changes ownership.
