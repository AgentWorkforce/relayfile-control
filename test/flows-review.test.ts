import assert from "node:assert/strict";
import test from "node:test";
import { BrokerError, BrokerUnavailable, httpRelaycastCredentialSource } from "../src/flows/broker.ts";
import { claimFailure } from "../src/flows/introspection.ts";
import { MemoryLogger } from "../src/observability/log.ts";
import { AUDIENCE, openQueue, seedTenant, startControlPlane } from "./support/flows.ts";

const FLOW = `version: '1.0'
name: review-flow
swarm:
  pattern: pipeline
workflows:
  - name: main
    steps:
      - name: generate
        type: deterministic
        command: printf 'hello\\n'
`;
const VERIFY_SECRET = "runtime-verify-secret-at-least-32-bytes";
const BROKER_SECRET = "broker-secret-that-is-at-least-32-bytes";

type ControlPlane = Awaited<ReturnType<typeof startControlPlane>>;

async function registerWorker(cp: ControlPlane, ownerToken: string, name = "w1") {
  const enrollment = await cp.api("/api/v1/workers/enrollment-tokens", { method: "POST", token: ownerToken, body: { name } });
  assert.equal(enrollment.status, 200);
  const registered = await cp.api("/api/v1/workers/register", {
    method: "POST",
    body: { enrollmentToken: enrollment.body.token, name },
  });
  assert.equal(registered.status, 200);
  return registered.body as { workerId: string; workerToken: string };
}

test("introspection claims are enforced locally and outages answer 503", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const invalid = [401, { error: "unauthorized", message: "Invalid or expired token" }];
  const status = async (token: string) => {
    const response = await cp.api("/api/v1/flows", { token });
    return [response.status, response.body];
  };
  assert.equal((await status(tenant.memberToken))[0], 200);

  const now = Math.floor(Date.now() / 1000);
  const cases: Array<[string, Record<string, unknown>]> = [
    ["wrong aud", { aud: "https://someone-else.example" }],
    ["missing aud", { aud: undefined }],
    ["expired", { exp: now - 1 }],
    ["issued in the future", { iat: now + 120 }],
    ["agent principal", { principal_type: "agent" }],
    ["service principal", { principal_type: "service" }],
    ["missing role", { role: undefined }],
    ["missing email", { email: null }],
  ];
  for (const [name, fields] of cases) {
    const token = cp.introspection.mint({
      userId: "u-claims",
      email: "claims@acme.test",
      organizationId: tenant.organizationId,
      workspaceId: tenant.workspaceId,
      role: "member",
    });
    cp.introspection.patch(token, fields);
    assert.deepEqual(await status(token), invalid, name);
  }
  // Small skew is tolerated.
  const skewed = cp.introspection.mint({
    userId: "u-skew",
    email: "skew@acme.test",
    organizationId: tenant.organizationId,
    workspaceId: tenant.workspaceId,
    role: "member",
  });
  cp.introspection.patch(skewed, { iat: now + 30, aud: AUDIENCE });
  assert.equal((await status(skewed))[0], 200);

  for (const outage of [500, 401, 503]) {
    cp.introspection.failWith = outage;
    const response = await cp.api("/api/v1/flows", { token: tenant.memberToken });
    assert.deepEqual(
      [response.status, response.body],
      [503, { error: "auth_unavailable", message: "Authentication is temporarily unavailable" }],
      `introspection answered ${outage}`,
    );
    assert.equal(response.headers.get("cache-control"), "no-store");
  }
  cp.introspection.failWith = null;
});

test("cookie-authenticated /cloud requests follow relay-control's CSRF rule", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const cookie = `agent_relay_session=${tenant.memberToken}`;
  const body = JSON.stringify({ workflow: FLOW, fileType: "yaml" });
  const post = (headers: Record<string, string>, raw = body) =>
    cp.api("/cloud/api/v1/workflows/run", { method: "POST", raw, headers: { cookie, ...headers } });
  const refused = [403, { error: "csrf_rejected", message: "Cross-origin request refused" }];

  // GET with the cookie needs no Origin.
  assert.equal((await cp.api("/cloud/api/v1/flows", { headers: { cookie } })).status, 200);
  for (const [name, headers] of [
    ["no origin, no fetch metadata", { "content-type": "application/json" }],
    ["foreign origin", { "content-type": "application/json", origin: "https://evil.example" }],
    ["null origin", { "content-type": "application/json", origin: "null" }],
    ["cross-site fetch metadata", { "content-type": "application/json", "sec-fetch-site": "cross-site" }],
    ["form content type", { "content-type": "application/x-www-form-urlencoded", origin: AUDIENCE }],
    ["text content type", { "content-type": "text/plain", origin: AUDIENCE }],
  ] as const) {
    const response = await post(headers);
    assert.deepEqual([response.status, response.body], refused, name);
  }
  assert.equal(cp.introspection.calls.filter((c) => c.token === tenant.memberToken).length, 1, "refused before introspection");

  const allowed = await post({ "content-type": "application/json; charset=utf-8", origin: "https://control.selfhosted.company" });
  assert.equal(allowed.status, 201);
  const sameOrigin = await post({ "content-type": "application/json", "sec-fetch-site": "same-origin" });
  assert.equal(sameOrigin.status, 201);
  // A bearer is not subject to the cookie rule.
  const bearer = await cp.api("/cloud/api/v1/workflows/run", {
    method: "POST",
    raw: body,
    headers: { authorization: `Bearer ${tenant.memberToken}`, cookie, "content-type": "application/json", origin: "https://evil.example" },
  });
  assert.equal(bearer.status, 201);
});

test("cli-credentials: broker errors are mapped and a finished run is refused", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const worker = await registerWorker(cp, tenant.ownerToken);
  const queue = openQueue(cp.origin, worker.workerId, worker.workerToken);
  t.after(() => queue.close());
  await queue.ready;
  const run = await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow: FLOW, fileType: "yaml" } });
  assert.equal(run.status, 201);
  const runId = run.body.runId as string;
  await queue.next("assignment");
  const path = `/api/v1/workers/${worker.workerId}/assignments/${runId}/cli-credentials`;
  const read = () => cp.api(path, { token: worker.workerToken, headers: { "x-cli-credential-ref": "ref-1" } });

  const ok = await read();
  assert.deepEqual([ok.status, ok.body], [200, { files: [], skipped: [] }]);
  assert.deepEqual(cp.broker.calls.at(-1), { op: "readForRun", args: ["ref-1", runId] });

  const failWith = (error: Error) => {
    cp.broker.readForRun = async () => {
      throw error;
    };
  };
  failWith(new BrokerError(409, "credential_reference_expired", "Reference expired"));
  let response = await read();
  assert.deepEqual([response.status, response.body], [409, { error: "Reference expired", code: "credential_reference_expired" }]);
  for (const refused of [401, 403]) {
    failWith(new BrokerError(refused, "unauthorized", "bad broker secret"));
    response = await read();
    assert.deepEqual(
      [response.status, response.body],
      [502, { error: "The credential broker refused this service", code: "credential_broker_rejected" }],
      `broker ${refused}`,
    );
  }
  failWith(new BrokerUnavailable("relay-control answered 503"));
  response = await read();
  assert.deepEqual([response.status, response.body], [500, { error: "internal", message: "Internal server error" }]);
  assert.equal(cp.app.logger.named("flows_internal_error").at(-1)?.error, "BrokerUnavailable");

  const completed = await cp.api(`/api/v1/workers/${worker.workerId}/assignments/${runId}/status`, {
    method: "POST",
    token: worker.workerToken,
    body: { phase: "completed", exitCode: 0 },
  });
  assert.equal(completed.status, 200);
  delete (cp.broker as { readForRun?: unknown }).readForRun;
  response = await read();
  assert.deepEqual([response.status, response.body], [404, { error: "Assignment not found", code: "assignment_not_found" }]);
});

test("a refused Relaycast binding releases the claim, logs once and does not spin", async (t) => {
  const cp = await startControlPlane({ queuePollMs: 10 });
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  let calls = 0;
  cp.flows.relaycastCredentials = httpRelaycastCredentialSource({
    baseUrl: "http://relay-control.test",
    secret: BROKER_SECRET,
    fetchImpl: (async () => {
      calls += 1;
      return Response.json({ error: "relaycast_not_configured", message: "no binding" }, { status: 404 });
    }) as typeof fetch,
  });
  const worker = await registerWorker(cp, tenant.ownerToken);
  const run = await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow: FLOW, fileType: "yaml" } });
  const queue = openQueue(cp.origin, worker.workerId, worker.workerToken);
  t.after(() => queue.close());
  await queue.ready;
  await new Promise((resolve) => setTimeout(resolve, 300));
  assert.equal(calls, 1, "one attempt, then the workspace waits");
  assert.equal(queue.events.some((e) => e.event === "assignment"), false);
  const current = await cp.api(`/api/v1/workflows/runs/${run.body.runId}`, { token: tenant.memberToken });
  assert.equal(current.body.assignmentStatus, "queued", "the claim was released");
  const events = cp.app.logger.named("flows_relaycast_binding_refused");
  assert.equal(events.length, 1);
  assert.deepEqual(
    { workspaceId: events[0]!.workspaceId, status: events[0]!.status, code: events[0]!.code },
    { workspaceId: tenant.workspaceId, status: 404, code: "relaycast_not_configured" },
  );
  assert.equal(cp.app.logger.named("flows_assignment_deferred").length, 0, "not reported as an outage");
});

test("runtime-credentials verify: own secret, constant-time bearer, worker credentials only", async (t) => {
  const off = await startControlPlane();
  t.after(() => off.stop());
  const unset = await off.api("/internal/v1/flows/runtime-credentials/verify", {
    method: "POST",
    token: VERIFY_SECRET,
    body: { token: "sh_wrk_x" },
  });
  assert.equal(unset.status, 404, "unset secret disables the route");

  const cp = await startControlPlane({}, { RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET: VERIFY_SECRET, RELAYFILE_CONTROL_BROKER_SECRET: BROKER_SECRET });
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const worker = await registerWorker(cp, tenant.ownerToken);
  const verify = (token: string | undefined, body: unknown, method = "POST") =>
    cp.api("/internal/v1/flows/runtime-credentials/verify", { method, ...(token ? { token } : {}), body });
  assert.equal((await verify(undefined, { token: worker.workerToken })).status, 401);
  assert.equal((await verify(`${VERIFY_SECRET.slice(0, -1)}x`, { token: worker.workerToken })).status, 401);
  assert.equal((await verify(BROKER_SECRET, { token: worker.workerToken })).status, 401, "the broker secret is not accepted");
  assert.equal((await cp.api("/internal/v1/flows/runtime-credentials/verify", { token: VERIFY_SECRET })).status, 405);
  assert.equal((await verify(VERIFY_SECRET, { nope: 1 })).status, 400);

  const live = await verify(VERIFY_SECRET, { token: worker.workerToken });
  assert.equal(live.status, 200);
  assert.deepEqual(live.body, {
    active: true,
    subject: `worker:${worker.workerId}`,
    workspaceId: tenant.workspaceId,
    scopes: ["runtime:env:read", "runtime:integrations:read"],
  });
  assert.equal(live.headers.get("cache-control"), "no-store");
  assert.doesNotMatch(live.text, new RegExp(worker.workerToken), "the credential is not echoed");
  for (const other of ["sh_wrk_enr_x", "sh_wrk_unknown", tenant.memberToken]) {
    assert.deepEqual((await verify(VERIFY_SECRET, { token: other })).body, { active: false }, other);
  }
  const revoked = await cp.api(`/api/v1/workers/${worker.workerId}`, { method: "DELETE", token: tenant.ownerToken });
  assert.equal(revoked.status, 200);
  assert.deepEqual((await verify(VERIFY_SECRET, { token: worker.workerToken })).body, { active: false }, "revoked");
});

test("internal workspace delete removes the workspace's Flows data and queues revocations", async (t) => {
  const cp = await startControlPlane({}, { RELAYFILE_CONTROL_BROKER_SECRET: BROKER_SECRET, RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET: VERIFY_SECRET });
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const worker = await registerWorker(cp, tenant.ownerToken);
  await cp.api("/api/v1/workers/enrollment-tokens", { method: "POST", token: tenant.ownerToken, body: { name: "spare" } });
  const flow = await cp.api("/api/v1/flows", { method: "POST", token: tenant.memberToken, body: { name: "review-flow", source: FLOW } });
  assert.equal(flow.status, 201);
  const runs: string[] = [];
  for (let i = 0; i < 2; i += 1) {
    const run = await cp.api("/api/v1/workflows/run", {
      method: "POST",
      token: tenant.memberToken,
      body: { workflow: FLOW, fileType: "yaml", envSecrets: { MY_SECRET: "s3cret" } },
    });
    assert.equal(run.status, 201);
    runs.push(run.body.runId);
  }
  // The other workspace keeps its data.
  const kept = await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.otherOwnerToken, body: { workflow: FLOW, fileType: "yaml" } });
  assert.equal(kept.status, 201);

  const path = `/internal/v1/flows/workspaces/${tenant.workspaceId}`;
  assert.equal((await cp.api(path, { method: "DELETE" })).status, 401);
  assert.equal((await cp.api(path, { method: "DELETE", token: VERIFY_SECRET })).status, 401, "only the broker secret");
  assert.equal((await cp.api(path, { token: BROKER_SECRET })).status, 405);
  assert.equal((await cp.api("/internal/v1/flows/workspaces/not-a-uuid", { method: "DELETE", token: BROKER_SECRET })).status, 400);

  const deleted = await cp.api(path, { method: "DELETE", token: BROKER_SECRET });
  assert.equal(deleted.status, 200);
  assert.deepEqual(deleted.body, {
    workspaceId: tenant.workspaceId,
    deleted: { flows: 1, runs: 2, workers: 1, enrollmentTokens: 2, revocationsQueued: 2 },
  });
  const count = async (sql: string, args: unknown[] = [tenant.workspaceId]) =>
    Number((await cp.database.query<{ n: string | number }>(sql, args)).rows[0]!.n);
  assert.equal(await count("SELECT count(*) AS n FROM selfhost_workflow_runs WHERE workspace_id = $1"), 0);
  assert.equal(await count("SELECT count(*) AS n FROM selfhost_flow_definitions WHERE workspace_id = $1"), 0);
  assert.equal(await count("SELECT count(*) AS n FROM selfhost_workers WHERE workspace_id = $1"), 0);
  assert.equal(await count("SELECT count(*) AS n FROM selfhost_worker_enrollment_tokens WHERE workspace_id = $1"), 0);
  assert.equal(await count("SELECT count(*) AS n FROM selfhost_work_assignments WHERE workspace_id = $1"), 0);
  assert.equal(await count("SELECT count(*) AS n FROM selfhost_run_events WHERE run_id = ANY($1::uuid[])", [runs]), 0);
  assert.equal(await count("SELECT count(*) AS n FROM selfhost_workflow_runs WHERE id = $1", [kept.body.runId]), 1);
  const queued = await cp.database.query<{ run_id: string }>("SELECT run_id FROM selfhost_cli_credential_revocations ORDER BY run_id");
  assert.deepEqual(queued.rows.map((r) => r.run_id), [...runs].sort());
  assert.equal(cp.app.logger.named("flows_workspace_deleted").length, 1);
  // The deleted worker's credential stops working.
  assert.equal((await cp.api(`/api/v1/workers/${worker.workerId}/heartbeat`, { method: "POST", token: worker.workerToken })).status, 401);

  const again = await cp.api(path, { method: "DELETE", token: BROKER_SECRET });
  assert.deepEqual([again.status, again.body.deleted], [200, { flows: 0, runs: 0, workers: 0, enrollmentTokens: 0, revocationsQueued: 0 }]);
});

test("store errors are logged by name only", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const logger = new MemoryLogger();
  cp.flows.logger = logger;
  const tenant = await seedTenant(cp);
  const worker = await registerWorker(cp, tenant.ownerToken);
  const queue = openQueue(cp.origin, worker.workerId, worker.workerToken);
  await queue.ready;
  await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow: FLOW, fileType: "yaml" } });
  await queue.next("assignment");
  queue.close();
  // Redelivery of the unacknowledged assignment fails: the event carries the error name, never its text.
  cp.flows.relaycastCredentials = async () => {
    throw new Error("secret-bearing message rk_live_primary");
  };
  const reconnected = openQueue(cp.origin, worker.workerId, worker.workerToken);
  t.after(() => reconnected.close());
  await reconnected.ready;
  await reconnected.next("ping");
  const event = logger.named("flows_assignment_redelivery_deferred")[0];
  assert.ok(event);
  assert.equal(event.error, "Error");
  assert.doesNotMatch(JSON.stringify(logger.events), /rk_live_primary/);
});

test("a relay-control outage during assignment holds the workspace with a growing delay", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const store = cp.flows;
  store.outageHoldInitialMs = 1_000;
  store.outageHoldMaxMs = 3_000;
  const first = await registerWorker(cp, tenant.ownerToken, "w1");
  const second = await registerWorker(cp, tenant.ownerToken, "w2");
  const run = await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow: FLOW, fileType: "yaml" } });
  assert.equal(run.status, 201);
  const real = store.relaycastCredentials!;
  let failures = 3;
  let calls = 0;
  store.relaycastCredentials = async (workspaceId) => {
    calls += 1;
    if (failures > 0) {
      failures -= 1;
      throw new BrokerUnavailable("relay-control answered 503");
    }
    return real(workspaceId);
  };
  let clock = Date.now();
  t.mock.method(Date, "now", () => clock);
  const claim = (workerId: string) => store.claimNext({ id: workerId, workspaceId: tenant.workspaceId, tags: [] });
  const assignmentStatus = async () =>
    (await cp.api(`/api/v1/workflows/runs/${run.body.runId}`, { token: tenant.memberToken })).body.assignmentStatus;

  // Hold after each failure: 1 s, then 2 s, then 3 s (doubled to 4 s, capped).
  for (const [attempt, holdMs] of [[1, 1_000], [2, 2_000], [3, 3_000]] as const) {
    await assert.rejects(claim(first.workerId), { name: "BrokerUnavailable" });
    assert.equal(calls, attempt);
    assert.equal(await assignmentStatus(), "queued", "the claim was released");
    for (const workerId of [first.workerId, second.workerId]) assert.equal(await claim(workerId), null, "held for every worker");
    clock += holdMs - 1;
    assert.equal(await claim(second.workerId), null, `still held just before ${holdMs} ms`);
    assert.equal(calls, attempt, "no attempt while held");
    clock += 1;
  }
  // The hold expired and relay-control answers again: the run is assigned.
  const assignment = await claim(first.workerId);
  assert.equal(assignment?.runId, run.body.runId);
  assert.equal(calls, 4);
  const events = await cp.api(`/api/v1/workflows/runs/${run.body.runId}/events`, { token: tenant.memberToken });
  assert.deepEqual(
    events.body.events.map((event: { eventType: string }) => event.eventType),
    ["run.queued", "run.relayfile_credential_unavailable", "run.assigned"],
    "run.assigned (and the Relayfile refusal) is written once, for the delivered attempt only",
  );

  // Success reset the delay: the next outage holds for the initial 1 s again.
  const next = await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow: FLOW, fileType: "yaml" } });
  failures = 1;
  await assert.rejects(claim(second.workerId), { name: "BrokerUnavailable" });
  clock += 999;
  assert.equal(await claim(second.workerId), null);
  clock += 1;
  assert.equal((await claim(second.workerId))?.runId, next.body.runId);
});

test("an outage hold is cleared by workspace delete", async (t) => {
  const cp = await startControlPlane({}, { RELAYFILE_CONTROL_BROKER_SECRET: BROKER_SECRET });
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const store = cp.flows;
  const worker = await registerWorker(cp, tenant.ownerToken);
  await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow: FLOW, fileType: "yaml" } });
  const real = store.relaycastCredentials!;
  store.relaycastCredentials = async () => {
    throw new BrokerUnavailable("relay-control answered 503");
  };
  await assert.rejects(store.claimNext({ id: worker.workerId, workspaceId: tenant.workspaceId, tags: [] }), { name: "BrokerUnavailable" });
  assert.equal((await cp.api(`/internal/v1/flows/workspaces/${tenant.workspaceId}`, { method: "DELETE", token: BROKER_SECRET })).status, 200);
  // The workspace starts over: a fresh worker and run are offered at once.
  store.relaycastCredentials = real;
  const fresh = await registerWorker(cp, tenant.ownerToken, "w-fresh");
  const run = await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow: FLOW, fileType: "yaml" } });
  assert.equal((await store.claimNext({ id: fresh.workerId, workspaceId: tenant.workspaceId, tags: [] }))?.runId, run.body.runId);
});

test("workspace delete queues revocations for exactly the deleted runs and stays idempotent", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const runs: string[] = [];
  for (let i = 0; i < 3; i += 1) {
    const run = await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow: FLOW, fileType: "yaml" } });
    assert.equal(run.status, 201);
    runs.push(run.body.runId);
  }
  // A finished run already has its revocation queued; it is not queued twice.
  await cp.database.query("INSERT INTO selfhost_cli_credential_revocations (run_id) VALUES ($1)", [runs[0]]);
  const deleted = await cp.flows.deleteWorkspace(tenant.workspaceId);
  assert.deepEqual(deleted, { flows: 0, runs: 3, workers: 0, enrollmentTokens: 0, revocationsQueued: 2 });
  const queued = await cp.database.query<{ run_id: string }>("SELECT run_id FROM selfhost_cli_credential_revocations ORDER BY run_id");
  assert.deepEqual(queued.rows.map((r) => r.run_id), [...runs].sort());
  assert.deepEqual(await cp.flows.deleteWorkspace(tenant.workspaceId), {
    flows: 0,
    runs: 0,
    workers: 0,
    enrollmentTokens: 0,
    revocationsQueued: 0,
  });
  assert.equal((await cp.database.query("SELECT run_id FROM selfhost_cli_credential_revocations")).rows.length, 3);
});

test("introspection aud is compared as a normalized origin and fails closed", () => {
  const now = Date.now();
  const answer = {
    active: true as const,
    sub: "u1",
    email: "u1@acme.test",
    principal_type: "desktop-user",
    org: "o1",
    workspace: "w1",
    scopes: [],
    exp: Math.floor(now / 1000) + 600,
    token_id: "t1",
  };
  const audience = "https://flows.example.com";
  for (const aud of ["https://Flows.Example.com:443/", "https://flows.example.com/", "https://flows.example.com"]) {
    assert.equal(claimFailure({ ...answer, aud }, audience, now), null, aud);
  }
  for (const aud of [
    undefined,
    "",
    "not a url",
    "http://flows.example.com",
    "https://flows.example.com:8443",
    "https://flows.example.com/path",
    "https://flows.example.com/?q=1",
    "https://user@flows.example.com",
    "https://other.example.com",
  ]) {
    const { aud: _drop, ...rest } = { ...answer, aud };
    assert.equal(claimFailure(aud === undefined ? rest : { ...rest, aud }, audience, now), "audience", String(aud));
  }
});
