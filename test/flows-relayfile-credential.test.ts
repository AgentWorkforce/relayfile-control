import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import {
  BROKER_PATHS,
  BrokerUnavailable,
  httpRelayfileCredentialBroker,
  RelayfileCredentialRefused,
  type RelayfileCredential,
} from "../src/flows/broker.ts";
import { createIntrospector, type ActiveIntrospection, type Introspection } from "../src/flows/introspection.ts";
import { verifyRelayfileCredential } from "../src/flows/relayfile-credential.ts";
import { relayfileMountPaths, relayfileScopesFor } from "../src/flows/relayfile-mount.ts";
import { seedTenant, startControlPlane } from "./support/flows.ts";

const RUN = "0b9e0f3c-4f7c-4f55-9d3c-2e1c1f7f2a10";
const ROOT = `/workflows/runs/${RUN}`;
const RELAYFILE_URL = "https://relayfile.example";
const BROKER_SECRET = "broker-secret-that-is-at-least-32-bytes";

const yamlWith = (integrations: string) => `version: '1.0'
name: rf-flow
${integrations}
swarm:
  pattern: pipeline
workflows:
  - name: main
    steps:
      - name: generate
        type: deterministic
        command: printf 'hello\\n'
`;

// ------------------------------------------------------------ mount paths

test("relayfile mount paths follow Cloud's rules", () => {
  const rows: Array<{ name: string; fileType: "yaml" | "ts"; workflow: string; expected: unknown }> = [
    { name: "ts declares no providers", fileType: "ts", workflow: "integrations: { github: {} }", expected: { mountPaths: [ROOT] } },
    { name: "yaml without integrations", fileType: "yaml", workflow: yamlWith(""), expected: { mountPaths: [ROOT] } },
    {
      name: "providers sorted, reserved keys dropped",
      fileType: "yaml",
      workflow: yamlWith("integrations:\n  slack: { channel: x }\n  relayfile: { a: 1 }\n  github: { repo: a/b }\n  subscriptions: [x]"),
      expected: { mountPaths: [ROOT, "/github", "/slack"] },
    },
    {
      name: "every mountable provider",
      fileType: "yaml",
      workflow: yamlWith("integrations:\n  slack: {}\n  shortcut: {}\n  linear: {}\n  jira: {}\n  gitlab: {}\n  github: {}"),
      expected: { mountPaths: [ROOT, "/github", "/gitlab", "/jira", "/linear", "/shortcut", "/slack"] },
    },
    {
      name: "duplicate keys dedupe",
      fileType: "yaml",
      workflow: yamlWith("integrations:\n  github: {}\n  github: { repo: x }"),
      expected: { mountPaths: [ROOT, "/github"] },
    },
    {
      name: "unknown provider is refused (with a duplicate present)",
      fileType: "yaml",
      workflow: yamlWith("integrations:\n  github: {}\n  github: {}\n  notion: {}"),
      expected: { refusal: { code: "invalid_mount_paths", reason: "unmountable_provider:notion" } },
    },
    { name: "unparseable yaml", fileType: "yaml", workflow: ": : not yaml [\n  - {", expected: { mountPaths: [ROOT] } },
    { name: "integrations is a list", fileType: "yaml", workflow: yamlWith("integrations:\n  - github"), expected: { mountPaths: [ROOT] } },
    { name: "integrations is a scalar", fileType: "yaml", workflow: yamlWith("integrations: github"), expected: { mountPaths: [ROOT] } },
    { name: "yaml document is a scalar", fileType: "yaml", workflow: "just text", expected: { mountPaths: [ROOT] } },
  ];
  for (const row of rows) {
    assert.deepEqual(relayfileMountPaths({ runId: RUN, workflow: row.workflow, fileType: row.fileType }), row.expected, row.name);
  }
});

test("relayfile scopes: read-only vs read-write", () => {
  assert.deepEqual(relayfileScopesFor([ROOT, "/github"], true), [
    `relayfile:fs:read:${ROOT}`,
    `relayfile:fs:read:${ROOT}/*`,
    "relayfile:fs:read:/github",
    "relayfile:fs:read:/github/*",
  ]);
  assert.deepEqual(relayfileScopesFor([ROOT], false), [
    `relayfile:fs:read:${ROOT}`,
    `relayfile:fs:read:${ROOT}/*`,
    `relayfile:fs:write:${ROOT}`,
    `relayfile:fs:write:${ROOT}/*`,
  ]);
});

// ------------------------------------------------------------ verification

test("relayfile credential verification table", () => {
  const nowMs = 1_800_000_000_000;
  const exp = nowMs / 1000 + 3600;
  const workspaceId = randomUUID();
  const credentialId = randomUUID();
  const mountPaths = [ROOT, "/github"];
  const scopes = relayfileScopesFor(mountPaths, false);
  const credential: RelayfileCredential = {
    relayfileToken: "sh_fr_fixture",
    relayfileTokenExpiresAt: new Date(exp * 1000).toISOString(),
    relayfileMountPaths: mountPaths,
    scopes,
    aud: RELAYFILE_URL,
    credentialId,
  };
  const answer: ActiveIntrospection = {
    active: true,
    sub: credentialId,
    email: null,
    principal_type: "agent",
    agent_name: `flows-run-${RUN}`,
    org: randomUUID(),
    workspace: workspaceId,
    scopes,
    exp,
    aud: RELAYFILE_URL,
    token_id: credentialId,
  };
  const rows: Array<{ name: string; answer?: Introspection; credential?: Partial<RelayfileCredential>; requested?: string[]; expected: string | null }> = [
    { name: "good", expected: null },
    { name: "aud with default port and trailing slash", answer: { ...answer, aud: "https://Relayfile.example:443/" }, expected: null },
    { name: "scopes in another order", answer: { ...answer, scopes: [...scopes].reverse() }, expected: null },
    { name: "mount paths in another order", credential: { relayfileMountPaths: ["/github", ROOT] }, expected: null },
    { name: "exp within 1 s", credential: { relayfileTokenExpiresAt: new Date(exp * 1000 + 900).toISOString() }, expected: null },
    { name: "inactive", answer: { active: false, reason: "revoked" }, expected: "revoked" },
    { name: "principal_type", answer: { ...answer, principal_type: "desktop-user" }, expected: "principal" },
    { name: "agent_name", answer: { ...answer, agent_name: "flows-run-other" }, expected: "principal" },
    { name: "agent_name missing", answer: { ...answer, agent_name: undefined }, expected: "principal" },
    { name: "token_id", answer: { ...answer, token_id: randomUUID() }, expected: "credential_id" },
    { name: "workspace", answer: { ...answer, workspace: randomUUID() }, expected: "workspace" },
    { name: "aud other origin", answer: { ...answer, aud: "https://relayfile.example:8443" }, expected: "audience" },
    { name: "aud missing", answer: { ...answer, aud: undefined }, expected: "audience" },
    { name: "aud not an origin", answer: { ...answer, aud: "https://relayfile.example/path" }, expected: "audience" },
    {
      name: "exp too soon",
      answer: { ...answer, exp: nowMs / 1000 + 59 },
      credential: { relayfileTokenExpiresAt: new Date(nowMs + 59_000).toISOString() },
      expected: "expiry",
    },
    { name: "exp mismatch > 1 s", credential: { relayfileTokenExpiresAt: new Date(exp * 1000 + 1_500).toISOString() }, expected: "expiry" },
    { name: "scope missing", answer: { ...answer, scopes: scopes.slice(1) }, expected: "scopes" },
    { name: "scope extra", answer: { ...answer, scopes: [...scopes, "relayfile:fs:read:/slack"] }, expected: "scopes" },
    {
      name: "scope out of mount (issuer agrees)",
      answer: { ...answer, scopes: [...scopes, "relayfile:fs:read:/slack"] },
      credential: { scopes: [...scopes, "relayfile:fs:read:/slack"] },
      expected: "scopes",
    },
    {
      name: "broader scope (issuer agrees)",
      answer: { ...answer, scopes: [...scopes, "relayfile:fs:*"] },
      credential: { scopes: [...scopes, "relayfile:fs:*"] },
      expected: "scopes",
    },
    { name: "no scopes", answer: { ...answer, scopes: [] }, credential: { scopes: [] }, expected: "scopes" },
    { name: "mount path mismatch", requested: [ROOT, "/github", "/slack"], expected: "mount_paths" },
    {
      name: "issuer mounted something else",
      credential: { relayfileMountPaths: [ROOT, "/gitlab"] },
      answer: { ...answer, scopes: relayfileScopesFor([ROOT, "/gitlab"], false) },
      expected: "scopes",
    },
    {
      name: "read-only grant for a read-write request (issuer and introspection agree)",
      credential: { scopes: relayfileScopesFor(mountPaths, true) },
      answer: { ...answer, scopes: relayfileScopesFor(mountPaths, true) },
      expected: "scopes",
    },
    {
      name: "grant wider than the mount paths (issuer and introspection agree)",
      credential: { scopes: [...scopes, "relayfile:fs:read:/slack", "relayfile:fs:read:/slack/*"] },
      answer: { ...answer, scopes: [...scopes, "relayfile:fs:read:/slack", "relayfile:fs:read:/slack/*"] },
      expected: "scopes",
    },
  ];
  for (const row of rows) {
    const issued = { ...credential, ...row.credential };
    const actual = verifyRelayfileCredential({
      credential: issued,
      answer: row.answer ?? answer,
      runId: RUN,
      workspaceId,
      relayfileOrigin: RELAYFILE_URL,
      requestedMountPaths: row.requested ?? mountPaths,
      readOnly: false,
      nowMs,
    });
    assert.equal(actual, row.expected, row.name);
  }
});

test("introspection: per-call audience, agent_name, and null email/machine_id", async () => {
  const requests: any[] = [];
  const exp = Math.floor(Date.now() / 1000) + 3600;
  const fetchImpl = (async (_url: string, init: RequestInit) => {
    const body = JSON.parse(String(init.body));
    requests.push(body);
    return Response.json({
      active: true, sub: "c1", email: null, machine_id: null, principal_type: "agent", agent_name: `flows-run-${RUN}`,
      org: "o1", workspace: "w1", scopes: ["relayfile:fs:read:/x"], aud: RELAYFILE_URL, iat: 0, exp, token_id: "c1",
    });
  }) as typeof fetch;
  const introspect = createIntrospector({ baseUrl: "http://rc.test", secret: "s", audience: "https://control.example", cacheSeconds: 30, fetchImpl });
  const answer = await introspect("sh_fr_one", "w1", RELAYFILE_URL);
  assert.deepEqual(requests[0], { token: "sh_fr_one", aud: RELAYFILE_URL, workspace: "w1" });
  assert.ok(answer.active);
  assert.equal(answer.agent_name, `flows-run-${RUN}`);
  assert.equal(answer.email, null);
  assert.equal(answer.principal_type, "agent");
  // The same token under the fixed audience is a separate cache entry, and fails the aud check.
  const fixed = await introspect("sh_fr_one", "w1");
  assert.equal(requests.length, 2);
  assert.equal(requests[1].aud, "https://control.example");
  assert.deepEqual(fixed, { active: false, reason: "audience" });
  await introspect("sh_fr_one", "w1", RELAYFILE_URL);
  assert.equal(requests.length, 2, "cached per audience");
});

// ------------------------------------------------------------ issuer client

test("relayfile credential client: request shape and answer mapping", async () => {
  const credentialId = randomUUID();
  const good = {
    relayfileToken: "sh_fr_abc",
    relayfileTokenExpiresAt: "2026-11-10T00:00:00.000Z",
    relayfileMountPaths: [ROOT],
    scopes: relayfileScopesFor([ROOT], false),
    aud: RELAYFILE_URL,
    credentialId,
  };
  const seen: Array<{ url: string; auth: string | null; body: any }> = [];
  const client = (answer: () => Response | Promise<Response>) => httpRelayfileCredentialBroker({
    baseUrl: "http://relay-control.internal",
    secret: BROKER_SECRET,
    fetchImpl: (async (url: string, init: RequestInit) => {
      seen.push({ url, auth: new Headers(init.headers).get("authorization"), body: JSON.parse(String(init.body)) });
      return answer();
    }) as typeof fetch,
  });
  const request = { workspaceId: "w1", runId: RUN, mountPaths: [ROOT], readOnly: false, aud: RELAYFILE_URL };
  assert.deepEqual(await client(() => Response.json(good)).issue(request), good);
  assert.equal(seen[0]!.url, `http://relay-control.internal${BROKER_PATHS.issueRelayfileCredential}`);
  assert.equal(seen[0]!.url, "http://relay-control.internal/internal/v1/flows/credentials/relayfile");
  assert.equal(seen[0]!.auth, `Bearer ${BROKER_SECRET}`);
  assert.deepEqual(seen[0]!.body, request);

  const refusals: Array<[number, string]> = [
    [400, "invalid_request"],
    [400, "invalid_mount_paths"],
    [404, "relayfile_not_configured"],
    [404, "workspace_not_found"],
    [409, "relayfile_audience_not_accepted"],
    [403, "forbidden"],
  ];
  for (const [status, code] of refusals) {
    await assert.rejects(
      client(() => Response.json({ error: code, message: "no" }, { status })).issue(request),
      (error: unknown) => error instanceof RelayfileCredentialRefused && error.status === status && error.code === code,
      `${status} ${code}`,
    );
  }
  const outages: Array<[string, () => Response | Promise<Response>]> = [
    ["500", () => Response.json({ error: "internal" }, { status: 500 })],
    ["503", () => new Response("", { status: 503 })],
    ["network", () => Promise.reject(new TypeError("fetch failed"))],
    ["not json", () => new Response("<html>")],
    ["empty token", () => Response.json({ ...good, relayfileToken: "" })],
    ["wrong prefix", () => Response.json({ ...good, relayfileToken: "relay_pa_x" })],
    ["bad expiry", () => Response.json({ ...good, relayfileTokenExpiresAt: "soon" })],
    ["no mount paths", () => Response.json({ ...good, relayfileMountPaths: [] })],
    ["scopes not strings", () => Response.json({ ...good, scopes: [1] })],
    ["no aud", () => Response.json({ ...good, aud: undefined })],
    ["credentialId not a uuid", () => Response.json({ ...good, credentialId: "c1" })],
  ];
  for (const [name, answer] of outages) {
    await assert.rejects(client(answer).issue(request), BrokerUnavailable, name);
  }
});

// ------------------------------------------------------------ end to end

type ControlPlane = Awaited<ReturnType<typeof startControlPlane>>;

async function setup(t: test.TestContext, flowsOverrides: Parameters<typeof startControlPlane>[0] = { workerRelayfileUrl: RELAYFILE_URL }) {
  const cp = await startControlPlane(flowsOverrides);
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const enrollment = await cp.api("/api/v1/workers/enrollment-tokens", { method: "POST", token: tenant.ownerToken, body: { name: "w1" } });
  const registered = await cp.api("/api/v1/workers/register", { method: "POST", body: { enrollmentToken: enrollment.body.token, name: "w1" } });
  assert.equal(registered.status, 200);
  const worker = registered.body as { workerId: string; workerToken: string };
  const startRun = async (workflow: string, fileType: "yaml" | "ts" = "yaml") => {
    const run = await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow, fileType } });
    assert.equal(run.status, 201);
    return run.body.runId as string;
  };
  const claim = () => cp.flows.claimNext({ id: worker.workerId, workspaceId: tenant.workspaceId, tags: [] });
  const payloadOf = (assignment: { workflowRef: { value: string } } | null) => {
    assert.ok(assignment);
    return JSON.parse(assignment.workflowRef.value);
  };
  const events = async (runId: string) =>
    (await cp.database.query<{ event_type: string; payload: unknown }>(
      "SELECT event_type, payload FROM selfhost_run_events WHERE run_id = $1 ORDER BY sequence",
      [runId],
    )).rows;
  const unavailable = async (runId: string) =>
    (await events(runId)).filter((e) => e.event_type === "run.relayfile_credential_unavailable").map((e) => e.payload);
  const runRow = async (runId: string) =>
    (await cp.database.query<Record<string, any>>("SELECT * FROM selfhost_workflow_runs WHERE id = $1", [runId])).rows[0]!;
  const revocations = async (runId: string) =>
    (await cp.database.query("SELECT run_id FROM selfhost_cli_credential_revocations WHERE run_id = $1", [runId])).rows.length;
  const refusedLogs = () => cp.app.logger.named("flows_relayfile_credential_refused");
  return { cp, tenant, worker, startRun, claim, payloadOf, events, unavailable, runRow, revocations, refusedLogs };
}

/** No issued token (or a distinctive piece of it) outside the ciphertext column. */
async function assertNoTokenLeak(cp: ControlPlane, runId: string) {
  const runEvents = JSON.stringify((await cp.database.query("SELECT * FROM selfhost_run_events WHERE run_id = $1", [runId])).rows);
  const row = { ...(await cp.database.query<Record<string, unknown>>("SELECT * FROM selfhost_workflow_runs WHERE id = $1", [runId])).rows[0] };
  delete row.relayfile_credential_ciphertext;
  const surfaces = [JSON.stringify(cp.app.logger.events), runEvents, JSON.stringify(row)];
  for (const credential of cp.relayfileIssuer.issued) {
    const secretPart = credential.relayfileToken.slice("sh_fr_".length, "sh_fr_".length + 16);
    for (const surface of surfaces) {
      assert.equal(surface.includes(credential.relayfileToken), false, "token leaked");
      assert.equal(surface.includes(secretPart), false, "token substring leaked");
    }
  }
  assert.doesNotMatch(runEvents, /sh_fr_/);
}

test("issued and verified: payload carries the credential, stored encrypted, reused, cleared on finish", async (t) => {
  const s = await setup(t);
  const runId = await s.startRun(yamlWith("integrations:\n  slack: {}\n  github: {}\n  relayfile: {}"));
  const payload = s.payloadOf(await s.claim());
  assert.equal(payload.relayfileUrl, RELAYFILE_URL);
  assert.match(payload.relayfileToken, /^sh_fr_/);
  assert.deepEqual(payload.relayfileMountPaths, [`/workflows/runs/${runId}`, "/github", "/slack"]);
  assert.ok(Date.parse(payload.relayfileTokenExpiresAt) > Date.now() + 3_600_000);
  const issuer = s.cp.relayfileIssuer;
  assert.equal(issuer.calls.length, 1);
  assert.deepEqual(issuer.calls[0], {
    workspaceId: s.tenant.workspaceId,
    runId,
    mountPaths: [`/workflows/runs/${runId}`, "/github", "/slack"],
    readOnly: false,
    aud: RELAYFILE_URL,
  });
  const verification = s.cp.introspection.calls.filter((call) => call.token === payload.relayfileToken);
  assert.deepEqual(verification.map((call) => [call.workspace, call.audience]), [[s.tenant.workspaceId, RELAYFILE_URL]]);
  const row = await s.runRow(runId);
  assert.ok(row.relayfile_credential_ciphertext);
  assert.equal(row.relayfile_credential_ciphertext.includes(payload.relayfileToken), false, "ciphertext hides the token");
  assert.equal(row.relayfile_credential_id, issuer.issued[0]!.credentialId);
  assert.equal(new Date(row.relayfile_credential_expires_at).toISOString(), payload.relayfileTokenExpiresAt);
  assert.equal(row.relayfile_credential_refusal, null);
  assert.deepEqual(await s.unavailable(runId), []);

  // Redelivery of the unacknowledged assignment reuses the stored credential.
  const redelivered = await s.cp.flows.unackedAssignments(s.worker.workerId);
  assert.equal(redelivered.length, 1);
  const again = s.payloadOf(redelivered[0]!);
  assert.equal(again.relayfileToken, payload.relayfileToken);
  assert.deepEqual(again.relayfileMountPaths, payload.relayfileMountPaths);
  assert.equal(issuer.calls.length, 1, "never re-issued for a live run");
  assert.equal(await s.revocations(runId), 0);
  await assertNoTokenLeak(s.cp, runId);

  // Terminal status: the columns are cleared and the run's revocation is queued.
  await s.cp.flows.reportStatus(s.worker.workerId, runId, { phase: "running" });
  await s.cp.flows.reportStatus(s.worker.workerId, runId, { phase: "completed", exitCode: 0 });
  const finished = await s.runRow(runId);
  assert.deepEqual(
    [finished.relayfile_credential_ciphertext, finished.relayfile_credential_id, finished.relayfile_credential_expires_at, finished.relayfile_credential_refusal],
    [null, null, null, null],
  );
  assert.equal(await s.revocations(runId), 1);
  assert.equal(s.refusedLogs().length, 0);
  await assertNoTokenLeak(s.cp, runId);
});

test("stored credential past its TTL: re-issued on redelivery, no refusal, no event", async (t) => {
  const s = await setup(t);
  const runId = await s.startRun(yamlWith("integrations:\n  github: {}"));
  const first = s.payloadOf(await s.claim());
  const issuer = s.cp.relayfileIssuer;
  assert.equal(issuer.calls.length, 1);
  // A fresh credential (TTL far away) is still reused.
  assert.equal(s.payloadOf((await s.cp.flows.unackedAssignments(s.worker.workerId))[0]!).relayfileToken, first.relayfileToken);
  assert.equal(issuer.calls.length, 1);

  for (const [name, offsetMs] of [["expired", -3_600_000], ["under the 60 s floor", 30_000]] as const) {
    await s.cp.database.query(
      "UPDATE selfhost_workflow_runs SET relayfile_credential_expires_at = $2 WHERE id = $1",
      [runId, new Date(Date.now() + offsetMs).toISOString()],
    );
    const before: number = issuer.calls.length;
    const again = s.payloadOf((await s.cp.flows.unackedAssignments(s.worker.workerId))[0]!);
    assert.equal(issuer.calls.length, before + 1, `${name}: re-issued`);
    assert.match(again.relayfileToken, /^sh_fr_/);
    assert.notEqual(again.relayfileToken, first.relayfileToken, name);
    assert.deepEqual(again.relayfileMountPaths, first.relayfileMountPaths, name);
    assert.ok(Date.parse(again.relayfileTokenExpiresAt) > Date.now() + 3_600_000, name);
    const row = await s.runRow(runId);
    assert.equal(row.relayfile_credential_id, issuer.issued[before]!.credentialId, name);
    assert.equal(new Date(row.relayfile_credential_expires_at).toISOString(), again.relayfileTokenExpiresAt, name);
    assert.equal(row.relayfile_credential_refusal, null, name);
  }
  assert.deepEqual(await s.unavailable(runId), []);
  assert.equal(await s.revocations(runId), 0, "run still live: revocation is the issuer's on re-issue");
  assert.equal(s.refusedLogs().length, 0);
  assert.equal(s.cp.app.logger.named("flows_relayfile_credential_reissue").length, 2);
  await assertNoTokenLeak(s.cp, runId);
});

test("issuer refusals: fields absent, one event with the code, one operator log, run still assigned", async (t) => {
  const rows: Array<{ status: number; code: string; expected: string }> = [
    { status: 404, code: "relayfile_not_configured", expected: "relayfile_not_configured" },
    { status: 404, code: "workspace_not_found", expected: "workspace_not_found" },
    { status: 409, code: "relayfile_audience_not_accepted", expected: "relayfile_audience_not_accepted" },
    { status: 400, code: "invalid_mount_paths", expected: "invalid_mount_paths" },
    { status: 400, code: "invalid_request", expected: "invalid_request" },
    { status: 403, code: "forbidden", expected: "unknown_refusal:403" },
    { status: 422, code: "something_new", expected: "unknown_refusal:422" },
  ];
  for (const row of rows) {
    const s = await setup(t);
    s.cp.relayfileIssuer.mode = { kind: "refuse", status: row.status, code: row.code };
    const callsBefore = s.cp.relayfileIssuer.calls.length;
    const logsBefore = s.refusedLogs().length;
    const runId = await s.startRun(yamlWith(""));
    const payload = s.payloadOf(await s.claim());
    assert.equal(payload.runId, runId);
    assert.equal(payload.relayfileUrl, RELAYFILE_URL, row.code);
    for (const field of ["relayfileToken", "relayfileTokenExpiresAt", "relayfileMountPaths"]) {
      assert.equal(field in payload, false, `${row.code}: ${field}`);
    }
    assert.deepEqual(await s.unavailable(runId), [{ code: row.expected }], row.code);
    const logs = s.refusedLogs().slice(logsBefore);
    assert.equal(logs.length, 1, row.code);
    assert.deepEqual(
      { workspaceId: logs[0]!.workspaceId, runId: logs[0]!.runId, status: logs[0]!.status, code: logs[0]!.code },
      { workspaceId: s.tenant.workspaceId, runId, status: row.status, code: row.expected },
    );
    assert.deepEqual((await s.runRow(runId)).relayfile_credential_refusal, { code: row.expected });
    const run = await s.cp.api(`/api/v1/workflows/runs/${runId}`, { token: s.tenant.memberToken });
    assert.equal(run.body.assignmentStatus, "assigned");

    // Redelivery reuses the decision: no second issuer call, event or log.
    const redelivered = await s.cp.flows.unackedAssignments(s.worker.workerId);
    assert.equal("relayfileToken" in s.payloadOf(redelivered[0]!), false);
    assert.equal(s.cp.relayfileIssuer.calls.length, callsBefore + 1, row.code);
    assert.equal((await s.unavailable(runId)).length, 1);
    assert.equal(s.refusedLogs().length, logsBefore + 1);
    assert.equal(await s.revocations(runId), 0, "nothing was issued, nothing to revoke early");
  }
});

test("verification failure: credential_invalid with the reason, no early revocation", async (t) => {
  const rows: Array<{ name: string; overrides?: Record<string, unknown>; body?: Partial<RelayfileCredential>; reason: string }> = [
    { name: "wrong agent", overrides: { agent_name: "flows-run-someone-else" }, reason: "principal" },
    { name: "user token", overrides: { principal_type: "desktop-user" }, reason: "principal" },
    { name: "token id", overrides: { token_id: randomUUID() }, reason: "credential_id" },
    { name: "short lived", overrides: { exp: Math.floor(Date.now() / 1000) + 30 }, reason: "expiry" },
    { name: "wider scopes", overrides: { scopes: ["relayfile:fs:read:/"] }, reason: "scopes" },
    { name: "inactive", overrides: { active: false, reason: "revoked" }, reason: "revoked" },
    { name: "aud elsewhere (client claim check)", overrides: { aud: "https://other.example" }, reason: "audience" },
  ];
  for (const row of rows) {
    const s = await setup(t);
    s.cp.relayfileIssuer.answerOverrides = row.overrides ?? {};
    const runId = await s.startRun(yamlWith(""), "yaml");
    const payload = s.payloadOf(await s.claim());
    assert.equal("relayfileToken" in payload, false, row.name);
    assert.equal(payload.relayfileUrl, RELAYFILE_URL);
    assert.deepEqual(await s.unavailable(runId), [{ code: "relayfile_credential_invalid", reason: row.reason }], row.name);
    const log = s.refusedLogs().at(-1)!;
    assert.deepEqual([log.runId, log.code, log.reason], [runId, "relayfile_credential_invalid", row.reason]);
    // No early revocation: the shared revoke {runId} would also end the run's
    // CLI references. Run end revokes the credential.
    assert.equal((await s.runRow(runId)).status, "pending");
    assert.equal(await s.revocations(runId), 0, row.name);
    assert.equal((await s.runRow(runId)).relayfile_credential_ciphertext, null);
    await assertNoTokenLeak(s.cp, runId);
  }
});

test("issuer or introspection outage: the claim is released and the workspace held", async (t) => {
  for (const kind of ["issuer", "introspection"] as const) {
    const s = await setup(t);
    s.cp.flows.outageHoldInitialMs = 1_000;
    const runId = await s.startRun(yamlWith(""));
    if (kind === "issuer") s.cp.relayfileIssuer.mode = { kind: "outage" };
    else s.cp.introspection.failWith = 503;
    let clock = Date.now();
    t.mock.method(Date, "now", () => clock);
    await assert.rejects(s.claim(), { name: kind === "issuer" ? "BrokerUnavailable" : "IntrospectionUnavailable" });
    const assignment = await s.cp.database.query<{ status: string }>("SELECT status FROM selfhost_work_assignments WHERE run_id = $1", [runId]);
    assert.equal(assignment.rows[0]!.status, "queued", `${kind}: claim released`);
    const callsBefore = s.cp.relayfileIssuer.calls.length;
    assert.equal(await s.claim(), null, `${kind}: workspace held`);
    assert.equal(s.cp.relayfileIssuer.calls.length, callsBefore, "no attempt while held");
    assert.deepEqual(await s.unavailable(runId), [], "an outage is not a refusal");
    assert.equal(s.refusedLogs().length, 0);
    // Recovered: the run is assigned with a credential.
    s.cp.relayfileIssuer.mode = { kind: "success" };
    s.cp.introspection.failWith = null;
    clock += 1_000;
    const payload = s.payloadOf(await s.claim());
    assert.match(payload.relayfileToken, /^sh_fr_/);
    t.mock.restoreAll();
  }
});

test("Relayfile unconfigured here: relayfile_not_configured event, no issuer call, no operator log", async (t) => {
  const s = await setup(t, { workerRelayfileUrl: "" });
  const runId = await s.startRun(yamlWith("integrations:\n  github: {}"));
  const payload = s.payloadOf(await s.claim());
  for (const field of ["relayfileUrl", "relayfileToken", "relayfileTokenExpiresAt", "relayfileMountPaths"]) {
    assert.equal(field in payload, false, field);
  }
  assert.deepEqual(await s.unavailable(runId), [{ code: "relayfile_not_configured" }]);
  assert.equal(s.cp.relayfileIssuer.calls.length, 0);
  assert.equal(s.refusedLogs().length, 0);
  await s.cp.flows.unackedAssignments(s.worker.workerId);
  assert.equal((await s.unavailable(runId)).length, 1, "once per run");
});

test("issuer seam missing on the store: relayfile_not_configured, URL still sent", async (t) => {
  const s = await setup(t);
  s.cp.flows.relayfileCredentials = undefined;
  const runId = await s.startRun(yamlWith(""));
  const payload = s.payloadOf(await s.claim());
  assert.equal(payload.relayfileUrl, RELAYFILE_URL);
  assert.equal("relayfileToken" in payload, false);
  assert.deepEqual(await s.unavailable(runId), [{ code: "relayfile_not_configured" }]);
});

test("unmountable provider: invalid_mount_paths with the reason, no issuer call", async (t) => {
  const s = await setup(t);
  const runId = await s.startRun(yamlWith("integrations:\n  github: {}\n  notion: {}"));
  const payload = s.payloadOf(await s.claim());
  assert.equal("relayfileToken" in payload, false);
  assert.deepEqual(await s.unavailable(runId), [{ code: "invalid_mount_paths", reason: "unmountable_provider:notion" }]);
  assert.equal(s.cp.relayfileIssuer.calls.length, 0);
  const log = s.refusedLogs();
  assert.equal(log.length, 1);
  assert.deepEqual([log[0]!.code, log[0]!.reason, "status" in log[0]!], ["invalid_mount_paths", "unmountable_provider:notion", false]);
});

test("TS workflows mount the run root only", async (t) => {
  const s = await setup(t);
  const runId = await s.startRun("export default async function run() {}\n", "ts");
  const payload = s.payloadOf(await s.claim());
  assert.deepEqual(payload.relayfileMountPaths, [`/workflows/runs/${runId}`]);
  assert.deepEqual(s.cp.relayfileIssuer.calls[0]!.mountPaths, [`/workflows/runs/${runId}`]);
});
