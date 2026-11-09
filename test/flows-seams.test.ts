import assert from "node:assert/strict";
import test from "node:test";
import { loadConfig, resolveFileSecrets, testConfig } from "../src/config.ts";
import { BrokerError, httpCliCredentialBroker } from "../src/flows/broker.ts";
import { createIntrospector } from "../src/flows/introspection.ts";
import { enqueueCredentialRevocation, RevokeOutbox, revokeRetryDelayMs } from "../src/flows/revoke-outbox.ts";
import { MemoryLogger } from "../src/observability/log.ts";
import { fixtureConfig, newPglite, startApp } from "./support/harness.ts";
import { FakeBroker, seedTenant, startControlPlane } from "./support/flows.ts";

const SECRET = "telemetry-secret-that-is-at-least-32-bytes-long";
const FLOW = `version: '1.0'
name: seam-flow
swarm:
  pattern: pipeline
workflows:
  - name: main
    steps:
      - name: generate
        type: deterministic
        command: printf 'hello\\n'
`;

test("RELAYFILE_CONTROL_PUBLIC_ORIGIN and RELAYCONTROL_BASE_URL must be absolute http(s) origins", () => {
  // Webhook-only deployments start without either.
  assert.equal(loadConfig({}).RELAYFILE_CONTROL_PUBLIC_ORIGIN, undefined);
  assert.equal(loadConfig({ RELAYFILE_CONTROL_PUBLIC_ORIGIN: "  " }).RELAYFILE_CONTROL_PUBLIC_ORIGIN, undefined);
  for (const bad of [
    "relayfile-control.example",
    "ftp://relayfile-control.example",
    "https://relayfile-control.example/path",
    "https://relayfile-control.example/?q=1",
    "https://relayfile-control.example/#x",
    "https://user:pw@relayfile-control.example",
    "/relative",
  ]) {
    assert.throws(() => loadConfig({ RELAYFILE_CONTROL_PUBLIC_ORIGIN: bad }), /RELAYFILE_CONTROL_PUBLIC_ORIGIN/, bad);
    assert.throws(() => loadConfig({ RELAYCONTROL_BASE_URL: bad }), /RELAYCONTROL_BASE_URL/, bad);
  }
  assert.equal(loadConfig({ RELAYCONTROL_BASE_URL: "http://Relay-Control.internal:8080/" }).RELAYCONTROL_BASE_URL, "http://relay-control.internal:8080");
  assert.equal(loadConfig({ RELAYFILE_CONTROL_PUBLIC_ORIGIN: "https://Files.Example.com:443/" }).RELAYFILE_CONTROL_PUBLIC_ORIGIN, "https://files.example.com");
  assert.equal(loadConfig({ RELAYFILE_CONTROL_PUBLIC_ORIGIN: "http://localhost:8080" }).RELAYFILE_CONTROL_PUBLIC_ORIGIN, "http://localhost:8080");
});

test("Flows secrets: _FILE indirection, 32-byte minimums, distinct values, FLOWS_ENABLED requirements", () => {
  const origin = { RELAYFILE_CONTROL_PUBLIC_ORIGIN: "https://files.example.com" };
  const files: Record<string, string> = { "/run/secrets/broker": "broker-secret\n", "/run/secrets/verify": `${"v".repeat(32)}\n` };
  const env = resolveFileSecrets({ ...origin, RELAYFILE_CONTROL_BROKER_SECRET_FILE: "/run/secrets/broker" }, (p) => files[p]!);
  assert.equal(env.RELAYFILE_CONTROL_BROKER_SECRET, "broker-secret");
  const verifyEnv = resolveFileSecrets({ RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET_FILE: "/run/secrets/verify" }, (p) => files[p]!);
  assert.equal(verifyEnv.RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET, "v".repeat(32));
  assert.equal(
    loadConfig({ RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET: verifyEnv.RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET }).RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET,
    "v".repeat(32),
  );
  for (const key of ["RELAYCONTROL_INTROSPECT_SECRET", "RELAYFILE_CONTROL_BROKER_SECRET", "RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET"]) {
    assert.throws(() => loadConfig({ [key]: "x".repeat(31) }), new RegExp(key), key);
  }
  // No two of the static shared secrets may be equal.
  const shared = "s".repeat(40);
  assert.throws(
    () => loadConfig({ RELAYFILE_CONTROL_BROKER_SECRET: shared, RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET: shared }),
    /invalid configuration: RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET$/,
  );
  assert.throws(
    () => loadConfig({ RELAYCONTROL_INTROSPECT_SECRET: shared, RELAYFILE_CONTROL_TELEMETRY_SECRET: shared }),
    /invalid configuration: RELAYFILE_CONTROL_TELEMETRY_SECRET$/,
  );
  assert.throws(
    () => loadConfig({ RELAYCONTROL_INTROSPECT_SECRET: shared, RELAYFILE_CONTROL_BROKER_SECRET: shared }),
    /invalid configuration: RELAYFILE_CONTROL_BROKER_SECRET$/,
  );
  assert.throws(
    () => resolveFileSecrets({ RELAYFILE_CONTROL_BROKER_SECRET: "x", RELAYFILE_CONTROL_BROKER_SECRET_FILE: "/run/secrets/broker" }, (p) => files[p]!),
    /not both/,
  );
  assert.throws(() => loadConfig({ ...origin, RELAYFILE_CONTROL_TELEMETRY_SECRET: "short" }), /RELAYFILE_CONTROL_TELEMETRY_SECRET/);
  assert.equal(loadConfig({ ...origin, RELAYFILE_CONTROL_TELEMETRY_SECRET: SECRET }).RELAYFILE_CONTROL_TELEMETRY_SECRET, SECRET);
  assert.throws(() => loadConfig({ ...origin, FLOWS_ENABLED: "true" }), /RELAYCONTROL_BASE_URL, RELAYCONTROL_INTROSPECT_SECRET, RELAYFILE_CONTROL_BROKER_SECRET, RELAYFILE_CONTROL_ENCRYPTION_KEY_ID, RELAYFILE_CONTROL_ENCRYPTION_KEY$/);
  const flows = {
    FLOWS_ENABLED: "true",
    RELAYCONTROL_BASE_URL: "http://relay-control.internal",
    RELAYCONTROL_INTROSPECT_SECRET: "i".repeat(32),
    RELAYFILE_CONTROL_BROKER_SECRET: "b".repeat(32),
    RELAYFILE_CONTROL_ENCRYPTION_KEY_ID: "k1",
    RELAYFILE_CONTROL_ENCRYPTION_KEY: "07".repeat(32),
  };
  // FLOWS_ENABLED needs the public origin (the introspection audience).
  assert.throws(() => loadConfig(flows), /invalid configuration: RELAYFILE_CONTROL_PUBLIC_ORIGIN$/);
  const enabled = loadConfig({ ...origin, ...flows });
  assert.equal(enabled.FLOWS_ENABLED, true);
});

test("introspection sends aud and the workspace, and caches by token hash for at most 30 s", async () => {
  const requests: Array<{ url: string; auth: string | null; body: any }> = [];
  let now = 1_000_000_000_000;
  const exp = Math.floor(now / 1000) + 3600;
  const fetchImpl = (async (url: string, init: RequestInit) => {
    const headers = new Headers(init.headers);
    requests.push({ url, auth: headers.get("authorization"), body: JSON.parse(String(init.body)) });
    const body = JSON.parse(String(init.body));
    if (body.token === "sh_at_inactive") return new Response(JSON.stringify({ active: false, reason: "expired" }));
    return new Response(JSON.stringify({
      active: true, sub: "u1", email: "a@b.test", principal_type: "desktop-user", agent_name: null, machine_id: null,
      org: "o1", workspace: body.workspace ?? "w1", role: "admin", scopes: ["x"], aud: body.aud, iat: 0, exp, token_id: "t1",
    }));
  }) as typeof fetch;
  const introspect = createIntrospector({
    baseUrl: "http://relay-control.internal/",
    secret: "introspect-secret",
    audience: "https://files.example.com",
    cacheSeconds: 300,
    fetchImpl,
    now: () => now,
  });
  const first = await introspect("sh_at_one", "w9");
  assert.equal(requests.length, 1);
  assert.equal(requests[0]!.url, "http://relay-control.internal/internal/v1/auth/introspect");
  assert.equal(requests[0]!.auth, "Bearer introspect-secret");
  assert.deepEqual(requests[0]!.body, { token: "sh_at_one", aud: "https://files.example.com", workspace: "w9" });
  assert.ok(first.active && first.workspace === "w9" && first.role === "admin" && first.token_id === "t1");

  await introspect("sh_at_one", "w9");
  assert.equal(requests.length, 1, "cached");
  await introspect("sh_at_one");
  assert.equal(requests.length, 2, "the workspace is part of the cache key");
  assert.deepEqual(requests[1]!.body, { token: "sh_at_one", aud: "https://files.example.com" }, "absent workspace is not sent");
  now += 30_001;
  await introspect("sh_at_one", "w9");
  assert.equal(requests.length, 3, "cacheSeconds is capped at 30");
  const inactive = await introspect("sh_at_inactive");
  assert.deepEqual(inactive, { active: false, reason: "expired" });

  const failing = createIntrospector({
    baseUrl: "http://relay-control.internal",
    secret: "s",
    audience: "https://files.example.com",
    fetchImpl: (async () => new Response("{}", { status: 401 })) as typeof fetch,
  });
  await assert.rejects(failing("sh_at_x"), /introspection answered 401/);
});

test("Flows user routes map introspection to relay-control's auth errors", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const none = await cp.api("/api/v1/flows");
  assert.equal(none.status, 401);
  assert.deepEqual(none.body, { error: "unauthorized", message: "Authentication required" });
  assert.equal(none.headers.get("cache-control"), "no-store");
  const unknown = await cp.api("/api/v1/flows", { token: "sh_at_unknown" });
  assert.deepEqual([unknown.status, unknown.body], [401, { error: "unauthorized", message: "Invalid or expired token" }]);
  const member = await cp.api("/api/v1/workers/enrollment-tokens", { method: "POST", token: tenant.memberToken, body: { name: "w" } });
  assert.deepEqual([member.status, member.body], [403, { error: "forbidden", message: "Workspace admin role required" }]);
  // The cookie counts only under /cloud.
  const cookie = { cookie: `agent_relay_session=${tenant.memberToken}` };
  assert.equal((await cp.api("/api/v1/flows", { headers: cookie })).status, 401);
  assert.equal((await cp.api("/cloud/api/v1/flows", { headers: cookie })).status, 200);
  // A route without a workspace sends none: the token's workspace is authoritative.
  assert.ok(cp.introspection.calls.every((call) => call.workspace === undefined));
  // A route naming one sends it; relay-control refuses another workspace's.
  const calls = cp.introspection.calls.length;
  const run = await cp.api("/api/v1/workflows/run", {
    method: "POST",
    token: tenant.memberToken,
    body: { workspaceId: tenant.workspaceId, workflow: FLOW, fileType: "yaml" },
  });
  assert.equal(run.status, 201);
  assert.deepEqual(cp.introspection.calls.slice(calls).map((call) => call.workspace), [undefined, tenant.workspaceId]);
  for (const path of ["/api/v1/workflows/run", "/api/v1/workflows/local-run", "/api/v1/workers/enrollment-tokens"]) {
    const other = await cp.api(path, {
      method: "POST",
      token: tenant.ownerToken,
      body: { workspaceId: tenant.otherWorkspaceId, workflow: FLOW, fileType: "yaml" },
    });
    assert.deepEqual([other.status, other.body], [404, { error: "workspace_not_found", message: "Workspace not found" }], path);
    assert.equal(cp.introspection.calls.at(-1)!.workspace, tenant.otherWorkspaceId, path);
  }
  const missing = await cp.api("/api/v1/workflows/nope/nope", { token: tenant.memberToken });
  assert.deepEqual([missing.status, missing.body], [404, { error: "not_found", message: "Route not found" }]);
});

test("telemetry: missing, wrong and right secret; unset secret answers 404", async (t) => {
  const off = await startApp();
  t.after(() => off.close());
  assert.equal((await fetch(`${off.url}/internal/v1/telemetry/flows`, { headers: { authorization: `Bearer ${SECRET}` } })).status, 404);

  const db = await newPglite();
  const app = await startApp({ db, config: fixtureConfig({ RELAYFILE_CONTROL_TELEMETRY_SECRET: SECRET }) });
  t.after(() => app.close());
  const url = `${app.url}/internal/v1/telemetry/flows`;
  assert.equal((await fetch(url)).status, 401, "missing");
  assert.equal((await fetch(url, { headers: { authorization: `Bearer ${SECRET.slice(0, -1)}x` } })).status, 401, "wrong");
  assert.equal((await fetch(url, { headers: { authorization: "Bearer short" } })).status, 401, "short");
  assert.equal((await fetch(url, { method: "POST", headers: { authorization: `Bearer ${SECRET}` } })).status, 405);

  const right = await fetch(url, { headers: { authorization: `Bearer ${SECRET}` } });
  assert.equal(right.status, 200);
  const body = await right.json() as { runs_by_status: object; workers_by_status: object; flow_definitions: number };
  assert.deepEqual(Object.keys(body).sort(), ["flow_definitions", "runs_by_status", "workers_by_status"]);
  assert.equal(typeof body.flow_definitions, "number");
});

test("telemetry counts runs, workers and flow definitions for the installation", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const flow = await cp.api("/api/v1/flows", {
    method: "POST",
    token: tenant.memberToken,
    body: { name: "seam-flow", source: FLOW },
  });
  assert.equal(flow.status, 201);
  const run = await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow: FLOW, fileType: "yaml" } });
  assert.equal(run.status, 201);
  const { flowsTelemetry } = await import("../src/flows/telemetry.ts");
  const counts = await flowsTelemetry(cp.database);
  assert.equal(counts.flow_definitions, 1);
  assert.equal(Object.values(counts.runs_by_status).reduce((a, b) => a + b, 0), 1);
  assert.deepEqual(counts.workers_by_status, {});
});

test("finishing a run enqueues a durable revocation; the outbox retries until the broker accepts", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const tenant = await seedTenant(cp);
  const run = await cp.api("/api/v1/workflows/run", { method: "POST", token: tenant.memberToken, body: { workflow: FLOW, fileType: "yaml" } });
  assert.equal(run.status, 201);
  const cancelled = await cp.api(`/api/v1/workflows/runs/${run.body.runId}/cancel`, { method: "POST", token: tenant.memberToken });
  assert.equal(cancelled.status, 200);
  const queued = await cp.database.query<{ run_id: string; attempts: number }>("SELECT run_id, attempts FROM selfhost_cli_credential_revocations");
  assert.deepEqual(queued.rows.map((r) => r.run_id), [run.body.runId]);
  assert.equal(cp.broker.calls.some((c) => c.op === "revokeForRun"), false, "no broker call inside the transaction");

  const logger = new MemoryLogger();
  cp.broker.revokeFailures = 2;
  const outbox = new RevokeOutbox({ db: cp.database, broker: cp.broker, logger, baseDelayMs: 0, maxDelayMs: 0 });
  assert.deepEqual(await outbox.drain(), { delivered: 0, failed: 1 });
  let row = await cp.database.query<{ attempts: number; last_error: string | null }>("SELECT attempts, last_error FROM selfhost_cli_credential_revocations");
  assert.equal(row.rows[0]!.attempts, 1);
  assert.equal(row.rows[0]!.last_error, "Error");
  assert.equal(logger.named("flows_revoke_retry").length, 1);
  assert.deepEqual(await outbox.drain(), { delivered: 0, failed: 1 });
  assert.deepEqual(await outbox.drain(), { delivered: 1, failed: 0 });
  row = await cp.database.query("SELECT attempts, last_error FROM selfhost_cli_credential_revocations");
  assert.equal(row.rows.length, 0, "delivered revocations leave the outbox");
  assert.equal(cp.broker.calls.filter((c) => c.op === "revokeForRun").length, 3);
  assert.deepEqual(await outbox.drain(), { delivered: 0, failed: 0 });

  // Enqueueing twice is a no-op. Only a 2xx deletes the row: a broker 404
  // (wrong route or base URL) is retried with backoff and logged.
  await enqueueCredentialRevocation(cp.database, run.body.runId);
  await enqueueCredentialRevocation(cp.database, run.body.runId);
  const goneLogger = new MemoryLogger();
  const gone = new RevokeOutbox({
    db: cp.database,
    logger: goneLogger,
    baseDelayMs: 0,
    maxDelayMs: 0,
    broker: Object.assign(new FakeBroker(), { revokeForRun: async () => { throw new BrokerError(404, "not_found", "none"); } }),
  });
  assert.deepEqual(await gone.drain(), { delivered: 0, failed: 1 });
  assert.deepEqual(await gone.drain(), { delivered: 0, failed: 1 });
  const kept = await cp.database.query<{ attempts: number; last_error: string }>("SELECT attempts, last_error FROM selfhost_cli_credential_revocations");
  assert.deepEqual(kept.rows.map((r) => [Number(r.attempts), r.last_error]), [[2, "not_found"]]);
  assert.deepEqual(
    goneLogger.named("flows_revoke_retry").map((e) => [e.status, e.reason]),
    [[404, "not_found"], [404, "not_found"]],
  );
  assert.deepEqual(await outbox.drain(), { delivered: 1, failed: 0 }, "a later 2xx delivers it");

  // A worker that dies mid-delivery leaves its lease; once it expires another
  // worker redelivers the row.
  await enqueueCredentialRevocation(cp.database, run.body.runId);
  const stuck = new RevokeOutbox({
    db: cp.database,
    leaseMs: 50,
    broker: Object.assign(new FakeBroker(), { revokeForRun: () => new Promise<void>(() => undefined) }),
  });
  void stuck.drain();
  await new Promise((resolve) => setTimeout(resolve, 20));
  const second = new RevokeOutbox({ db: cp.database, broker: cp.broker });
  assert.deepEqual(await second.drain(), { delivered: 0, failed: 0 }, "leased rows are hidden");
  await new Promise((resolve) => setTimeout(resolve, 80));
  assert.deepEqual(await second.drain(), { delivered: 1, failed: 0 }, "an expired lease is redelivered");
});

test("revoke backoff grows exponentially and is capped", () => {
  assert.equal(revokeRetryDelayMs(1, 5_000, 600_000), 5_000);
  assert.equal(revokeRetryDelayMs(2, 5_000, 600_000), 10_000);
  assert.equal(revokeRetryDelayMs(4, 5_000, 600_000), 40_000);
  assert.equal(revokeRetryDelayMs(50, 5_000, 600_000), 600_000);
});

test("HTTP broker sends the broker secret as a bearer and maps errors", async () => {
  const seen: Array<{ url: string; auth: string | null; body: unknown }> = [];
  const broker = httpCliCredentialBroker({
    baseUrl: "http://relay-control.internal/",
    secret: "broker-secret",
    fetchImpl: (async (url: string, init: RequestInit) => {
      seen.push({ url, auth: new Headers(init.headers).get("authorization"), body: init.body ? JSON.parse(String(init.body)) : null });
      if (url.endsWith("/revoke")) return new Response(JSON.stringify({ error: "run_not_found", message: "no" }), { status: 404 });
      if (url.endsWith("/references")) return new Response(null, { status: 204 });
      return new Response("{}", { status: 503 });
    }) as typeof fetch,
  });
  assert.equal(await broker.issueRunReference("ws", "run"), null);
  assert.deepEqual(seen[0], {
    url: "http://relay-control.internal/internal/v1/flows/credentials/references",
    auth: "Bearer broker-secret",
    body: { workspaceId: "ws", runId: "run" },
  });
  await assert.rejects(broker.revokeForRun("run"), (e: unknown) => e instanceof BrokerError && e.status === 404 && e.code === "run_not_found");
  await assert.rejects(broker.readForRun("ref", "run"), /503/);
  assert.equal(testConfig().FLOWS_ENABLED, false);
});
