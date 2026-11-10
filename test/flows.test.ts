import assert from "node:assert/strict";
import { gzipSync } from "node:zlib";
import test from "node:test";
import { summarizeDefinition } from "../src/flows/definition.ts";
import { GcsBackend, validateObjectKey } from "../src/flows/objects.ts";
import { openQueue, seedTenant, startControlPlane } from "./support/flows.ts";

const DETERMINISTIC_FLOW = `version: '1.0'
name: hello-selfhost
description: Deterministic smoke flow
swarm:
  pattern: pipeline
workflows:
  - name: main
    steps:
      - name: generate
        type: deterministic
        command: printf 'hello\\n'
      - name: shout
        type: deterministic
        dependsOn: [generate]
        command: echo HELLO
`;

test("definition summaries cover v1 and v2 YAML and ignore TypeScript", () => {
  const v1 = summarizeDefinition(DETERMINISTIC_FLOW, "yaml");
  assert.equal(v1.format, "relayflows-v1");
  assert.equal(v1.name, "hello-selfhost");
  assert.deepEqual(v1.steps.map((step) => [step.stepName, step.stepType, step.dependsOn]), [
    ["generate", "deterministic", []],
    ["shout", "deterministic", ["generate"]],
  ]);
  const v2 = summarizeDefinition("version: '0.1.0'\nname: x\nsteps:\n  - id: a\n    type: deterministic\n", "yaml");
  assert.equal(v2.format, "relayflows-v2");
  assert.equal(v2.steps[0]!.stepName, "a");
  assert.equal(summarizeDefinition("export default {}", "ts").steps.length, 0);
  assert.equal(summarizeDefinition(": : not yaml [", "yaml").steps.length, 0);
});

test("object keys reject traversal and control characters", () => {
  assert.equal(validateObjectKey("runner.log"), "runner.log");
  assert.equal(validateObjectKey("step/agent.log"), "step/agent.log");
  for (const bad of ["", "/abs", "a/../b", "a//b", "./a", "a\\b", "a\u0000b", "a\nb"]) {
    assert.equal(validateObjectKey(bad), null, bad);
  }
});

test("GCS backend uses metadata-server credentials and the JSON API", async () => {
  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const fakeFetch = (async (url: string, init?: RequestInit) => {
    calls.push({ url, init });
    if (url.includes("metadata")) {
      return new Response(JSON.stringify({ access_token: "ya29.test", expires_in: 3600 }));
    }
    if (url.includes("/upload/")) return new Response("{}");
    if (url.includes("missing")) return new Response("", { status: 404 });
    return new Response("stored-bytes");
  }) as typeof fetch;
  const backend = new GcsBackend("relay-runs", "flows", fakeFetch, "http://metadata.test/token");
  const location = await backend.put("run-1/runner.log", Buffer.from("hi"), "text/plain");
  assert.equal(location, "gs://relay-runs/flows/run-1/runner.log");
  assert.equal((await backend.get("run-1/runner.log"))?.toString(), "stored-bytes");
  assert.equal(await backend.get("missing"), null);
  assert.equal(calls.filter((call) => call.url.includes("metadata")).length, 1, "token is cached");
  assert.match(calls[1]!.url, /uploadType=media&name=flows%2Frun-1%2Frunner\.log/);
  assert.equal((calls[1]!.init!.headers as Record<string, string>).authorization, "Bearer ya29.test");
  assert.throws(() => new GcsBackend("Bad Bucket!"), /bucket/);
});

test("fake worker: enroll → register → queue → assignment → ack → status → complete", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const { api, origin } = cp;
  const tenant = await seedTenant(cp);

  // Enrollment tokens are admin-only and shown once.
  const forbidden = await api("/api/v1/workers/enrollment-tokens", {
    method: "POST",
    token: tenant.memberToken,
    body: { name: "w1" },
  });
  assert.equal(forbidden.status, 403);
  const enrollment = await api("/cloud/api/v1/workers/enrollment-tokens", {
    method: "POST",
    token: tenant.ownerToken,
    body: { name: "w1", tags: ["linux", "linux", " "] },
  });
  assert.equal(enrollment.status, 200);
  assert.match(enrollment.body.token, /^sh_wrk_enr_/);
  assert.match(enrollment.body.registerCommand, /agent-relay cloud worker register --token sh_wrk_enr_\S+ --name w1 --base-url https:\/\/control\.selfhosted\.company/);
  const stored = await cp.database.query<{ token_hash: string }>(
    "SELECT token_hash FROM selfhost_worker_enrollment_tokens",
  );
  assert.notEqual(stored.rows[0]!.token_hash, enrollment.body.token, "tokens are stored hashed");
  const listed = await api("/api/v1/workers/enrollment-tokens", { token: tenant.ownerToken });
  assert.equal(listed.body.tokens[0].status, "active");
  assert.doesNotMatch(JSON.stringify(listed.body), new RegExp(enrollment.body.token));

  // Register (the CLI's exact request), then the token is spent.
  const badRegister = await api("/api/v1/workers/register", {
    method: "POST",
    body: { enrollmentToken: "sh_wrk_enr_nope", name: "w1" },
  });
  assert.equal(badRegister.status, 401);
  assert.equal(badRegister.body.error, "Invalid enrollment token");
  const registered = await api("/cloud/api/v1/workers/register", {
    method: "POST",
    body: {
      enrollmentToken: enrollment.body.token,
      name: "w1",
      hostInfo: { hostname: "box", platform: "linux", arch: "x64", node: "v22" },
    },
  });
  assert.equal(registered.status, 200);
  const { workerId, workerToken, heartbeatIntervalMs } = registered.body;
  assert.match(workerToken, /^sh_wrk_(?!enr_)/);
  assert.equal(heartbeatIntervalMs, 30_000);
  const reused = await api("/api/v1/workers/register", {
    method: "POST",
    body: { enrollmentToken: enrollment.body.token, name: "w2" },
  });
  assert.equal(reused.status, 401, "enrollment tokens are single use");

  let workers = await api("/api/v1/workers", { token: tenant.memberToken });
  assert.equal(workers.body.workers[0].status, "pending");
  assert.deepEqual(workers.body.workers[0].tags, ["linux"]);
  assert.equal((await api(`/api/v1/workers/${workerId}/heartbeat`, {
    method: "POST",
    token: "sh_wrk_wrong",
  })).status, 401);
  const heartbeat = await api(`/api/v1/workers/${workerId}/heartbeat`, { method: "POST", token: workerToken });
  assert.deepEqual(heartbeat.body, { ok: true, nextHeartbeatMs: 30_000 });
  workers = await api("/api/v1/workers", { token: tenant.memberToken });
  assert.equal(workers.body.workers[0].status, "online");
  assert.equal(workers.body.workers[0].hostInfo.platform, "linux");
  assert.equal(
    (await api("/api/v1/workers", { token: tenant.otherOwnerToken })).body.workers.length,
    0,
    "workers are workspace scoped",
  );

  // Define a flow and trigger it; the worker receives it over SSE.
  const flow = await api("/api/v1/flows", {
    method: "POST",
    token: tenant.memberToken,
    body: { name: "hello-selfhost", source: DETERMINISTIC_FLOW },
  });
  assert.equal(flow.status, 201);
  assert.equal(flow.body.flow.stepCount, 2);
  assert.equal((await api("/api/v1/flows", {
    method: "POST",
    token: tenant.memberToken,
    body: { name: "hello-selfhost", source: DETERMINISTIC_FLOW },
  })).status, 409);

  const queue = openQueue(origin, workerId, workerToken);
  t.after(() => queue.close());
  await queue.ready;
  await queue.next("ping");

  const invoked = await api(`/api/v1/flows/${flow.body.flow.id}/invoke`, {
    method: "POST",
    token: tenant.memberToken,
    body: { inputs: { task: "smoke" }, envSecrets: { MY_SECRET: "s3cret" } },
  });
  assert.equal(invoked.status, 201);
  const runId = invoked.body.runId as string;
  assert.equal(invoked.body.status, "pending");

  const assignment = await queue.next("assignment");
  assert.equal(assignment.data.runId, runId);
  assert.equal(assignment.data.workerId, workerId);
  assert.equal(assignment.data.status, "assigned");
  assert.equal(assignment.data.workflowRef.type, "inline");
  const payload = JSON.parse(assignment.data.workflowRef.value);
  assert.equal(payload.runId, runId);
  assert.equal(payload.workspaceId, tenant.workspaceId);
  assert.equal(payload.relayWorkspaceId, "rc-primary");
  assert.equal(payload.relaycastApiKey, "rk_live_primary");
  assert.equal(payload.relaycastBaseUrl, "https://relaycast.selfhosted.company");
  // Relayfile is not configured here (SELFHOST_FLOWS_WORKER_RELAYFILE_URL
  // empty): no URL, and the token fields are absent, never empty.
  assert.equal("relayfileUrl" in payload, false);
  assert.equal("relayfileToken" in payload, false);
  assert.equal("relayfileTokenExpiresAt" in payload, false);
  assert.equal("relayfileMountPaths" in payload, false);
  assert.equal(payload.workflow, DETERMINISTIC_FLOW);
  assert.equal(payload.fileType, "yaml");
  assert.equal(payload.sourceFileType, "yaml");
  assert.equal(payload.workflowFileName, "hello-selfhost.yaml");
  assert.deepEqual(payload.envSecrets, { MY_SECRET: "s3cret", RELAY_SECRET_ENV_NAMES: '["MY_SECRET"]' });
  const runRow = await cp.database.query<{ env_secrets_ciphertext: string }>(
    "SELECT env_secrets_ciphertext FROM selfhost_workflow_runs WHERE id = $1",
    [runId],
  );
  assert.doesNotMatch(runRow.rows[0]!.env_secrets_ciphertext, /s3cret/, "env secrets are encrypted");

  // Only the assigned worker may act on the assignment.
  const ack = await api(`/api/v1/workers/${workerId}/assignments/${runId}/ack`, {
    method: "POST",
    token: workerToken,
  });
  assert.equal(ack.status, 200);
  assert.equal(ack.body.status, "assigned");
  const running = await api(`/api/v1/workers/${workerId}/assignments/${runId}/status`, {
    method: "POST",
    token: workerToken,
    body: { phase: "running" },
  });
  assert.deepEqual(running.body, { ok: true, status: "running" });
  let run = await api(`/api/v1/workflows/runs/${runId}`, { token: tenant.memberToken });
  assert.equal(run.body.status, "running");
  assert.equal(run.body.workerName, "w1");

  const put = await api(`/api/v1/workers/${workerId}/assignments/${runId}/storage/runner.log`, {
    method: "PUT",
    token: workerToken,
    raw: "[step] generate started\n[step] generate completed\n",
    headers: { "content-type": "text/plain" },
  });
  assert.equal(put.status, 200);
  const head = await fetch(`${origin}/api/v1/workers/${workerId}/assignments/${runId}/storage/runner.log`, {
    method: "HEAD",
    headers: { authorization: `Bearer ${workerToken}` },
  });
  assert.equal(head.status, 200);
  assert.equal((await api(`/api/v1/workers/${workerId}/assignments/${runId}/storage/../x`, {
    token: workerToken,
  })).status, 404, "URL normalization never escapes the run prefix");
  assert.equal((await api(`/api/v1/workers/${workerId}/assignments/${runId}/storage/missing.txt`, {
    token: workerToken,
  })).status, 404);

  const completed = await api(`/api/v1/workers/${workerId}/assignments/${runId}/status`, {
    method: "POST",
    token: workerToken,
    body: { phase: "completed", exitCode: 0, durationMs: 1234, summary: `Workflow ${runId} completed.` },
  });
  assert.deepEqual(completed.body, { ok: true, status: "completed" });
  run = await api(`/cloud/api/v1/workflows/runs/${runId}`, { token: tenant.memberToken });
  assert.equal(run.body.status, "completed");
  assert.equal(run.body.exitCode, 0);
  assert.equal(run.body.durationMs, 1234);
  assert.equal(run.body.flowId, flow.body.flow.id);
  assert.equal(run.body.hasEnvSecrets, true);
  assert.equal(JSON.stringify(run.body).includes("s3cret"), false);

  const events = await api(`/api/v1/workflows/runs/${runId}/events`, { token: tenant.memberToken });
  assert.deepEqual(events.body.events.map((event: { eventType: string }) => event.eventType), [
    "run.queued",
    "run.relayfile_credential_unavailable",
    "run.assigned",
    "run.acknowledged",
    "run.running",
    "run.completed",
  ]);
  const steps = await api(`/api/v1/workflows/runs/${runId}/steps`, { token: tenant.memberToken });
  assert.equal(steps.body.source, "declared");
  assert.deepEqual(steps.body.steps.map((step: { stepName: string; status: string }) =>
    [step.stepName, step.status]), [["generate", "completed"], ["shout", "completed"]]);
  const logs = await api(`/api/v1/workflows/runs/${runId}/logs?offset=0`, { token: tenant.memberToken });
  assert.equal(logs.body.available, true);
  assert.match(logs.body.content, /generate completed/);
  assert.equal(logs.body.done, true);
  const artifacts = await api(`/api/v1/workflows/runs/${runId}/artifacts`, { token: tenant.memberToken });
  assert.deepEqual(artifacts.body.artifacts.map((a: { key: string; kind: string }) => [a.key, a.kind]), [
    ["runner.log", "log"],
  ]);
  const download = await fetch(`${origin}/api/v1/workflows/runs/${runId}/artifacts/runner.log`, {
    headers: { authorization: `Bearer ${tenant.memberToken}` },
  });
  assert.match(download.headers.get("content-disposition") ?? "", /attachment/);

  // Worker storage closes once the assignment is finished; repeat reports are idempotent.
  assert.equal((await api(`/api/v1/workers/${workerId}/assignments/${runId}/storage/runner.log`, {
    token: workerToken,
  })).status, 404);
  assert.equal((await api(`/api/v1/workers/${workerId}/assignments/${runId}/status`, {
    method: "POST",
    token: workerToken,
    body: { phase: "failed", error: "late" },
  })).body.status, "completed");

  // Flow page data and run lists.
  const flowDetail = await api(`/api/v1/flows/${flow.body.flow.id}`, { token: tenant.memberToken });
  assert.equal(flowDetail.body.flow.lastRun.status, "completed");
  assert.equal(flowDetail.body.recentRuns.length, 1);
  const runs = await api("/api/v1/workflows/runs", { token: tenant.memberToken });
  assert.equal(runs.body.totalRuns, 1);
  assert.equal(runs.body.runs[0].runId, runId);
  assert.equal((await api(`/api/v1/workflows/runs/${runId}`, { token: tenant.otherOwnerToken })).status, 404);

  // Retry re-queues the same definition; resume passes --resume semantics.
  const resumed = await api(`/api/v1/workflows/runs/${runId}/resume`, {
    method: "POST",
    token: tenant.memberToken,
    body: { startFrom: "shout" },
  });
  assert.equal(resumed.status, 201);
  const resumedAssignment = await queue.next("assignment");
  assert.equal(resumedAssignment.data.runId, resumed.body.runId);
  const resumedPayload = JSON.parse(resumedAssignment.data.workflowRef.value);
  assert.equal(resumedPayload.startFrom, "shout");
  assert.equal(resumedPayload.resumeRunId, undefined);
  assert.deepEqual(resumedPayload.envSecrets, { MY_SECRET: "s3cret", RELAY_SECRET_ENV_NAMES: '["MY_SECRET"]' });
  run = await api(`/api/v1/workflows/runs/${runId}`, { token: tenant.memberToken });
  assert.equal(run.body.resumedBy[0].runId, resumed.body.runId);

  // Cancelling assigned-but-unacked work makes the worker's ack fail.
  const cancelled = await api(`/api/v1/workflows/runs/${resumed.body.runId}/cancel`, {
    method: "POST",
    token: tenant.memberToken,
  });
  assert.deepEqual(cancelled.body, { runId: resumed.body.runId, status: "cancelled" });
  assert.equal((await api(`/api/v1/workflows/runs/${resumed.body.runId}/cancel`, {
    method: "POST",
    token: tenant.memberToken,
  })).status, 409);
  const lateAck = await api(`/api/v1/workers/${workerId}/assignments/${resumed.body.runId}/ack`, {
    method: "POST",
    token: workerToken,
  });
  assert.equal(lateAck.status, 409);
  const failedReport = await api(`/api/v1/workers/${workerId}/assignments/${resumed.body.runId}/status`, {
    method: "POST",
    token: workerToken,
    body: { phase: "failed", error: "Run was cancelled" },
  });
  assert.equal(failedReport.status, 200);
  run = await api(`/api/v1/workflows/runs/${resumed.body.runId}`, { token: tenant.memberToken });
  assert.equal(run.body.status, "cancelled", "a cancelled run stays cancelled");

  // Revocation: stream receives revoke, token stops working.
  assert.equal((await api(`/api/v1/workers/${workerId}`, {
    method: "DELETE",
    token: tenant.memberToken,
  })).status, 403);
  const revoked = await api(`/api/v1/workers/${workerId}`, { method: "DELETE", token: tenant.ownerToken });
  assert.deepEqual(revoked.body, { workerId, status: "revoked" });
  await queue.next("revoke");
  assert.equal((await api(`/api/v1/workers/${workerId}/heartbeat`, {
    method: "POST",
    token: workerToken,
  })).status, 401);
});

test("pinned and tagged work, code archives, queue timeouts, and redelivery", async (t) => {
  const cp = await startControlPlane({ queueWaitMs: 400 });
  t.after(() => cp.stop());
  const { api, origin } = cp;
  const tenant = await seedTenant(cp);
  const enroll = async (name: string, tags: string[] = []) => {
    const token = await api("/api/v1/workers/enrollment-tokens", {
      method: "POST",
      token: tenant.ownerToken,
      body: { name, tags },
    });
    const registered = await api("/api/v1/workers/register", {
      method: "POST",
      body: { enrollmentToken: token.body.token, name },
    });
    assert.equal(registered.status, 200);
    return registered.body as { workerId: string; workerToken: string };
  };
  const plain = await enroll("plain");
  const gpu = await enroll("gpu", ["gpu"]);

  // A tagged run is only claimable by a worker carrying the tag.
  const archive = gzipSync(Buffer.from("fake tar"));
  const tagged = await api("/api/v1/workflows/run", {
    method: "POST",
    token: tenant.memberToken,
    body: {
      workflow: DETERMINISTIC_FLOW,
      fileType: "yaml",
      runtime: { id: "worker", config: { tags: ["gpu"] } },
      codeArchiveBase64: archive.toString("base64"),
    },
  });
  assert.equal(tagged.status, 201);
  const plainQueue = openQueue(origin, plain.workerId, plain.workerToken);
  t.after(() => plainQueue.close());
  await plainQueue.ready;
  await new Promise((resolve) => setTimeout(resolve, 150));
  assert.equal(plainQueue.events.some((event) => event.event === "assignment"), false);

  const gpuQueue = openQueue(origin, gpu.workerId, gpu.workerToken);
  await gpuQueue.ready;
  const delivered = await gpuQueue.next("assignment");
  const payload = JSON.parse(delivered.data.workflowRef.value);
  assert.equal(payload.s3CodeKey, "code.tar.gz");
  const code = await fetch(
    `${origin}/api/v1/workers/${gpu.workerId}/assignments/${tagged.body.runId}/storage/code.tar.gz`,
    { headers: { authorization: `Bearer ${gpu.workerToken}` } },
  );
  assert.equal(code.status, 200);
  assert.deepEqual(Buffer.from(await code.arrayBuffer()), archive);
  assert.equal((await fetch(
    `${origin}/api/v1/workers/${plain.workerId}/assignments/${tagged.body.runId}/storage/code.tar.gz`,
    { headers: { authorization: `Bearer ${plain.workerToken}` } },
  )).status, 404, "other workers cannot read the assignment's storage");

  // A dropped stream redelivers assigned-but-unacked work.
  gpuQueue.close();
  const reconnected = openQueue(origin, gpu.workerId, gpu.workerToken);
  t.after(() => reconnected.close());
  await reconnected.ready;
  assert.equal((await reconnected.next("assignment")).data.runId, tagged.body.runId);
  reconnected.close();

  // Work pinned to a busy/absent worker times out in the queue.
  const pinned = await api("/api/v1/workflows/run", {
    method: "POST",
    token: tenant.memberToken,
    body: { workflow: DETERMINISTIC_FLOW, fileType: "yaml", runtime: { id: "worker", config: { name: "gpu" } } },
  });
  assert.equal(pinned.status, 201);
  assert.equal((await api("/api/v1/workflows/run", {
    method: "POST",
    token: tenant.memberToken,
    body: { workflow: DETERMINISTIC_FLOW, fileType: "yaml", runtime: { id: "worker", config: { name: "nope" } } },
  })).status, 404);
  assert.equal((await api("/api/v1/workflows/run", {
    method: "POST",
    token: tenant.memberToken,
    body: { workflow: DETERMINISTIC_FLOW, fileType: "yaml", runtime: { id: "daytona" } },
  })).status, 400);
  await new Promise((resolve) => setTimeout(resolve, 500));
  await cp.flows.maintenance();
  const timedOut = await api(`/api/v1/workflows/runs/${pinned.body.runId}`, { token: tenant.memberToken });
  assert.equal(timedOut.body.status, "failed");
  assert.equal(timedOut.body.error, "queue-timeout");
  assert.equal(timedOut.body.assignmentStatus, "timeout");

  const failedOnly = await api("/api/v1/workflows/runs?status=failed", { token: tenant.memberToken });
  assert.equal(failedOnly.body.runs.length, 1);
  const paged = await api("/api/v1/workflows/runs?limit=1", { token: tenant.memberToken });
  assert.equal(paged.body.runs.length, 1);
  assert.ok(paged.body.nextCursor);
  const second = await api(`/api/v1/workflows/runs?limit=1&cursor=${paged.body.nextCursor}`, {
    token: tenant.memberToken,
  });
  assert.notEqual(second.body.runs[0].runId, paged.body.runs[0].runId);
  assert.equal((await api("/api/v1/workflows/runs?cursor=garbage", { token: tenant.memberToken })).status, 400);
});

test("v2 cloud-mirror ingestion records events, steps, logs, and the terminal callback", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const { api } = cp;
  const tenant = await seedTenant(cp);
  const local = await api("/api/v1/workflows/local-run", {
    method: "POST",
    token: tenant.memberToken,
    body: { workflow: "version: '0.1.0'\nname: mirrored\nsteps:\n  - id: greet\n    type: deterministic\n    command: echo hi\n", fileType: "yaml", relayflowVersion: "v2" },
  });
  assert.equal(local.status, 201);
  const { runId, accessToken, callbackToken } = local.body;
  assert.match(accessToken, /^sh_run_/);
  assert.equal(local.body.status, "running");
  assert.equal((await api(`/api/v1/workflows/runs/${runId}/events`, {
    method: "POST",
    token: tenant.memberToken,
    body: { eventType: "x" },
  })).status, 401, "ingestion requires the run token");

  const event = await api(`/api/v1/workflows/runs/${runId}/events`, {
    method: "POST",
    token: accessToken,
    body: { eventType: "relayflow.step.started", stepName: "greet", payload: { stepType: "deterministic" } },
  });
  assert.equal(event.status, 201);
  const snapshot = await api(`/api/v1/workflows/runs/${runId}/steps/snapshot`, {
    method: "POST",
    token: accessToken,
    body: {
      sequence: 1,
      capturedAt: new Date().toISOString(),
      steps: [{ stepName: "greet", journalRunId: "j1", stepType: "deterministic", state: "running", attempt: 1, elapsedMs: 10 }],
    },
  });
  assert.equal(snapshot.status, 202);
  let steps = await api(`/api/v1/workflows/runs/${runId}/steps`, { token: tenant.memberToken });
  assert.equal(steps.body.source, "reported");
  assert.equal(steps.body.live, true);
  assert.equal(steps.body.steps[0].status, "running");

  assert.equal((await api(`/api/v1/workflows/runs/${runId}/storage/runner.log`, {
    method: "PUT",
    token: accessToken,
    raw: "runner output\n",
    headers: { "content-type": "text/plain" },
  })).status, 200);
  assert.equal((await api(`/api/v1/workflows/runs/${runId}/storage/greet/agent.log`, {
    method: "PUT",
    token: accessToken,
    raw: "step output\n",
  })).status, 200);
  const final = await api(`/api/v1/workflows/runs/${runId}/steps`, {
    method: "POST",
    token: accessToken,
    body: {
      steps: [{
        stepName: "greet", stepType: "deterministic", agent: "", preset: "", cli: "", sandboxId: "",
        startTime: "2026-11-03T00:00:00.000Z", endTime: "2026-11-03T00:00:01.000Z", durationMs: 1000,
        exitCode: 0, outputSummary: "hi", status: "completed", completionReason: "success", retryCount: 0,
      }],
    },
  });
  assert.equal(final.status, 201);
  const callback = await api("/api/v1/workflows/callback", {
    method: "POST",
    token: accessToken,
    body: { runId, callbackToken, status: "completed", result: { ok: true, status: "completed", completionReason: "success", runId } },
  });
  assert.deepEqual(callback.body, { runId, status: "completed" });
  assert.equal((await api(`/api/v1/workflows/runs/${runId}/steps/snapshot`, {
    method: "POST",
    token: accessToken,
    body: { sequence: 2, capturedAt: new Date().toISOString(), steps: [] },
  })).status, 409);

  steps = await api(`/api/v1/workflows/runs/${runId}/steps`, { token: tenant.memberToken });
  assert.equal(steps.body.live, false);
  assert.equal(steps.body.steps[0].source, "final");
  assert.equal(steps.body.steps[0].outputSummary, "hi");
  const stepLog = await api(`/api/v1/workflows/runs/${runId}/logs?step=greet`, { token: tenant.memberToken });
  assert.equal(stepLog.body.content, "step output\n");
  const run = await api(`/api/v1/workflows/runs/${runId}`, { token: tenant.memberToken });
  assert.equal(run.body.status, "completed");
  assert.equal(run.body.dispatchType, "local");
  const badCallback = await api("/api/v1/workflows/callback", {
    method: "POST",
    token: accessToken,
    body: { runId, callbackToken: "sh_cb_wrong", status: "failed" },
  });
  assert.equal(badCallback.status, 401);
});

test("openapi documents every Relayflows route", async () => {
  const { readFile } = await import("node:fs/promises");
  const { parse } = await import("yaml");
  const spec = parse(await readFile(new URL("../openapi.yaml", import.meta.url), "utf8")) as {
    paths: Record<string, Record<string, unknown>>;
  };
  const expected: Array<[string, string[]]> = [
    ["/api/v1/workers/enrollment-tokens", ["get", "post"]],
    ["/api/v1/workers/enrollment-tokens/{tokenId}", ["delete"]],
    ["/api/v1/workers", ["get"]],
    ["/api/v1/workers/{workerId}", ["get", "delete"]],
    ["/api/v1/workers/register", ["post"]],
    ["/api/v1/workers/{workerId}/heartbeat", ["post"]],
    ["/api/v1/workers/{workerId}/queue", ["get"]],
    ["/api/v1/workers/{workerId}/assignments/{runId}/ack", ["post"]],
    ["/api/v1/workers/{workerId}/assignments/{runId}/status", ["post"]],
    ["/api/v1/workers/{workerId}/assignments/{runId}/storage/{objectKey}", ["get", "head", "put"]],
    ["/api/v1/workers/{workerId}/assignments/{runId}/cli-credentials", ["get"]],
    ["/api/v1/workers/{workerId}/assignments/{runId}/cli-credentials/{credentialId}", ["put"]],
    ["/api/v1/flows", ["get", "post"]],
    ["/api/v1/flows/{flowId}", ["get", "patch", "delete"]],
    ["/api/v1/flows/{flowId}/invoke", ["post"]],
    ["/api/v1/workflows/run", ["post"]],
    ["/api/v1/workflows/runs", ["get"]],
    ["/api/v1/workflows/runs/{runId}", ["get"]],
    ["/api/v1/workflows/runs/{runId}/workflow", ["get"]],
    ["/api/v1/workflows/runs/{runId}/cancel", ["post"]],
    ["/api/v1/workflows/runs/{runId}/resume", ["post"]],
    ["/api/v1/workflows/runs/{runId}/retry", ["post"]],
    ["/api/v1/workflows/runs/{runId}/steps", ["get", "post"]],
    ["/api/v1/workflows/runs/{runId}/steps/snapshot", ["post"]],
    ["/api/v1/workflows/runs/{runId}/events", ["get", "post"]],
    ["/api/v1/workflows/runs/{runId}/storage/{objectKey}", ["put"]],
    ["/api/v1/workflows/runs/{runId}/logs", ["get"]],
    ["/api/v1/workflows/runs/{runId}/artifacts", ["get"]],
    ["/api/v1/workflows/runs/{runId}/artifacts/{objectKey}", ["get"]],
    ["/api/v1/workflows/local-run", ["post"]],
    ["/api/v1/workflows/callback", ["post"]],
  ];
  for (const [path, methods] of expected) {
    for (const method of methods) {
      assert.ok(spec.paths[path]?.[method], `${method.toUpperCase()} ${path} is documented`);
    }
  }
});

test("runtime environment merge: run secrets win and names are listed for redaction", async () => {
  const { mergeRunEnvironment, httpRuntimeEnvironmentSource, RuntimeEnvironmentUnavailable } =
    await import("../src/flows/runtime-env.ts");
  const merged = mergeRunEnvironment(
    {
      env: { ANTHROPIC_API_KEY: "sk-ws", SHARED: "workspace", RELAY_SECRET_ENV_NAMES: "spoof" },
      secretEnvNames: ["ANTHROPIC_API_KEY", "SHARED"],
      skipped: [],
      integrationErrors: [],
    },
    { SHARED: "run" },
  )!;
  assert.equal(merged.SHARED, "run");
  assert.equal(merged.ANTHROPIC_API_KEY, "sk-ws");
  assert.deepEqual(JSON.parse(merged.RELAY_SECRET_ENV_NAMES!), ["ANTHROPIC_API_KEY", "SHARED"]);
  assert.equal(mergeRunEnvironment(null, undefined), undefined);

  const seen: string[] = [];
  const source = httpRuntimeEnvironmentSource({
    baseUrl: "http://control.internal/",
    token: "internal-token",
    fetchImpl: (async (url: string, init: RequestInit) => {
      seen.push(`${url} ${(init.headers as Record<string, string>).authorization}`);
      if (url.includes("down")) return new Response("{}", { status: 503 });
      if (url.includes("missing")) return new Response(JSON.stringify({ error: "not_found" }), { status: 404 });
      return new Response(JSON.stringify({ env: { A: "1", B: 2 }, secretEnvNames: ["A"] }));
    }) as typeof fetch,
  });
  assert.deepEqual((await source("ws")).env, { A: "1" });
  assert.equal(seen[0], "http://control.internal/internal/v1/workspaces/ws/environment?includeIntegrations=1 Bearer internal-token");
  assert.deepEqual((await source("missing")).env, {}, "installations without the admin module still run");
  await assert.rejects(source("down"), RuntimeEnvironmentUnavailable);
});

test("assignment payloads carry the workspace runtime environment at delivery time", async (t) => {
  let available = false;
  const calls: string[] = [];
  const cp = await startControlPlane({
    runtimeEnvironment: async (workspaceId) => {
      calls.push(workspaceId);
      if (!available) throw new Error("store unreachable");
      return {
        env: { ANTHROPIC_API_KEY: "sk-workspace", MY_SECRET: "workspace-value", WORKFORCE_INTEGRATION_GITHUB_TOKEN: "gho_x" },
        secretEnvNames: ["ANTHROPIC_API_KEY", "MY_SECRET", "WORKFORCE_INTEGRATION_GITHUB_TOKEN"],
        skipped: [{ name: "RELAY_FOO", reason: "reserved" }],
        integrationErrors: [],
      };
    },
  });
  t.after(() => cp.stop());
  const { api, origin } = cp;
  const tenant = await seedTenant(cp);
  const token = await api("/api/v1/workers/enrollment-tokens", { method: "POST", token: tenant.ownerToken, body: { name: "env" } });
  const worker = (await api("/api/v1/workers/register", {
    method: "POST",
    body: { enrollmentToken: token.body.token, name: "env" },
  })).body as { workerId: string; workerToken: string };
  const run = await api("/api/v1/workflows/run", {
    method: "POST",
    token: tenant.memberToken,
    body: { workflow: DETERMINISTIC_FLOW, fileType: "yaml", envSecrets: { MY_SECRET: "run-value" } },
  });
  const queue = openQueue(origin, worker.workerId, worker.workerToken);
  t.after(() => queue.close());
  await queue.ready;
  await new Promise((resolve) => setTimeout(resolve, 150));
  assert.ok(calls.length >= 1, "delivery consulted the runtime environment");
  assert.equal(queue.events.some((event) => event.event === "assignment"), false, "no payload while the store is down");
  let current = await api(`/api/v1/workflows/runs/${run.body.runId}`, { token: tenant.memberToken });
  assert.equal(current.body.assignmentStatus, "queued", "the claim was released for retry");

  available = true;
  const delivered = await queue.next("assignment");
  const payload = JSON.parse(delivered.data.workflowRef.value);
  assert.equal(payload.envSecrets.ANTHROPIC_API_KEY, "sk-workspace");
  assert.equal(payload.envSecrets.WORKFORCE_INTEGRATION_GITHUB_TOKEN, "gho_x");
  assert.equal(payload.envSecrets.MY_SECRET, "run-value", "run envSecrets win");
  assert.deepEqual(JSON.parse(payload.envSecrets.RELAY_SECRET_ENV_NAMES), [
    "ANTHROPIC_API_KEY",
    "MY_SECRET",
    "WORKFORCE_INTEGRATION_GITHUB_TOKEN",
  ]);
  current = await api(`/api/v1/workflows/runs/${run.body.runId}`, { token: tenant.memberToken });
  assert.equal(JSON.stringify(current.body).includes("sk-workspace"), false);
  const events = await api(`/api/v1/workflows/runs/${run.body.runId}/events`, { token: tenant.memberToken });
  const warning = events.body.events.find((event: { eventType: string }) => event.eventType === "run.environment_warnings");
  assert.deepEqual(warning.payload, { skipped: ["RELAY_FOO"] });
  assert.equal(JSON.stringify(events.body).includes("sk-workspace"), false, "values never reach run events");

  // The admin module's runtimeCredentialVerifier hook accepts worker tokens.
  const verified = await cp.flows.verifyRuntimeCredential(worker.workerToken);
  assert.deepEqual(verified, {
    subject: `worker:${worker.workerId}`,
    workspaceId: tenant.workspaceId,
    scopes: ["runtime:env:read", "runtime:integrations:read"],
  });
  assert.equal(await cp.flows.verifyRuntimeCredential("sh_wrk_enr_x"), null);
  assert.equal(await cp.flows.verifyRuntimeCredential(tenant.memberToken), null);
});

test("flow source syntax is checked before create/update, preserving the last valid definition", async (t) => {
  const cp = await startControlPlane();
  t.after(() => cp.stop());
  const { api } = cp;
  const tenant = await seedTenant(cp);
  for (const [fileType, source] of [["ts", "this is not valid TypeScript !!!"], ["yaml", "steps: [unterminated"]] as const) {
    const invalid = await api("/cloud/api/v1/flows", {
      method: "POST", token: tenant.ownerToken,
      body: { name: `invalid-${fileType}`, fileType, source },
    });
    assert.equal(invalid.status, 400, invalid.text);
    assert.equal(invalid.body.code, "flow_source_invalid");
  }
  // A UTF-8 payload can fit the route's character cap but exceed the byte cap.
  // Oversized invalid source must be refused before reaching a syntax parser.
  const oversized = await api("/cloud/api/v1/flows", {
    method: "POST", token: tenant.ownerToken,
    body: { name: "oversized", fileType: "ts", source: "this is not valid TypeScript !!!" + "é".repeat(128 * 1024 + 1) },
  });
  assert.equal(oversized.status, 413, oversized.text);
  assert.equal(oversized.body.code, "workflow_too_large");
  const source = 'type Input = { text: string }; export default async function run(input: Input) { return input.text; }';
  const saved = await api("/cloud/api/v1/flows", {
    method: "POST", token: tenant.ownerToken, body: { name: "valid-flow", fileType: "ts", source },
  });
  assert.equal(saved.status, 201, saved.text);
  const id = saved.body.flow.id;
  const invalid = await api(`/cloud/api/v1/flows/${id}`, {
    method: "PATCH", token: tenant.ownerToken, body: { source: "this is not valid TypeScript !!!" },
  });
  assert.equal(invalid.status, 400, invalid.text);
  assert.equal(invalid.body.code, "flow_source_invalid");
  const current = await api(`/cloud/api/v1/flows/${id}`, { token: tenant.ownerToken });
  assert.equal(current.body.flow.source, source);
  assert.equal(current.body.flow.updatedAt, saved.body.flow.updatedAt);
  const count = await cp.database.query<{ count: number }>("SELECT count(*)::int AS count FROM selfhost_flow_definitions");
  assert.equal(count.rows[0]!.count, 1);
});
