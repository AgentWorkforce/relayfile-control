import type { IncomingMessage, ServerResponse } from "node:http";
import { MAX_OBJECT_BYTES, validateObjectKey } from "./objects.ts";
import {
  cleanTags,
  FlowsError,
  TERMINAL_RUN_STATUSES,
  type FlowsStore,
} from "./store.ts";

type JsonObject = Record<string, unknown>;

/**
 * The authenticated user as relay-control's introspection answer describes
 * it. Every id is a relay-control id; this service holds no user or
 * workspace rows of its own.
 */
export type Principal = {
  userId: string;
  email: string;
  organizationId: string;
  workspaceId: string;
  role: "owner" | "admin" | "member";
  scopes: string[];
  tokenId: string;
  expiresAt: number;
};

export type FlowsUserAccess = (
  request: IncomingMessage,
  admin?: boolean,
) => Promise<{
  principal: Principal;
  workspace: { id: string; organizationId: string; role: Principal["role"] };
}>;

const JSON_LIMIT = 4 * 1024 * 1024;

function send(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  response.end(JSON.stringify(body));
}

function bearer(request: IncomingMessage): string | null {
  const match = /^Bearer\s+(.+)$/i.exec(request.headers.authorization ?? "");
  return match?.[1]?.trim() || null;
}

async function rawBody(request: IncomingMessage, limit: number): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > limit) throw new FlowsError(413, "payload_too_large", "Request body is too large");
    chunks.push(buffer);
  }
  return Buffer.concat(chunks);
}

async function jsonBody(request: IncomingMessage, limit = JSON_LIMIT): Promise<JsonObject> {
  const raw = (await rawBody(request, limit)).toString("utf8");
  if (!raw.trim()) return {};
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new FlowsError(400, "invalid_request", "Invalid request body");
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new FlowsError(400, "invalid_request", "Invalid request body");
  }
  return parsed as JsonObject;
}

function str(body: JsonObject, name: string, required: true, max?: number): string;
function str(body: JsonObject, name: string, required?: false, max?: number): string | undefined;
function str(body: JsonObject, name: string, required = false, max = 100_000): string | undefined {
  const value = body[name];
  if (value === undefined || value === null) {
    if (required) throw new FlowsError(400, "invalid_request", `${name} is required`);
    return undefined;
  }
  if (typeof value !== "string" || (required && !value.trim())) {
    throw new FlowsError(400, "invalid_request", `${name} must be a string`);
  }
  if (value.length > max) throw new FlowsError(400, "invalid_request", `${name} is too long`);
  return value;
}

function fileTypeOf(value: unknown, fallback?: "yaml" | "ts"): "yaml" | "ts" {
  if (value === undefined && fallback) return fallback;
  if (value === "yaml" || value === "ts") return value;
  if (value === "yml") return "yaml";
  throw new FlowsError(400, "invalid_request", "fileType must be yaml or ts");
}

function envSecretsOf(value: unknown): Record<string, string> | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "object" || Array.isArray(value)) {
    throw new FlowsError(400, "invalid_request", "envSecrets must be an object of strings");
  }
  const entries = Object.entries(value as JsonObject);
  for (const [key, secret] of entries) {
    if (!/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(key) || typeof secret !== "string" || secret.length > 16_384) {
      throw new FlowsError(400, "invalid_request", "envSecrets must map env var names to strings");
    }
  }
  return Object.fromEntries(entries) as Record<string, string>;
}

function runtimeOf(body: JsonObject): { workerId?: string; workerName?: string; tags?: string[] } {
  const runtime = body.runtime;
  if (runtime === undefined || runtime === null) {
    return {
      workerId: str(body, "workerId"),
      tags: body.tags === undefined ? undefined : cleanTags(body.tags),
    };
  }
  if (typeof runtime !== "object" || Array.isArray(runtime)) {
    throw new FlowsError(400, "invalid_request", "runtime must be an object");
  }
  const descriptor = runtime as JsonObject;
  const id = descriptor.id ?? descriptor.kind;
  if (id !== undefined && id !== "worker") {
    throw new FlowsError(400, "runtime_unsupported", "Self-hosted runs execute on registered workers");
  }
  const config = (descriptor.config ?? {}) as JsonObject;
  return {
    workerId: typeof config.workerId === "string" ? config.workerId : undefined,
    workerName: typeof config.name === "string" ? config.name : undefined,
    tags: config.tags === undefined ? undefined : cleanTags(config.tags),
  };
}

function codeArchiveOf(body: JsonObject): Buffer | undefined {
  const value = body.codeArchiveBase64;
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string" || !/^[A-Za-z0-9+/=\s]+$/.test(value)) {
    throw new FlowsError(400, "invalid_request", "codeArchiveBase64 must be base64");
  }
  const archive = Buffer.from(value, "base64");
  if (archive.length < 2 || archive[0] !== 0x1f || archive[1] !== 0x8b) {
    throw new FlowsError(400, "invalid_request", "codeArchiveBase64 must be a gzip tarball");
  }
  return archive;
}

function match(path: string, pattern: RegExp): string[] | null {
  const found = pattern.exec(path);
  if (!found) return null;
  try {
    return found.slice(1).map((part) => decodeURIComponent(part));
  } catch {
    throw new FlowsError(400, "invalid_request", "Invalid path encoding");
  }
}

function contentTypeFor(key: string, declared?: string): string {
  const base = declared?.split(";")[0]?.trim().toLowerCase();
  if (base && /^[a-z0-9.+-]+\/[a-z0-9.+-]+$/.test(base)) return base;
  if (key.endsWith(".log") || key.endsWith(".txt") || key.endsWith(".md")) return "text/plain";
  if (key.endsWith(".json") || key.endsWith(".jsonl")) return "application/json";
  if (key.endsWith(".tar.gz") || key.endsWith(".tgz")) return "application/gzip";
  return "application/octet-stream";
}

function objectKind(key: string): "log" | "code" | "artifact" {
  if (key === "code.tar.gz" || /^paths\//.test(key)) return "code";
  if (key.endsWith(".log")) return "log";
  return "artifact";
}

export function createFlowsHandler(deps: {
  store: FlowsStore;
  publicBaseUrl: string;
  userAccess: FlowsUserAccess;
}): (request: IncomingMessage, response: ServerResponse, path: string, url: URL) => Promise<boolean> {
  const { store, publicBaseUrl, userAccess } = deps;
  const config = store.config;
  let lastMaintenance = 0;

  async function maintain(force = false): Promise<void> {
    if (!force && Date.now() - lastMaintenance < 5_000) return;
    lastMaintenance = Date.now();
    await store.maintenance();
  }

  async function workerAuth(request: IncomingMessage, workerId: string) {
    const worker = await store.authenticateWorker(workerId, bearer(request));
    if (!worker) throw new FlowsError(401, "unauthorized", "Unauthorized");
    return worker;
  }

  async function runFor(request: IncomingMessage, runId: string) {
    const access = await userAccess(request);
    const run = await store.getRun(access.workspace.id, runId);
    if (!run) throw new FlowsError(404, "run_not_found", "Run not found");
    return { access, run };
  }

  async function runTokenAuth(request: IncomingMessage, runId: string) {
    const run = await store.authenticateRunToken(runId, bearer(request));
    if (!run) throw new FlowsError(401, "unauthorized", "Invalid run token");
    return run;
  }

  async function queue(request: IncomingMessage, response: ServerResponse, workerId: string): Promise<void> {
    const worker = await workerAuth(request, workerId);
    await store.touchWorker(worker.id);
    await maintain(true);
    response.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache, no-transform",
      connection: "keep-alive",
      "x-accel-buffering": "no",
    });
    let closed = false;
    let wake: (() => void) | undefined;
    response.on("close", () => {
      closed = true;
      wake?.();
    });
    const emit = (event: string, data: unknown) => {
      if (!closed) response.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
    };
    const unsubscribe = store.onWork(() => wake?.());
    try {
      emit("ping", {});
      for (const assignment of await store.unackedAssignments(worker.id)) emit("assignment", assignment);
      let lastPing = Date.now();
      while (!closed) {
        if (await store.isWorkerRevoked(worker.id)) {
          emit("revoke", {});
          break;
        }
        await maintain();
        let assignment = null;
        try {
          assignment = await store.claimNext(worker);
        } catch (error) {
          console.error("[selfhost-api] assignment delivery deferred", {
            workerId: worker.id,
            error: error instanceof Error ? error.message : String(error),
          });
        }
        if (assignment) {
          emit("assignment", assignment);
          continue;
        }
        if (Date.now() - lastPing >= config.queuePingMs) {
          emit("ping", {});
          await store.touchWorker(worker.id);
          lastPing = Date.now();
        }
        await new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, config.queuePollMs);
          wake = () => {
            clearTimeout(timer);
            resolve();
          };
        });
        wake = undefined;
      }
    } catch (error) {
      console.error("[selfhost-api] worker queue stream failed", {
        workerId: worker.id,
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      unsubscribe();
      if (!response.writableEnded) response.end();
    }
  }

  async function serveObject(
    request: IncomingMessage,
    response: ServerResponse,
    runId: string,
    key: string,
    download: boolean,
  ): Promise<void> {
    if (request.method === "HEAD") {
      const meta = await store.objects.head(runId, key);
      response.writeHead(meta ? 200 : 404, meta
        ? { "content-type": meta.contentType, "content-length": String(meta.size), etag: meta.etag }
        : {});
      response.end();
      return;
    }
    const object = await store.objects.get(runId, key);
    if (!object) throw new FlowsError(404, "object_not_found", "Object not found");
    const safeType = download && !["text/plain", "application/json"].includes(object.contentType)
      ? "application/octet-stream"
      : object.contentType;
    response.writeHead(200, {
      "content-type": safeType,
      "content-length": String(object.body.length),
      etag: object.etag,
      "cache-control": "no-store",
      "x-content-type-options": "nosniff",
      ...(download
        ? {
          "content-disposition": `attachment; filename="${key.split("/").pop()!.replace(/[^A-Za-z0-9._-]/g, "_")}"`,
          "content-security-policy": "sandbox; default-src 'none'",
        }
        : {}),
    });
    response.end(object.body);
  }

  async function handle(
    request: IncomingMessage,
    response: ServerResponse,
    path: string,
    url: URL,
  ): Promise<boolean> {
    const method = request.method ?? "GET";
    let params: string[] | null;

    // ------------------------------------------------ worker protocol
    if (path === "/api/v1/workers/register" && method === "POST") {
      const body = await jsonBody(request, 64 * 1024);
      const enrollmentToken = str(body, "enrollmentToken", true, 512);
      const name = str(body, "name", true, 128);
      const hostInfo = body.hostInfo;
      if (hostInfo !== undefined && (!hostInfo || typeof hostInfo !== "object" || Array.isArray(hostInfo))) {
        throw new FlowsError(400, "invalid_request", "Invalid request body");
      }
      const registered = await store.registerWorker({
        enrollmentToken,
        name,
        hostInfo: hostInfo as Record<string, unknown> | undefined,
      });
      send(response, 200, registered);
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workers\/([^/]+)\/heartbeat$/)) && method === "POST") {
      const worker = await workerAuth(request, params[0]!);
      await maintain();
      await store.touchWorker(worker.id);
      send(response, 200, { ok: true, nextHeartbeatMs: config.heartbeatIntervalMs });
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workers\/([^/]+)\/queue$/)) && method === "GET") {
      await queue(request, response, params[0]!);
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workers\/([^/]+)\/assignments\/([^/]+)\/ack$/)) && method === "POST") {
      const worker = await workerAuth(request, params[0]!);
      await store.touchWorker(worker.id);
      send(response, 200, await store.ackAssignment(worker.id, params[1]!));
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workers\/([^/]+)\/assignments\/([^/]+)\/status$/)) && method === "POST") {
      const worker = await workerAuth(request, params[0]!);
      const body = await jsonBody(request, 256 * 1024);
      const phase = body.phase;
      if (phase !== "running" && phase !== "completed" && phase !== "failed") {
        throw new FlowsError(400, "invalid_request", "Invalid request body");
      }
      const numberOr = (value: unknown) => typeof value === "number" && Number.isFinite(value) ? Math.trunc(value) : undefined;
      const result = body.result;
      await store.touchWorker(worker.id);
      send(response, 200, await store.reportStatus(worker.id, params[1]!, {
        phase,
        exitCode: numberOr(body.exitCode),
        durationMs: numberOr(body.durationMs),
        summary: typeof body.summary === "string" ? body.summary.slice(0, 2_000) : undefined,
        error: typeof body.error === "string" ? body.error.slice(0, 4_000) : undefined,
        result: result && typeof result === "object" && !Array.isArray(result) ? result as JsonObject : undefined,
      }));
      return true;
    }
    // Subscription logins for the run (Codex/Grok auth files). Needs both the
    // worker credential and the run's short-lived reference from the payload.
    if ((params = match(path, /^\/api\/v1\/workers\/([^/]+)\/assignments\/([^/]+)\/cli-credentials$/) ??
      match(path, /^\/api\/v1\/workers\/([^/]+)\/assignments\/([^/]+)\/cli-credentials\/([^/]+)$/)) &&
      (method === "GET" || method === "PUT")) {
      const worker = await workerAuth(request, params[0]!);
      const runId = params[1]!;
      if (!store.cliCredentials || !await store.workerCanAccessRun(worker.id, runId)) {
        throw new FlowsError(404, "assignment_not_found", "Assignment not found");
      }
      const reference = typeof request.headers["x-cli-credential-ref"] === "string"
        ? request.headers["x-cli-credential-ref"]
        : null;
      try {
        if (method === "GET" && !params[2]) {
          send(response, 200, await store.cliCredentials.readForRun(reference, runId));
          return true;
        }
        if (method === "PUT" && params[2]) {
          const body = await jsonBody(request, 128 * 1024);
          send(response, 200, await store.cliCredentials.writeBack(reference, runId, params[2], body.content));
          return true;
        }
      } catch (error) {
        const coded = error as { status?: unknown; code?: unknown; message?: unknown };
        if (typeof coded.status === "number" && typeof coded.code === "string") {
          throw new FlowsError(coded.status, coded.code, String(coded.message ?? coded.code));
        }
        throw error;
      }
      throw new FlowsError(405, "method_not_allowed", "Method not allowed");
    }
    if ((params = match(path, /^\/api\/v1\/workers\/([^/]+)\/assignments\/([^/]+)\/storage\/(.+)$/)) &&
      (method === "GET" || method === "HEAD" || method === "PUT")) {
      const worker = await workerAuth(request, params[0]!);
      const runId = params[1]!;
      const key = validateObjectKey(params[2]!);
      if (!key) throw new FlowsError(400, "invalid_object_key", "Invalid object key");
      if (!await store.workerCanAccessRun(worker.id, runId)) {
        throw new FlowsError(404, "assignment_not_found", "Assignment not found");
      }
      if (method === "PUT") {
        if (key === "code.tar.gz") throw new FlowsError(403, "forbidden", "Code archives are read-only");
        const body = await rawBody(request, MAX_OBJECT_BYTES);
        const meta = await store.objects.put(runId, key, body, contentTypeFor(key, request.headers["content-type"]));
        send(response, 200, { ok: true, etag: meta.etag });
        return true;
      }
      await serveObject(request, response, runId, key, false);
      return true;
    }

    // ------------------------------------------ dashboard: workers
    if (path === "/api/v1/workers/enrollment-tokens") {
      if (method === "GET") {
        const access = await userAccess(request, true);
        send(response, 200, { tokens: await store.listEnrollmentTokens(access.workspace.id) });
        return true;
      }
      if (method === "POST") {
        const access = await userAccess(request, true);
        const body = await jsonBody(request, 16 * 1024);
        const workspaceId = str(body, "workspaceId");
        if (workspaceId && workspaceId !== access.workspace.id) {
          throw new FlowsError(404, "workspace_not_found", "Workspace not found");
        }
        const name = str(body, "name", false, 128)?.trim() || undefined;
        const expiresInMinutes = body.expiresInMinutes;
        if (expiresInMinutes !== undefined &&
          (typeof expiresInMinutes !== "number" || !Number.isInteger(expiresInMinutes) ||
            expiresInMinutes < 1 || expiresInMinutes > 7 * 24 * 60)) {
          throw new FlowsError(400, "invalid_request", "expiresInMinutes must be 1 to 10080");
        }
        const created = await store.createEnrollmentToken({
          workspaceId: access.workspace.id,
          userId: access.principal.userId,
          userEmail: access.principal.email,
          name,
          tags: cleanTags(body.tags),
          ttlMs: typeof expiresInMinutes === "number" ? expiresInMinutes * 60_000 : undefined,
        });
        const workerName = name ?? "<worker-name>";
        send(response, 200, {
          id: created.id,
          token: created.token,
          expiresAt: created.expiresAt,
          createdAt: created.createdAt,
          cloudUrl: publicBaseUrl,
          registerCommand:
            `agent-relay cloud worker register --token ${created.token} --name ${workerName} --base-url ${publicBaseUrl}`,
          startCommand: `agent-relay cloud worker start --name ${workerName} --base-url ${publicBaseUrl}`,
        });
        return true;
      }
    }
    if ((params = match(path, /^\/api\/v1\/workers\/enrollment-tokens\/([^/]+)$/)) && method === "DELETE") {
      const access = await userAccess(request, true);
      if (!await store.revokeEnrollmentToken(access.workspace.id, params[0]!)) {
        throw new FlowsError(404, "token_not_found", "Enrollment token not found or already used");
      }
      send(response, 200, { id: params[0], status: "revoked" });
      return true;
    }
    if (path === "/api/v1/workers" && method === "GET") {
      const access = await userAccess(request);
      await maintain();
      send(response, 200, { workers: await store.listWorkers(access.workspace.id) });
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workers\/([^/]+)$/))) {
      if (method === "GET") {
        const access = await userAccess(request);
        const worker = await store.getWorker(access.workspace.id, params[0]!);
        if (!worker) throw new FlowsError(404, "worker_not_found", "Worker not found");
        send(response, 200, { worker });
        return true;
      }
      if (method === "DELETE") {
        const access = await userAccess(request, true);
        const worker = await store.revokeWorker(access.workspace.id, params[0]!);
        if (!worker) throw new FlowsError(404, "worker_not_found", "Worker not found");
        send(response, 200, { workerId: worker.id, status: worker.status });
        return true;
      }
    }

    // --------------------------------------------- dashboard: flows
    if (path === "/api/v1/flows") {
      if (method === "GET") {
        const access = await userAccess(request);
        send(response, 200, { flows: await store.listFlows(access.workspace.id) });
        return true;
      }
      if (method === "POST") {
        const access = await userAccess(request);
        const body = await jsonBody(request);
        const flow = await store.createFlow({
          workspaceId: access.workspace.id,
          userId: access.principal.userId,
          name: str(body, "name", true, 100),
          description: str(body, "description", false, 1_000),
          fileType: fileTypeOf(body.fileType, "yaml"),
          source: str(body, "source", true, 262_144),
        });
        send(response, 201, { flow });
        return true;
      }
    }
    if ((params = match(path, /^\/api\/v1\/flows\/([^/]+)$/))) {
      const flowId = params[0]!;
      if (method === "GET") {
        const access = await userAccess(request);
        const flow = await store.getFlow(access.workspace.id, flowId);
        if (!flow) throw new FlowsError(404, "flow_not_found", "Flow not found");
        const recent = await store.listRuns(access.workspace.id, { flowId, limit: 20 });
        send(response, 200, { flow, recentRuns: recent.runs });
        return true;
      }
      if (method === "PATCH" || method === "PUT") {
        const access = await userAccess(request);
        const body = await jsonBody(request);
        const flow = await store.updateFlow({
          workspaceId: access.workspace.id,
          flowId,
          name: str(body, "name", false, 100),
          description: body.description === null ? null : str(body, "description", false, 1_000),
          fileType: body.fileType === undefined ? undefined : fileTypeOf(body.fileType),
          source: str(body, "source", false, 262_144),
        });
        if (!flow) throw new FlowsError(404, "flow_not_found", "Flow not found");
        send(response, 200, { flow });
        return true;
      }
      if (method === "DELETE") {
        const access = await userAccess(request);
        if (!await store.deleteFlow(access.workspace.id, flowId)) {
          throw new FlowsError(404, "flow_not_found", "Flow not found");
        }
        send(response, 200, { flowId, status: "deleted" });
        return true;
      }
    }
    if ((params = match(path, /^\/api\/v1\/flows\/([^/]+)\/(?:invoke|runs)$/)) && method === "POST") {
      const access = await userAccess(request);
      const flow = await store.getFlow(access.workspace.id, params[0]!);
      if (!flow) throw new FlowsError(404, "flow_not_found", "Flow not found");
      const body = await jsonBody(request);
      const runtime = runtimeOf(body);
      const created = await store.createRun({
        workspaceId: access.workspace.id,
        userId: access.principal.userId,
        userEmail: access.principal.email,
        flowId: flow.id,
        workflow: flow.source,
        fileType: flow.fileType,
        workflowName: flow.name,
        trigger: "manual",
        inputs: body.inputs,
        envSecrets: envSecretsOf(body.envSecrets),
        ...runtime,
      });
      send(response, 201, {
        runId: created.run.runId,
        status: created.run.status,
        assignmentId: created.assignmentId,
        run: created.run,
      });
      return true;
    }

    // ---------------------------------------------- dashboard: runs
    if (path === "/api/v1/workflows/run" && method === "POST") {
      const access = await userAccess(request);
      const body = await jsonBody(request);
      if (body.workspaceId !== undefined && body.workspaceId !== access.workspace.id) {
        throw new FlowsError(404, "workspace_not_found", "Workspace not found");
      }
      const flowId = str(body, "flowId");
      const flow = flowId ? await store.getFlow(access.workspace.id, flowId) : null;
      if (flowId && !flow) throw new FlowsError(404, "flow_not_found", "Flow not found");
      const resume = str(body, "resume");
      if (resume) {
        const resumed = await store.resumeRun({
          workspaceId: access.workspace.id,
          userId: access.principal.userId,
          userEmail: access.principal.email,
          runId: resume,
          mode: "resume",
          startFrom: str(body, "startFrom", false, 256),
        });
        send(response, 201, { runId: resumed.run.runId, status: resumed.run.status, assignmentId: resumed.assignmentId });
        return true;
      }
      const created = await store.createRun({
        workspaceId: access.workspace.id,
        userId: access.principal.userId,
        userEmail: access.principal.email,
        flowId: flow?.id ?? null,
        workflow: flow?.source ?? str(body, "workflow", true, 262_144),
        fileType: flow?.fileType ?? fileTypeOf(body.fileType),
        workflowName: flow?.name ?? str(body, "name", false, 200),
        workflowFileName: str(body, "workflowFileName", false, 200),
        trigger: "api",
        inputs: body.inputs,
        envSecrets: envSecretsOf(body.envSecrets),
        codeArchive: codeArchiveOf(body),
        ...runtimeOf(body),
      });
      send(response, 201, {
        runId: created.run.runId,
        status: created.run.status,
        assignmentId: created.assignmentId,
        dispatchedTo: created.run.workerName,
      });
      return true;
    }
    if (path === "/api/v1/workflows/runs" && method === "GET") {
      const access = await userAccess(request);
      await maintain();
      const limit = url.searchParams.get("limit");
      const status = url.searchParams.get("status") ?? undefined;
      if (status && !["pending", "running", "completed", "failed", "cancelled"].includes(status)) {
        throw new FlowsError(400, "invalid_request", "Invalid status filter");
      }
      send(response, 200, await store.listRuns(access.workspace.id, {
        cursor: url.searchParams.get("cursor") ?? undefined,
        flowId: url.searchParams.get("flowId") ?? undefined,
        status,
        limit: limit ? Number(limit) || undefined : undefined,
      }));
      return true;
    }
    if (path === "/api/v1/workflows/local-run" && method === "POST") {
      const access = await userAccess(request);
      const body = await jsonBody(request);
      if (body.workspaceId !== undefined && body.workspaceId !== access.workspace.id) {
        throw new FlowsError(404, "workspace_not_found", "Workspace not found");
      }
      const resumedFrom = str(body, "resumedFromRunId");
      const created = await store.createRun({
        workspaceId: access.workspace.id,
        userId: access.principal.userId,
        userEmail: access.principal.email,
        workflow: str(body, "workflow", true, 262_144),
        fileType: fileTypeOf(body.fileType),
        dispatchType: "local",
        trigger: "local",
        inputs: body.inputs,
        resumedFromRunId: resumedFrom && (await store.getRun(access.workspace.id, resumedFrom)) ? resumedFrom : undefined,
      });
      const runUrl = config.dashboardUrl?.startsWith("https://")
        ? `${config.dashboardUrl}/dashboard/#/flows/runs/${created.run.runId}`
        : undefined;
      send(response, 201, {
        runId: created.run.runId,
        status: created.run.status,
        dispatchType: "local",
        accessToken: created.accessToken,
        callbackToken: created.callbackToken,
        apiUrl: publicBaseUrl,
        ...(runUrl ? { runUrl } : {}),
      });
      return true;
    }
    if (path === "/api/v1/workflows/callback" && method === "POST") {
      const body = await jsonBody(request, 512 * 1024);
      const result = body.result;
      send(response, 200, await store.callback({
        runId: str(body, "runId", true, 64),
        accessToken: bearer(request),
        callbackToken: str(body, "callbackToken", false, 512) ??
          (typeof request.headers["x-callback-token"] === "string" ? request.headers["x-callback-token"] : null),
        status: str(body, "status", true, 32),
        result: result && typeof result === "object" && !Array.isArray(result) ? result as JsonObject : undefined,
        error: str(body, "error", false, 4_000),
      }));
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)$/)) && method === "GET") {
      const { run } = await runFor(request, params[0]!);
      send(response, 200, run);
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)\/workflow$/)) && method === "GET") {
      const { access, run } = await runFor(request, params[0]!);
      send(response, 200, {
        runId: run.runId,
        fileType: run.fileType,
        workflowFileName: run.workflowFileName,
        workflow: await store.getRunSource(access.workspace.id, run.runId),
      });
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)\/cancel$/)) && method === "POST") {
      const access = await userAccess(request);
      const run = await store.cancelRun(access.workspace.id, params[0]!, access.principal.email);
      if (!run) throw new FlowsError(404, "run_not_found", "Run not found");
      send(response, 200, { runId: run.runId, status: run.status });
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)\/(resume|retry)$/)) && method === "POST") {
      const access = await userAccess(request);
      const body = await jsonBody(request, 16 * 1024);
      const resumed = await store.resumeRun({
        workspaceId: access.workspace.id,
        userId: access.principal.userId,
        userEmail: access.principal.email,
        runId: params[0]!,
        mode: params[1] === "retry" || body.mode === "retry" ? "retry" : "resume",
        startFrom: str(body, "startFrom", false, 256),
        workerId: str(body, "workerId", false, 64),
      });
      send(response, 201, {
        runId: resumed.run.runId,
        status: resumed.run.status,
        resumedFromRunId: params[0],
        assignmentId: resumed.assignmentId,
      });
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)\/steps$/))) {
      if (method === "GET") {
        const access = await userAccess(request);
        const steps = await store.listSteps(access.workspace.id, params[0]!);
        if (!steps) throw new FlowsError(404, "run_not_found", "Run not found");
        send(response, 200, steps);
        return true;
      }
      if (method === "POST") {
        await runTokenAuth(request, params[0]!);
        const body = await jsonBody(request);
        if (!Array.isArray(body.steps) || body.steps.length > 500 ||
          body.steps.some((step) => !step || typeof step !== "object" || typeof (step as JsonObject).stepName !== "string")) {
          throw new FlowsError(400, "invalid_request", "steps must be an array of step reports");
        }
        const count = await store.replaceFinalSteps(params[0]!, body.steps as JsonObject[]);
        send(response, 201, { runId: params[0], steps: count });
        return true;
      }
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)\/steps\/snapshot$/)) && method === "POST") {
      const run = await runTokenAuth(request, params[0]!);
      if (TERMINAL_RUN_STATUSES.includes(run.status)) {
        send(response, 409, { error: "Snapshot refused", reason: "finalized" });
        return true;
      }
      const body = await jsonBody(request, 66 * 1024);
      const steps = Array.isArray(body.steps) ? body.steps : [];
      if (steps.length > 64 || steps.some((step) =>
        !step || typeof step !== "object" || typeof (step as JsonObject).stepName !== "string" ||
        typeof (step as JsonObject).state !== "string")) {
        throw new FlowsError(400, "invalid_request", "Invalid snapshot");
      }
      const count = await store.upsertSnapshotSteps(params[0]!, (steps as JsonObject[]).map((step) => ({
        stepName: String(step.stepName),
        stepType: typeof step.stepType === "string" ? step.stepType : undefined,
        state: String(step.state),
        attempt: typeof step.attempt === "number" ? step.attempt : undefined,
        elapsedMs: typeof step.elapsedMs === "number" ? Math.trunc(step.elapsedMs) : undefined,
        completionReason: typeof step.completionReason === "string" ? step.completionReason : undefined,
        label: typeof step.label === "string" ? step.label : undefined,
        dependsOn: Array.isArray(step.dependsOn) ? step.dependsOn.filter((d): d is string => typeof d === "string") : undefined,
        detail: Object.fromEntries(Object.entries(step).filter(([key]) =>
          ["gate", "model", "turns", "toolCalls", "costUsd", "artifactCount", "artifacts", "logKey", "journalRunId"].includes(key))),
      })));
      send(response, 202, { runId: params[0], sequence: body.sequence ?? null, steps: count });
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)\/events$/))) {
      if (method === "GET") {
        const { run } = await runFor(request, params[0]!);
        const after = Number(url.searchParams.get("after") ?? "0");
        const limit = Number(url.searchParams.get("limit") ?? "100");
        const sort = url.searchParams.get("sort") === "desc" ? "desc" : "asc";
        send(response, 200, {
          events: await store.listEvents(run.runId, {
            after: Number.isFinite(after) ? after : 0,
            limit: Number.isFinite(limit) ? limit : 100,
            sort,
          }),
        });
        return true;
      }
      if (method === "POST") {
        await runTokenAuth(request, params[0]!);
        const body = await jsonBody(request, 256 * 1024);
        const eventType = str(body, "eventType", true, 128);
        const payload = body.payload;
        const sequence = await store.appendEvent(params[0]!, {
          eventType,
          stepName: str(body, "stepName", false, 256),
          payload: payload && typeof payload === "object" && !Array.isArray(payload) ? payload as JsonObject : {},
        });
        send(response, 201, { runId: params[0], sequence });
        return true;
      }
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)\/storage\/(.+)$/)) && method === "PUT") {
      await runTokenAuth(request, params[0]!);
      const key = validateObjectKey(params[1]!);
      if (!key || key === "code.tar.gz") throw new FlowsError(400, "invalid_object_key", "Invalid object key");
      if (request.headers["if-none-match"] === "*" && await store.objects.head(params[0]!, key)) {
        send(response, 412, { error: "Object already exists" });
        return true;
      }
      const body = await rawBody(request, MAX_OBJECT_BYTES);
      const meta = await store.objects.put(params[0]!, key, body, contentTypeFor(key, request.headers["content-type"]));
      send(response, 200, { ok: true, etag: meta.etag });
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)\/logs$/)) && method === "GET") {
      const { run } = await runFor(request, params[0]!);
      const step = url.searchParams.get("step");
      const file = url.searchParams.get("file") ?? "runner.log";
      const key = step ? validateObjectKey(`${step}/agent.log`) : validateObjectKey(file);
      if (!key || !key.endsWith(".log")) throw new FlowsError(400, "invalid_request", "Invalid log file");
      const offset = Math.max(0, Number(url.searchParams.get("offset") ?? "0") || 0);
      const object = await store.objects.get(run.runId, key);
      const terminal = TERMINAL_RUN_STATUSES.includes(run.status);
      if (!object) {
        send(response, 200, {
          content: "",
          offset: 0,
          totalSize: 0,
          done: terminal,
          available: false,
          source: "none",
        });
        return true;
      }
      const reset = offset > object.body.length;
      const start = reset ? 0 : offset;
      const end = Math.min(object.body.length, start + 256 * 1024);
      send(response, 200, {
        content: object.body.subarray(start, end).toString("utf8"),
        offset: end,
        totalSize: object.body.length,
        done: terminal && end >= object.body.length,
        available: true,
        source: "object-storage",
        ...(reset ? { reset: true } : {}),
      });
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)\/artifacts$/)) && method === "GET") {
      const { run } = await runFor(request, params[0]!);
      const objects = await store.objects.list(run.runId);
      send(response, 200, {
        artifacts: objects.map((object) => ({ ...object, kind: objectKind(object.key) })),
      });
      return true;
    }
    if ((params = match(path, /^\/api\/v1\/workflows\/runs\/([^/]+)\/artifacts\/(.+)$/)) &&
      (method === "GET" || method === "HEAD")) {
      const { run } = await runFor(request, params[0]!);
      const key = validateObjectKey(params[1]!);
      if (!key) throw new FlowsError(400, "invalid_object_key", "Invalid object key");
      await serveObject(request, response, run.runId, key, true);
      return true;
    }

    return false;
  }

  return async (request, response, path, url) => {
    try {
      return await handle(request, response, path, url);
    } catch (error) {
      if (error instanceof FlowsError) {
        if (response.headersSent) {
          response.end();
          return true;
        }
        send(response, error.status, { error: error.message, code: error.code });
        return true;
      }
      throw error;
    }
  };
}
