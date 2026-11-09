import type { IncomingMessage, ServerResponse } from "node:http";
import type { Logger } from "../observability/log.ts";
import { FlowsError, type FlowsStore } from "./store.ts";
import { presentedBearer, telemetrySecretMatches } from "./telemetry.ts";

/**
 * Internal routes relay-control calls on this service (not user-facing).
 *
 * - `POST /internal/v1/flows/runtime-credentials/verify`, bearer
 *   RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET (unset = 404). Body
 *   `{"token": "<worker credential>"}`. Answers 200
 *   `{active: true, subject, workspaceId, scopes}` for a live worker
 *   credential, else `{active: false}`. relay-control's admin
 *   runtime-environment route uses it to accept worker credentials.
 * - `DELETE /internal/v1/flows/workspaces/{id}`, bearer
 *   RELAYFILE_CONTROL_BROKER_SECRET. Deletes the workspace's Flows data and
 *   queues CLI-credential revocations for its runs. Idempotent: 200 with
 *   the counts deleted (all zero on a repeat).
 *
 * Secrets are compared in constant time and never logged.
 */
export const RUNTIME_VERIFY_PATH = "/internal/v1/flows/runtime-credentials/verify";
const WORKSPACE_PATH = /^\/internal\/v1\/flows\/workspaces\/([^/]+)$/;
const MAX_VERIFY_BODY = 8 * 1024;

export type InternalFlowsDeps = {
  store: FlowsStore | undefined;
  runtimeVerifySecret: string | undefined;
  brokerSecret: string | undefined;
  logger: Logger;
};

function send(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
  response.end(JSON.stringify(body));
}

async function readJson(request: IncomingMessage, limit: number): Promise<Record<string, unknown>> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.length;
    if (size > limit) throw new FlowsError(413, "payload_too_large", "Request body is too large");
    chunks.push(buffer);
  }
  try {
    const parsed: unknown = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) return parsed as Record<string, unknown>;
  } catch {
    // fall through
  }
  throw new FlowsError(400, "invalid_request", "Invalid request body");
}

/** Answers the internal Flows routes; false when the path is not one of them. */
export function createInternalFlowsHandler(deps: InternalFlowsDeps) {
  return async (request: IncomingMessage, response: ServerResponse, path: string): Promise<boolean> => {
    const verify = path === RUNTIME_VERIFY_PATH;
    const workspace = verify ? null : WORKSPACE_PATH.exec(path);
    if (!verify && !workspace) return false;
    const secret = verify ? deps.runtimeVerifySecret : deps.brokerSecret;
    if (!secret || !deps.store) {
      request.resume();
      send(response, 404, { error: "not_found" });
      return true;
    }
    const method = verify ? "POST" : "DELETE";
    if (request.method !== method) {
      request.resume();
      send(response, 405, { error: "method_not_allowed" });
      return true;
    }
    if (!telemetrySecretMatches(presentedBearer(request), secret)) {
      request.resume();
      send(response, 401, { error: "unauthorized" });
      return true;
    }
    try {
      if (verify) {
        const body = await readJson(request, MAX_VERIFY_BODY);
        if (typeof body.token !== "string" || !body.token) {
          throw new FlowsError(400, "invalid_request", "token is required");
        }
        const verified = await deps.store.verifyRuntimeCredential(body.token);
        send(response, 200, verified ? { active: true, ...verified } : { active: false });
        return true;
      }
      request.resume();
      let workspaceId: string;
      try {
        workspaceId = decodeURIComponent(workspace![1]!);
      } catch {
        throw new FlowsError(400, "invalid_request", "Invalid path encoding");
      }
      const deleted = await deps.store.deleteWorkspace(workspaceId);
      deps.logger.event("flows_workspace_deleted", { area: "flows", workspaceId, ...deleted });
      send(response, 200, { workspaceId, deleted });
      return true;
    } catch (error) {
      if (error instanceof FlowsError) {
        send(response, error.status, { error: error.code, message: error.message });
        return true;
      }
      throw error;
    }
  };
}
