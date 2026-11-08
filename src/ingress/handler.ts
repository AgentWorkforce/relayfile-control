import type { IncomingMessage, ServerResponse } from "node:http";
import type { Config } from "../config.ts";
import type { Db } from "../db/db.ts";
import type { BindingLaneCache } from "../bindings/store.ts";
import { classify } from "../envelope/classify.ts";
import { allowListHeaders, type Ingress, type NativeIds } from "../envelope/model.ts";
import { ulid } from "../envelope/ulid.ts";
import type { Entitlement } from "../entitlement/entitlement.ts";
import { laneHash, type Logger } from "../observability/log.ts";
import type { Metrics } from "../observability/metrics.ts";
import type { CooldownStore } from "../queue/cooldown.ts";
import { insertEnvelope, type QueueDepth } from "../queue/store.ts";
import type { FlagStore } from "../queue/flags.ts";
import { BodyTooLargeError, headerRecord, readCappedBody } from "./body.ts";
import {
  detectSkipHookdeckProvider,
  hasNangoSignature,
  linearTimestampFresh,
  verifyDropbox,
  verifyGitHub,
  verifyHookdeck,
  verifyLinear,
  verifyNango,
  verifySlack,
} from "./verify.ts";

export type IngressDeps = {
  config: Config;
  db: Db;
  logger: Logger;
  metrics: Metrics;
  entitlement: Entitlement;
  depth: QueueDepth;
  cooldowns: CooldownStore;
  bindings: BindingLaneCache;
  flags: FlagStore;
};

export const REQUEST_ID_HEADER = "x-relayfile-control-request-id";

type Reply = { status: number; body: unknown; headers?: Record<string, string>; reason?: string; text?: string };

class StorageError extends Error {}

function send(res: ServerResponse, requestId: string, reply: Reply): void {
  const headers: Record<string, string> = { [REQUEST_ID_HEADER]: requestId, "cache-control": "no-store", ...reply.headers };
  if (reply.text !== undefined) {
    res.writeHead(reply.status, { ...headers, "content-type": "text/plain", "x-content-type-options": "nosniff" });
    res.end(reply.text);
    return;
  }
  res.writeHead(reply.status, { ...headers, "content-type": "application/json" });
  res.end(JSON.stringify(reply.body));
}

const reject = (status: number, error: string, extra: Record<string, unknown> = {}, headers?: Record<string, string>): Reply => ({
  status,
  body: { error, ...extra },
  reason: error,
  ...(headers ? { headers } : {}),
});

function secretsFor(config: Config, ingress: Ingress, headers: Record<string, string>): string | null {
  switch (ingress) {
    case "github":
      return config.GITHUB_WEBHOOK_SECRET.length ? null : "GITHUB_WEBHOOK_SECRET";
    case "slack":
      return config.SLACK_SIGNING_SECRET.length ? null : "SLACK_SIGNING_SECRET";
    case "linear":
      return config.LINEAR_WEBHOOK_SECRET.length ? null : "LINEAR_WEBHOOK_SECRET";
    case "nango":
      return config.NANGO_WEBHOOK_SECRET.length ? null : "NANGO_WEBHOOK_SECRET";
    case "hookdeck":
      if (headers["x-dropbox-signature"]) return config.DROPBOX_APP_SECRET.length ? null : "DROPBOX_APP_SECRET";
      if (!config.HOOKDECK_WEBHOOK_SECRET.length) return "HOOKDECK_WEBHOOK_SECRET";
      return config.NANGO_WEBHOOK_SECRET.length ? null : "NANGO_WEBHOOK_SECRET";
  }
}

/**
 * Hookdeck, in Cloud's exact order (1a59dec route.ts + nango route handler):
 * Dropbox signature first; else Hookdeck signature (skipped for detected
 * gitlab/bitbucket/granola/shortcut); then the Nango signature is required.
 * The raw providers verify their own native signatures in Cloud handlers that
 * PR 1 does not ship, so without a Nango signature they fail closed (401).
 */
function verifyHookdeckRoute(config: Config, raw: Buffer, headers: Record<string, string>): { ok: boolean; rawProvider?: string } {
  if (headers["x-dropbox-signature"]) {
    return { ok: verifyDropbox(raw, headers, config.DROPBOX_APP_SECRET), rawProvider: "dropbox" };
  }
  const skip = detectSkipHookdeckProvider(headers, raw);
  if (!skip && !verifyHookdeck(raw, headers, config.HOOKDECK_WEBHOOK_SECRET)) return { ok: false };
  if (!hasNangoSignature(headers)) return { ok: false };
  return { ok: verifyNango(raw, headers, config.NANGO_WEBHOOK_SECRET) };
}

function verifyFor(config: Config, ingress: Ingress, raw: Buffer, headers: Record<string, string>): { ok: boolean; rawProvider?: string } {
  switch (ingress) {
    case "github":
      return { ok: verifyGitHub(raw, headers, config.GITHUB_WEBHOOK_SECRET) };
    case "slack":
      return { ok: verifySlack(raw, headers, config.SLACK_SIGNING_SECRET) };
    case "linear":
      return { ok: verifyLinear(raw, headers, config.LINEAR_WEBHOOK_SECRET) };
    case "nango":
      return { ok: verifyNango(raw, headers, config.NANGO_WEBHOOK_SECRET) };
    case "hookdeck":
      return verifyHookdeckRoute(config, raw, headers);
  }
}

function drainDiscarded(req: IncomingMessage, maxWaitMs: number): Promise<void> {
  if (req.readableEnded) return Promise.resolve();
  return new Promise<void>((resolve) => {
    const timer = setTimeout(done, maxWaitMs);
    function done() {
      clearTimeout(timer);
      resolve();
    }
    req.once("end", done);
    req.once("error", done);
    req.once("close", done);
    req.resume();
  });
}

export function unresolvedLane(provider: string, connectionId: string | null, nativeIds: NativeIds): string {
  const id = connectionId ?? nativeIds.installation_id ?? nativeIds.team_id ?? nativeIds.organization_id ?? nativeIds.cloud_id ?? nativeIds.email ?? "-";
  return `unresolved:${provider}:${id}`;
}

async function admission(deps: IngressDeps, lane: string): Promise<Reply | null> {
  const { config } = deps;
  const retryAfter = config.ADMISSION_RETRY_AFTER_SECONDS;
  const refuse = (sub: string, queued: number, retry = retryAfter): Reply => ({
    status: 429,
    body: { error: "ingest_backpressure", reason: sub, retryAfterSeconds: retry },
    headers: { "retry-after": String(retry) },
    reason: `ingest_backpressure:${sub}`,
  });
  if (config.INGRESS_PAUSED || (await deps.flags.ingressPaused())) return refuse("ingress_paused", 0);
  const laneQueued = await deps.depth.lane(lane);
  if (laneQueued >= config.LANE_MAX_QUEUED) {
    const cooled = await deps.cooldowns.remaining(lane);
    return refuse("lane_queue_full", laneQueued, Math.max(retryAfter, cooled));
  }
  const total = await deps.depth.global();
  if (total >= config.GLOBAL_MAX_QUEUED) return refuse("global_queue_full", total);
  const cooled = await deps.cooldowns.remaining(lane);
  if (cooled > 0 && (await deps.depth.oldestAgeSeconds()) >= config.MAX_QUEUE_AGE_SECONDS) {
    return refuse("lane_stale_cooldown", laneQueued, Math.max(retryAfter, cooled));
  }
  return null;
}

/** One webhook request, end to end (§2). Never throws. */
export async function handleWebhook(req: IncomingMessage, res: ServerResponse, ingress: Ingress, deps: IngressDeps, url: URL): Promise<void> {
  const requestId = ulid();
  const started = Date.now();
  let logged: Record<string, unknown> = {};
  let reply: Reply;
  try {
    reply = await processWebhook(req, ingress, deps, url, requestId, (fields) => {
      logged = { ...logged, ...fields };
    });
  } catch (error) {
    void error;
    reply = reject(503, "storage_unavailable");
  }
  const status = reply.status;
  const reason = reply.reason ?? (status < 300 ? "accepted" : "error");
  deps.metrics.inc("ingress_requests_total", { ingress, status, reason: reason.split(":")[0] ?? reason });
  if (status >= 300) {
    deps.metrics.inc("ingress_rejected_total", { ingress, reason });
    if (reason === "ingress_not_configured") deps.metrics.inc("ingress_unconfigured_total", { ingress });
    deps.logger.event("ingress_rejected", { area: "ingress", requestId, ingress, status, reason, durationMs: Date.now() - started, ...logged });
  } else if (logged.accepted) {
    deps.logger.event("ingress_accepted", { area: "ingress", requestId, ingress, durationMs: Date.now() - started, ...logged, accepted: undefined });
  }
  if (!res.headersSent) send(res, requestId, reply);
}

async function processWebhook(
  req: IncomingMessage,
  ingress: Ingress,
  deps: IngressDeps,
  url: URL,
  requestId: string,
  note: (fields: Record<string, unknown>) => void,
): Promise<Reply> {
  const { config } = deps;
  if (ingress === "hookdeck" && req.method === "GET") {
    const challenge = url.searchParams.get("challenge");
    if (challenge === null) return { status: 405, body: null, text: "Method Not Allowed", headers: { allow: "POST" }, reason: "method_not_allowed" };
    return { status: 200, body: null, text: challenge, reason: "challenge" };
  }
  if (req.method !== "POST") {
    req.resume();
    return { ...reject(405, "method_not_allowed"), headers: { allow: ingress === "hookdeck" ? "GET, POST" : "POST" } };
  }
  if (!deps.entitlement.isEntitled()) {
    req.resume();
    return reject(403, "feature_not_licensed", { feature: "relayfile" });
  }
  const headers = headerRecord(req);
  const missing = secretsFor(config, ingress, headers);
  if (missing) {
    req.resume();
    return reject(503, "ingress_not_configured");
  }

  let raw: Buffer;
  try {
    raw = await readCappedBody(req, ingress === "github" ? config.INGRESS_MAX_BODY_BYTES : config.INGRESS_MAX_BODY_BYTES_OTHER);
  } catch (error) {
    if (error instanceof BodyTooLargeError) {
      // The rest of the body is discarded as it arrives (never buffered). Wait
      // briefly for the sender to finish so it can read the 413 instead of a
      // reset, then close the connection.
      await drainDiscarded(req, 5000);
      return { ...reject(413, "payload_too_large"), headers: { connection: "close" } };
    }
    return reject(400, "invalid_body");
  }

  const verified = verifyFor(config, ingress, raw, headers);
  if (!verified.ok) return reject(401, "invalid_signature");

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw.toString("utf8"));
  } catch {
    return reject(400, "invalid_json");
  }

  if (ingress === "slack" && parsed && typeof parsed === "object" && (parsed as { type?: unknown }).type === "url_verification") {
    return { status: 200, body: { challenge: (parsed as { challenge?: unknown }).challenge ?? null }, reason: "url_verification" };
  }
  if (ingress === "linear" && !linearTimestampFresh(parsed)) return reject(401, "invalid_signature");

  const classified = classify({ ingress, headers, body: parsed, ...(verified.rawProvider ? { rawProvider: verified.rawProvider } : {}) });
  if (classified.drop) {
    deps.metrics.inc("ingest_dropped_total", { ingress, reason: classified.reason });
    deps.logger.event("ingress_dropped", { area: "ingress", requestId, ingress, reason: classified.reason, connectionId: classified.connectionId ?? undefined });
    return { status: 202, body: { accepted: true, dropped: true, reason: classified.reason }, reason: classified.reason };
  }

  let lane: string;
  try {
    if (ingress === "github" && classified.nativeIds.installation_id) {
      const known = await deps.bindings.knownInstallation(classified.nativeIds.installation_id);
      if (!known) {
        deps.metrics.inc("ingress_unknown_installation_total", { ingress });
        if (!config.INGRESS_REJECT_UNKNOWN_INSTALLATION) {
          return { status: 202, body: { accepted: true, ignored: true, reason: "unknown_installation" }, reason: "unknown_installation" };
        }
        return reject(404, "unknown_installation");
      }
    }
    lane = (await deps.bindings.lane(classified.connectionId, classified.nativeIds)) ??
      unresolvedLane(classified.provider, classified.connectionId, classified.nativeIds);
    note({ laneHash: laneHash(lane), provider: classified.provider, kind: classified.kind });
    const refused = await admission(deps, lane);
    if (refused) return refused;
  } catch {
    throw new StorageError();
  }

  let result;
  try {
    result = await insertEnvelope(deps.db, {
      kind: classified.kind,
      ingress,
      provider: classified.provider,
      requestId,
      providerDeliveryId: classified.deliveryId,
      eventType: classified.eventType,
      connectionId: classified.connectionId,
      nativeIds: classified.nativeIds,
      headers: allowListHeaders(headers),
      raw,
      parsed,
      receivedAt: new Date(),
      lane,
      workspaceId: lane.startsWith("rw_") ? lane : null,
      inlineMaxBytes: config.INLINE_PAYLOAD_MAX_BYTES,
    });
  } catch {
    throw new StorageError();
  }
  if (result.duplicate) {
    deps.metrics.inc("claims_duplicates_total", { surface: `ingress:${ingress}` });
    return { status: 200, body: { accepted: true, duplicate: true }, reason: "duplicate" };
  }
  note({ accepted: true, bytes: raw.length, storage: result.inline ? "inline" : "blob" });
  return { status: 202, body: { accepted: true, requestId, queue: "webhook-events" }, reason: "accepted" };
}
