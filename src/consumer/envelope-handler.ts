import type { Config } from "../config.ts";
import type { Db } from "../db/db.ts";
import { resolveBindings, upsertPendingBinding, type Binding } from "../bindings/store.ts";
import { providerFamily } from "../envelope/classify.ts";
import { dedupeKey, relayfileEnvelopeId, type EnvelopeRow } from "../envelope/model.ts";
import { laneHash, type Logger } from "../observability/log.ts";
import type { Metrics } from "../observability/metrics.ts";
import { claim, completeClaim, failClaim } from "../queue/claims.ts";
import { readPayload, relane } from "../queue/store.ts";
import { type RelayfileClient, TerminalHandlerError, type WebhookEnvelopeBody } from "../relayfile/client.ts";
import { envelopePolicy } from "./policy.ts";

export type HandlerOutcome =
  | { type: "ack"; reason: string; relayfileStatus?: number; relayfileOutcome?: string; relayfileMs?: number; payloadReadMs?: number }
  | { type: "fanout"; workspaceIds: string[]; reason: string };

/** A retryable condition that is not a Relayfile error (e.g. a pending binding). */
export class RetryableHandlerError extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "RetryableHandlerError";
  }
}

export type HandlerContext = {
  db: Db;
  config: Config;
  logger: Logger;
  metrics: Metrics;
  relayfile: RelayfileClient | null;
  worker: string;
  signal: AbortSignal;
};

export type EnvelopeHandler = (row: EnvelopeRow, ctx: HandlerContext) => Promise<HandlerOutcome>;

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

const seg = (value: string) => value.replace(/[^A-Za-z0-9._-]/g, "_").slice(0, 120) || "_";

/**
 * PR 1 default file path for an envelope (agreed with layered-control; PR 2
 * handlers replace these with canonical per-provider paths). Slack must sit
 * under `teams/` on OSS.
 */
export function defaultEnvelopePath(row: Pick<EnvelopeRow, "provider" | "event_type" | "delivery_id" | "native_ids">): string {
  const family = seg(providerFamily(row.provider));
  if (family === "slack") {
    return `/slack/teams/${seg(row.native_ids.team_id ?? "unknown")}/events/${seg(row.delivery_id)}.json`;
  }
  return `/${family}/webhooks/${seg(row.event_type ?? "event")}/${seg(row.delivery_id)}.json`;
}

const PROVIDER_EVENT_TYPE = /^[a-z_]{1,60}(\.[a-z_]{1,60})?$/;

/** The provider object carried as file content: the Nango `payload` for Nango envelopes. */
function providerObject(row: EnvelopeRow, payload: unknown): unknown {
  if ((row.kind === "nango-forward" || row.kind === "nango-sync") && isObject(payload) && isObject(payload.payload) && typeof payload.type === "string") {
    return payload.payload;
  }
  return payload;
}

export function buildEnvelopeBody(row: EnvelopeRow, workspaceId: string, payload: unknown): WebhookEnvelopeBody {
  const path = defaultEnvelopePath(row);
  const eventType = row.event_type ?? "";
  const body: WebhookEnvelopeBody = {
    // HARD (layered-relayfile): envelopeId is always derived with the stable deliveryId.
    envelopeId: relayfileEnvelopeId({ provider: row.provider, workspaceId, deliveryId: row.delivery_id, path: "", providerEventType: eventType, eventType }),
    workspaceId,
    provider: row.provider,
    deliveryId: row.delivery_id,
    receivedAt: new Date(row.received_at).toISOString(),
    correlationId: row.request_id,
    payload: {
      event_type: "file.created",
      path,
      content: JSON.stringify(providerObject(row, payload)),
      contentType: "application/json",
      ...(PROVIDER_EVENT_TYPE.test(eventType) ? { providerEventType: eventType } : {}),
    },
  };
  if (Object.keys(row.headers).length > 0) body.headers = row.headers;
  return body;
}

function baseUrlFor(binding: Binding | undefined, config: Config): string | null {
  return binding?.relayfile_base_url ?? config.RELAYFILE_BASE_URL ?? null;
}

/**
 * PR 1 handler: resolve the workspace (§5), then deliver the envelope to
 * Relayfile `/v1/internal/webhook-envelopes` (§6.1). Provider-writes and the
 * record writer are PR 2.
 */
export const forwardEnvelope: EnvelopeHandler = async (row, ctx) => {
  if (row.kind === "nango-auth") {
    if (row.connection_id) await upsertPendingBinding(ctx.db, row.provider, row.connection_id, null);
    return { type: "ack", reason: "nango_auth_recorded" };
  }

  let workspaceId = row.workspace_id;
  let binding: Binding | undefined;
  const resolution = await resolveBindings(ctx.db, { connectionId: row.connection_id, nativeIds: row.native_ids });
  if (workspaceId) {
    binding = resolution.kind === "matched" ? resolution.bindings.find((b) => b.workspace_id === workspaceId) : undefined;
  } else {
    if (resolution.kind === "pending") throw new RetryableHandlerError("binding_pending");
    if (resolution.kind === "unmatched") return { type: "ack", reason: "unmatched_workspace" };
    const workspaces = [...new Set(resolution.bindings.map((b) => b.workspace_id).filter((w): w is string => Boolean(w)))];
    if (workspaces.length > 1) {
      if (row.ingress === "github" && resolution.via === "native") {
        return { type: "fanout", workspaceIds: workspaces, reason: "installation_fanout" };
      }
      ctx.logger.event("ambiguous_binding", { area: "consumer", requestId: row.request_id, provider: row.provider, candidates: workspaces.length });
      ctx.metrics.inc("ambiguous_binding_total", { provider: providerFamily(row.provider) });
    }
    workspaceId = workspaces[0] ?? null;
    if (!workspaceId) return { type: "ack", reason: "unmatched_workspace" };
    binding = resolution.bindings.find((b) => b.workspace_id === workspaceId);
    if (!(await relane(ctx.db, row.id, ctx.worker, workspaceId, workspaceId))) throw new RetryableHandlerError("lease_lost");
  }

  const baseUrl = baseUrlFor(binding, ctx.config);
  if (!baseUrl || !ctx.relayfile) throw new RetryableHandlerError("relayfile_not_configured");

  const claimSurface = row.kind === "nango-forward" ? "nango-forward" : null;
  const claimId = claimSurface ? dedupeKey([row.request_id, workspaceId]) : null;
  if (claimSurface && claimId) {
    const claimed = await claim(ctx.db, { surface: claimSurface, dedupeId: claimId, leaseSeconds: ctx.config.CLAIM_LEASE_SECONDS, envelopeId: row.id });
    if (!claimed.claimed) {
      ctx.metrics.inc("claims_duplicates_total", { surface: claimSurface });
      return { type: "ack", reason: "duplicate" };
    }
  }

  try {
    const readStarted = Date.now();
    const payload = await readPayload(ctx.db, row, ctx.config.PAYLOAD_READ_TIMEOUT_MS);
    const payloadReadMs = Date.now() - readStarted;
    const body = buildEnvelopeBody(row, workspaceId, payload);
    const started = Date.now();
    let result;
    try {
      result = await ctx.relayfile.postEnvelope(baseUrl, body, {
        policy: envelopePolicy(row.provider),
        signal: ctx.signal,
        onBackpressure: (event) => {
          ctx.metrics.inc("relayfile_backpressure_total", { surface: "webhook-envelopes", status: event.status, reason: event.relayfileReason ?? "none" });
          ctx.logger.event("relayfile_backpressure", {
            area: "relayfile",
            requestId: row.request_id,
            laneHash: laneHash(workspaceId!),
            provider: row.provider,
            surface: "webhook-envelopes",
            status: event.status,
            relayfileReason: event.relayfileReason,
            retryAfterMs: event.retryAfterMs,
            attempt: event.attempt,
            budgetRemainingMs: event.budgetRemainingMs,
          });
        },
      });
    } finally {
      ctx.metrics.observe("relayfile_request_seconds", (Date.now() - started) / 1000, { surface: "webhook-envelopes" });
    }
    if (claimSurface && claimId) await completeClaim(ctx.db, claimSurface, claimId);
    return {
      type: "ack",
      reason: result.outcome === "duplicate" ? "relayfile_duplicate_envelope" : `relayfile_${result.outcome}`,
      relayfileStatus: result.status,
      relayfileOutcome: result.outcome,
      relayfileMs: Date.now() - started,
      payloadReadMs,
    };
  } catch (error) {
    if (claimSurface && claimId) await failClaim(ctx.db, claimSurface, claimId).catch(() => undefined);
    if (error instanceof TerminalHandlerError && error.code === "relayfile_payload_too_large") {
      // ingress accepts github up to 25 MiB; OSS caps the whole envelope at 1 MiB (layered-control ask).
      ctx.metrics.inc("relayfile_envelope_too_large_total", { provider: providerFamily(row.provider), ingress: row.ingress });
      ctx.logger.event("relayfile_envelope_too_large", {
        area: "relayfile",
        requestId: row.request_id,
        provider: row.provider,
        ingress: row.ingress,
        payloadBytes: row.payload_bytes,
        reason: "relayfile_payload_too_large",
      });
    }
    throw error;
  }
};
