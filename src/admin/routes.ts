import { timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import { z } from "zod";
import type { Db } from "../db/db.ts";
import { deleteBinding, InvalidWorkspaceIdError, listBindings, upsertBinding } from "../bindings/store.ts";
import type { BindingLaneCache } from "../bindings/store.ts";
import type { Logger } from "../observability/log.ts";
import type { CooldownStore } from "../queue/cooldown.ts";
import { listArchive, replayArchive } from "../queue/dlq.ts";
import type { FlagStore } from "../queue/flags.ts";
import type { QueueDepth } from "../queue/store.ts";
import { BodyTooLargeError, readCappedBody } from "../ingress/body.ts";

export type AdminDeps = {
  db: Db;
  tokens: readonly string[];
  logger: Logger;
  cooldowns: CooldownStore;
  flags: FlagStore;
  depth: QueueDepth;
  bindings: BindingLaneCache;
};

/** §9: constant-time bearer compare; a comma list allows rotation. */
export function adminAuthorized(header: string | undefined, tokens: readonly string[]): boolean {
  if (!header || tokens.length === 0) return false;
  const match = /^Bearer (.+)$/.exec(header.trim());
  if (!match) return false;
  const given = Buffer.from(match[1] ?? "");
  let ok = false;
  for (const token of tokens) {
    const want = Buffer.from(token);
    if (want.length === given.length && timingSafeEqual(want, given)) ok = true;
  }
  return ok;
}

const json = (res: ServerResponse, status: number, body: unknown) => {
  res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" });
  res.end(JSON.stringify(body));
};

const NativeIdsSchema = z
  .object({
    installation_id: z.string().max(128).optional(),
    team_id: z.string().max(128).optional(),
    enterprise_id: z.string().max(128).optional(),
    organization_id: z.string().max(128).optional(),
    cloud_id: z.string().max(128).optional(),
    email: z.string().max(320).optional(),
  })
  .strict();

const BindingSchema = z
  .object({
    workspaceId: z.string().max(140),
    provider: z.string().min(1).max(64),
    connectionId: z.string().min(1).max(256),
    providerConfigKey: z.string().max(128).nullable().optional(),
    nativeIds: NativeIdsSchema.optional(),
    relayfileBaseUrl: z.string().url().nullable().optional(),
    status: z.enum(["active", "disabled"]).optional(),
  })
  .strict();

const ReplaySchema = z
  .object({
    ids: z.array(z.string().max(64)).max(10_000).optional(),
    lane: z.string().max(256).optional(),
    provider: z.string().max(64).optional(),
    since: z.string().datetime().optional(),
    limit: z.number().int().min(1).max(10_000).optional(),
  })
  .strict();

class InvalidQueryError extends Error {}

/** `limit` query param: absent → default; otherwise an integer in [1, max], else 400. */
function limitParam(url: URL, def = 100, max = 1000): number {
  const raw = url.searchParams.get("limit");
  if (raw === null || raw === "") return def;
  if (!/^\d+$/.test(raw)) throw new InvalidQueryError();
  const n = Number(raw);
  if (n < 1 || n > max) throw new InvalidQueryError();
  return n;
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  const raw = await readCappedBody(req, 1024 * 1024);
  return raw.length ? JSON.parse(raw.toString("utf8")) : {};
}

/** Returns true when the route was handled. Mounted only on the internal listener. */
export async function handleAdmin(req: IncomingMessage, res: ServerResponse, url: URL, deps: AdminDeps): Promise<boolean> {
  if (!url.pathname.startsWith("/api/v1/admin/")) return false;
  if (!adminAuthorized(req.headers.authorization, deps.tokens)) {
    req.resume();
    json(res, 401, { error: "unauthorized" });
    return true;
  }
  const route = `${req.method} ${url.pathname}`;
  try {
    switch (route) {
      case "POST /api/v1/admin/bindings": {
        const parsed = BindingSchema.safeParse(await readJson(req));
        if (!parsed.success) return json(res, 400, { error: "invalid_request", fields: parsed.error.issues.map((i) => i.path.join(".")) }), true;
        const binding = await upsertBinding(deps.db, parsed.data);
        deps.bindings.clear();
        json(res, 200, { binding });
        return true;
      }
      case "GET /api/v1/admin/bindings": {
        const bindings = await listBindings(deps.db, {
          ...(url.searchParams.get("workspaceId") ? { workspaceId: url.searchParams.get("workspaceId")! } : {}),
          ...(url.searchParams.get("provider") ? { provider: url.searchParams.get("provider")! } : {}),
          limit: limitParam(url),
        });
        json(res, 200, { bindings });
        return true;
      }
      case "DELETE /api/v1/admin/bindings": {
        const provider = url.searchParams.get("provider");
        const connectionId = url.searchParams.get("connectionId");
        if (!provider || !connectionId) return json(res, 400, { error: "invalid_request" }), true;
        const deleted = await deleteBinding(deps.db, provider, connectionId);
        deps.bindings.clear();
        json(res, deleted ? 200 : 404, { deleted });
        return true;
      }
      case "GET /api/v1/admin/dlq":
      case "POST /api/v1/admin/dlq/list": {
        const rows = await listArchive(deps.db, {
          ...(url.searchParams.get("lane") ? { lane: url.searchParams.get("lane")! } : {}),
          ...(url.searchParams.get("provider") ? { provider: url.searchParams.get("provider")! } : {}),
          limit: limitParam(url),
        });
        json(res, 200, { rows });
        return true;
      }
      case "POST /api/v1/admin/dlq/replay": {
        const parsed = ReplaySchema.safeParse(await readJson(req));
        if (!parsed.success || (!parsed.data.ids?.length && !parsed.data.lane && !parsed.data.provider)) {
          return json(res, 400, { error: "invalid_request" }), true;
        }
        // Explicit ids are replayed in full: the limit defaults to their count.
        const selector = parsed.data.ids?.length && parsed.data.limit === undefined ? { ...parsed.data, limit: parsed.data.ids.length } : parsed.data;
        const ids = await replayArchive(deps.db, selector, "admin");
        deps.logger.event("dlq_replay", { area: "admin", count: ids.length });
        json(res, 200, { replayed: ids.length, ids });
        return true;
      }
      case "GET /api/v1/admin/cooldowns":
        json(res, 200, { cooldowns: await deps.cooldowns.list() });
        return true;
      case "DELETE /api/v1/admin/cooldowns": {
        const lane = url.searchParams.get("lane");
        if (!lane) return json(res, 400, { error: "invalid_request" }), true;
        json(res, 200, { cleared: await deps.cooldowns.clear(lane) });
        return true;
      }
      case "GET /api/v1/admin/queue/stats": {
        deps.depth.invalidate();
        json(res, 200, {
          queued: await deps.depth.global(),
          oldestAgeSeconds: await deps.depth.oldestAgeSeconds(),
          topLanes: await deps.depth.topLanes(20),
          ingressPaused: await deps.flags.ingressPaused(),
        });
        return true;
      }
      case "POST /api/v1/admin/ingress/pause":
      case "POST /api/v1/admin/ingress/resume": {
        const paused = url.pathname.endsWith("/pause");
        await deps.flags.setIngressPaused(paused, "admin");
        deps.logger.event("ingress_pause_changed", { area: "admin", paused });
        json(res, 200, { ingressPaused: paused });
        return true;
      }
      default:
        req.resume();
        json(res, 404, { error: "not_found" });
        return true;
    }
  } catch (error) {
    if (error instanceof InvalidWorkspaceIdError) return json(res, 400, { error: "invalid_workspace_id" }), true;
    if (error instanceof BodyTooLargeError) return json(res, 413, { error: "payload_too_large" }), true;
    if (error instanceof SyntaxError) return json(res, 400, { error: "invalid_json" }), true;
    if (error instanceof InvalidQueryError) return json(res, 400, { error: "invalid_request" }), true;
    deps.logger.event("admin_error", { area: "admin", route, errorName: error instanceof Error ? error.name : typeof error });
    json(res, 503, { error: "storage_unavailable" });
    return true;
  }
}
