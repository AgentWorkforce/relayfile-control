import type { IncomingMessage, RequestListener, ServerResponse } from "node:http";
import type { Config } from "./config.ts";
import type { Db } from "./db/db.ts";
import { migrationStatus } from "./db/migrate.ts";
import { BindingLaneCache } from "./bindings/store.ts";
import type { Entitlement } from "./entitlement/entitlement.ts";
import type { Ingress } from "./envelope/model.ts";
import { handleWebhook, type IngressDeps } from "./ingress/handler.ts";
import { handleAdmin } from "./admin/routes.ts";
import type { Logger } from "./observability/log.ts";
import { Metrics } from "./observability/metrics.ts";
import { CooldownStore } from "./queue/cooldown.ts";
import { FlagStore } from "./queue/flags.ts";
import { QueueDepth } from "./queue/store.ts";

export type AppDeps = { config: Config; db: Db; logger: Logger; entitlement: Entitlement; metrics?: Metrics; migrationsDir?: string };

export type App = {
  publicHandler: RequestListener;
  adminHandler: RequestListener;
  metrics: Metrics;
  cooldowns: CooldownStore;
  depth: QueueDepth;
  flags: FlagStore;
  bindings: BindingLaneCache;
};

const WEBHOOK_ROUTES: Record<string, Ingress> = {
  "/api/v1/webhooks/github": "github",
  "/api/v1/webhooks/nango": "nango",
  "/api/v1/webhooks/hookdeck": "hookdeck",
  "/api/v1/webhooks/slack": "slack",
  "/api/v1/webhooks/linear": "linear",
};

export function webhookIngressFor(pathname: string): Ingress | null {
  const path = pathname.replace(/\/+$/, "");
  // `/cloud/api/v1/webhooks/*` aliases so Cloud registrations can be repointed by host only (§12).
  const normalized = path.startsWith("/cloud/") ? path.slice("/cloud".length) : path;
  return WEBHOOK_ROUTES[normalized] ?? null;
}

const json = (res: ServerResponse, status: number, body: unknown) => {
  res.writeHead(status, { "content-type": "application/json", "cache-control": "no-store" });
  res.end(JSON.stringify(body));
};

export function createApp(deps: AppDeps): App {
  const metrics = deps.metrics ?? new Metrics();
  const cooldowns = new CooldownStore(deps.db, { enabled: deps.config.COOLDOWN_ENABLED, maxKeys: deps.config.COOLDOWN_MAX_KEYS });
  const depth = new QueueDepth(deps.db);
  const flags = new FlagStore(deps.db);
  const bindings = new BindingLaneCache(deps.db);
  const ingressDeps: IngressDeps = { config: deps.config, db: deps.db, logger: deps.logger, metrics, entitlement: deps.entitlement, depth, cooldowns, bindings, flags };

  async function readyz(res: ServerResponse): Promise<void> {
    let db = false;
    let migrations: { current: string | null; latest: string | null; upToDate: boolean } = { current: null, latest: null, upToDate: false };
    try {
      await deps.db.query("SELECT 1");
      db = true;
      migrations = await migrationStatus(deps.db, deps.migrationsDir);
    } catch {
      db = false;
    }
    let paused = deps.config.INGRESS_PAUSED;
    try {
      paused = paused || (await flags.ingressPaused());
    } catch {
      // reported via db=false
    }
    // Ruling 7: only DB / migration failure makes the pod unready; entitlement is reported, not gating.
    const ready = db && migrations.upToDate;
    json(res, ready ? 200 : 503, {
      ready,
      db,
      migration: migrations,
      entitlement: deps.entitlement.status(),
      ingressPaused: paused,
      role: deps.config.ROLE,
    });
  }

  const common = async (req: IncomingMessage, res: ServerResponse, url: URL): Promise<boolean> => {
    if (req.method === "GET" && url.pathname === "/healthz") return json(res, 200, { ok: true }), true;
    if (req.method === "GET" && url.pathname === "/readyz") return await readyz(res), true;
    return false;
  };

  const publicHandler: RequestListener = (req, res) => {
    void (async () => {
      const url = new URL(req.url ?? "/", "http://localhost");
      if (await common(req, res, url)) return;
      const ingress = deps.config.ROLE === "consumer" ? null : webhookIngressFor(url.pathname);
      if (ingress) return handleWebhook(req, res, ingress, ingressDeps, url);
      req.resume();
      json(res, 404, { error: "not_found" });
    })().catch(() => {
      if (!res.headersSent) json(res, 500, { error: "internal_error" });
    });
  };

  const adminHandler: RequestListener = (req, res) => {
    void (async () => {
      const url = new URL(req.url ?? "/", "http://localhost");
      if (await common(req, res, url)) return;
      if (req.method === "GET" && url.pathname === "/metrics") {
        res.writeHead(200, { "content-type": "text/plain; version=0.0.4" });
        res.end(metrics.render());
        return;
      }
      if (await handleAdmin(req, res, url, { db: deps.db, tokens: deps.config.RELAYFILE_CONTROL_ADMIN_TOKEN, logger: deps.logger, cooldowns, flags, depth, bindings })) return;
      req.resume();
      json(res, 404, { error: "not_found" });
    })().catch(() => {
      if (!res.headersSent) json(res, 500, { error: "internal_error" });
    });
  };

  return { publicHandler, adminHandler, metrics, cooldowns, depth, flags, bindings };
}
