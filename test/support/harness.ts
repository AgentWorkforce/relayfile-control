import { createHash, createHmac, randomBytes } from "node:crypto";
import { createServer, type IncomingMessage, type RequestListener, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { PGlite } from "@electric-sql/pglite";
import { createApp, type App } from "../../src/app.ts";
import { type Config, testConfig } from "../../src/config.ts";
import { type Db, pgDb, pgliteDb } from "../../src/db/db.ts";
import { migrate } from "../../src/db/migrate.ts";
import { StaticEntitlement } from "../../src/entitlement/entitlement.ts";
import { MemoryLogger } from "../../src/observability/log.ts";

/**
 * Fixture secrets. Random per process, never real: nothing here can verify a
 * real provider delivery.
 */
export const FIXTURE = {
  github: `fixture-gh-${randomBytes(6).toString("hex")}`,
  nango: `fixture-nango-${randomBytes(6).toString("hex")}`,
  hookdeckLegacy: `fixture-hd-${randomBytes(6).toString("hex")}`,
  hookdeckStandard: `whsec_${randomBytes(24).toString("base64")}`,
  dropbox: `fixture-dbx-${randomBytes(6).toString("hex")}`,
  slack: `fixture-slack-${randomBytes(6).toString("hex")}`,
  linear: `fixture-linear-${randomBytes(6).toString("hex")}`,
  relayHmac: `fixture-relay-${randomBytes(12).toString("hex")}`,
  admin: `fixture-admin-${randomBytes(12).toString("hex")}`,
};

export function fixtureConfig(overrides: Partial<Record<keyof Config, string>> = {}): Config {
  return testConfig({
    GITHUB_WEBHOOK_SECRET: FIXTURE.github,
    NANGO_WEBHOOK_SECRET: FIXTURE.nango,
    HOOKDECK_WEBHOOK_SECRET: `${FIXTURE.hookdeckLegacy},${FIXTURE.hookdeckStandard}`,
    DROPBOX_APP_SECRET: FIXTURE.dropbox,
    SLACK_SIGNING_SECRET: FIXTURE.slack,
    LINEAR_WEBHOOK_SECRET: FIXTURE.linear,
    RELAYFILE_INTERNAL_HMAC_SECRET: FIXTURE.relayHmac,
    RELAYFILE_CONTROL_ADMIN_TOKEN: FIXTURE.admin,
    CONSUMER_IDLE_POLL_MS: "20",
    ...overrides,
  });
}

export async function newPglite(): Promise<Db> {
  const db = pgliteDb(new PGlite() as never);
  await migrate(db);
  return db;
}

/** Real Postgres for opt-in concurrency tests: a fresh schema per call. */
export async function newRealPg(url: string, max = 30): Promise<{ db: Db; drop: () => Promise<void> }> {
  const schema = `t_${randomBytes(6).toString("hex")}`;
  const admin = pgDb(url, { max: 1 });
  await admin.exec(`CREATE SCHEMA ${schema}`);
  await admin.close();
  const sep = url.includes("?") ? "&" : "?";
  const db = pgDb(`${url}${sep}options=${encodeURIComponent(`-c search_path=${schema}`)}`, { max });
  await migrate(db);
  return {
    db,
    drop: async () => {
      await db.close();
      const a = pgDb(url, { max: 1 });
      await a.exec(`DROP SCHEMA ${schema} CASCADE`);
      await a.close();
    },
  };
}

// ---------------------------------------------------------------- signing helpers

export const sign = {
  github: (body: string, secret = FIXTURE.github) => ({ "x-hub-signature-256": `sha256=${createHmac("sha256", secret).update(body).digest("hex")}` }),
  nango: (body: string, secret = FIXTURE.nango) => ({ "x-nango-hmac-sha256": createHmac("sha256", secret).update(body).digest("hex") }),
  nangoLegacy: (body: string, secret = FIXTURE.nango) => ({ "x-nango-signature": createHash("sha256").update(secret + body).digest("hex") }),
  hookdeckLegacy: (body: string, secret = FIXTURE.hookdeckLegacy) => ({ "x-hookdeck-signature": createHmac("sha256", secret).update(body).digest("base64") }),
  hookdeckStandard: (body: string, id = "msg_fixture", ts = Math.floor(Date.now() / 1000), secret = FIXTURE.hookdeckStandard) => ({
    "webhook-id": id,
    "webhook-timestamp": String(ts),
    "webhook-signature": `v1,${createHmac("sha256", Buffer.from(secret.slice(6), "base64")).update(`${id}.${ts}.${body}`).digest("base64")}`,
  }),
  dropbox: (body: string, secret = FIXTURE.dropbox) => ({ "x-dropbox-signature": createHmac("sha256", secret).update(body).digest("hex") }),
  slack: (body: string, ts = Math.floor(Date.now() / 1000), secret = FIXTURE.slack) => ({
    "x-slack-request-timestamp": String(ts),
    "x-slack-signature": `v0=${createHmac("sha256", secret).update(`v0:${ts}:${body}`).digest("hex")}`,
  }),
  linear: (body: string, secret = FIXTURE.linear) => ({ "linear-signature": createHmac("sha256", secret).update(body).digest("hex") }),
};

// ---------------------------------------------------------------- servers

export async function listen(handler: RequestListener): Promise<{ url: string; server: Server; close: () => Promise<void> }> {
  const server = createServer(handler);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", () => resolve()));
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    server,
    close: () => new Promise<void>((resolve) => {
      server.closeAllConnections();
      server.close(() => resolve());
    }),
  };
}

export type TestApp = {
  app: App;
  db: Db;
  config: Config;
  logger: MemoryLogger;
  entitlement: StaticEntitlement;
  url: string;
  adminUrl: string;
  close: () => Promise<void>;
  post: (path: string, body: string, headers?: Record<string, string>) => Promise<{ status: number; json: any; headers: Headers; text: string }>;
};

export async function startApp(opts: { db?: Db; config?: Config } = {}): Promise<TestApp> {
  const db = opts.db ?? (await newPglite());
  const config = opts.config ?? fixtureConfig();
  const logger = new MemoryLogger();
  const entitlement = new StaticEntitlement(true);
  const app = createApp({ config, db, logger, entitlement });
  const pub = await listen(app.publicHandler);
  const adm = await listen(app.adminHandler);
  return {
    app,
    db,
    config,
    logger,
    entitlement,
    url: pub.url,
    adminUrl: adm.url,
    close: async () => {
      await pub.close();
      await adm.close();
    },
    post: async (path, body, headers = {}) => {
      const res = await fetch(pub.url + path, { method: "POST", body, headers: { "content-type": "application/json", ...headers } });
      const text = await res.text();
      let json: any = null;
      try {
        json = JSON.parse(text);
      } catch {
        json = null;
      }
      return { status: res.status, json, headers: res.headers, text };
    },
  };
}

// ---------------------------------------------------------------- fake Relayfile

export type FakeRelayfileRequest = { workspaceId: string; body: any; headers: Record<string, string>; at: number };
export type FakeResponse = { status: number; headers?: Record<string, string>; body?: unknown };

/**
 * In-process Relayfile `/v1/internal/webhook-envelopes`. Verifies the §6.1
 * signature and timestamp, rejects an exact replay (same signature twice),
 * records every request, and answers via the scripted `respond`.
 */
export async function fakeRelayfile(opts: {
  secret?: string;
  flavor?: "oss" | "cloud";
  respond?: (req: FakeRelayfileRequest, n: number) => FakeResponse;
  maxBodyBytes?: number;
}) {
  const secret = opts.secret ?? FIXTURE.relayHmac;
  const seen = new Set<string>();
  const requests: FakeRelayfileRequest[] = [];
  const rejected: { reason: string }[] = [];
  const stored = new Map<string, any>();
  let n = 0;
  const readBody = (req: IncomingMessage) =>
    new Promise<string>((resolve) => {
      const chunks: Buffer[] = [];
      req.on("data", (c: Buffer) => chunks.push(c));
      req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    });
  const server = await listen(async (req, res) => {
    const raw = await readBody(req);
    const headers = Object.fromEntries(Object.entries(req.headers).map(([k, v]) => [k, String(v)]));
    const reply = (r: FakeResponse) => {
      res.writeHead(r.status, { "content-type": "application/json", ...r.headers });
      res.end(r.body === undefined ? "" : JSON.stringify(r.body));
    };
    if (req.method !== "POST" || req.url !== "/v1/internal/webhook-envelopes") return reply({ status: 404, body: { error: "not_found" } });
    const ts = headers["x-relay-timestamp"] ?? "";
    const sig = headers["x-relay-signature"] ?? "";
    const expected = createHmac("sha256", secret).update(`${ts}\n${raw}`).digest("hex");
    if (sig !== expected) {
      rejected.push({ reason: "bad_signature" });
      return reply({ status: 401, body: { error: "invalid_signature" } });
    }
    if (Math.abs(Date.now() - Date.parse(ts)) > 300_000) {
      rejected.push({ reason: "stale_timestamp" });
      return reply({ status: 401, body: { error: "stale_timestamp" } });
    }
    if (seen.has(sig)) {
      rejected.push({ reason: "replay" });
      return reply({ status: 401, body: { error: "replay" } });
    }
    seen.add(sig);
    if (!headers["x-correlation-id"]) return reply({ status: 400, body: { error: "missing_correlation_id" } });
    if (opts.maxBodyBytes !== undefined && Buffer.byteLength(raw) > opts.maxBodyBytes) {
      return reply({ status: 413, body: { error: "payload_too_large" } });
    }
    const body = JSON.parse(raw);
    const allowed = new Set(["envelopeId", "workspaceId", "provider", "deliveryId", "receivedAt", "correlationId", "payload", "headers"]);
    if (Object.keys(body).some((k) => !allowed.has(k))) return reply({ status: 400, body: { error: "additional_properties" } });
    const record: FakeRelayfileRequest = { workspaceId: body.workspaceId, body, headers, at: Date.now() };
    requests.push(record);
    n += 1;
    const scripted = opts.respond?.(record, n) ?? { status: 202 };
    if (scripted.status === 202) {
      const key = JSON.stringify([body.workspaceId, body.provider, body.deliveryId]);
      if (stored.has(key)) return reply({ status: 409, body: { error: "duplicate_envelope" } });
      stored.set(key, body);
      return reply({ status: 202, body: { status: "queued", id: body.envelopeId } });
    }
    return reply(scripted);
  });
  return { url: server.url, requests, rejected, stored, close: server.close };
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Make every queued row visible now (skips the ≥60 s retry delay in tests). */
export async function makeVisible(db: Db): Promise<void> {
  await db.query("UPDATE webhook_envelopes SET visible_at = now() WHERE state = 'queued'");
}
