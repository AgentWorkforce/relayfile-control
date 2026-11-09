import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { upsertBinding } from "../src/bindings/store.ts";
import { createApp } from "../src/app.ts";
import { RefreshingEntitlement } from "../src/entitlement/entitlement.ts";
import { MemoryLogger } from "../src/observability/log.ts";
import { fixtureConfig, listen, newPglite, sign, startApp, type TestApp } from "./support/harness.ts";

const gh = (body: object, delivery = `d-${Math.random()}`, extra: Record<string, string> = {}) => {
  const raw = JSON.stringify(body);
  return { raw, headers: { ...sign.github(raw), "x-github-delivery": delivery, "x-github-event": "issues", ...extra } };
};

describe("ingress routes and response contract (§2)", () => {
  let t: TestApp;
  before(async () => {
    t = await startApp();
    await upsertBinding(t.db, { workspaceId: "rw_one", provider: "github", connectionId: "gh-42", nativeIds: { installation_id: "42" } });
    await upsertBinding(t.db, { workspaceId: "rw_slack", provider: "slack", connectionId: "sl-1", nativeIds: { team_id: "T1" } });
  });
  after(async () => t.close());

  it("github: 202 accepted with requestId, request-id header, lane resolved at ingress", async () => {
    const { raw, headers } = gh({ action: "opened", installation: { id: 42 } }, "gh-1");
    const r = await t.post("/api/v1/webhooks/github", raw, headers);
    assert.equal(r.status, 202);
    assert.deepEqual(Object.keys(r.json).sort(), ["accepted", "queue", "requestId"]);
    assert.equal(r.json.queue, "webhook-events");
    assert.equal(r.headers.get("x-relayfile-control-request-id"), r.json.requestId);
    const { rows } = await t.db.query<{ lane: string; delivery_id: string; workspace_id: string }>(
      "SELECT lane, delivery_id, workspace_id FROM webhook_envelopes WHERE request_id = $1",
      [r.json.requestId],
    );
    assert.deepEqual(rows[0], { lane: "rw_one", delivery_id: "github:gh-1", workspace_id: "rw_one" });
    const accepted = t.logger.named("ingress_accepted").at(-1)!;
    assert.equal(accepted.kind, "provider-webhook");
    assert.equal(accepted.storage, "inline");
  });

  it("github redelivery of the same delivery id → 200 duplicate", async () => {
    const { raw, headers } = gh({ action: "opened", installation: { id: 42 } }, "gh-dup");
    assert.equal((await t.post("/api/v1/webhooks/github", raw, headers)).status, 202);
    const again = await t.post("/api/v1/webhooks/github", raw, headers);
    assert.equal(again.status, 200);
    assert.deepEqual(again.json, { accepted: true, duplicate: true });
  });

  it("/cloud alias routes to the same ingress", async () => {
    const { raw, headers } = gh({ installation: { id: 42 } });
    assert.equal((await t.post("/cloud/api/v1/webhooks/github", raw, headers)).status, 202);
  });

  it("401 invalid_signature with no detail; non-2xx carry the request id and log a reason", async () => {
    const r = await t.post("/api/v1/webhooks/github", "{}", { "x-hub-signature-256": "sha256=" + "0".repeat(64) });
    assert.equal(r.status, 401);
    assert.deepEqual(r.json, { error: "invalid_signature" });
    assert.ok(r.headers.get("x-relayfile-control-request-id"));
    const rejected = t.logger.named("ingress_rejected").at(-1)!;
    assert.equal(rejected.reason, "invalid_signature");
    assert.equal(rejected.status, 401);
  });

  it("400 invalid_json only after a valid signature", async () => {
    const raw = "not json";
    const r = await t.post("/api/v1/webhooks/github", raw, { ...sign.github(raw), "x-github-delivery": "x", "x-github-event": "e" });
    assert.equal(r.status, 400);
    assert.deepEqual(r.json, { error: "invalid_json" });
  });

  it("unknown installation → 202 ignored (INGRESS_REJECT_UNKNOWN_INSTALLATION off)", async () => {
    const { raw, headers } = gh({ installation: { id: 999 } });
    const r = await t.post("/api/v1/webhooks/github", raw, headers);
    assert.equal(r.status, 202);
    assert.equal(r.json.ignored, true);
  });

  it("nango: 202, and a body without provider identifier → 202 dropped", async () => {
    const ok = JSON.stringify({ type: "forward", from: "notion", connectionId: "c-n", payload: { id: "page" } });
    assert.equal((await t.post("/api/v1/webhooks/nango", ok, { ...sign.nango(ok), "x-nango-delivery-id": "n1" })).status, 202);
    const drop = JSON.stringify({ type: "forward", connectionId: "c-n", payload: {} });
    const r = await t.post("/api/v1/webhooks/nango", drop, sign.nango(drop));
    assert.equal(r.status, 202);
    assert.deepEqual(r.json, { accepted: true, dropped: true, reason: "missing_nango_provider_identifier" });
  });

  it("slack url_verification answered before queueing; event lanes by team", async () => {
    const v = JSON.stringify({ type: "url_verification", challenge: "abc" });
    const r = await t.post("/api/v1/webhooks/slack", v, sign.slack(v));
    assert.equal(r.status, 200);
    assert.deepEqual(r.json, { challenge: "abc" });
    const ev = JSON.stringify({ type: "event_callback", team_id: "T1", event_id: "Ev9", event: { type: "message" } });
    const e = await t.post("/api/v1/webhooks/slack", ev, sign.slack(ev));
    assert.equal(e.status, 202);
    const { rows } = await t.db.query<{ lane: string }>("SELECT lane FROM webhook_envelopes WHERE request_id = $1", [e.json.requestId]);
    assert.equal(rows[0]!.lane, "rw_slack");
  });

  it("linear: signature + webhookTimestamp window", async () => {
    const fresh = JSON.stringify({ type: "Issue", action: "create", webhookTimestamp: Date.now(), organizationId: "o" });
    assert.equal((await t.post("/api/v1/webhooks/linear", fresh, { ...sign.linear(fresh), "linear-delivery": "L1" })).status, 202);
    const stale = JSON.stringify({ type: "Issue", webhookTimestamp: Date.now() - 120_000 });
    assert.equal((await t.post("/api/v1/webhooks/linear", stale, sign.linear(stale))).status, 401);
  });

  it("hookdeck: GET ?challenge echoes text/plain; plain GET 405 Allow: POST", async () => {
    const c = await fetch(`${t.url}/api/v1/webhooks/hookdeck?challenge=xyz`);
    assert.equal(c.status, 200);
    assert.equal(c.headers.get("content-type"), "text/plain");
    assert.equal(await c.text(), "xyz");
    const g = await fetch(`${t.url}/api/v1/webhooks/hookdeck`);
    assert.equal(g.status, 405);
    assert.equal(g.headers.get("allow"), "POST");
  });

  it("hookdeck: Hookdeck signature AND Nango signature both required (Cloud order)", async () => {
    const body = JSON.stringify({ type: "forward", from: "jira", connectionId: "c-j", payload: {} });
    assert.equal((await t.post("/api/v1/webhooks/hookdeck", body, { ...sign.hookdeckStandard(body), ...sign.nango(body) })).status, 202);
    assert.equal((await t.post("/api/v1/webhooks/hookdeck", body, { ...sign.hookdeckLegacy(body), ...sign.nango(body) })).status, 202);
    assert.equal((await t.post("/api/v1/webhooks/hookdeck", body, sign.hookdeckLegacy(body))).status, 401, "no nango sig");
    assert.equal((await t.post("/api/v1/webhooks/hookdeck", body, sign.nango(body))).status, 401, "no hookdeck sig");
  });

  it("hookdeck: raw providers (gitlab) without a Nango signature fail closed in PR 1", async () => {
    const body = JSON.stringify({ object_kind: "push" });
    const r = await t.post("/api/v1/webhooks/hookdeck", body, { "x-gitlab-event": "Push Hook", "x-gitlab-token": "fixture" });
    assert.equal(r.status, 401);
  });

  it("hookdeck: Dropbox branch is checked first and verified with the app secret", async () => {
    const body = JSON.stringify({ list_folder: { accounts: ["dbid:x"] } });
    const ok = await t.post("/api/v1/webhooks/hookdeck", body, sign.dropbox(body));
    assert.equal(ok.status, 202);
    const { rows } = await t.db.query<{ kind: string; provider: string }>("SELECT kind, provider FROM webhook_envelopes WHERE request_id = $1", [ok.json.requestId]);
    assert.deepEqual(rows[0], { kind: "hookdeck-raw", provider: "dropbox" });
    assert.equal((await t.post("/api/v1/webhooks/hookdeck", body, sign.dropbox(body, "wrong"))).status, 401);
  });

  it("413 payload_too_large: declared and streamed bodies over the 1 MiB non-github cap", async () => {
    const big = JSON.stringify({ x: "y".repeat(1024 * 1024 + 10) });
    const r = await t.post("/api/v1/webhooks/nango", big, sign.nango(big));
    assert.equal(r.status, 413);
    assert.deepEqual(r.json, { error: "payload_too_large" });
  });

  it("unknown route 404, wrong method 405", async () => {
    assert.equal((await t.post("/api/v1/webhooks/nope", "{}")).status, 404);
    const r = await fetch(`${t.url}/api/v1/webhooks/github`);
    assert.equal(r.status, 405);
  });
});

describe("ingress gates", () => {
  it("503 ingress_not_configured (never 401) when the secret is missing, counted", async () => {
    const t = await startApp({ config: fixtureConfig({ GITHUB_WEBHOOK_SECRET: "" }) });
    const { raw, headers } = gh({});
    const r = await t.post("/api/v1/webhooks/github", raw, headers);
    assert.equal(r.status, 503);
    assert.deepEqual(r.json, { error: "ingress_not_configured" });
    assert.equal(t.app.metrics.counter("ingress_unconfigured_total", { ingress: "github" }), 1);
    await t.close();
  });

  it("403 feature_not_licensed when not entitled", async () => {
    const t = await startApp();
    t.entitlement.set(false);
    const { raw, headers } = gh({});
    const r = await t.post("/api/v1/webhooks/github", raw, headers);
    assert.equal(r.status, 403);
    assert.equal(r.json.error, "feature_not_licensed");
    await t.close();
  });

  it("429 ingest_backpressure + Retry-After: lane cap, global cap, paused", async () => {
    const t = await startApp({ config: fixtureConfig({ LANE_MAX_QUEUED: "2", GLOBAL_MAX_QUEUED: "3" }) });
    const nango = (cid: string) => {
      const b = JSON.stringify({ type: "forward", from: "notion", connectionId: cid, payload: { r: Math.random() } });
      return t.post("/api/v1/webhooks/nango", b, sign.nango(b));
    };
    assert.equal((await nango("a")).status, 202);
    assert.equal((await nango("a")).status, 202);
    const lane = await nango("a");
    assert.equal(lane.status, 429);
    assert.equal(lane.json.error, "ingest_backpressure");
    assert.equal(lane.json.reason, "lane_queue_full");
    assert.equal(lane.headers.get("retry-after"), String(lane.json.retryAfterSeconds));
    assert.equal((await nango("b")).status, 202);
    t.app.depth.invalidate();
    const global = await nango("c");
    assert.equal(global.status, 429);
    assert.equal(global.json.reason, "global_queue_full");
    const reasons = t.logger.named("ingress_rejected").map((e) => e.reason);
    assert.deepEqual(reasons, ["ingest_backpressure:lane_queue_full", "ingest_backpressure:global_queue_full"]);
    await t.close();

    const p = await startApp();
    await p.app.flags.setIngressPaused(true, "test");
    const b = JSON.stringify({ type: "forward", from: "notion", connectionId: "z", payload: {} });
    const paused = await p.post("/api/v1/webhooks/nango", b, sign.nango(b));
    assert.equal(paused.status, 429);
    assert.equal(paused.json.reason, "ingress_paused");
    await p.close();
  });

  it("lane_stale_cooldown: cooled lane with an old backlog is refused", async () => {
    const t = await startApp({ config: fixtureConfig({ MAX_QUEUE_AGE_SECONDS: "1", COOLDOWN_ENABLED: "true" }) });
    const send = () => {
      const b = JSON.stringify({ type: "forward", from: "notion", connectionId: "cool", payload: { r: Math.random() } });
      return t.post("/api/v1/webhooks/nango", b, sign.nango(b));
    };
    assert.equal((await send()).status, 202);
    await t.db.query("UPDATE webhook_envelopes SET visible_at = now() - interval '10 seconds'");
    await t.app.cooldowns.arm("unresolved:notion:cool", 120, "test", "workspace_busy");
    const r = await send();
    assert.equal(r.status, 429);
    assert.equal(r.json.reason, "lane_stale_cooldown");
    assert.ok(r.json.retryAfterSeconds >= 119);
    await t.close();
  });

  it("503 storage_unavailable when Postgres is unreachable", async () => {
    const t = await startApp();
    const broken = { ...t.db, query: async () => { throw new Error("connection refused"); }, tx: async () => { throw new Error("connection refused"); } };
    const t2 = await startApp({ db: broken as never });
    const { raw, headers } = gh({ installation: { id: 42 } });
    const r = await t2.post("/api/v1/webhooks/github", raw, headers);
    assert.equal(r.status, 503);
    assert.deepEqual(r.json, { error: "storage_unavailable" });
    await t2.close();
    await t.close();
  });
});

describe("health, readiness, metrics, admin", () => {
  it("/readyz: 200 with entitlement in the body even when not entitled", async () => {
    const t = await startApp();
    t.entitlement.set(false, "license_expired");
    const r = await fetch(`${t.url}/readyz`);
    assert.equal(r.status, 200);
    const body = await r.json();
    assert.equal(body.ready, true);
    assert.equal(body.entitlement.entitled, false);
    assert.equal(body.migration.upToDate, true);
    await t.close();
  });

  it("/readyz reports entitlement=unconfigured and ingress is open when no entitlement source is set", async () => {
    const db = await newPglite();
    const config = fixtureConfig();
    const entitlement = new RefreshingEntitlement({ graceSeconds: config.ENTITLEMENT_GRACE_SECONDS });
    await entitlement.refresh();
    const app = createApp({ config, db, logger: new MemoryLogger(), entitlement });
    const pub = await listen(app.publicHandler);
    try {
      const r = await fetch(`${pub.url}/readyz`);
      assert.equal(r.status, 200);
      const body = await r.json();
      assert.equal(body.ready, true);
      assert.equal(body.entitlement.entitled, true);
      assert.equal(body.entitlement.source, "unconfigured");
      assert.equal(body.entitlement.reason, "no_entitlement_source_configured");
      // Ingress is past the entitlement gate: an unsigned GitHub delivery is refused for its signature, not 403.
      const refused = await fetch(`${pub.url}/api/v1/webhooks/github`, { method: "POST", headers: { "content-type": "application/json" }, body: "{}" });
      assert.notEqual(refused.status, 403);
      assert.notEqual(((await refused.json()) as { error?: string }).error, "feature_not_licensed");
    } finally {
      await pub.close();
    }
  });

  it("/metrics only on the admin listener; admin requires the bearer", async () => {
    const t = await startApp();
    assert.equal((await fetch(`${t.url}/metrics`)).status, 404);
    assert.equal((await fetch(`${t.url}/api/v1/admin/bindings`)).status, 404);
    const m = await fetch(`${t.adminUrl}/metrics`);
    assert.equal(m.status, 200);
    assert.equal((await fetch(`${t.adminUrl}/api/v1/admin/bindings`)).status, 401);
    assert.equal((await fetch(`${t.adminUrl}/api/v1/admin/bindings`, { headers: { authorization: "Bearer wrong" } })).status, 401);
    await t.close();
  });

  it("admin: bindings upsert/list, invalid workspace id refused, pause/resume, queue stats", async () => {
    const t = await startApp();
    const auth = { authorization: `Bearer ${t.config.RELAYFILE_CONTROL_ADMIN_TOKEN[0]}`, "content-type": "application/json" };
    const bad = await fetch(`${t.adminUrl}/api/v1/admin/bindings`, { method: "POST", headers: auth, body: JSON.stringify({ workspaceId: "ws_1", provider: "github", connectionId: "c" }) });
    assert.equal(bad.status, 400);
    assert.equal((await bad.json()).error, "invalid_workspace_id");
    const ok = await fetch(`${t.adminUrl}/api/v1/admin/bindings`, { method: "POST", headers: auth, body: JSON.stringify({ workspaceId: "rw_1", provider: "github", connectionId: "c", nativeIds: { installation_id: "7" } }) });
    assert.equal(ok.status, 200);
    const list = await (await fetch(`${t.adminUrl}/api/v1/admin/bindings?provider=github`, { headers: auth })).json();
    assert.equal(list.bindings.length, 1);
    assert.equal((await fetch(`${t.adminUrl}/api/v1/admin/ingress/pause`, { method: "POST", headers: auth })).status, 200);
    const stats = await (await fetch(`${t.adminUrl}/api/v1/admin/queue/stats`, { headers: auth })).json();
    assert.equal(stats.ingressPaused, true);
    await fetch(`${t.adminUrl}/api/v1/admin/ingress/resume`, { method: "POST", headers: auth });
    const dlq = await fetch(`${t.adminUrl}/api/v1/admin/dlq/replay`, { method: "POST", headers: auth, body: "{}" });
    assert.equal(dlq.status, 400, "replay needs a selector");
    await t.close();
  });
});
