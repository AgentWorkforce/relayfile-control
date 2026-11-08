// Regression tests for the PR #2 review findings (cubic, 2026-11-08). One `it` per fixed finding.
import assert from "node:assert/strict";
import { generateKeyPairSync, sign as edSign } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { describe, it } from "node:test";
import { resolveBindings, upsertBinding } from "../src/bindings/store.ts";
import { parseListen, testConfig } from "../src/config.ts";
import { Consumer } from "../src/consumer/consumer.ts";
import type { EnvelopeHandler } from "../src/consumer/envelope-handler.ts";
import { handlerDeadlineMs } from "../src/consumer/policy.ts";
import { listMigrations } from "../src/db/migrate.ts";
import { LICENSE_SIGNATURE_PREFIX, RefreshingEntitlement } from "../src/entitlement/entitlement.ts";
import { verifyStandardWebhook } from "../src/ingress/verify.ts";
import { jsonLogger, MemoryLogger } from "../src/observability/log.ts";
import { Metrics } from "../src/observability/metrics.ts";
import { CooldownStore } from "../src/queue/cooldown.ts";
import { claim } from "../src/queue/claims.ts";
import { claimNext, insertEnvelope, markDead, readPayload } from "../src/queue/store.ts";
import { archiveExhausted } from "../src/queue/dlq.ts";
import { StaticEntitlement } from "../src/entitlement/entitlement.ts";
import { RelayfileClient } from "../src/relayfile/client.ts";
import { envelope } from "./support/fixtures.ts";
import { fakeRelayfile, FIXTURE, fixtureConfig, listen, newPglite, sign, startApp } from "./support/harness.ts";

function consumer(db: Awaited<ReturnType<typeof newPglite>>, cfg: Record<string, string> = {}, relayUrl?: string, handler?: EnvelopeHandler) {
  const config = fixtureConfig({ ...(relayUrl ? { RELAYFILE_BASE_URL: relayUrl } : {}), ...cfg });
  const cooldowns = new CooldownStore(db, { enabled: config.COOLDOWN_ENABLED, maxKeys: 100 });
  return {
    cooldowns,
    c: new Consumer({
      db, config, logger: new MemoryLogger(), metrics: new Metrics(), entitlement: new StaticEntitlement(true), cooldowns,
      relayfile: new RelayfileClient({ hmacSecret: FIXTURE.relayHmac, sleep: async () => undefined }),
      ...(handler ? { handler } : {}),
    }),
  };
}

describe("review fixes: routing and lanes", () => {
  it("binding resolution is scoped to the provider family (P1)", async () => {
    const db = await newPglite();
    await upsertBinding(db, { workspaceId: "rw_slack", provider: "slack", connectionId: "shared-conn", nativeIds: { team_id: "T1" } });
    const r = await resolveBindings(db, { provider: "notion", connectionId: "shared-conn", nativeIds: {} });
    assert.equal(r.kind, "unmatched", "a notion delivery never routes to the slack binding with the same connection id");
    const s = await resolveBindings(db, { provider: "slack-relay", connectionId: "shared-conn", nativeIds: {} });
    assert.equal(s.kind, "matched");
    await db.close();
  });

  it("a resolved delivery is relaned and requeued, so the destination lane's cool-down applies (P1)", async () => {
    const db = await newPglite();
    const fake = await fakeRelayfile({});
    await upsertBinding(db, { workspaceId: "rw_busy", provider: "notion", connectionId: "c1" });
    const { c, cooldowns } = consumer(db, { COOLDOWN_ENABLED: "true" }, fake.url);
    await cooldowns.arm("rw_busy", 600, "test", "workspace_busy");
    await insertEnvelope(db, envelope("unresolved:notion:c1", { kind: "nango-forward", ingress: "nango", provider: "notion", connectionId: "c1" }));
    await c.drain();
    const { rows } = await db.query<{ state: string; lane: string; attempts: number }>("SELECT state, lane, attempts FROM webhook_envelopes");
    assert.deepEqual(rows[0], { state: "queued", lane: "rw_busy", attempts: 0 }, "moved to the workspace lane, attempt refunded, held by its cool-down");
    assert.equal(fake.requests.length, 0);
    await cooldowns.clear("rw_busy");
    await c.drain();
    assert.equal(fake.requests.length, 1);
    await fake.close();
    await db.close();
  });

  it("COOLDOWN_ENABLED=false ignores previously armed rows when claiming (P2)", async () => {
    const db = await newPglite();
    await db.query("INSERT INTO lane_cooldowns (lane, until) VALUES ('rw_a', now() + interval '1 hour')");
    await insertEnvelope(db, envelope("rw_a"));
    assert.equal(await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2, cooldownEnabled: true }), null);
    assert.ok(await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2, cooldownEnabled: false }));
    await db.close();
  });

  it("an in-flight handler claim is retried, never acked as duplicate (P1)", async () => {
    const db = await newPglite();
    const fake = await fakeRelayfile({});
    await upsertBinding(db, { workspaceId: "rw_a", provider: "notion", connectionId: "c1" });
    const row = envelope("rw_a", { kind: "nango-forward", ingress: "nango", provider: "notion", connectionId: "c1" });
    await insertEnvelope(db, row);
    await claim(db, { surface: "nango-forward", dedupeId: `${Buffer.byteLength(row.requestId)}:${row.requestId}|4:rw_a`, leaseSeconds: 900 });
    const { c } = consumer(db, {}, fake.url);
    await c.drain();
    const { rows } = await db.query<{ state: string; last_outcome: any }>("SELECT state, last_outcome FROM webhook_envelopes");
    assert.deepEqual([rows[0]!.state, rows[0]!.last_outcome.code], ["queued", "claim_in_flight"]);
    await fake.close();
    await db.close();
  });

  it("a pinned workspace whose binding is gone retries instead of using the global URL (P2)", async () => {
    const db = await newPglite();
    const fake = await fakeRelayfile({});
    await insertEnvelope(db, envelope("rw_gone", { kind: "nango-forward", ingress: "nango", provider: "notion", connectionId: "c9" }));
    const { c } = consumer(db, {}, fake.url);
    await c.drain();
    const { rows } = await db.query<{ last_outcome: any }>("SELECT last_outcome FROM webhook_envelopes");
    assert.equal(rows[0]!.last_outcome.code, "binding_missing_for_workspace");
    assert.equal(fake.requests.length, 0);
    await fake.close();
    await db.close();
  });

  it("a lost lease aborts the handler and records lease_lost (P2)", async () => {
    const db = await newPglite();
    let aborted = false;
    const handler: EnvelopeHandler = (_row, ctx) => new Promise((_, reject) => {
      ctx.signal.addEventListener("abort", () => { aborted = true; reject(new Error("aborted")); });
      // Another worker steals the lease.
      void db.query("UPDATE webhook_envelopes SET leased_by = 'other-worker'");
    });
    const { c } = consumer(db, { LEASE_SECONDS: "5" }, undefined, handler);
    await insertEnvelope(db, envelope("rw_a"));
    const row = (await claimNext(db, { worker: "me", leaseSeconds: 5, perLaneMax: 2 }))!;
    assert.equal(await c.process(row, "me"), "lease_lost");
    assert.equal(aborted, true);
    await db.close();
  });

  it("emails match case-insensitively; omitted upsert fields keep stored values (P2)", async () => {
    const db = await newPglite();
    await upsertBinding(db, { workspaceId: "rw_m", provider: "google-mail", connectionId: "g1", nativeIds: { email: "Ops@Example.COM" }, relayfileBaseUrl: "https://rf.example" });
    assert.equal((await resolveBindings(db, { provider: "google-mail", connectionId: null, nativeIds: { email: "ops@example.com" } })).kind, "matched");
    const b = await upsertBinding(db, { workspaceId: "rw_m", provider: "google-mail", connectionId: "g1", status: "disabled" });
    assert.deepEqual([b.native_ids, b.relayfile_base_url, b.status], [{ email: "ops@example.com" }, "https://rf.example", "disabled"]);
    await db.close();
  });

  it("google-mail gets 120 s but other google-* providers do not (P2)", () => {
    assert.equal(handlerDeadlineMs("google-mail"), 120_000);
    assert.equal(handlerDeadlineMs("google-mail-relay"), 120_000);
    assert.equal(handlerDeadlineMs("google-calendar"), 15_000);
  });
});

describe("review fixes: ingress and payloads", () => {
  it("a validly signed payload containing \\u0000 is stored as a blob, not rejected (P2)", async () => {
    const db = await newPglite();
    const raw = Buffer.from('{"text":"a\\u0000b"}');
    const r = await insertEnvelope(db, envelope("rw_a", { raw, parsed: JSON.parse(raw.toString()) }));
    assert.ok(!r.duplicate && !r.inline);
    const row = (await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }))!;
    assert.deepEqual(await readPayload(db, row, 1000), { text: "a\u0000b" });
    await db.close();
  });

  it("lane_stale_cooldown uses the candidate lane's age, not the whole queue (P2)", async () => {
    const t = await startApp({ config: fixtureConfig({ MAX_QUEUE_AGE_SECONDS: "1", COOLDOWN_ENABLED: "true" }) });
    await insertEnvelope(t.db, envelope("unresolved:notion:old"));
    await t.db.query("UPDATE webhook_envelopes SET visible_at = now() - interval '1 hour'");
    await t.app.cooldowns.arm("unresolved:notion:fresh", 120, "t", "busy");
    const b = JSON.stringify({ type: "forward", from: "notion", connectionId: "fresh", payload: {} });
    assert.equal((await t.post("/api/v1/webhooks/nango", b, sign.nango(b))).status, 202, "lane B's stale backlog does not refuse cooled lane A");
    await t.close();
  });

  it("a whsec_ key that decodes to almost nothing never verifies (P2)", () => {
    const body = "{}";
    const id = "m";
    const ts = Math.floor(Date.now() / 1000);
    const headers = { "webhook-id": id, "webhook-timestamp": String(ts), "webhook-signature": "v1,anything" };
    assert.equal(verifyStandardWebhook(Buffer.from(body), headers, "whsec_A"), false);
    assert.equal(verifyStandardWebhook(Buffer.from(body), headers, "whsec_AAAA"), false);
  });
});

describe("review fixes: config, entitlement, admin, observability", () => {
  it("blank env values take the default, never 0/false (P1/P2)", () => {
    const c = testConfig({ DLQ_MAX_PRESERVATION_CYCLES: "", ENTITLEMENT_GRACE_SECONDS: " ", COOLDOWN_ENABLED: "", MAX_ATTEMPTS: "" });
    assert.deepEqual([c.DLQ_MAX_PRESERVATION_CYCLES, c.ENTITLEMENT_GRACE_SECONDS, c.COOLDOWN_ENABLED, c.MAX_ATTEMPTS], [3, 86_400, false, 5]);
  });

  it("parseListen rejects an empty or non-digit port (P2)", () => {
    assert.throws(() => parseListen(""));
    assert.throws(() => parseListen("127.0.0.1:"));
    assert.throws(() => parseListen("127.0.0.1:abc"));
    assert.deepEqual(parseListen("127.0.0.1:3101"), { host: "127.0.0.1", port: 3101 });
  });

  it("a definitive denial ends grace for good; an outage right after it does not re-entitle (P1)", async () => {
    let mode: "yes" | "no" | "down" = "yes";
    const srv = await listen((_req, res) => {
      if (mode === "down") { res.writeHead(503); return res.end(); }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ capabilities: { relayfile: mode === "yes" } }));
    });
    const e = new RefreshingEntitlement({ relaycontrolBaseUrl: srv.url, serviceToken: "fixture", graceSeconds: 3600 });
    assert.equal((await e.refresh()).entitled, true);
    mode = "no";
    assert.equal((await e.refresh()).entitled, false);
    mode = "down";
    assert.equal((await e.refresh()).entitled, false, "outage after a revocation stays revoked");
    await srv.close();
  });

  it("a tampered licence ends grace (P1); remote needs a service token (P2)", async () => {
    const { publicKey, privateKey } = generateKeyPairSync("ed25519");
    const rawPub = publicKey.export({ format: "der", type: "spki" }).subarray(12).toString("base64");
    const claims = { v: 1, license_id: "00000000-0000-4000-8000-000000000002", customer: "f", issued_at: "2026-01-01T00:00:00Z", expires_at: "2099-01-01T00:00:00Z", features: ["relayfile"], seats: 1 };
    const payload = Buffer.from(JSON.stringify(claims));
    const sig = edSign(null, Buffer.concat([Buffer.from(LICENSE_SIGNATURE_PREFIX), payload]), privateKey);
    const dir = path.join(process.env.TMPDIR ?? ".", `lic-${Date.now()}`);
    await mkdir(dir, { recursive: true });
    const file = path.join(dir, "license");
    await writeFile(file, `${payload.toString("base64url")}.${sig.toString("base64url")}`);
    const e = new RefreshingEntitlement({ licenseFile: file, publicKey: rawPub, graceSeconds: 3600 });
    assert.equal((await e.refresh()).entitled, true);
    await writeFile(file, `${Buffer.from(JSON.stringify({ ...claims, seats: 99 })).toString("base64url")}.${sig.toString("base64url")}`);
    const s = await e.refresh();
    assert.deepEqual([s.entitled, s.reason], [false, "license_bad_signature"]);
    const noToken = new RefreshingEntitlement({ relaycontrolBaseUrl: "http://127.0.0.1:9", graceSeconds: 0 });
    assert.equal((await noToken.refresh()).reason, "relaycontrol_service_token_missing");
  });

  it("admin: a bad limit is 400 (not 503), and explicit replay ids default the limit to their count (P2)", async () => {
    const t = await startApp();
    const auth = { authorization: `Bearer ${FIXTURE.admin}`, "content-type": "application/json" };
    assert.equal((await fetch(`${t.adminUrl}/api/v1/admin/bindings?limit=abc`, { headers: auth })).status, 400);
    assert.equal((await fetch(`${t.adminUrl}/api/v1/admin/dlq?limit=0`, { headers: auth })).status, 400);
    const ids: string[] = [];
    for (let i = 0; i < 3; i += 1) {
      await insertEnvelope(t.db, envelope("rw_a"));
      const row = (await claimNext(t.db, { worker: "w", leaseSeconds: 60, perLaneMax: 5 }))!;
      await markDead(t.db, row.id, "w", { disposition: "dead" });
      ids.push(row.id);
    }
    assert.equal((await archiveExhausted(t.db, 0)).length, 3);
    const r = await fetch(`${t.adminUrl}/api/v1/admin/dlq/replay`, { method: "POST", headers: auth, body: JSON.stringify({ ids }) });
    assert.equal(r.status, 200);
    assert.equal((await r.json()).replayed, 3);
    await t.close();
  });

  it("metrics: odd label values collapse to 'other' and series per metric are capped (P2)", () => {
    const m = new Metrics();
    m.inc("x_total", { reason: "has spaces and \"quotes\"" });
    assert.equal(m.counter("x_total", { reason: "other" }), 1);
    for (let i = 0; i < 2000; i += 1) m.inc("y_total", { reason: `r${i}` });
    const series = m.render().split("\n").filter((l) => l.startsWith("relayfile_control_y_total{")).length;
    assert.equal(series, 500);
    assert.match(m.render(), /relayfile_control_metric_series_dropped_total 1500/);
  });

  it("logs: nested and case-variant secret keys are dropped (P2)", () => {
    const lines: string[] = [];
    jsonLogger((l) => lines.push(l)).event("e", { Authorization: "Bearer x", nested: { adminToken: "t", payload: { a: 1 }, ok: 1 }, config: { GITHUB_WEBHOOK_SECRET: "set" } });
    const out = JSON.parse(lines[0]!);
    assert.equal(out.Authorization, undefined);
    assert.deepEqual(out.nested, { ok: 1 });
    assert.deepEqual(out.config, { GITHUB_WEBHOOK_SECRET: "set" });
  });

  it("duplicate migration version prefixes fail loudly (P2)", async () => {
    const dir = path.join(process.env.TMPDIR ?? ".", `mig-${Date.now()}`);
    await mkdir(dir, { recursive: true });
    await writeFile(path.join(dir, "0001_a.sql"), "SELECT 1;");
    await writeFile(path.join(dir, "0001_b.sql"), "SELECT 1;");
    await assert.rejects(listMigrations(dir), /duplicate migration version 0001/);
  });
});
