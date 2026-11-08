import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { upsertBinding, upsertPendingBinding } from "../src/bindings/store.ts";
import type { Config } from "../src/config.ts";
import { Consumer } from "../src/consumer/consumer.ts";
import type { Db } from "../src/db/db.ts";
import { StaticEntitlement } from "../src/entitlement/entitlement.ts";
import { relayfileEnvelopeId } from "../src/envelope/model.ts";
import { MemoryLogger } from "../src/observability/log.ts";
import { Metrics } from "../src/observability/metrics.ts";
import { CooldownStore } from "../src/queue/cooldown.ts";
import { insertEnvelope } from "../src/queue/store.ts";
import { RelayfileClient } from "../src/relayfile/client.ts";
import { envelope } from "./support/fixtures.ts";
import { fakeRelayfile, FIXTURE, fixtureConfig, makeVisible, newPglite, type FakeResponse } from "./support/harness.ts";

async function setup(respond?: (n: number) => FakeResponse, cfg: Partial<Record<keyof Config, string>> = {}, extra: { deadlineMs?: (p: string) => number; maxBodyBytes?: number } = {}) {
  const db = await newPglite();
  const fake = await fakeRelayfile({ ...(respond ? { respond: (_r, n) => respond(n) } : {}), ...(extra.maxBodyBytes ? { maxBodyBytes: extra.maxBodyBytes } : {}) });
  const config = fixtureConfig({ RELAYFILE_BASE_URL: fake.url, ...cfg });
  const logger = new MemoryLogger();
  const metrics = new Metrics();
  const entitlement = new StaticEntitlement(true);
  const cooldowns = new CooldownStore(db, { enabled: config.COOLDOWN_ENABLED, maxKeys: 100 });
  const relayfile = new RelayfileClient({ hmacSecret: FIXTURE.relayHmac, sleep: async () => undefined });
  const consumer = new Consumer({ db, config, logger, metrics, entitlement, cooldowns, relayfile, random: () => 0, ...(extra.deadlineMs ? { deadlineMs: extra.deadlineMs } : {}) });
  await upsertBinding(db, { workspaceId: "rw_a", provider: "github", connectionId: "gh-a", nativeIds: { installation_id: "1" } });
  return { db, fake, config, logger, metrics, entitlement, cooldowns, consumer, close: async () => { await fake.close(); await db.close(); } };
}

const ghRow = (over = {}) => envelope("unresolved:github:1", { nativeIds: { installation_id: "1" }, providerDeliveryId: `github:${Math.random()}`, headers: { "x-github-event": "issues" }, eventType: "issues.opened", ...over });

async function only<T>(db: Db, sql: string): Promise<T> {
  return (await db.query<T>(sql)).rows[0]!;
}

describe("consumer → fake Relayfile (§4.4, §6)", () => {
  it("ack: signed envelope with the §14.3 payload shape, stable ids, relaned to the workspace", async () => {
    const s = await setup();
    const ins = await insertEnvelope(s.db, ghRow({ parsed: { action: "opened", installation: { id: 1 } }, raw: Buffer.from('{"action":"opened","installation":{"id":1}}') }));
    assert.ok(!ins.duplicate);
    assert.equal(await s.consumer.drain(), 1);
    assert.equal(s.fake.requests.length, 1);
    const req = s.fake.requests[0]!;
    const row = await only<{ request_id: string; delivery_id: string; state: string; lane: string }>(s.db, "SELECT request_id, delivery_id, state, lane FROM webhook_envelopes");
    assert.equal(row.state, "done");
    assert.equal(row.lane, "rw_a");
    assert.equal(req.headers["x-correlation-id"], row.request_id);
    assert.equal(req.body.workspaceId, "rw_a");
    assert.equal(req.body.deliveryId, row.delivery_id);
    assert.equal(req.body.envelopeId, relayfileEnvelopeId({ provider: "github", workspaceId: "rw_a", deliveryId: row.delivery_id, eventType: "issues.opened", providerEventType: "issues.opened" }));
    assert.equal(req.body.payload.event_type, "file.created");
    assert.match(req.body.payload.path, /^\/github\/webhooks\/issues\.opened\/github_/);
    assert.deepEqual(JSON.parse(req.body.payload.content), { action: "opened", installation: { id: 1 } });
    assert.equal(req.body.payload.providerEventType, "issues.opened");
    assert.equal(req.body.payload.data, undefined, "never nest raw provider data under payload.data");
    assert.equal(s.logger.named("consume_outcome")[0]!.disposition, "ack");
    await s.close();
  });

  it("409 duplicate_envelope is an ack", async () => {
    const s = await setup(() => ({ status: 409, body: { error: "duplicate_envelope" } }));
    await insertEnvelope(s.db, ghRow());
    await s.consumer.drain();
    assert.equal((await only<{ state: string }>(s.db, "SELECT state FROM webhook_envelopes")).state, "done");
    await s.close();
  });

  it("413 is ack_terminal with a reason and the OSS-1MiB metric", async () => {
    const s = await setup(undefined, {}, { maxBodyBytes: 200 });
    await insertEnvelope(s.db, ghRow({ parsed: { big: "x".repeat(500) }, raw: Buffer.from(JSON.stringify({ big: "x".repeat(500) })) }));
    await s.consumer.drain();
    const row = await only<{ state: string; last_outcome: any }>(s.db, "SELECT state, last_outcome FROM webhook_envelopes");
    assert.equal(row.state, "done");
    assert.equal(row.last_outcome.disposition, "ack_terminal");
    assert.equal(row.last_outcome.code, "relayfile_payload_too_large");
    assert.equal(s.metrics.counter("relayfile_envelope_too_large_total", { provider: "github", ingress: "github" }), 1);
    assert.equal(s.logger.named("relayfile_envelope_too_large").length, 1);
    await s.close();
  });

  it("HARD: 429 forever ⇒ maxRetries+1 posts per attempt, lengthening queue delays, cool-down, then DLQ with the full reason chain", async () => {
    const s = await setup(() => ({ status: 429, headers: { "retry-after": "7" }, body: { reason: "workspace_busy" } }), { MAX_ATTEMPTS: "3" });
    await insertEnvelope(s.db, ghRow());
    const delays: number[] = [];
    for (let attempt = 1; attempt <= 3; attempt += 1) {
      await s.db.query("DELETE FROM lane_cooldowns");
      await makeVisible(s.db);
      const before = s.fake.requests.length;
      assert.equal(await s.consumer.runOnce(), true);
      assert.equal(s.fake.requests.length - before, 2, "github: 1 in-place retry ⇒ 2 posts");
      const row = await only<{ state: string; last_outcome: any }>(s.db, "SELECT state, last_outcome FROM webhook_envelopes");
      assert.deepEqual(
        { ...row.last_outcome, retryDelaySeconds: undefined },
        {
          disposition: attempt < 3 ? "retry" : "dead",
          code: "relayfile_backpressure",
          status: 429,
          surface: "webhook-envelopes",
          relayfileReason: "workspace_busy",
          retryAfterSeconds: 7,
          inPlaceAttempts: 2,
          attempts: attempt,
          retryDelaySeconds: undefined,
        },
      );
      delays.push(row.last_outcome.retryDelaySeconds);
      assert.equal(row.state, attempt < 3 ? "queued" : "dead");
    }
    assert.deepEqual(delays, [60, 120, 240], "schedule lengthens; hint (7 s) is a floor");
    assert.ok((await s.cooldowns.remaining("rw_a")) > 0, "cool-down armed");
    const hist = await only<{ n: number }>(s.db, "SELECT jsonb_array_length(outcome_history)::int AS n FROM webhook_envelopes");
    assert.equal(hist.n, 3);
    assert.equal(s.logger.named("relayfile_backpressure").length, 6, "every 429 logged, including in-place ones");
    assert.equal(s.metrics.counterSum("relayfile_backpressure_total", { surface: "webhook-envelopes" }), 6);
    await s.close();
  });

  it("no Retry-After ⇒ retryAfterSeconds defaults to 60 (HARD)", async () => {
    const s = await setup(() => ({ status: 503, body: { reason: "storage_full" } }));
    await insertEnvelope(s.db, ghRow());
    await s.consumer.drain();
    const row = await only<{ last_outcome: any }>(s.db, "SELECT last_outcome FROM webhook_envelopes");
    assert.equal(row.last_outcome.retryAfterSeconds, 60);
    assert.equal(row.last_outcome.status, 503);
    assert.equal(row.last_outcome.relayfileReason, "storage_full");
    await s.close();
  });

  it("429 then 202 ⇒ ack (fresh signature per attempt passes the replay guard)", async () => {
    const s = await setup((n) => (n === 1 ? { status: 429, headers: { "retry-after": "1" } } : { status: 202 }));
    await insertEnvelope(s.db, ghRow());
    await s.consumer.drain();
    assert.equal((await only<{ state: string }>(s.db, "SELECT state FROM webhook_envelopes")).state, "done");
    assert.equal(s.fake.requests.length, 2);
    assert.equal(s.fake.rejected.length, 0);
    assert.notEqual(s.fake.requests[0]!.headers["x-relay-signature"], s.fake.requests[1]!.headers["x-relay-signature"]);
    await s.close();
  });

  it("slack: no in-place retry under the 15 s deadline (1 post per attempt)", async () => {
    const s = await setup(() => ({ status: 429, headers: { "retry-after": "1" } }));
    await upsertBinding(s.db, { workspaceId: "rw_s", provider: "slack", connectionId: "sl", nativeIds: { team_id: "T" } });
    await insertEnvelope(s.db, envelope("unresolved:slack:T", { ingress: "slack", provider: "slack", nativeIds: { team_id: "T" } }));
    await s.consumer.drain();
    assert.equal(s.fake.requests.length, 1);
    await s.close();
  });

  it("final 500 ⇒ retry on the schedule, no cool-down", async () => {
    const s = await setup(() => ({ status: 500 }));
    await insertEnvelope(s.db, ghRow());
    await s.consumer.drain();
    const row = await only<{ state: string; last_outcome: any }>(s.db, "SELECT state, last_outcome FROM webhook_envelopes");
    assert.equal(row.state, "queued");
    assert.equal(row.last_outcome.code, "relayfile_server_error");
    assert.equal(await s.cooldowns.remaining("rw_a"), 0);
    await s.close();
  });

  it("unmatched workspace ⇒ ack with reason; pending binding ⇒ retry (never acked unwritten)", async () => {
    const s = await setup();
    await insertEnvelope(s.db, envelope("unresolved:notion:none", { kind: "nango-forward", ingress: "nango", provider: "notion", connectionId: "none" }));
    await s.consumer.drain();
    assert.equal((await only<{ last_outcome: any }>(s.db, "SELECT last_outcome FROM webhook_envelopes")).last_outcome.reason, "unmatched_workspace");
    await s.db.query("DELETE FROM webhook_envelopes");
    await upsertPendingBinding(s.db, "notion", "pend", null);
    await insertEnvelope(s.db, envelope("unresolved:notion:pend", { kind: "nango-forward", ingress: "nango", provider: "notion", connectionId: "pend" }));
    await s.consumer.drain();
    const row = await only<{ state: string; last_outcome: any }>(s.db, "SELECT state, last_outcome FROM webhook_envelopes");
    assert.deepEqual([row.state, row.last_outcome.code], ["queued", "binding_pending"]);
    assert.equal(s.fake.requests.length, 0);
    await s.close();
  });

  it("nango-auth upserts a pending binding", async () => {
    const s = await setup();
    await insertEnvelope(s.db, envelope("unresolved:jira:new", { kind: "nango-auth", ingress: "nango", provider: "jira", connectionId: "new" }));
    await s.consumer.drain();
    assert.equal((await only<{ status: string }>(s.db, "SELECT status FROM integration_bindings WHERE connection_id = 'new'")).status, "pending");
    await s.close();
  });

  it("GitHub installation fan-out: one child per workspace, each delivered once", async () => {
    const s = await setup();
    await upsertBinding(s.db, { workspaceId: "rw_b", provider: "github", connectionId: "gh-b", nativeIds: { installation_id: "1" } });
    await insertEnvelope(s.db, ghRow());
    await s.consumer.drain();
    assert.deepEqual(s.fake.requests.map((r) => r.workspaceId).sort(), ["rw_a", "rw_b"]);
    const ids = new Set(s.fake.requests.map((r) => r.body.envelopeId));
    assert.equal(ids.size, 2);
    const corr = new Set(s.fake.requests.map((r) => r.headers["x-correlation-id"]));
    assert.equal(corr.size, 1, "children keep the parent request id");
    await s.close();
  });

  it("handler deadline aborts the attempt and retries", async () => {
    const s = await setup(undefined, {}, { deadlineMs: () => 50 });
    const slow = new Consumer({
      db: s.db, config: s.config, logger: s.logger, metrics: s.metrics, entitlement: s.entitlement, cooldowns: s.cooldowns, relayfile: null,
      handler: async (_row, ctx) => new Promise((_, reject) => ctx.signal.addEventListener("abort", () => reject(new Error("aborted")))),
      deadlineMs: () => 50,
    });
    await insertEnvelope(s.db, ghRow());
    await slow.drain();
    const row = await only<{ state: string; last_outcome: any }>(s.db, "SELECT state, last_outcome FROM webhook_envelopes");
    assert.deepEqual([row.state, row.last_outcome.code], ["queued", "handler_deadline_exceeded"]);
    await s.close();
  });

  it("not entitled ⇒ consumer pauses, rows stay queued", async () => {
    const s = await setup();
    s.entitlement.set(false);
    await insertEnvelope(s.db, ghRow());
    assert.equal(await s.consumer.drain(), 0);
    assert.equal((await only<{ state: string; attempts: number }>(s.db, "SELECT state, attempts FROM webhook_envelopes")).state, "queued");
    await s.close();
  });

  it("start/stop runs slots concurrently and drains", async () => {
    const s = await setup(undefined, { CONSUMER_CONCURRENCY: "4" });
    for (let i = 0; i < 12; i += 1) await insertEnvelope(s.db, ghRow());
    s.consumer.start();
    for (let i = 0; i < 100 && s.fake.requests.length < 12; i += 1) await new Promise((r) => setTimeout(r, 25));
    await s.consumer.stop();
    assert.equal(s.fake.requests.length, 12);
    await s.close();
  });
});
