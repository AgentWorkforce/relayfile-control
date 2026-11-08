/**
 * Real-Postgres concurrency tests (§13 "Integration (real PG, opt-in)").
 * Run with RELAYFILE_CONTROL_PG_TEST_URL=postgres://… ; skipped otherwise.
 * Each test gets its own schema, dropped afterwards.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Consumer } from "../src/consumer/consumer.ts";
import type { EnvelopeHandler } from "../src/consumer/envelope-handler.ts";
import type { Db } from "../src/db/db.ts";
import { migrate } from "../src/db/migrate.ts";
import { StaticEntitlement } from "../src/entitlement/entitlement.ts";
import { MemoryLogger } from "../src/observability/log.ts";
import { Metrics } from "../src/observability/metrics.ts";
import { claim } from "../src/queue/claims.ts";
import { CooldownStore } from "../src/queue/cooldown.ts";
import { archiveExhausted, preserveDead, replayArchive } from "../src/queue/dlq.ts";
import { claimNext, insertEnvelope, markDead, markDone, reapExpiredLeases } from "../src/queue/store.ts";
import { envelope } from "./support/fixtures.ts";
import { fixtureConfig, newRealPg, sleep } from "./support/harness.ts";

const URL = process.env.RELAYFILE_CONTROL_PG_TEST_URL;
const opts = { skip: URL ? false : "RELAYFILE_CONTROL_PG_TEST_URL not set" };

function consumerWith(db: Db, handler: EnvelopeHandler, cfg: Record<string, string> = {}) {
  const config = fixtureConfig({ CONSUMER_IDLE_POLL_MS: "10", ...cfg });
  return new Consumer({
    db,
    config,
    logger: new MemoryLogger(),
    metrics: new Metrics(),
    entitlement: new StaticEntitlement(true),
    cooldowns: new CooldownStore(db, { enabled: true, maxKeys: 100 }),
    relayfile: null,
    handler,
  });
}

async function bulkInsert(db: Db, lane: string, n: number): Promise<void> {
  // Set-based insert for the 10k flood (one statement, trigger still maintains depth).
  await db.query(
    `INSERT INTO webhook_envelopes (id, kind, ingress, provider, request_id, delivery_id, has_provider_delivery_id,
        payload, payload_sha256, payload_bytes, received_at, lane, workspace_id, visible_at)
     SELECT $1 || lpad(g::text, 8, '0'), 'provider-webhook', 'github', 'github', 'req-' || $1 || g, 'd-' || $1 || g, false,
            '{}'::jsonb, 'x', 2, now(), $2, $2, now()
       FROM generate_series(1, $3) g`,
    [lane.replace(/\W/g, ""), lane, n],
  );
}

async function waitFor(cond: () => Promise<boolean>, timeoutMs: number): Promise<boolean> {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    if (await cond()) return true;
    await sleep(25);
  }
  return false;
}

describe("real Postgres: claims and queue under concurrency", opts, () => {
  it("concurrent migrators do not race (advisory lock)", async () => {
    const { db, drop } = await newRealPg(URL!, 30, { migrate: false });
    const results = await Promise.all([migrate(db), migrate(db), migrate(db)]);
    assert.deepEqual(results.flat(), ["0001"], "each pending version applied exactly once across the three migrators");
    const { rows } = await db.query<{ n: number }>("SELECT count(*)::int AS n FROM schema_migrations");
    assert.equal(rows[0]!.n, 1);
    await drop();
  });

  it("handler-claim race: 50 concurrent claims on one key ⇒ exactly one winner", async () => {
    const { db, drop } = await newRealPg(URL!);
    const results = await Promise.all(Array.from({ length: 50 }, () => claim(db, { surface: "slack", dedupeId: "race", leaseSeconds: 900 })));
    assert.equal(results.filter((r) => r.claimed).length, 1);
    await drop();
  });

  it("ingress dedupe race: 30 concurrent inserts of one delivery ⇒ one envelope", async () => {
    const { db, drop } = await newRealPg(URL!);
    const results = await Promise.all(Array.from({ length: 30 }, () => insertEnvelope(db, envelope("rw_a", { providerDeliveryId: "github:same" }))));
    assert.equal(results.filter((r) => !r.duplicate).length, 1);
    const { rows } = await db.query<{ n: number }>("SELECT count(*)::int AS n FROM webhook_envelopes");
    assert.equal(rows[0]!.n, 1);
    await drop();
  });

  it("two consumers racing SKIP LOCKED: every envelope handled exactly once", async () => {
    const { db, drop } = await newRealPg(URL!);
    for (let l = 0; l < 20; l += 1) await bulkInsert(db, `rw_l${l}`, 10);
    const seen = new Map<string, number>();
    const handler: EnvelopeHandler = async (row) => {
      seen.set(row.id, (seen.get(row.id) ?? 0) + 1);
      await sleep(2);
      return { type: "ack", reason: "test" };
    };
    const a = consumerWith(db, handler);
    const b = consumerWith(db, handler);
    a.start();
    b.start();
    const drained = await waitFor(async () => (await db.query<{ n: number }>("SELECT count(*)::int AS n FROM webhook_envelopes WHERE state <> 'done'")).rows[0]!.n === 0, 20_000);
    await a.stop();
    await b.stop();
    assert.ok(drained);
    assert.equal(seen.size, 200);
    assert.ok([...seen.values()].every((n) => n === 1), "no double handling");
    await drop();
  });

  it("strict per-lane cap under 20 concurrent claimers (ruling 3)", async () => {
    const { db, drop } = await newRealPg(URL!);
    await bulkInsert(db, "rw_hot", 60);
    let inflight = 0;
    let maxInflight = 0;
    let maxDbLeased = 0;
    const handler: EnvelopeHandler = async () => {
      inflight += 1;
      maxInflight = Math.max(maxInflight, inflight);
      const { rows } = await db.query<{ n: number }>("SELECT count(*)::int AS n FROM webhook_envelopes WHERE lane = 'rw_hot' AND state = 'leased'");
      maxDbLeased = Math.max(maxDbLeased, rows[0]!.n);
      await sleep(20);
      inflight -= 1;
      return { type: "ack", reason: "test" };
    };
    const consumers = [consumerWith(db, handler, { CONSUMER_CONCURRENCY: "10" }), consumerWith(db, handler, { CONSUMER_CONCURRENCY: "10" })];
    consumers.forEach((c) => c.start());
    const drained = await waitFor(async () => (await db.query<{ n: number }>("SELECT count(*)::int AS n FROM webhook_envelopes WHERE state <> 'done'")).rows[0]!.n === 0, 30_000);
    await Promise.all(consumers.map((c) => c.stop()));
    assert.ok(drained, "lane drained");
    assert.ok(maxInflight <= 2, `in-flight per lane ${maxInflight} > 2`);
    assert.ok(maxDbLeased <= 2, `leased per lane ${maxDbLeased} > 2`);
    assert.equal(maxInflight, 2, "the cap is actually used");
    await drop();
  });

  it("fairness: a 10 000-row single-lane flood does not stop a second lane from draining", async () => {
    const { db, drop } = await newRealPg(URL!);
    await bulkInsert(db, "rw_flood", 10_000);
    await bulkInsert(db, "rw_other", 50);
    const handler: EnvelopeHandler = async () => {
      await sleep(5);
      return { type: "ack", reason: "test" };
    };
    const c = consumerWith(db, handler, { CONSUMER_CONCURRENCY: "10" });
    const started = Date.now();
    c.start();
    const ok = await waitFor(async () => (await db.query<{ n: number }>("SELECT count(*)::int AS n FROM webhook_envelopes WHERE lane = 'rw_other' AND state <> 'done'")).rows[0]!.n === 0, 20_000);
    const elapsed = Date.now() - started;
    const flood = (await db.query<{ n: number }>("SELECT count(*)::int AS n FROM webhook_envelopes WHERE lane = 'rw_flood' AND state = 'queued'")).rows[0]!.n;
    await c.stop();
    console.log(`[fairness] second lane drained in ${elapsed} ms with ${flood} flood rows still queued`);
    assert.ok(ok, "second lane drained");
    assert.ok(elapsed < 20_000);
    assert.ok(flood > 9_000, "the flood was still there: the second lane did not just wait its turn");
    await drop();
  });

  it("lease expiry takeover; the old holder is fenced out", async () => {
    const { db, drop } = await newRealPg(URL!);
    await insertEnvelope(db, envelope("rw_a"));
    const first = (await claimNext(db, { worker: "w1", leaseSeconds: 1, perLaneMax: 2 }))!;
    await sleep(1100);
    assert.deepEqual(await reapExpiredLeases(db, 5), { requeued: 1, dead: 0 });
    const second = (await claimNext(db, { worker: "w2", leaseSeconds: 60, perLaneMax: 2 }))!;
    assert.equal(second.id, first.id);
    assert.equal(second.attempts, 2);
    assert.equal(await markDone(db, first.id, "w1", { disposition: "ack" }), false);
    assert.equal(await markDone(db, second.id, "w2", { disposition: "ack" }), true);
    await drop();
  });

  it("DLQ preservation cycles, archive and replay", async () => {
    const { db, drop } = await newRealPg(URL!);
    await insertEnvelope(db, envelope("rw_a"));
    for (let cycle = 0; cycle <= 3; cycle += 1) {
      await db.query("UPDATE webhook_envelopes SET visible_at = now()");
      const row = (await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }))!;
      await markDead(db, row.id, "w", { disposition: "dead", cycle });
      await preserveDead(db, 3);
    }
    const archived = await archiveExhausted(db, 3);
    assert.equal(archived.length, 1);
    assert.equal((await replayArchive(db, { ids: [archived[0]!.id] }, "it")).length, 1);
    const { rows } = await db.query<{ state: string; attempts: number; preservation_cycle: number }>("SELECT state, attempts, preservation_cycle FROM webhook_envelopes");
    assert.deepEqual(rows, [{ state: "queued", attempts: 0, preservation_cycle: 0 }]);
    await drop();
  });
});
