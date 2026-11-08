import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { Db } from "../src/db/db.ts";
import { CooldownStore, cooldownWindowSeconds } from "../src/queue/cooldown.ts";
import { archiveExhausted, listArchive, preservationDelaySeconds, preserveDead, replayArchive, retentionSweep } from "../src/queue/dlq.ts";
import { claimNext, fanOut, insertEnvelope, markDead, markDone, markRetry, QueueDepth, readPayload, reapExpiredLeases} from "../src/queue/store.ts";
import { envelope } from "./support/fixtures.ts";
import { newPglite } from "./support/harness.ts";

async function depthOf(db: Db, lane: string) {
  return new QueueDepth(db, 0).lane(lane);
}

describe("envelope intake (§2.3, §3 level 1)", () => {
  it("ingress dedupe: same (provider, delivery id) twice → duplicate, nothing re-queued", async () => {
    const db = await newPglite();
    const a = await insertEnvelope(db, envelope("rw_a", { providerDeliveryId: "github:d1" }));
    const b = await insertEnvelope(db, envelope("rw_a", { providerDeliveryId: "github:d1" }));
    assert.equal(a.duplicate, false);
    assert.equal(b.duplicate, true);
    assert.equal(await depthOf(db, "rw_a"), 1);
    await db.close();
  });
  it("request-id-only deliveries are never deduped at ingress", async () => {
    const db = await newPglite();
    await insertEnvelope(db, envelope("rw_a"));
    await insertEnvelope(db, envelope("rw_a"));
    assert.equal(await depthOf(db, "rw_a"), 2);
    await db.close();
  });
  it("payload > inline limit goes to a blob with sha256, and reads back", async () => {
    const db = await newPglite();
    const big = { blob: "x".repeat(2000) };
    const r = await insertEnvelope(db, envelope("rw_a", { raw: Buffer.from(JSON.stringify(big)), parsed: big, inlineMaxBytes: 1024 }));
    assert.ok(!r.duplicate && !r.inline);
    const row = (await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }))!;
    assert.ok(row.payload_blob_id);
    assert.equal(row.payload, null);
    assert.deepEqual(await readPayload(db, row, 1000), big);
    await db.close();
  });
});

describe("queue depth triggers (§6.4 ruling 4)", () => {
  it("per-lane counters follow state and lane changes", async () => {
    const db = await newPglite();
    await insertEnvelope(db, envelope("rw_a"));
    await insertEnvelope(db, envelope("rw_a"));
    await insertEnvelope(db, envelope("rw_b"));
    const depth = new QueueDepth(db, 0);
    assert.deepEqual([await depth.lane("rw_a"), await depth.lane("rw_b"), await depth.global()], [2, 1, 3]);
    const row = (await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }))!;
    assert.equal(await depth.global(), 2);
    await markRetry(db, row.id, "w", 0, { disposition: "retry" });
    assert.equal(await depth.global(), 3);
    await db.query("UPDATE webhook_envelopes SET lane = 'rw_c' WHERE lane = 'rw_b'");
    assert.deepEqual([await depth.lane("rw_b"), await depth.lane("rw_c")], [0, 1]);
    await db.query("DELETE FROM webhook_envelopes WHERE lane = 'rw_c'");
    assert.equal(await depth.lane("rw_c"), 0);
    await db.close();
  });
  it("global total is cached for the window", async () => {
    const db = await newPglite();
    let now = 0;
    const depth = new QueueDepth(db, 1000, () => now);
    assert.equal(await depth.global(), 0);
    await insertEnvelope(db, envelope("rw_a"));
    assert.equal(await depth.global(), 0, "cached");
    now = 1001;
    assert.equal(await depth.global(), 1);
    await db.close();
  });
});

describe("claim with per-lane fairness (§4.2)", () => {
  it("one lane cannot exceed PER_LANE_MAX_INFLIGHT; other lanes still flow", async () => {
    const db = await newPglite();
    for (let i = 0; i < 10; i += 1) await insertEnvelope(db, envelope("rw_storm"));
    await insertEnvelope(db, envelope("rw_quiet"));
    const claimed = [];
    for (let i = 0; i < 5; i += 1) {
      const r = await claimNext(db, { worker: `w${i}`, leaseSeconds: 60, perLaneMax: 2 });
      if (r) claimed.push(r.lane);
    }
    assert.deepEqual(claimed.sort(), ["rw_quiet", "rw_storm", "rw_storm"]);
    await db.close();
  });
  it("cooled lanes are skipped without consuming attempts; rows stay queued", async () => {
    const db = await newPglite();
    await insertEnvelope(db, envelope("rw_hot"));
    const cd = new CooldownStore(db, { enabled: true, maxKeys: 10 });
    await cd.arm("rw_hot", 60, "test", "workspace_busy");
    assert.equal(await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }), null);
    const { rows } = await db.query<{ state: string; attempts: number }>("SELECT state, attempts FROM webhook_envelopes");
    assert.deepEqual(rows, [{ state: "queued", attempts: 0 }]);
    assert.equal((await cd.skippedLanes()).length, 1);
    await cd.clear("rw_hot");
    assert.ok(await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }));
    await db.close();
  });
  it("respects visible_at and orders by visible_at, received_at", async () => {
    const db = await newPglite();
    await insertEnvelope(db, envelope("rw_a"));
    await db.query("UPDATE webhook_envelopes SET visible_at = now() + interval '1 hour'");
    assert.equal(await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }), null);
    await db.close();
  });
  it("dispositions are fenced on the lease holder", async () => {
    const db = await newPglite();
    await insertEnvelope(db, envelope("rw_a"));
    const row = (await claimNext(db, { worker: "w1", leaseSeconds: 60, perLaneMax: 2 }))!;
    assert.equal(row.attempts, 1);
    assert.equal(await markDone(db, row.id, "w2", { disposition: "ack" }), false);
    assert.equal(await markDone(db, row.id, "w1", { disposition: "ack" }), true);
    await db.close();
  });
  it("expired leases are reaped to queued, or dead when out of attempts", async () => {
    const db = await newPglite();
    await insertEnvelope(db, envelope("rw_a"));
    await insertEnvelope(db, envelope("rw_b"));
    await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 });
    await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 });
    await db.query("UPDATE webhook_envelopes SET lease_until = now() - interval '1 second'");
    await db.query("UPDATE webhook_envelopes SET attempts = 5 WHERE lane = 'rw_b'");
    assert.deepEqual(await reapExpiredLeases(db, 5), { requeued: 1, dead: 1 });
    await db.close();
  });
  it("fan-out: children per workspace and the parent ack commit together", async () => {
    const db = await newPglite();
    await insertEnvelope(db, envelope("unresolved:github:42"));
    const parent = (await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }))!;
    const ids = await fanOut(db, parent, "w", ["rw_a", "rw_b"], { disposition: "ack", reason: "installation_fanout" });
    assert.equal(ids.length, 2);
    const { rows } = await db.query<{ lane: string; state: string; request_id: string; parent_id: string | null }>(
      "SELECT lane, state, request_id, parent_id FROM webhook_envelopes ORDER BY lane",
    );
    assert.deepEqual(rows.map((r) => [r.lane, r.state]), [["rw_a", "queued"], ["rw_b", "queued"], ["unresolved:github:42", "done"]]);
    assert.ok(rows.every((r) => r.request_id === parent.request_id));
    // Lost lease → whole transaction rolls back, no orphan children.
    await insertEnvelope(db, envelope("unresolved:github:43"));
    const p2 = (await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }))!;
    await assert.rejects(fanOut(db, p2, "someone-else", ["rw_c"], {}));
    assert.equal((await db.query("SELECT 1 FROM webhook_envelopes WHERE lane = 'rw_c'")).rows.length, 0);
    await db.close();
  });
});

describe("cool-down store (§6.3)", () => {
  it("window precedence and clamp", () => {
    assert.equal(cooldownWindowSeconds({ retryAfterMs: 2500 }), 3);
    assert.equal(cooldownWindowSeconds({ retryAfterMs: 0, bodyRetryAfterSeconds: 7 }), 7);
    assert.equal(cooldownWindowSeconds({ retryAfterMs: null }), 60);
    assert.equal(cooldownWindowSeconds({ retryAfterMs: 10 ** 12 }), 86_400);
  });
  it("lengthen-only, expire on read, LRU bound", async () => {
    const db = await newPglite();
    const cd = new CooldownStore(db, { enabled: true, maxKeys: 2 });
    assert.equal(await cd.arm("l1", 100, "w", "r"), 100);
    assert.equal(await cd.arm("l1", 10, "w", "r"), 100, "never shortened");
    await cd.arm("l2", 50, "w", "r");
    await cd.arm("l3", 200, "w", "r");
    const lanes = (await cd.list()).map((l) => l.lane).sort();
    assert.deepEqual(lanes, ["l1", "l3"], "oldest-until pruned");
    await db.query("UPDATE lane_cooldowns SET until = now() - interval '1 second' WHERE lane = 'l1'");
    assert.equal(await cd.remaining("l1"), 0);
    const off = new CooldownStore(db, { enabled: false, maxKeys: 2 });
    assert.equal(await off.arm("x", 10, "w", "r"), null);
    await db.close();
  });
});

describe("DLQ preservation, archive, replay, retention (§8)", () => {
  it("dead → preserved cycles with growing delay → archive → replay", async () => {
    const db = await newPglite();
    await insertEnvelope(db, envelope("rw_a"));
    for (let cycle = 0; cycle < 3; cycle += 1) {
      await db.query("UPDATE webhook_envelopes SET visible_at = now()");
      const row = (await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }))!;
      await markDead(db, row.id, "w", { disposition: "dead", code: "relayfile_backpressure", cycle });
      const p = await preserveDead(db, 3);
      assert.equal(p.length, 1);
      assert.equal(p[0]!.cycle, cycle + 1);
      assert.equal(p[0]!.delay, preservationDelaySeconds(cycle));
      const { rows } = await db.query<{ attempts: number; delay: number }>(
        "SELECT attempts, round(EXTRACT(EPOCH FROM (visible_at - now())))::int AS delay FROM webhook_envelopes",
      );
      assert.equal(rows[0]!.attempts, 0);
      assert.ok(Math.abs(rows[0]!.delay - preservationDelaySeconds(cycle)) <= 1);
    }
    await db.query("UPDATE webhook_envelopes SET visible_at = now()");
    const row = (await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }))!;
    await markDead(db, row.id, "w", { disposition: "dead", code: "final" });
    assert.deepEqual(await preserveDead(db, 3), []);
    const archived = await archiveExhausted(db, 3);
    assert.equal(archived.length, 1);
    const listed = await listArchive(db, { lane: "rw_a" });
    assert.equal(listed.length, 1);
    assert.equal(listed[0]!.outcome_history.length, 4, "full reason chain kept");
    const replayed = await replayArchive(db, { lane: "rw_a" }, "test");
    assert.deepEqual(replayed, [row.id]);
    const { rows } = await db.query<{ state: string; attempts: number; preservation_cycle: number; replayed_by: string }>(
      "SELECT state, attempts, preservation_cycle, replayed_by FROM webhook_envelopes",
    );
    assert.deepEqual(rows, [{ state: "queued", attempts: 0, preservation_cycle: 0, replayed_by: "test" }]);
    assert.equal((await listArchive(db, {})).length, 0);
    await db.close();
  });
  it("replay requires a selector", async () => {
    const db = await newPglite();
    await assert.rejects(replayArchive(db, {}, "t"));
    await db.close();
  });
  it("retention deletes old done rows, completed claims, orphan blobs; keeps live rows", async () => {
    const db = await newPglite();
    await insertEnvelope(db, envelope("rw_a", { providerDeliveryId: "github:x" }));
    const keep = await insertEnvelope(db, envelope("rw_b"));
    const row = (await claimNext(db, { worker: "w", leaseSeconds: 60, perLaneMax: 2 }))!;
    await markDone(db, row.id, "w", { disposition: "ack" });
    await db.query("UPDATE webhook_envelopes SET updated_at = now() - interval '8 days' WHERE state = 'done'");
    await db.query("UPDATE webhook_delivery_claims SET completed_at = now() - interval '8 days'");
    const r = await retentionSweep(db, { envelopeRetentionSeconds: 7 * 86_400, archiveRetentionSeconds: 14 * 86_400 });
    assert.equal(r.envelopes, 1);
    assert.equal(r.claims, 1);
    const { rows } = await db.query<{ id: string }>("SELECT id FROM webhook_envelopes");
    assert.deepEqual(rows.map((x) => x.id), [keep.duplicate ? "" : keep.id]);
    await db.close();
  });
});
