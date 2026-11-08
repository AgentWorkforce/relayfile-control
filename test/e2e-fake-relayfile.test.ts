/**
 * §13 end-to-end: the scripted Nov 7 storm against an in-process fake
 * Relayfile, both flavours. One workspace answers 429 `workspace_busy` for two
 * (simulated) minutes, then recovers. Time is compressed by warping
 * `visible_at` / cool-down `until` backwards; the retry schedule, cool-down
 * windows and DLQ rules themselves are untouched.
 *
 * Writes `$TMPDIR/e2e-storm-report-<flavour>.md`.
 */
import assert from "node:assert/strict";
import { writeFile } from "node:fs/promises";
import path from "node:path";
import { describe, it } from "node:test";
import { upsertBinding } from "../src/bindings/store.ts";
import { Consumer } from "../src/consumer/consumer.ts";
import { Sweeper } from "../src/consumer/sweeper.ts";
import { RelayfileClient } from "../src/relayfile/client.ts";
import { fakeRelayfile, FIXTURE, fixtureConfig, sign, startApp } from "./support/harness.ts";

const STORM_SECONDS = 120;
const STEP_SECONDS = 15;

async function runStorm(flavour: "oss" | "cloud", cooldown: boolean) {
  let simulated = 0; // seconds since the storm started
  const fake = await fakeRelayfile({
    flavor: flavour,
    respond: (req, n) => {
      if (req.workspaceId !== "rw_storm" || simulated >= STORM_SECONDS) return { status: 202 };
      // OSS also sheds with a plain 500 now and then; cloud with 503 outbox-full.
      if (n % 7 === 0) return flavour === "oss" ? { status: 500 } : { status: 503, body: { reason: "root_event_outbox_full" } };
      return { status: 429, headers: { "retry-after": "30" }, body: { reason: "workspace_busy", details: { retryAfterSeconds: 30 } } };
    },
  });
  const config = fixtureConfig({
    RELAYFILE_BASE_URL: fake.url,
    RELAYFILE_FLAVOR: flavour,
    LANE_MAX_QUEUED: "25",
    CONSUMER_CONCURRENCY: "10",
    PER_LANE_MAX_INFLIGHT: "2",
    MAX_ATTEMPTS: "5",
    COOLDOWN_ENABLED: cooldown ? "true" : "false",
  });
  const t = await startApp({ config });
  await upsertBinding(t.db, { workspaceId: "rw_storm", provider: "notion", connectionId: "conn-storm" });
  await upsertBinding(t.db, { workspaceId: "rw_calm", provider: "notion", connectionId: "conn-calm" });

  const relayfile = new RelayfileClient({ hmacSecret: FIXTURE.relayHmac, sleep: async () => undefined });
  const consumer = new Consumer({ db: t.db, config, logger: t.logger, metrics: t.app.metrics, entitlement: t.entitlement, cooldowns: t.app.cooldowns, relayfile });
  const sweeper = new Sweeper({ db: t.db, config, logger: t.logger, metrics: t.app.metrics, cooldowns: t.app.cooldowns, depth: t.app.depth });

  try {
    const delivery = (conn: string, i: number) => {
      const body = JSON.stringify({ type: "forward", from: "notion", connectionId: conn, payload: { id: `${conn}-page-${i}`, object: "page" } });
      return { body, headers: { ...sign.nango(body), "x-nango-delivery-id": `${conn}-${i}` } };
    };

    // Phase 1: the storm workspace floods; the calm one keeps sending.
    const ingress: { conn: string; i: number; status: number; reason?: string }[] = [];
    for (let i = 0; i < 60; i += 1) {
      for (const conn of ["conn-storm", ...(i < 20 ? ["conn-calm"] : [])]) {
        const d = delivery(conn, i);
        const r = await t.post("/api/v1/webhooks/nango", d.body, d.headers);
        ingress.push({ conn, i, status: r.status, ...(r.json?.reason ? { reason: r.json.reason } : {}) });
      }
    }

    consumer.start();
    let calmDoneAt: number | null = null;
    let cooldownSeenArmed = false;
    const warp = async (seconds: number) => {
      simulated += seconds;
      await t.db.query("UPDATE webhook_envelopes SET visible_at = visible_at - make_interval(secs => $1) WHERE state = 'queued'", [seconds]);
      await t.db.query("UPDATE lane_cooldowns SET until = until - make_interval(secs => $1)", [seconds]);
    };
    const counts = async () =>
      (await t.db.query<{ lane: string; state: string; n: number }>("SELECT lane, state, count(*)::int AS n FROM webhook_envelopes GROUP BY lane, state")).rows;

    for (let step = 0; step < 200; step += 1) {
      await new Promise((r) => setTimeout(r, 60));
      if ((await t.app.cooldowns.list()).length > 0) cooldownSeenArmed = true;
      await sweeper.fast();
      await sweeper.slow();
      const c = await counts();
      const calmOpen = c.filter((r) => r.lane === "rw_calm" && r.state !== "done").reduce((a, r) => a + r.n, 0);
      const calmTotal = c.filter((r) => r.lane === "rw_calm").reduce((a, r) => a + r.n, 0);
      if (calmDoneAt === null && calmTotal > 0 && calmOpen === 0) calmDoneAt = simulated;
      // Providers keep redelivering what ingress refused with 429 (with their own backoff);
      // here once per simulated step after the storm, until each one is accepted.
      if (simulated >= STORM_SECONDS + 60) {
        for (const r of ingress.filter((x) => x.status === 429)) {
          const d = delivery(r.conn, r.i);
          r.status = (await t.post("/api/v1/webhooks/nango", d.body, d.headers)).status;
        }
        const after = await counts();
        const open = after.filter((r) => r.state !== "done").reduce((a, r) => a + r.n, 0);
        if (ingress.every((x) => x.status !== 429) && open === 0) break;
      }
      await warp(STEP_SECONDS);
    }
    await consumer.stop();
    await sweeper.slow();

    const finalCounts = await counts();
    const dead = finalCounts.filter((r) => r.state === "dead").reduce((a, r) => a + r.n, 0);
    const archived = (await t.db.query<{ n: number }>("SELECT count(*)::int AS n FROM webhook_dlq_archive")).rows[0]!.n;
    const done = await t.db.query<{ workspace_id: string; delivery_id: string; last_outcome: any }>(
      "SELECT workspace_id, delivery_id, last_outcome FROM webhook_envelopes WHERE state = 'done'",
    );
    const unwrittenAcks = done.rows.filter((r) => !fake.stored.has(JSON.stringify([r.workspace_id, "notion", r.delivery_id])));
    const outcomes = t.logger.named("consume_outcome");
    const bpOutcomes = outcomes.filter((o) => o.code === "relayfile_backpressure");
    const ingressRejected = t.logger.named("ingress_rejected");
    const accepted202 = ingress.filter((x) => x.status === 202).length;
    const report = {
      flavour,
      cooldownEnabled: cooldown,
      simulatedSeconds: simulated,
      stormSeconds: STORM_SECONDS,
      ingress: {
        sent: ingress.length,
        refused429: ingressRejected.filter((e) => e.status === 429).length,
        refusedReasons: [...new Set(ingressRejected.map((e) => e.reason))],
        finalAccepted: accepted202,
      },
      calmLaneDrainedAtSimulatedSeconds: calmDoneAt,
      consumeOutcomes: outcomes.length,
      backpressureOutcomes: bpOutcomes.length,
      backpressureOutcomesWithReason: bpOutcomes.filter((o) => typeof o.relayfileReason === "string").length,
      cooldownArmedEvents: t.logger.named("cooldown_armed").length,
      cooldownSkipEvents: t.logger.named("forward_cooldown_skip").length,
      cooldownSeenArmed,
      cooldownLanesAtEnd: (await t.app.cooldowns.list()).length,
      relayfileRequests: fake.requests.length,
      relayfileStored: fake.stored.size,
      relayfileRejectedSignatures: fake.rejected.length,
      unwrittenAcks: unwrittenAcks.length,
      dead,
      dlqArchived: archived,
      finalStates: finalCounts,
    };
    const name = `${flavour}-cooldown-${cooldown ? "on" : "off"}`;
    const file = path.join(process.env.TMPDIR ?? ".", `e2e-storm-report-${name}.md`);
    await writeFile(file, `# e2e storm report (${name})\n\n\`\`\`json\n${JSON.stringify(report, null, 2)}\n\`\`\`\n`);
    return { report, file, ingress };
  } finally {
    await consumer.stop();
    await t.close();
    await fake.close();
  }
}

for (const flavour of ["oss", "cloud"] as const) {
  for (const cooldown of [true, false]) {
    describe(`e2e: Nov 7 storm against fake Relayfile (${flavour}, cool-down ${cooldown ? "on" : "off (default)"})`, () => {
      it("ingress sheds with reasons, other lanes unaffected, cool-down armed and released, no unwritten acks, no drops, DLQ empty", async () => {
        const { report, file } = await runStorm(flavour, cooldown);
        console.log(`[e2e ${flavour} cooldown=${cooldown}] report: ${file}\n${JSON.stringify({ ...report, finalStates: undefined })}`);
        assert.ok(report.ingress.refused429 > 0, "ingress 429s once the lane cap is hit");
        assert.deepEqual(report.ingress.refusedReasons, ["ingest_backpressure:lane_queue_full"]);
        assert.ok(report.calmLaneDrainedAtSimulatedSeconds !== null && report.calmLaneDrainedAtSimulatedSeconds < STORM_SECONDS, "calm lane drains during the storm");
        if (cooldown) {
          assert.ok(report.cooldownArmedEvents > 0 && report.cooldownSeenArmed, "cool-down armed");
        } else {
          assert.equal(report.cooldownArmedEvents, 0, "default off: never armed");
          assert.equal(report.cooldownSeenArmed, false);
        }
        assert.equal(report.cooldownLanesAtEnd, 0, "cool-down released");
        assert.ok(report.backpressureOutcomes > 0);
        assert.equal(report.backpressureOutcomesWithReason, report.backpressureOutcomes, "every attempt logged with relayfileReason");
        assert.equal(report.unwrittenAcks, 0, "zero acks of unwritten envelopes");
        assert.equal(report.relayfileStored, 80, "zero drops: every one of the 80 deliveries reached Relayfile exactly once");
        assert.equal(report.ingress.finalAccepted, 80);
        assert.equal(report.dead, 0, "DLQ empty");
        assert.equal(report.dlqArchived, 0, "DLQ archive empty");
        assert.equal(report.relayfileRejectedSignatures, 0);
      });
    });
  }
}
