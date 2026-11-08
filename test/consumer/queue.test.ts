// Ported from the cloud#6175 additions to cloud packages/webhook-worker/test/queue.test.ts @ 1a59dec
// (+148, describe "forward redelivery backoff (Relayfile 429 amplification)"). Test names are Cloud's.
// Cloud's worker forwards to cloud-web and reads the HTTP answer; here the consumer handles the
// envelope itself, so each cloud-web answer maps to the error the handler raises:
//   503 + Retry-After (relayfile_backpressure)  → RelayfileBackpressureError(retryAfterMs)
//   header-less 502                             → RelayfileUnexpectedStatusError(502)
//   forward timeout                             → the handler deadline
//   503 + Retry-After without the code          → RelayfileBackpressureError (header is the hint)
//   permanent 422                               → TerminalHandlerError (ack_terminal)
// `message.retry({delaySeconds})` ≙ the row's `retryDelaySeconds` / `visible_at`; `message.ack` ≙ state done.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Consumer } from "../../src/consumer/consumer.ts";
import type { EnvelopeHandler } from "../../src/consumer/envelope-handler.ts";
import { StaticEntitlement } from "../../src/entitlement/entitlement.ts";
import { MemoryLogger } from "../../src/observability/log.ts";
import { Metrics } from "../../src/observability/metrics.ts";
import { CooldownStore } from "../../src/queue/cooldown.ts";
import { insertEnvelope } from "../../src/queue/store.ts";
import { RelayfileBackpressureError } from "../../src/relayfile/backpressure.ts";
import { RelayfileUnexpectedStatusError, TerminalHandlerError } from "../../src/relayfile/client.ts";
import { envelope } from "../support/fixtures.ts";
import { fixtureConfig, newPglite } from "../support/harness.ts";

type Outcome = { state: string; disposition: string; delaySeconds: number | undefined; visibleInSeconds: number };

async function process(handler: EnvelopeHandler, opts: { attempts?: number; random?: number; deadlineMs?: number } = {}): Promise<Outcome> {
  const db = await newPglite();
  const config = fixtureConfig({ MAX_ATTEMPTS: "10" });
  const consumer = new Consumer({
    db, config, logger: new MemoryLogger(), metrics: new Metrics(), entitlement: new StaticEntitlement(true),
    cooldowns: new CooldownStore(db, { enabled: false, maxKeys: 10 }), relayfile: null, handler,
    random: () => opts.random ?? 0,
    ...(opts.deadlineMs ? { deadlineMs: () => opts.deadlineMs! } : {}),
  });
  await insertEnvelope(db, envelope("rw_a", { kind: "nango-forward", ingress: "nango", provider: "notion" }));
  // message.attempts is 1-based at delivery; the claim increments, so start one below.
  if (opts.attempts) await db.query("UPDATE webhook_envelopes SET attempts = $1", [opts.attempts - 1]);
  await consumer.drain();
  const { rows } = await db.query<{ state: string; last_outcome: any; v: number }>(
    "SELECT state, last_outcome, round(EXTRACT(EPOCH FROM (visible_at - now())))::int AS v FROM webhook_envelopes",
  );
  await db.close();
  const r = rows[0]!;
  return { state: r.state, disposition: r.last_outcome.disposition, delaySeconds: r.last_outcome.retryDelaySeconds, visibleInSeconds: r.v };
}

function expectForwardRetryDelayWithin(o: Outcome, minSeconds: number, maxSeconds: number) {
  assert.equal(o.disposition, "retry");
  assert.ok(Number.isInteger(o.delaySeconds), "integer delay");
  assert.ok(o.delaySeconds! >= minSeconds && o.delaySeconds! <= maxSeconds, `${o.delaySeconds} not in [${minSeconds}, ${maxSeconds}]`);
  assert.ok(Math.abs(o.visibleInSeconds - o.delaySeconds!) <= 2, "visible_at carries the delay");
  return o.delaySeconds!;
}

const throwing = (e: () => unknown): EnvelopeHandler => async () => { throw e(); };

describe("forward redelivery backoff (Relayfile 429 amplification)", () => {
  it("honours a cloud-web 503 Retry-After (Relayfile backpressure) and does not ack", async () => {
    const o = await process(throwing(() => new RelayfileBackpressureError({ status: 429, retryAfterMs: 120_000, attempts: 2, surface: "webhook-envelopes" })), { random: Math.random() });
    expectForwardRetryDelayWithin(o, 120, 900);
    assert.notEqual(o.state, "done");
  });

  it("grows the delay with message.attempts for a header-less 502 (attempt 3 -> [240, 300])", async () => {
    const o = await process(throwing(() => new RelayfileUnexpectedStatusError(502, "webhook-envelopes", null)), { attempts: 3, random: Math.random() });
    expectForwardRetryDelayWithin(o, 240, 300);
    assert.notEqual(o.state, "done");
  });

  it("spreads a burst: random 0 and 0.999 give the schedule's lower and upper edge", async () => {
    const e = () => new RelayfileUnexpectedStatusError(502, "webhook-envelopes", null);
    const low = await process(throwing(e), { attempts: 2, random: 0 });
    const high = await process(throwing(e), { attempts: 2, random: 0.999 });
    assert.equal(low.delaySeconds, 120);
    assert.equal(high.delaySeconds, 150);
  });

  it("uses the schedule for a forward timeout", async () => {
    const hang: EnvelopeHandler = (_row, ctx) => new Promise((_, reject) => ctx.signal.addEventListener("abort", () => reject(new Error("aborted"))));
    const o = await process(hang, { attempts: 4, random: Math.random(), deadlineMs: 30 });
    expectForwardRetryDelayWithin(o, 480, 600);
    assert.notEqual(o.state, "done");
  });

  it("honours a Retry-After longer than the 900 s schedule cap as a hard minimum (lengthened by up to +25 %, never shortened)", async () => {
    const e = () => new RelayfileBackpressureError({ status: 429, retryAfterMs: 3_600_000, attempts: 1, surface: "webhook-envelopes" });
    const floor = await process(throwing(e), { random: 0 });
    const ceiling = await process(throwing(e), { random: 0.999 });
    // The server hint is the floor; the same positive-only jitter spreads a burst above it.
    assert.equal(floor.delaySeconds, 3600);
    expectForwardRetryDelayWithin(ceiling, 3601, 4500);
    assert.notEqual(floor.state, "done");
    assert.notEqual(ceiling.state, "done");
  });

  it("caps a Retry-After at the Cloudflare 24 h delay limit", async () => {
    const o = await process(throwing(() => new RelayfileBackpressureError({ status: 429, retryAfterMs: 172_800_000, attempts: 1, surface: "webhook-envelopes" })));
    assert.equal(o.delaySeconds, 86_400);
    assert.notEqual(o.state, "done");
  });

  it("still acks a permanent 422 without scheduling a retry", async () => {
    const o = await process(throwing(() => new TerminalHandlerError("relayfile_rejected_422", 422)), { attempts: 5 });
    assert.equal(o.state, "done");
    assert.equal(o.disposition, "ack_terminal");
    assert.equal(o.delaySeconds, undefined);
  });
});
