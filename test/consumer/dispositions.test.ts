// Ported from the cloud#6175 additions to cloud packages/web/lib/integrations/nango-webhook-route-handler.test.ts
// @ 1a59dec (+69, "Relayfile 429 backpressure"). Test names are Cloud's. Cloud's route answers a typed
// backpressure error with 503 + Retry-After and its queue redelivers; with no HTTP hop here the same
// classification is the consumer's `retry` disposition (plan §6.2): "503 + Retry-After" ≙ disposition
// `retry` with `retryAfterSeconds`, never an ack; "502 shape" ≙ `retry` on the plain schedule.
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { Consumer } from "../../src/consumer/consumer.ts";
import { detectRelayfileBackpressure, RELAYFILE_BACKPRESSURE_DEFAULT_RETRY_AFTER_SECONDS } from "../../src/consumer/failure.ts";
import { StaticEntitlement } from "../../src/entitlement/entitlement.ts";
import { MemoryLogger } from "../../src/observability/log.ts";
import { Metrics } from "../../src/observability/metrics.ts";
import { CooldownStore } from "../../src/queue/cooldown.ts";
import { insertEnvelope } from "../../src/queue/store.ts";
import { RelayfileBackpressureError } from "../../src/relayfile/backpressure.ts";
import { envelope } from "../support/fixtures.ts";
import { fixtureConfig, newPglite } from "../support/harness.ts";

/** Stand-in for the PR 2 record writer's error (Cloud mocks it the same way). */
class RelayfilePrimaryWriteError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RelayfilePrimaryWriteError";
  }
}

async function runWith(makeError: () => unknown, cooldown = false) {
  const db = await newPglite();
  const config = fixtureConfig({ COOLDOWN_ENABLED: cooldown ? "true" : "false" });
  const cooldowns = new CooldownStore(db, { enabled: config.COOLDOWN_ENABLED, maxKeys: 100 });
  const consumer = new Consumer({
    db, config, logger: new MemoryLogger(), metrics: new Metrics(), entitlement: new StaticEntitlement(true), cooldowns, relayfile: null,
    handler: async () => { throw makeError(); },
    random: () => 0,
  });
  await insertEnvelope(db, envelope("rw_a", { kind: "nango-forward", ingress: "nango", provider: "github-relay" }));
  await consumer.drain();
  const row = (await db.query<{ state: string; last_outcome: any }>("SELECT state, last_outcome FROM webhook_envelopes")).rows[0]!;
  const cooled = await cooldowns.remaining("rw_a");
  await db.close();
  return { row, cooled };
}

describe("Relayfile 429 backpressure", () => {
  const backpressure = (retryAfterMs: number | null) =>
    new RelayfileBackpressureError({ status: 429, retryAfterMs, attempts: 3, surface: "provider-writes" });

  const cases = [
    ["thrown directly", () => backpressure(4_500), 5],
    ["as RelayfilePrimaryWriteError.cause", () => Object.assign(
      new RelayfilePrimaryWriteError("Relayfile provider write failed for github/PullRequest: 1 error(s)"),
      { cause: backpressure(120_000) },
    ), 120],
    ["across a chunk boundary (name only, no constructor identity)", () => Object.assign(
      new Error("Relayfile provider-writes backpressure (429) after 3 attempt(s)"),
      { name: "RelayfileBackpressureError", retryAfterMs: 2_000 },
    ), 2],
    ["with no server delay (defaults to 60 s)", () => backpressure(null), 60],
    // The Jira native-comment path rethrows the envelope admission 429 it
    // captured from the adapter write callback (one attempt, raw server hint).
    ["from the Jira envelope admission (webhook-envelopes, one attempt)", () =>
      new RelayfileBackpressureError({ status: 429, retryAfterMs: 20_000, attempts: 1, surface: "webhook-envelopes" }), 20],
  ] as const;

  for (const [label, makeError, expectedSeconds] of cases) {
    it(`returns a retryable 503 with Retry-After when ${label}`, async () => {
      const { row } = await runWith(makeError);
      assert.equal(row.state, "queued", "retried, never acked");
      assert.equal(row.last_outcome.disposition, "retry");
      assert.equal(row.last_outcome.code, "relayfile_backpressure");
      assert.equal(row.last_outcome.retryAfterSeconds, expectedSeconds);
      assert.ok(row.last_outcome.retryDelaySeconds >= expectedSeconds, "queue delay honours Retry-After as a floor");
    });
  }

  it("a RelayfilePrimaryWriteError whose cause is not backpressure keeps the 502 shape", async () => {
    const { row, cooled } = await runWith(() => Object.assign(
      new RelayfilePrimaryWriteError("Relayfile provider write failed for github/PullRequest: 1 error(s)"),
      { cause: new Error("GitHub Relayfile provider write failed (500)") },
    ), true);
    assert.equal(row.state, "queued");
    assert.equal(row.last_outcome.disposition, "retry");
    assert.notEqual(row.last_outcome.code, "relayfile_backpressure");
    assert.equal(row.last_outcome.retryAfterSeconds, undefined);
    assert.equal(cooled, 0, "a non-backpressure failure never arms the cool-down");
  });

  // PORT (addition): detection helper contract, mirrored from Cloud's detectRelayfileBackpressure.
  it("PORT: detectRelayfileBackpressure matches instanceof, name, or cause; default is 60 s", () => {
    assert.ok(detectRelayfileBackpressure(backpressure(1)));
    assert.ok(detectRelayfileBackpressure({ name: "RelayfileBackpressureError" }));
    assert.ok(detectRelayfileBackpressure({ cause: { name: "RelayfileBackpressureError", retryAfterMs: 5 } }));
    assert.equal(detectRelayfileBackpressure(new Error("x")), undefined);
    assert.equal(detectRelayfileBackpressure(null), undefined);
    assert.equal(RELAYFILE_BACKPRESSURE_DEFAULT_RETRY_AFTER_SECONDS, 60);
  });
});
