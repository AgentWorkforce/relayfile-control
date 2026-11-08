// Ported from cloud packages/web/lib/integrations/github-webhook-ingest.admission-retry.test.ts @ 1a59dec
// (cloud#6175). Test names are Cloud's. Cloud drives createGitHubWebhookIngest().send() under
// vi.mock + fake timers; here the same wiring is RelayfileClient.postEnvelope() with an injected
// fetch, clock and sleep. Assertions that differ in mechanism are marked PORT.
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { afterEach, beforeEach, describe, it } from "node:test";
import { RelayfileBackpressureError } from "../../src/relayfile/backpressure.ts";
import { RelayfileClient, type WebhookEnvelopeBody } from "../../src/relayfile/client.ts";

const SECRET = "fixture-admission-secret";

function fakeResponse(status: number, headers: Record<string, string> = {}) {
  const res = new Response("fixture", { status, headers });
  const body = res.body!;
  let cancels = 0;
  const realCancel = body.cancel.bind(body);
  Object.defineProperty(body, "cancel", { value: async (r?: unknown) => { cancels += 1; return realCancel(r); }, configurable: true });
  return { res, cancelled: () => cancels };
}

const envelope: WebhookEnvelopeBody = {
  envelopeId: "github_fixture",
  workspaceId: "rw_1",
  provider: "github",
  deliveryId: "del-1",
  receivedAt: "2026-01-01T00:00:00.000Z",
  correlationId: "corr-1",
  payload: { event_type: "file.created", path: "/github/repos/AgentWorkforce/cloud/issues/1__x/meta.json", content: "{}", providerEventType: "issue_comment.created" },
};

type Call = { url: string; init: RequestInit & { headers: Record<string, string> } };

let clock = Date.parse("2026-01-01T00:00:00.000Z");
let calls: Call[] = [];
let timeouts: number[] = [];
const realTimeout = AbortSignal.timeout.bind(AbortSignal);

function client(responses: (() => Response | Promise<Response>)[], random = () => 0) {
  let i = 0;
  const fetchImpl = (async (url: string, init: Call["init"]) => {
    calls.push({ url, init });
    const next = responses[Math.min(i, responses.length - 1)]!;
    i += 1;
    return next();
  }) as unknown as typeof fetch;
  return new RelayfileClient({
    hmacSecret: SECRET,
    fetch: fetchImpl,
    now: () => clock,
    sleep: async (ms) => { clock += ms; },
    random,
  });
}

beforeEach(() => {
  clock = Date.parse("2026-01-01T00:00:00.000Z");
  calls = [];
  timeouts = [];
  // call-through spy: keeps the real signal
  (AbortSignal as { timeout: (ms: number) => AbortSignal }).timeout = (ms: number) => {
    timeouts.push(ms);
    return realTimeout(ms);
  };
});
afterEach(() => {
  (AbortSignal as { timeout: (ms: number) => AbortSignal }).timeout = realTimeout;
});

describe("createGitHubWebhookIngest admission retry (consumer wiring)", () => {
  it("14s first attempt + 5s Retry-After + 201 inside the 30s budget; strictly newer signed timestamp; cancels replaced 429 only", async () => {
    const t0 = clock;
    const first = fakeResponse(429, { "retry-after": "5" });
    const second = fakeResponse(201);
    const c = client([
      () => {
        clock += 14_000; // simulate the measured ~14s attempt
        return first.res;
      },
      () => second.res,
    ]);
    const result = await c.postEnvelope("https://relayfile.example", envelope);

    assert.equal(result.status, 201); // PORT: Cloud returns {status:"queued"}; this client reports the HTTP status
    assert.equal(calls.length, 2);

    // Signer receives the full, correct arguments (timestamp, same body, secret).
    const [a, b] = calls as [Call, Call];
    const ts1 = a.init.headers["x-relay-timestamp"]!;
    const ts2 = b.init.headers["x-relay-timestamp"]!;
    assert.equal(a.init.headers["x-relay-signature"], createHmac("sha256", SECRET).update(`${ts1}\n${a.init.body}`).digest("hex"));
    assert.equal(b.init.headers["x-relay-signature"], createHmac("sha256", SECRET).update(`${ts2}\n${b.init.body}`).digest("hex"));
    assert.equal(a.init.body, b.init.body);

    // Strictly newer timestamp; total 19s (14 s attempt + the 5 s hint as a
    // hard minimum at the jitter floor) inside the 30s retry-start budget.
    assert.ok(Date.parse(ts2) > Date.parse(ts1));
    assert.equal(Date.parse(ts2) - t0, 19_000);
    assert.equal(Date.parse(ts2) - Date.parse(ts1), 19_000);

    // Same durable request; manual redirect; distinct per-attempt signals.
    assert.equal(JSON.parse(String(a.init.body)).envelopeId, JSON.parse(String(b.init.body)).envelopeId);
    assert.equal(a.init.headers["x-correlation-id"], "corr-1");
    assert.equal(b.init.headers["x-correlation-id"], "corr-1");
    assert.equal(a.init.redirect, "manual");
    assert.equal(b.init.redirect, "manual");
    assert.notEqual(a.init.signal, b.init.signal);
    assert.ok(a.init.signal instanceof AbortSignal);

    // Per-attempt 15s bound applied to BOTH posts (not just one of them).
    assert.deepEqual(timeouts, [15_000, 15_000]);

    // The replaced 429 is cancelled exactly once by the loop.
    assert.equal(first.cancelled(), 1);
    assert.equal(second.res.bodyUsed, true); // PORT: the client consumes the final 2xx body (reads `status`) instead of cancelling it
  });

  it("defers to the durable queue when Retry-After exceeds the budget (single attempt, cancelled once); the final 429 is now a typed RelayfileBackpressureError so the route answers 503 + Retry-After", async () => {
    const only = fakeResponse(429, { "retry-after": "120" });
    const c = client([() => only.res]);
    await assert.rejects(c.postEnvelope("https://relayfile.example", envelope), (error: unknown) => {
      assert.ok(error instanceof RelayfileBackpressureError);
      assert.equal(error.name, "RelayfileBackpressureError");
      assert.equal(error.status, 429);
      assert.equal(error.retryAfterMs, 120_000);
      assert.equal(error.attempts, 1);
      assert.equal(error.surface, "webhook-envelopes");
      return true;
    });
    assert.equal(calls.length, 1);
    assert.equal(only.cancelled(), 1);
  });

  it("throws a typed RelayfileBackpressureError for the durable queue after exhausting the retry budget on a final 429 (was a generic Error)", async () => {
    const c = client([() => fakeResponse(429, { "retry-after": "1" }).res]);
    await assert.rejects(c.postEnvelope("https://relayfile.example", envelope), RelayfileBackpressureError);
    assert.equal(calls.length, 2); // 1 + ADMISSION_MAX_RETRIES (exactly one in-place retry)
  });

  it("treats a 409 (duplicate envelope) as accepted with no retry", async () => {
    const dup = fakeResponse(409);
    const c = client([() => dup.res]);
    const result = await c.postEnvelope("https://relayfile.example", envelope);
    assert.equal(result.outcome, "duplicate");
    assert.equal(calls.length, 1);
    assert.equal(dup.cancelled(), 1);
  });
});
