// Ported from cloud packages/web/lib/integrations/github-webhook-admission-retry.test.ts @ 1a59dec (cloud#6175), verbatim.
import { describe, expect, it, vi } from "../support/vitest-compat.ts";

import {
  ADMISSION_MAX_RETRIES,
  ADMISSION_RETRY_BUDGET_MS,
  fetchEnvelopeAdmissionWithRetry,
  parseRetryAfterMs,
  resolveAdmissionRetryDelayMs,
} from "../../src/relayfile/admission-retry.ts";

// A real Response (so the value satisfies AdmissionRetryDeps.post's Response
// type without a cast) with a spy pinning body.cancel for cancellation counts.
const fake = (status: number, headers: Record<string, string> = {}, onCancel?: () => void) => {
  const res = new Response("fixture", { status, headers });
  const body = res.body;
  if (!body) throw new Error("fixture response has no body");
  const cancel = vi.fn(async () => { onCancel?.(); });
  Object.defineProperty(body, "cancel", { value: cancel, configurable: true });
  return { res, cancelled: () => cancel.mock.calls.length };
};

describe("parseRetryAfterMs", () => {
  it("parses delta-seconds and HTTP-date", () => {
    expect(parseRetryAfterMs("5", 0)).toBe(5_000);
    expect(parseRetryAfterMs("0", 0)).toBe(0);
    expect(parseRetryAfterMs(new Date(10_000).toUTCString(), 4_000)).toBe(6_000);
  });
  it("returns null for absent/malformed/non-finite values", () => {
    for (const v of [null, undefined, "", "   ", "1.5", "soon", "-1", "Infinity", "-Infinity", "NaN", "1e3", "0x10", "9".repeat(400)]) {
      expect(parseRetryAfterMs(v, 0)).toBeNull();
    }
  });
});

describe("resolveAdmissionRetryDelayMs", () => {
  it("defers absent / over-budget / equal-to-deadline, honors within-budget", () => {
    expect(resolveAdmissionRetryDelayMs(null, 0, 30_000)).toBeNull();
    expect(resolveAdmissionRetryDelayMs("120", 0, 30_000)).toBeNull();
    expect(resolveAdmissionRetryDelayMs("30", 0, 30_000)).toBeNull();
    expect(resolveAdmissionRetryDelayMs("3", 0, 30_000)).toBe(3_000);
  });
});

describe("fetchEnvelopeAdmissionWithRetry", () => {
  const makeDeps = (
    statuses: number[],
    headersFor: (i: number) => Record<string, string> = () => ({}),
    overshootMs = 0,
    cancelAdvanceMs = 0,
    cancelThrows = false,
  ) => {
    let i = 0;
    let clock = 0;
    const postTimes: number[] = [];
    const sleeps: number[] = [];
    const responses: Array<{ cancelled: () => number }> = [];
    const post = async () => {
      const f = fake(statuses[Math.min(i, statuses.length - 1)], headersFor(i), () => { clock += cancelAdvanceMs; if (cancelThrows) throw new Error("cancel rejected"); });
      i += 1;
      postTimes.push(clock);
      responses.push(f);
      return f.res;
    };
    return {
      postTimes,
      sleeps,
      responses,
      // `random` pinned to 0: a server hint sleeps exactly its floor (the
      // lengthening jitter is covered in relayfile-backpressure.test.ts).
      deps: { post, now: () => clock, sleep: async (ms: number) => { sleeps.push(ms); clock += ms + overshootMs; }, random: () => 0 },
    };
  };

  it("429 then 201 retries once; cancels only the replaced 429", async () => {
    const { postTimes, responses, deps } = makeDeps([429, 201], () => ({ "retry-after": "3" }));
    expect((await fetchEnvelopeAdmissionWithRetry(deps)).status).toBe(201);
    expect(postTimes.length).toBe(2);
    expect(responses[0].cancelled()).toBe(1);
    expect(responses[1].cancelled()).toBe(0); // caller cancels the final response
  });

  it("429 with an over-budget Retry-After defers: returns the 429, no retry, no cancel", async () => {
    const { postTimes, responses, deps } = makeDeps([429], () => ({ "retry-after": "120" }));
    expect((await fetchEnvelopeAdmissionWithRetry(deps)).status).toBe(429);
    expect(postTimes.length).toBe(1);
    expect(responses[0].cancelled()).toBe(0);
  });

  it("429 with absent / malformed Retry-After now retries on the jittered fallback inside the same budget (shared Relayfile backpressure policy; no longer defers immediately)", async () => {
    for (const header of [undefined, "tomorrow"]) {
      const { postTimes, sleeps, responses, deps } = makeDeps([429, 201], (): Record<string, string> => (header === undefined ? {} : { "retry-after": header }));
      expect((await fetchEnvelopeAdmissionWithRetry({ ...deps, random: () => 0.5 })).status).toBe(201);
      expect(postTimes.length).toBe(2);
      expect(sleeps).toEqual([1_000]); // fallback base 1 s, jitter centred at random=0.5
      expect(responses[0].cancelled()).toBe(1);
    }
  });

  it("409 is returned with no retry and no cancel", async () => {
    const { postTimes, responses, deps } = makeDeps([409], () => ({ "retry-after": "1" }));
    expect((await fetchEnvelopeAdmissionWithRetry(deps)).status).toBe(409);
    expect(postTimes.length).toBe(1);
    expect(responses[0].cancelled()).toBe(0);
  });

  it("exhaustion cancels each retried 429 but not the final 429", async () => {
    const { postTimes, responses, deps } = makeDeps([429], () => ({ "retry-after": "1" }));
    expect((await fetchEnvelopeAdmissionWithRetry(deps)).status).toBe(429);
    expect(ADMISSION_MAX_RETRIES).toBe(1);
    expect(postTimes.length).toBe(2); // exactly one in-place retry
    expect(responses.slice(0, -1).every((r) => r.cancelled() === 1)).toBe(true);
    expect(responses.at(-1)!.cancelled()).toBe(0);
  });

  it("shares one deadline and waits exactly the server delay", async () => {
    const { postTimes, sleeps, deps } = makeDeps([429, 201], () => ({ "retry-after": "3" }));
    expect((await fetchEnvelopeAdmissionWithRetry(deps)).status).toBe(201);
    expect(sleeps).toEqual([3_000]);
    expect(postTimes).toEqual([0, 3_000]);
    for (const start of postTimes) expect(start).toBeLessThan(ADMISSION_RETRY_BUDGET_MS);
  });

  it("429, 429 returns the second 429 after exactly two attempts: a third request is never made", async () => {
    const { postTimes, sleeps, responses, deps } = makeDeps([429, 429, 201], () => ({ "retry-after": "3" }));
    expect((await fetchEnvelopeAdmissionWithRetry(deps)).status).toBe(429);
    expect(postTimes).toEqual([0, 3_000]);
    expect(sleeps).toEqual([3_000]);
    expect(responses[0].cancelled()).toBe(1);
    expect(responses[1].cancelled()).toBe(0); // the final 429 is the caller's
  });

  it("overshooting sleep crosses the deadline: no POST and no cancel of the deferred 429", async () => {
    const { postTimes, responses, deps } = makeDeps([429], () => ({ "retry-after": "1" }), 40_000);
    expect((await fetchEnvelopeAdmissionWithRetry(deps)).status).toBe(429);
    expect(postTimes).toEqual([0]); // never retried
    expect(responses[0].cancelled()).toBe(0); // deferred, not discarded
  });

  it("a rejected cancel on the discarded 429 does not abort an eligible retry", async () => {
    const { postTimes, responses, deps } = makeDeps([429, 201], () => ({ "retry-after": "3" }), 0, 0, true);
    expect((await fetchEnvelopeAdmissionWithRetry(deps)).status).toBe(201);
    expect(postTimes).toEqual([0, 3_000]);
    expect(responses[0].cancelled()).toBe(1);
  });

  it("after a rejected cancel, a crossed deadline still stops the retry", async () => {
    const { postTimes, responses, deps } = makeDeps([429], () => ({ "retry-after": "3" }), 0, 40_000, true);
    expect((await fetchEnvelopeAdmissionWithRetry(deps)).status).toBe(429);
    expect(postTimes).toEqual([0]);
    expect(responses[0].cancelled()).toBe(1);
  });

  it("slow cancel consumes the remaining budget: no POST after cancel", async () => {
    // 3 s sleep leaves ~27 s; the cancel then burns 40 s, so the post-cancel
    // guard must trip and the deferred 429 returned without a second POST.
    const { postTimes, responses, deps } = makeDeps([429], () => ({ "retry-after": "3" }), 0, 40_000);
    expect((await fetchEnvelopeAdmissionWithRetry(deps)).status).toBe(429);
    expect(postTimes).toEqual([0]); // never retried after the slow cancel
    expect(responses[0].cancelled()).toBe(1); // cancelled by the loop; caller recancels (idempotent)
  });
});
