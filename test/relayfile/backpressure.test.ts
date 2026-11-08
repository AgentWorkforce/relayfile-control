// Ported from cloud packages/web/lib/integrations/relayfile-backpressure.test.ts @ 1a59dec (cloud#6175).
// Cloud's tests are verbatim (vitest → ./support/vitest-compat.ts); additions are marked PORT.
import { describe, expect, it, vi } from "../support/vitest-compat.ts";

import {
  RELAYFILE_BACKPRESSURE_FALLBACK,
  RELAYFILE_BACKPRESSURE_HINT_MAX_BYTES,
  RELAYFILE_BACKPRESSURE_HINT_READ_TIMEOUT_MS,
  RELAYFILE_BACKPRESSURE_SERVER_HINT_JITTER_RATIO,
  RelayfileBackpressureError,
  RelayfileServerError,
  fetchWithRelayfileBackpressure,
  resolveRelayfileBackpressureDelayMs,
} from "../../src/relayfile/backpressure.ts";

const { baseMs, maxMs, jitterRatio } = RELAYFILE_BACKPRESSURE_FALLBACK;
const hintJitterRatio = RELAYFILE_BACKPRESSURE_SERVER_HINT_JITTER_RATIO;

function respond(status: number, init: { headers?: Record<string, string>; body?: string } = {}) {
  return new Response(init.body ?? "fixture", { status, headers: init.headers });
}

function jsonBody(value: unknown) {
  return { headers: { "content-type": "application/json" }, body: JSON.stringify(value) };
}

/**
 * A pull-only body (highWaterMark 0: nothing is pulled until someone reads)
 * that serves `text` in `chunkSize` slices and counts what was pulled. With
 * `stallAfterBytes`, it stops producing (pull never settles) past that point.
 */
function countingBody(text: string, chunkSize: number, opts: { stallAfterBytes?: number } = {}) {
  const bytes = new TextEncoder().encode(text);
  const stats = { pulls: 0, pulledBytes: 0, cancelled: false };
  const stream = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (opts.stallAfterBytes !== undefined && stats.pulledBytes >= opts.stallAfterBytes) {
        return new Promise<void>(() => {});
      }
      if (stats.pulledBytes >= bytes.byteLength) {
        controller.close();
        return;
      }
      const chunk = bytes.slice(stats.pulledBytes, stats.pulledBytes + chunkSize);
      stats.pulls += 1;
      stats.pulledBytes += chunk.byteLength;
      controller.enqueue(chunk);
    },
    cancel() {
      stats.cancelled = true;
    },
  }, { highWaterMark: 0 });
  return { stream, stats, totalBytes: bytes.byteLength };
}

/** A JSON 429 whose hint sits at the START, padded to `size` bytes with a body sentinel. */
function oversizedHintJson(retryAfterSeconds: number, size: number, sentinel = "SENTINEL-OVERSIZED-BODY-c0d1") {
  const head = JSON.stringify({ code: "workspace_busy", details: { retryAfterSeconds }, message: sentinel, padding: "" });
  return head.slice(0, -2) + "x".repeat(Math.max(0, size - head.length)) + "\"}";
}

/**
 * Deterministic clock/sleep harness; `post` returns the scripted responses in
 * order. `random` is pinned to 0 so a server hint sleeps exactly its floor
 * (the lengthening jitter is covered by its own tests); spread an explicit
 * `random` after the harness to override.
 */
function harness(script: Array<() => Response>, opts: { overshootMs?: number } = {}) {
  let clock = 0;
  let i = 0;
  const sleeps: number[] = [];
  const postTimes: number[] = [];
  const post = vi.fn(async () => {
    postTimes.push(clock);
    const next = script[Math.min(i, script.length - 1)];
    i += 1;
    return next();
  });
  return {
    post,
    sleeps,
    postTimes,
    now: () => clock,
    sleep: async (ms: number) => { sleeps.push(ms); clock += ms + (opts.overshootMs ?? 0); },
    random: () => 0,
  };
}

describe("resolveRelayfileBackpressureDelayMs", () => {
  // random 0.5 centres the fallback jitter; `floor` pins a server hint to its minimum.
  const base = { attempt: 0, nowMs: 0, deadlineMs: 30_000, random: () => 0.5 };
  const floor = { random: () => 0 };
  const ceiling = { random: () => 0.999999 };

  it("honours Retry-After delta-seconds", () => {
    expect(resolveRelayfileBackpressureDelayMs({ ...base, ...floor, retryAfterHeader: "5" })).toBe(5_000);
  });

  it("honours an IMF-fixdate Retry-After", () => {
    expect(resolveRelayfileBackpressureDelayMs({
      ...base,
      ...floor,
      nowMs: 4_000,
      deadlineMs: 34_000,
      retryAfterHeader: new Date(10_000).toUTCString(),
    })).toBe(6_000);
  });

  it("a positive server hint is a hard minimum, lengthened by a bounded positive-only jitter", () => {
    // Header, IMF-fixdate and body hints all share the same [hint, hint*(1+ratio)] band.
    const hints = [
      { retryAfterHeader: "5" },
      { retryAfterHeader: new Date(5_000).toUTCString() },
      { retryAfterHeader: null, bodyRetryAfterSeconds: 5 },
    ];
    for (const hint of hints) {
      const low = resolveRelayfileBackpressureDelayMs({ ...base, ...hint, ...floor });
      const mid = resolveRelayfileBackpressureDelayMs({ ...base, ...hint, random: () => 0.5 });
      const high = resolveRelayfileBackpressureDelayMs({ ...base, ...hint, ...ceiling });
      expect(low).toBe(5_000); // never below the hint
      expect(mid).toBe(Math.round(5_000 * (1 + hintJitterRatio * 0.5)));
      expect(high).toBeGreaterThan(5_000);
      expect(high).toBeLessThanOrEqual(Math.round(5_000 * (1 + hintJitterRatio)));
      // A common 5 s hint yields distinct allowed delays for distinct samples.
      expect(new Set([low, mid, high]).size).toBe(3);
    }
    // A misbehaving `random` cannot shorten the hint or over-lengthen it.
    expect(resolveRelayfileBackpressureDelayMs({ ...base, retryAfterHeader: "5", random: () => -1 })).toBe(5_000);
    expect(resolveRelayfileBackpressureDelayMs({ ...base, retryAfterHeader: "5", random: () => Number.NaN })).toBe(5_000);
    expect(resolveRelayfileBackpressureDelayMs({ ...base, retryAfterHeader: "5", random: () => 7 }))
      .toBe(Math.round(5_000 * (1 + hintJitterRatio)));
  });

  it("a zero / already-elapsed server hint uses the jittered fallback, never an immediate retry", () => {
    const zeroHints = [
      { retryAfterHeader: "0" },
      { retryAfterHeader: new Date(4_000).toUTCString(), nowMs: 10_000, deadlineMs: 40_000 }, // past date
      { retryAfterHeader: null, bodyRetryAfterSeconds: 0 },
    ];
    for (const hint of zeroHints) {
      expect(resolveRelayfileBackpressureDelayMs({ ...base, ...hint })).toBe(baseMs);
      expect(resolveRelayfileBackpressureDelayMs({ ...base, ...hint, ...floor })).toBe(Math.round(baseMs * (1 - jitterRatio)));
      expect(resolveRelayfileBackpressureDelayMs({ ...base, ...hint, attempt: 2 })).toBe(4 * baseMs);
    }
  });

  it("malformed headers (1.5, 1s, 400 digits) fall back to the jittered schedule", () => {
    for (const header of ["1.5", "1s", "9".repeat(400)]) {
      expect(resolveRelayfileBackpressureDelayMs({ ...base, retryAfterHeader: header })).toBe(baseMs);
    }
  });

  it("absent header uses the body's retryAfterSeconds", () => {
    expect(resolveRelayfileBackpressureDelayMs({ ...base, ...floor, retryAfterHeader: null, bodyRetryAfterSeconds: 7 })).toBe(7_000);
  });

  it("ignores a negative / non-finite body hint", () => {
    for (const value of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(resolveRelayfileBackpressureDelayMs({ ...base, retryAfterHeader: null, bodyRetryAfterSeconds: value })).toBe(baseMs);
    }
  });

  it("absent both: fallback stays within base*2^attempt*(1±jitter) and the exponential term caps at maxMs", () => {
    for (const attempt of [0, 1, 2]) {
      const nominal = baseMs * 2 ** attempt;
      const low = resolveRelayfileBackpressureDelayMs({ ...base, attempt, deadlineMs: 60_000, retryAfterHeader: undefined, random: () => 0 });
      const high = resolveRelayfileBackpressureDelayMs({ ...base, attempt, deadlineMs: 60_000, retryAfterHeader: undefined, random: () => 0.999999 });
      expect(low).toBe(Math.round(nominal * (1 - jitterRatio)));
      expect(high!).toBeLessThanOrEqual(Math.round(nominal * (1 + jitterRatio)));
      expect(high!).toBeGreaterThan(nominal);
    }
    // attempt 10: 1 s * 1024 would be 1024 s; the term is capped at maxMs.
    expect(resolveRelayfileBackpressureDelayMs({ ...base, attempt: 10, deadlineMs: 60_000, retryAfterHeader: undefined })).toBe(maxMs);
    expect(resolveRelayfileBackpressureDelayMs({ ...base, attempt: 10, deadlineMs: 60_000, retryAfterHeader: undefined, random: () => 0.999999 })!)
      .toBeLessThanOrEqual(maxMs * (1 + jitterRatio));
  });

  it("never retries earlier than the server asked (server delay wins over a shorter fallback)", () => {
    // attempt 0 fallback would be ~1 s; the server asked for 20 s.
    expect(resolveRelayfileBackpressureDelayMs({ ...base, ...floor, retryAfterHeader: "20" })).toBe(20_000);
    expect(resolveRelayfileBackpressureDelayMs({ ...base, ...floor, retryAfterHeader: null, bodyRetryAfterSeconds: 20 })).toBe(20_000);
    expect(resolveRelayfileBackpressureDelayMs({ ...base, ...ceiling, retryAfterHeader: "20" })).toBeGreaterThanOrEqual(20_000);
  });

  it("defers (null) when the delay would start at or past the deadline", () => {
    expect(resolveRelayfileBackpressureDelayMs({ ...base, ...floor, retryAfterHeader: "30" })).toBeNull();
    expect(resolveRelayfileBackpressureDelayMs({ ...base, ...floor, retryAfterHeader: "120" })).toBeNull();
    expect(resolveRelayfileBackpressureDelayMs({ ...base, nowMs: 29_500, retryAfterHeader: null })).toBeNull();
    // The lengthened hint is what must fit: 25 s fits at the floor, 25 s * 1.25 does not.
    expect(resolveRelayfileBackpressureDelayMs({ ...base, ...floor, retryAfterHeader: "25" })).toBe(25_000);
    expect(resolveRelayfileBackpressureDelayMs({ ...base, ...ceiling, retryAfterHeader: "25" })).toBeNull();
  });
});

describe("fetchWithRelayfileBackpressure", () => {
  const surface = "provider-writes" as const;

  it("429 then 200 returns the 200", async () => {
    const h = harness([() => respond(429, { headers: { "retry-after": "2" } }), () => respond(200)]);
    const res = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 2, budgetMs: 30_000 });
    expect(res.status).toBe(200);
    expect(h.post).toHaveBeenCalledTimes(2);
    expect(h.sleeps).toEqual([2_000]);
  });

  it("sleeps the lengthened hint but reports the raw server hint in the typed error", async () => {
    const h = harness([() => respond(429, { headers: { "retry-after": "2" } })]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 1, budgetMs: 30_000, random: () => 0.999999 }).catch((e: unknown) => e);
    expect(h.post).toHaveBeenCalledTimes(2);
    expect(h.sleeps).toHaveLength(1);
    expect(h.sleeps[0]).toBeGreaterThan(2_000);
    expect(h.sleeps[0]).toBeLessThanOrEqual(Math.round(2_000 * (1 + hintJitterRatio)));
    expect(error).toMatchObject({ retryAfterMs: 2_000, attempts: 2 });
  });

  it("200 and 500 are returned untouched with no retry", async () => {
    for (const status of [200, 500, 503]) {
      const h = harness([() => respond(status)]);
      const res = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 2, budgetMs: 30_000 });
      expect(res.status).toBe(status);
      expect(h.post).toHaveBeenCalledTimes(1);
      expect(h.sleeps).toEqual([]);
    }
  });

  // PORT (additions, not in Cloud): RELAYFILE_BACKPRESSURE_RETRY_503 (default off, plan §6.2).
  it("PORT: with retry503, 503 is retried like 429 and a final 503 is a RelayfileBackpressureError", async () => {
    const h = harness([() => respond(503, { headers: { "retry-after": "1" } })]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 2, budgetMs: 30_000, retry503: true }).catch((e: unknown) => e);
    expect(h.post).toHaveBeenCalledTimes(3);
    expect(error).toBeInstanceOf(RelayfileBackpressureError);
    expect(error).toMatchObject({ status: 503, retryAfterMs: 1_000, attempts: 3, surface });
  });

  it("PORT: with retry503, an OSS 500 gets the bounded retry and a final 500 is a RelayfileServerError", async () => {
    const h = harness([() => respond(500)]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 1, budgetMs: 30_000, retry503: true }).catch((e: unknown) => e);
    expect(h.post).toHaveBeenCalledTimes(2);
    expect(error).toBeInstanceOf(RelayfileServerError);
    expect(error).toMatchObject({ status: 500, attempts: 2, surface });
  });

  it("PORT: with retry503, a 500 then 202 succeeds", async () => {
    const h = harness([() => respond(500), () => respond(202)]);
    const res = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 1, budgetMs: 30_000, retry503: true });
    expect(res.status).toBe(202);
    expect(h.post).toHaveBeenCalledTimes(2);
  });

  it("PORT: onBackpressure sees every retryable response with relayfileReason from the body", async () => {
    const seen: unknown[] = [];
    const h = harness([() => respond(429, { headers: { "content-type": "application/json", "retry-after": "1" }, body: JSON.stringify({ reason: "workspace_busy" }) })]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 1, budgetMs: 30_000, onBackpressure: (e) => seen.push(e) }).catch((e: unknown) => e);
    expect(seen).toHaveLength(2); // each retryable response observed exactly once
    expect(seen[0]).toMatchObject({ status: 429, retryAfterMs: 1_000, attempt: 1, relayfileReason: "workspace_busy" });
    expect(error).toMatchObject({ relayfileReason: "workspace_busy", attempts: 2 });
  });

  it("caps at exactly maxRetries + 1 posts, then throws a typed error with retryAfterMs/attempts/surface", async () => {
    const h = harness([() => respond(429, { headers: { "retry-after": "1" } })]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 2, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(h.post).toHaveBeenCalledTimes(3);
    expect(error).toBeInstanceOf(RelayfileBackpressureError);
    expect(error).toMatchObject({ name: "RelayfileBackpressureError", status: 429, retryAfterMs: 1_000, attempts: 3, surface });
  });

  it("final 429 without a header reports the body's details.retryAfterSeconds", async () => {
    const h = harness([() => respond(429, jsonBody({ code: "workspace_busy", details: { retryAfterSeconds: 4 } }))]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 1, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(h.sleeps).toEqual([4_000]);
    expect(error).toMatchObject({ retryAfterMs: 4_000, attempts: 2 });
  });

  it("final 429 with neither header nor body hint reports retryAfterMs null", async () => {
    const h = harness([() => respond(429)]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 0, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(error).toMatchObject({ retryAfterMs: null, attempts: 1 });
  });

  it("defers when the server delay does not fit: one post, typed error", async () => {
    const h = harness([() => respond(429, { headers: { "retry-after": "120" } })]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 2, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(h.post).toHaveBeenCalledTimes(1);
    expect(h.sleeps).toEqual([]);
    expect(error).toMatchObject({ retryAfterMs: 120_000, attempts: 1 });
  });

  it("re-checks the deadline after sleeping: an overshooting sleep stops the retry", async () => {
    const h = harness([() => respond(429, { headers: { "retry-after": "1" } })], { overshootMs: 40_000 });
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 2, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(h.postTimes).toEqual([0]);
    expect(error).toBeInstanceOf(RelayfileBackpressureError);
  });

  it("a rejecting cancel on the replaced 429 does not abort the retry", async () => {
    const first = respond(429, { headers: { "retry-after": "1" } });
    Object.defineProperty(first.body!, "cancel", { value: async () => { throw new Error("errored"); }, configurable: true });
    const h = harness([() => first, () => respond(201)]);
    expect((await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 2, budgetMs: 30_000 })).status).toBe(201);
  });

  it("never overlaps attempts: each post settles before the delay is computed (max 1 in flight)", async () => {
    let inFlight = 0;
    let maxInFlight = 0;
    let clock = 0;
    const events: string[] = [];
    const post = vi.fn(async () => {
      inFlight += 1;
      maxInFlight = Math.max(maxInFlight, inFlight);
      events.push("post-start");
      await new Promise((resolve) => setTimeout(resolve, 5));
      inFlight -= 1;
      events.push("post-settled");
      return respond(429, { headers: { "retry-after": "1" } });
    });
    const sleep = async (ms: number) => {
      expect(inFlight).toBe(0);
      events.push("sleep");
      clock += ms;
    };
    await fetchWithRelayfileBackpressure({ post, now: () => clock, sleep, surface, maxRetries: 2, budgetMs: 30_000 }).catch(() => undefined);
    expect(maxInFlight).toBe(1);
    expect(events).toEqual(["post-start", "post-settled", "sleep", "post-start", "post-settled", "sleep", "post-start", "post-settled"]);
  });

  it("a per-attempt timeout (rejected post) propagates immediately: no further post, no sleep", async () => {
    for (const name of ["TimeoutError", "AbortError"]) {
      const sleep = vi.fn(async () => {});
      const post = vi.fn(async (): Promise<Response> => { throw new DOMException("The operation timed out.", name); });
      await expect(fetchWithRelayfileBackpressure({ post, sleep, surface, maxRetries: 2, budgetMs: 30_000 })).rejects.toMatchObject({ name });
      expect(post).toHaveBeenCalledTimes(1);
      expect(sleep).not.toHaveBeenCalled();
    }
    // Also when the timeout hits a RETRY attempt: it propagates, nothing more is posted.
    let calls = 0;
    const sleep = vi.fn(async () => {});
    const post = vi.fn(async (): Promise<Response> => {
      calls += 1;
      if (calls === 1) return respond(429, { headers: { "retry-after": "1" } });
      throw new DOMException("The operation timed out.", "TimeoutError");
    });
    await expect(fetchWithRelayfileBackpressure({ post, sleep, surface, maxRetries: 2, budgetMs: 30_000 })).rejects.toMatchObject({ name: "TimeoutError" });
    expect(post).toHaveBeenCalledTimes(2);
    expect(sleep).toHaveBeenCalledTimes(1);
  });

  it.each([
    ["workspace_busy", jsonBody({ code: "workspace_busy" })],
    ["queue_full", jsonBody({ code: "queue_full" })],
    ["write_admission_limit", jsonBody({ code: "write_admission_limit" })],
    ["inflight_limit", jsonBody({ code: "inflight_limit" })],
    ["inflight_body_bytes", jsonBody({ code: "inflight_body_bytes" })],
    ["oldest_age", jsonBody({ code: "oldest_age" })],
    ["oldest-age", jsonBody({ code: "oldest-age" })],
    ["platform_overload", jsonBody({ error: { code: "platform_overload" } })],
    ["non-JSON body", { headers: { "content-type": "text/html" }, body: "<html>busy</html>" }],
    ["malformed JSON body", { headers: { "content-type": "application/json" }, body: "{not json" }],
  ])("every 429 reason backs off the same way regardless of body (%s)", async (_label, init) => {
    const h = harness([() => respond(429, init)]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 2, budgetMs: 30_000, random: () => 0.5 }).catch((e: unknown) => e);
    expect(h.post).toHaveBeenCalledTimes(3);
    expect(h.sleeps).toEqual([1_000, 2_000]);
    expect(error).toMatchObject({ name: "RelayfileBackpressureError", attempts: 3, retryAfterMs: null });
  });

  it("the typed error never carries the response body (sentinel)", async () => {
    const sentinel = "SENTINEL-RESPONSE-BODY-7f3a";
    const h = harness([() => respond(429, jsonBody({ code: "workspace_busy", message: sentinel }))]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 0, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RelayfileBackpressureError);
    expect((error as Error).message).not.toContain(sentinel);
    expect(JSON.stringify(error)).not.toContain(sentinel);
    expect(String((error as Error).stack)).not.toContain(sentinel);
  });

  it("the typed error has no `code` (so the record writer's own workspace_busy loop does not stack on it)", async () => {
    const h = harness([() => respond(429, jsonBody({ code: "workspace_busy" }))]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 0, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect((error as { code?: unknown }).code).toBeUndefined();
  });
});

describe("bounded retry-hint body reader", () => {
  const surface = "provider-writes" as const;
  const json = { "content-type": "application/json" };
  const bound = RELAYFILE_BACKPRESSURE_HINT_MAX_BYTES;
  // The reader stops on the chunk that crosses the bound; the Response/tee
  // plumbing may have pulled at most one more chunk from the source.
  const maxPulled = (chunkSize: number) => bound + 2 * chunkSize;

  it("a small streamed body hint (many chunks, no Content-Length) is still honoured", async () => {
    const body = countingBody(JSON.stringify({ code: "workspace_busy", details: { retryAfterSeconds: 7 } }), 8);
    const h = harness([() => new Response(body.stream, { status: 429, headers: json }), () => respond(200)]);
    const res = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 1, budgetMs: 30_000 });
    expect(res.status).toBe(200);
    expect(h.sleeps).toEqual([7_000]);
  });

  it("a 64 KiB+ body (reviewer repro, maxRetries 0) is read only up to the bound and its hint is ignored", async () => {
    const sentinel = "SENTINEL-OVERSIZED-BODY-5e9a";
    const text = oversizedHintJson(1, 65_572, sentinel);
    expect(text.length).toBe(65_572);
    expect(() => JSON.parse(text)).not.toThrow();
    const chunkSize = 256;
    const body = countingBody(text, chunkSize);
    const h = harness([() => new Response(body.stream, { status: 429, headers: json })]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 0, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RelayfileBackpressureError);
    expect(error).toMatchObject({ retryAfterMs: null, attempts: 1 });
    expect(body.stats.pulledBytes).toBeLessThanOrEqual(maxPulled(chunkSize));
    expect(body.stats.pulledBytes).toBeLessThan(body.totalBytes);
    for (const text of [(error as Error).message, JSON.stringify(error), String((error as Error).stack)]) {
      expect(text).not.toContain(sentinel);
      expect(text).not.toContain("xxxxxxxx");
    }
  });

  it("an oversized body's hint never starts a retry early: the fallback schedule applies, same attempt count", async () => {
    const chunkSize = 512;
    const bodies: Array<ReturnType<typeof countingBody>> = [];
    const h = harness([() => {
      // A 30 s hint would defer; a 0 s hint would retry immediately. Neither may apply.
      const body = countingBody(oversizedHintJson(0, 70_000), chunkSize);
      bodies.push(body);
      return new Response(body.stream, { status: 429, headers: json });
    }]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 2, budgetMs: 30_000, random: () => 0.5 }).catch((e: unknown) => e);
    expect(h.post).toHaveBeenCalledTimes(3);
    expect(h.sleeps).toEqual([baseMs, 2 * baseMs]);
    expect(h.postTimes).toEqual([0, baseMs, 3 * baseMs]);
    expect(error).toMatchObject({ name: "RelayfileBackpressureError", attempts: 3, retryAfterMs: null });
    for (const body of bodies) expect(body.stats.pulledBytes).toBeLessThanOrEqual(maxPulled(chunkSize));
  });

  it("a streamed body with no Content-Length stops at the bound (tiny chunks)", async () => {
    const chunkSize = 16;
    const body = countingBody(oversizedHintJson(3, 32 * 1_024), chunkSize);
    const h = harness([() => new Response(body.stream, { status: 429, headers: json })]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 0, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(error).toMatchObject({ retryAfterMs: null, attempts: 1 });
    expect(body.stats.pulledBytes).toBeGreaterThan(bound);
    expect(body.stats.pulledBytes).toBeLessThanOrEqual(maxPulled(chunkSize));
  });

  it("an understated Content-Length does not lift the bound", async () => {
    const chunkSize = 128;
    const body = countingBody(oversizedHintJson(3, 32 * 1_024), chunkSize);
    const h = harness([() => new Response(body.stream, { status: 429, headers: { ...json, "content-length": "64" } })]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 0, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(error).toMatchObject({ retryAfterMs: null });
    expect(body.stats.pulledBytes).toBeLessThanOrEqual(maxPulled(chunkSize));
  });

  it("a declared Content-Length above the bound is not read at all", async () => {
    const body = countingBody(oversizedHintJson(3, 32 * 1_024), 128);
    const h = harness([() => new Response(body.stream, { status: 429, headers: { ...json, "content-length": String(32 * 1_024) } })]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 0, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(error).toMatchObject({ retryAfterMs: null });
    expect(body.stats.pulls).toBe(0);
  });

  it("a valid Retry-After header never reads the body, even a huge one", async () => {
    const bodies: Array<ReturnType<typeof countingBody>> = [];
    const h = harness([() => {
      const body = countingBody(oversizedHintJson(9, 70_000), 256);
      bodies.push(body);
      return new Response(body.stream, { status: 429, headers: { ...json, "retry-after": "2" } });
    }]);
    const error = await fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 1, budgetMs: 30_000 }).catch((e: unknown) => e);
    expect(h.sleeps).toEqual([2_000]);
    expect(error).toMatchObject({ retryAfterMs: 2_000, attempts: 2 });
    expect(bodies).toHaveLength(2);
    for (const body of bodies) expect(body.stats.pulls).toBe(0);
  });

  it("a stalled body times out: no hint, fallback delay, and the retry decision is not blocked", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    try {
      const body = countingBody(JSON.stringify({ details: { retryAfterSeconds: 20 } }), 4, { stallAfterBytes: 8 });
      const h = harness([() => new Response(body.stream, { status: 429, headers: json }), () => respond(200)]);
      const pending = fetchWithRelayfileBackpressure({ ...h, surface, maxRetries: 1, budgetMs: 30_000, random: () => 0.5 });
      await vi.advanceTimersByTimeAsync(RELAYFILE_BACKPRESSURE_HINT_READ_TIMEOUT_MS);
      const res = await pending;
      expect(res.status).toBe(200);
      expect(h.sleeps).toEqual([baseMs]);
      expect(body.stats.pulledBytes).toBe(8);
    } finally {
      vi.useRealTimers();
    }
  });
});
