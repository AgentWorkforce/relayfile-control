// Ported from cloud packages/webhook-worker/test/queue-retry-delay.test.ts @ 1a59dec.
import { describe, expect, it } from "./support/vitest-compat.ts";
import {
  FORWARD_RETRY_MAX_DELAY_SECONDS,
  FORWARD_RETRY_MIN_DELAY_SECONDS,
  FORWARD_RETRY_PLATFORM_MAX_DELAY_SECONDS,
  FORWARD_RETRY_JITTER_RATIO,
  parseForwardRetryAfterSeconds,
  resolveForwardRetryDelaySeconds,
} from "../src/queue/retry-delay.ts";

describe("resolveForwardRetryDelaySeconds", () => {
  it.each([
    [1, 60],
    [2, 120],
    [3, 240],
    [4, 480],
    [5, 900],
    [6, 900],
  ])("attempt %i: lower bound %i at random=0, upper bound <= 900 at random=0.999", (attempts, lower) => {
    const low = resolveForwardRetryDelaySeconds({ attempts, retryAfterSeconds: null, random: () => 0 });
    const high = resolveForwardRetryDelaySeconds({ attempts, retryAfterSeconds: null, random: () => 0.999 });

    expect(low).toBe(lower);
    expect(high).toBeGreaterThanOrEqual(low);
    expect(high).toBeLessThanOrEqual(FORWARD_RETRY_MAX_DELAY_SECONDS);
    expect(high).toBeLessThanOrEqual(Math.min(900, Math.ceil(lower * 1.25)));
    expect(Number.isInteger(low) && Number.isInteger(high)).toBe(true);
  });

  it("a larger Retry-After wins over the schedule", () => {
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 300, random: () => 0 })).toBe(300);
  });

  it("a smaller Retry-After never shortens the schedule", () => {
    expect(resolveForwardRetryDelaySeconds({ attempts: 3, retryAfterSeconds: 5, random: () => 0 })).toBe(240);
  });

  it("a server Retry-After is a hard minimum, lengthened by up to +25 % (positive-only jitter)", () => {
    const low = resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 300, random: () => 0 });
    const mid = resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 300, random: () => 0.5 });
    const high = resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 300, random: () => 0.999 });
    expect(low).toBe(300); // never below the hint
    expect(mid).toBe(Math.ceil(300 * (1 + FORWARD_RETRY_JITTER_RATIO * 0.5)));
    expect(high).toBeGreaterThan(300);
    expect(high).toBeLessThanOrEqual(Math.ceil(300 * (1 + FORWARD_RETRY_JITTER_RATIO)));
    // A common hint yields distinct allowed delays for distinct samples.
    expect(new Set([low, mid, high]).size).toBe(3);
    // A hint below the schedule is still dominated by the (jittered) schedule.
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 5, random: () => 0.999 })).toBeLessThanOrEqual(75);
    // A misbehaving `random` cannot shorten the hint.
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 300, random: () => -1 })).toBe(300);
  });

  it("honours a Retry-After above the 900 s schedule cap, up to the Cloudflare 24 h delay limit", () => {
    // A truncated hint would redeliver while the workspace is still shedding.
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 3600, random: () => 0 })).toBe(3600);
    const jittered = resolveForwardRetryDelaySeconds({ attempts: 6, retryAfterSeconds: 3600, random: () => 0.999 });
    expect(jittered).toBeGreaterThanOrEqual(3600);
    expect(jittered).toBeLessThanOrEqual(Math.ceil(3600 * (1 + FORWARD_RETRY_JITTER_RATIO)));
    // The 24 h platform cap also bounds the jittered hint.
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 86_400, random: () => 0.999 })).toBe(
      FORWARD_RETRY_PLATFORM_MAX_DELAY_SECONDS,
    );
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 80_000, random: () => 0.999 })).toBe(
      FORWARD_RETRY_PLATFORM_MAX_DELAY_SECONDS,
    );
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 10_000, random: () => 0 })).toBe(10_000);
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 86_400, random: () => 0 })).toBe(
      FORWARD_RETRY_PLATFORM_MAX_DELAY_SECONDS,
    );
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 1_000_000, random: () => 0 })).toBe(
      FORWARD_RETRY_PLATFORM_MAX_DELAY_SECONDS,
    );
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 900.2, random: () => 0 })).toBe(901);
  });

  it("clamps the schedule to [60, 900] for any input", () => {
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: 900, random: () => 0 })).toBe(900);
    expect(resolveForwardRetryDelaySeconds({ attempts: 0, retryAfterSeconds: null, random: () => 0 })).toBe(
      FORWARD_RETRY_MIN_DELAY_SECONDS,
    );
    expect(resolveForwardRetryDelaySeconds({ attempts: Number.NaN, retryAfterSeconds: -5, random: () => 0 })).toBe(60);
    expect(resolveForwardRetryDelaySeconds({ attempts: 1_000, retryAfterSeconds: null, random: () => 5 })).toBe(900);
    expect(resolveForwardRetryDelaySeconds({ attempts: 1, retryAfterSeconds: null, random: () => -1 })).toBe(60);
  });
});

describe("parseForwardRetryAfterSeconds", () => {
  it("reads a delta-seconds Retry-After header", () => {
    expect(parseForwardRetryAfterSeconds(new Response("", { headers: { "Retry-After": " 120 " } }), "")).toBe(120);
  });

  it("falls back to the JSON body's retryAfterSeconds", () => {
    expect(parseForwardRetryAfterSeconds(new Response(""), JSON.stringify({ retryAfterSeconds: 45 }))).toBe(45);
  });

  it("returns null when there is no hint", () => {
    expect(parseForwardRetryAfterSeconds(new Response(""), "")).toBeNull();
    expect(parseForwardRetryAfterSeconds(new Response(""), JSON.stringify({ error: "boom" }))).toBeNull();
  });

  it("returns null for malformed hints", () => {
    expect(
      parseForwardRetryAfterSeconds(
        new Response("", { headers: { "Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT" } }),
        "<html>bad gateway</html>",
      ),
    ).toBeNull();
    expect(parseForwardRetryAfterSeconds(new Response("", { headers: { "Retry-After": "-5" } }), "false")).toBeNull();
    expect(parseForwardRetryAfterSeconds(new Response(""), JSON.stringify({ retryAfterSeconds: "60" }))).toBeNull();
    expect(parseForwardRetryAfterSeconds(new Response(""), JSON.stringify({ retryAfterSeconds: -1 }))).toBeNull();
  });
});
