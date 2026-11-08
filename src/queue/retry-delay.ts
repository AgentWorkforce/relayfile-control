// Ported from cloud packages/webhook-worker/src/queue-retry-delay.ts @ 1a59dec
// (plan §4.4). The schedule and the lengthen-only hint rule are unchanged; the
// 24 h cap is now the platform maximum for `visible_at` rather than Cloudflare's.

/**
 * Redelivery delay for a failed cloud-web forward.
 *
 * A fixed 60 s delay re-ran every failed forward in lockstep: under Relayfile
 * saturation (HTTP 429 shed by a workspace's admission gate) all retries of a
 * burst landed together again 60 s later, re-saturating the same workspace.
 * This schedule grows exponentially with Cloudflare's 1-based
 * `message.attempts`, adds up to +25 % jitter to spread a burst, and clamps
 * the schedule to [60 s, 900 s]. A larger server `Retry-After` always wins
 * over the schedule and is honoured as a hard minimum: it is lengthened by the
 * same up-to-+25 % jitter (never shortened) so retries that received the same
 * hint do not land together, and is bounded only by the Cloudflare Queues
 * `delaySeconds` limit (24 h): truncating a server hint to the schedule cap
 * would redeliver early, while the workspace is still shedding.
 *
 *   attempts: 1    2    3    4    5    6+
 *   base:     60  120  240  480  900  900   (seconds, before jitter/clamp)
 */
export const FORWARD_RETRY_MIN_DELAY_SECONDS = 60;
/** Cap for the locally computed exponential schedule. */
export const FORWARD_RETRY_MAX_DELAY_SECONDS = 900;
/** Cloudflare Queues `delaySeconds` limit (24 h); the only cap on a (jittered) server `Retry-After`. */
export const FORWARD_RETRY_PLATFORM_MAX_DELAY_SECONDS = 86_400;
export const FORWARD_RETRY_BASE_DELAY_SECONDS = 60;
export const FORWARD_RETRY_JITTER_RATIO = 0.25;

export function resolveForwardRetryDelaySeconds({
  attempts,
  retryAfterSeconds,
  random = Math.random,
}: {
  attempts: number;
  retryAfterSeconds: number | null;
  random?: () => number;
}): number {
  const exponent = Math.max(0, (Number.isFinite(attempts) ? Math.floor(attempts) : 1) - 1);
  // Clamp the uniform sample so a misbehaving `random` cannot exceed [0, 0.25).
  const sample = Math.min(Math.max(random(), 0), 1);
  const jitter = Math.min(sample * FORWARD_RETRY_JITTER_RATIO, FORWARD_RETRY_JITTER_RATIO);
  const scheduled = Math.min(
    FORWARD_RETRY_MAX_DELAY_SECONDS,
    Math.max(
      FORWARD_RETRY_MIN_DELAY_SECONDS,
      FORWARD_RETRY_BASE_DELAY_SECONDS * 2 ** Math.min(exponent, 30) * (1 + jitter),
    ),
  );
  // Lengthen-only: the hint is the floor, the same jitter sample stretches it.
  const serverRequested =
    typeof retryAfterSeconds === "number" && Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0
      ? Math.min(FORWARD_RETRY_PLATFORM_MAX_DELAY_SECONDS, Math.max(retryAfterSeconds, retryAfterSeconds * (1 + jitter)))
      : 0;
  return Math.ceil(Math.max(serverRequested, scheduled));
}

/**
 * Server-requested redelivery hint from a cloud-web rejection: the
 * `Retry-After` header (delta-seconds only; HTTP-dates are ignored) or a
 * numeric `retryAfterSeconds` in the JSON body. Null when neither is usable.
 */
export function parseForwardRetryAfterSeconds(response: Response, responseText: string): number | null {
  const header = response.headers.get("retry-after")?.trim();
  if (header && /^\d+$/.test(header)) {
    const seconds = Number(header);
    if (Number.isFinite(seconds)) return seconds;
  }
  if (!responseText) return null;
  try {
    const parsed: unknown = JSON.parse(responseText);
    if (parsed && typeof parsed === "object") {
      const value = (parsed as { retryAfterSeconds?: unknown }).retryAfterSeconds;
      if (typeof value === "number" && Number.isFinite(value) && value >= 0) return value;
    }
  } catch {
    // Non-JSON body: no hint.
  }
  return null;
}
