// Ported from cloud packages/web/lib/integrations/github-webhook-admission-retry.ts @ 1a59dec (cloud#6175).
// The constants are the plan §6.2 ADMISSION_* policy; PROVIDER_WRITE_* are added for PR 2.
/**
 * Bounded, in-place retry for the Relayfile per-workspace write-admission 429
 * (`write_admission_limit`) on the GitHub envelope-admission call.
 *
 * Design constraints (review):
 *  - A single wall-clock budget (ADMISSION_RETRY_BUDGET_MS); the caller's own
 *    timeout is never reused across attempts.
 *  - `Retry-After` may be delta-seconds or an HTTP-date.
 *  - BEHAVIOUR CHANGE (Relayfile 429 backoff): the loop now delegates to the
 *    shared `retryRelayfileBackpressure` helper (relayfile-backpressure.ts). An
 *    absent/malformed `Retry-After` no longer defers to the durable queue
 *    immediately; it falls back to the JSON body's `retryAfterSeconds` and then
 *    to a jittered exponential delay (1 s, 2 s, ... capped 8 s, +/-20 %) inside
 *    the SAME 30 s budget and retry cap. Deferring at once meant a header-less
 *    429 went straight to a fixed 60 s queue redelivery of the whole forward.
 *  - A server delay longer than the remaining budget defers to the queue; we
 *    never retry earlier than the server asked.
 *  - Callers must refresh the request signature (and give each attempt its own
 *    abort signal) between attempts; the reusable envelope id keeps replay
 *    idempotent.
 */
import { parseRetryAfterMs, retryRelayfileBackpressure } from "./backpressure.ts";

/** Preserves the original per-attempt fetch allowance (was `AbortSignal.timeout(15_000)`). */
export const ADMISSION_ATTEMPT_TIMEOUT_MS = 15_000;
/**
 * Bounds when a RETRY attempt may START; each attempt keeps its own 15 s
 * allowance. Justified by the measured real case: a first admission attempt
 * took ~14 s before the 429 plus `Retry-After: 5`, so a smaller total would
 * abort before the retry we need. 30 s covers 14 s + 5 s + starting the retry
 * and sits well inside the GitHub caller's
 * `LONG_RUNNING_CLOUD_WEB_FORWARD_TIMEOUT_MS = 120_000`.
 *
 * HARD MAXIMUM for the request/start schedule is
 * `ADMISSION_RETRY_BUDGET_MS + ADMISSION_ATTEMPT_TIMEOUT_MS = 45 s`
 * (last allowed attempt start at 30 s + its own up-to-15 s fetch). This bounds
 * when attempts START and each attempt's own fetch; it does not bound
 * `await response.body.cancel()` between attempts (a local stream teardown,
 * expected to settle immediately, but not internally time-bounded).
 */
export const ADMISSION_RETRY_BUDGET_MS = 30_000;
/**
 * Exactly one in-place retry (two attempts in total). The measured incident
 * case needs the single retry after the server's `Retry-After`; a second
 * in-place retry only adds a third request to a gate that is still shedding.
 * Anything beyond that belongs to the durable queue's redelivery schedule.
 */
export const ADMISSION_MAX_RETRIES = 1;

export { parseRetryAfterMs };

/**
 * Header-only resolver kept for its existing callers/tests: null for an
 * absent/malformed header or a server-requested delay that does not fit the
 * remaining budget. The admission loop itself now uses
 * `resolveRelayfileBackpressureDelayMs` (which adds the body/fallback tiers).
 */
export function resolveAdmissionRetryDelayMs(
  retryAfterHeader: string | null | undefined,
  nowMs: number,
  deadlineMs: number,
): number | null {
  const requestedMs = parseRetryAfterMs(retryAfterHeader, nowMs);
  if (requestedMs === null) return null;
  // `>=`: an attempt that would START at or past the deadline has no budget left.
  if (nowMs + requestedMs >= deadlineMs) return null;
  return requestedMs;
}

export type AdmissionRetryDeps = {
  /** Issue one admission request (the caller applies ADMISSION_ATTEMPT_TIMEOUT_MS). */
  post: () => Promise<Response>;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
};

/**
 * Issue the admission request, retrying a 429 in place within the budget.
 * Returns the final Response (a 429 after exhaustion/deferral is returned
 * un-cancelled for the caller). `send` in github-webhook-ingest.ts uses
 * `fetchWithRelayfileBackpressure` with these same bounds so that a final 429
 * surfaces as a typed `RelayfileBackpressureError`.
 */
export async function fetchEnvelopeAdmissionWithRetry(
  { post, now, sleep, random }: AdmissionRetryDeps,
): Promise<Response> {
  const { response } = await retryRelayfileBackpressure({
    post,
    maxRetries: ADMISSION_MAX_RETRIES,
    budgetMs: ADMISSION_RETRY_BUDGET_MS,
    ...(now ? { now } : {}),
    ...(sleep ? { sleep } : {}),
    ...(random ? { random } : {}),
  });
  return response;
}

/** §6.2 provider-writes policy (PR 2 consumes it). Cloud sets no separate per-attempt timeout there. */
export const PROVIDER_WRITE_MAX_RETRIES = 1;
export const PROVIDER_WRITE_RETRY_BUDGET_MS = 30_000;
