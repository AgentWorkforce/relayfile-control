import { RelayfileBackpressureError } from "../relayfile/backpressure.ts";

// Ported from cloud packages/web/lib/integrations/nango-webhook-route-handler.ts @ 1a59dec
// (cloud#6175). Cloud answers a typed backpressure error with 503 + Retry-After
// and its queue consumer redelivers; here there is no HTTP hop, so the same
// classification feeds the consumer's `retry` disposition directly (§6.2).

/** Retry-After used when Relayfile named no delay (Cloud's constant, same value). */
export const RELAYFILE_BACKPRESSURE_DEFAULT_RETRY_AFTER_SECONDS = 60;

function isNamedError(value: unknown, name: string): boolean {
  return Boolean(value) && typeof value === "object" && (value as { name?: unknown }).name === name;
}

function readRetryAfterMs(value: unknown): number | null {
  if (!value || typeof value !== "object") return null;
  const ms = (value as { retryAfterMs?: unknown }).retryAfterMs;
  return typeof ms === "number" && Number.isFinite(ms) && ms >= 0 ? ms : null;
}

/**
 * Relayfile backpressure, thrown directly or carried as `cause` (e.g. a
 * RelayfilePrimaryWriteError from the PR 2 record writer). Detected by
 * `instanceof` OR `name` (no constructor identity across bundles) on the error
 * or its cause, exactly as Cloud. Undefined when the error is not backpressure.
 */
export function detectRelayfileBackpressure(error: unknown): { retryAfterMs: number | null; source: unknown } | undefined {
  if (error instanceof RelayfileBackpressureError || isNamedError(error, "RelayfileBackpressureError")) {
    return { retryAfterMs: readRetryAfterMs(error), source: error };
  }
  const cause = error && typeof error === "object" ? (error as { cause?: unknown }).cause : undefined;
  if (cause instanceof RelayfileBackpressureError || isNamedError(cause, "RelayfileBackpressureError")) {
    return { retryAfterMs: readRetryAfterMs(cause) ?? readRetryAfterMs(error), source: cause };
  }
  return undefined;
}

/** `max(1, ceil(retryAfterMs/1000))`, or the 60 s default when Relayfile named no delay. */
export function backpressureRetryAfterSeconds(retryAfterMs: number | null): number {
  return retryAfterMs === null ? RELAYFILE_BACKPRESSURE_DEFAULT_RETRY_AFTER_SECONDS : Math.max(1, Math.ceil(retryAfterMs / 1_000));
}
