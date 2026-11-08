// Ported from cloud packages/web/lib/integrations/relayfile-backpressure.ts @ 1a59dec
// (cloud#6175, plan §6.2). Changes from Cloud, all per the plan:
//  - 503 is retried like 429 (layered-relayfile: cloud storage_full /
//    root_event_outbox_full, OSS secret-unset), and OSS plain 500 gets the same
//    bounded retry (plan §14.3). `retryStatuses` names the set.
//  - A final 429/503 throws RelayfileBackpressureError; a final 500 throws
//    RelayfileServerError (queue schedule, no cool-down).
//  - `onBackpressure` observes every retryable response, including ones retried
//    in place, for the `relayfile_backpressure` event (§10).

/**
 * Shared bounded backoff for Relayfile per-workspace admission 429s
 * (`workspace_busy` / `queue_full` / `write_admission_limit` and any other
 * 429 reason: the body code is never consulted to decide whether to retry).
 *
 * Relayfile sheds load with a 429 when a workspace's Durable Object admission
 * gate is saturated. Retrying such a 429 in lockstep (or not at all, then
 * redelivering the whole webhook) amplifies a burst into a 429 storm, so every
 * Cloud -> Relayfile internal write surface shares one policy:
 *
 *  - One wall-clock budget bounds when a RETRY may START; each attempt keeps
 *    its own caller-supplied per-attempt timeout (the caller's `post()` must
 *    create a fresh AbortSignal and a fresh request signature per call).
 *  - Retries are strictly sequential: `post()` is awaited to settle before a
 *    delay is computed, so attempts never overlap. A rejected `post()` (abort,
 *    timeout, network) propagates immediately with no further attempt.
 *  - Delay precedence: `Retry-After` header (delta-seconds or IMF-fixdate),
 *    then the JSON body's `details.retryAfterSeconds` / `retryAfterSeconds`,
 *    then a jittered exponential fallback. A server-requested delay is a HARD
 *    MINIMUM: it is only ever lengthened, by a uniform positive jitter of up to
 *    `RELAYFILE_BACKPRESSURE_SERVER_HINT_JITTER_RATIO`, so callers shed by the
 *    same gate with the same hint do not return in lockstep and we never retry
 *    earlier than the server asked. A zero / already-elapsed hint (`Retry-After:
 *    0`, a past HTTP-date, body `retryAfterSeconds: 0`) is not a licence to spin:
 *    it takes the jittered fallback schedule instead. The body is only
 *    consulted when the header is absent/malformed, and only its first
 *    `RELAYFILE_BACKPRESSURE_HINT_MAX_BYTES` bytes within
 *    `RELAYFILE_BACKPRESSURE_HINT_READ_TIMEOUT_MS`: a larger, slower, or
 *    unparseable body yields no hint (the fallback schedule applies).
 *  - A delay that would start at or past the deadline defers to the durable
 *    queue: the final 429 becomes a typed `RelayfileBackpressureError` that the
 *    webhook route maps to a retryable 503 + `Retry-After`.
 *  - 429, 503 and 500 are retried here (see the port note above). Every other
 *    status is returned untouched for the caller to judge.
 *  - Response bodies are never logged or copied into error messages.
 */

/**
 * Parse a `Retry-After` value (delta-seconds or IMF-fixdate HTTP-date). Null
 * when absent or malformed. Lives here (re-exported unchanged by
 * github-webhook-admission-retry.ts) so the two modules do not import each
 * other.
 */
export function parseRetryAfterMs(
  value: string | null | undefined,
  nowMs: number,
): number | null {
  const raw = value?.trim();
  if (!raw) return null;
  // Delta-seconds must be a bare non-negative integer; reject "1.5" / "1s" etc.
  // A long digit string ("9"*400) overflows to Infinity, so require a finite ms.
  if (/^\d+$/.test(raw)) {
    const delayMs = Number(raw) * 1_000;
    return Number.isFinite(delayMs) ? delayMs : null;
  }
  // Otherwise accept only the RFC 7231 IMF-fixdate form (so Date.parse cannot
  // silently reinterpret junk like "1.5" as a date).
  if (/^[A-Za-z]{3}, \d{2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2} GMT$/.test(raw)) {
    const dateMs = Date.parse(raw);
    if (Number.isFinite(dateMs)) return Math.max(0, dateMs - nowMs);
  }
  return null;
}

export type RelayfileBackpressureSurface = "webhook-envelopes" | "provider-writes";

/** Statuses that are "back off and retry, bounded": admission/storage pressure. */
export const BACKPRESSURE_STATUSES: ReadonlySet<number> = new Set([429, 503]);
/** Retried in place like backpressure, but not treated as a workspace being busy. */
export const RETRY_IN_PLACE_STATUSES: ReadonlySet<number> = new Set([429, 503, 500]);

/**
 * Thrown when Relayfile still answers 429 after the bounded in-place retries.
 *
 * Deliberately carries no `code` property: the record writer's
 * `bulkWriteWithWorkspaceBusyRetry` retries errors shaped as
 * `{ status: 429, code: "workspace_busy" | "queue_full" }` for up to 240 s, and
 * stacking that loop on top of this one would re-create the amplification this
 * helper exists to remove. The durable queue owns the next attempt.
 */
export class RelayfileBackpressureError extends Error {
  readonly status: number;
  readonly retryAfterMs: number | null;
  readonly attempts: number;
  readonly surface: RelayfileBackpressureSurface;
  /** Relayfile's body `reason`/`error` (≤120 chars) when it could be read. */
  readonly relayfileReason: string | null;

  constructor(input: {
    status: number;
    retryAfterMs: number | null;
    attempts: number;
    surface: RelayfileBackpressureSurface;
    relayfileReason?: string | null;
  }) {
    super(
      `Relayfile ${input.surface} backpressure (${input.status}) after ${input.attempts} attempt(s)` +
        (input.retryAfterMs === null ? "" : `; retry after ${input.retryAfterMs}ms`),
    );
    this.name = "RelayfileBackpressureError";
    this.status = input.status;
    this.retryAfterMs = input.retryAfterMs;
    this.attempts = input.attempts;
    this.surface = input.surface;
    this.relayfileReason = input.relayfileReason ?? null;
  }
}

/** A final OSS 500 after the bounded in-place retry: normal queue schedule, no cool-down. */
export class RelayfileServerError extends Error {
  readonly status: number;
  readonly attempts: number;
  readonly surface: RelayfileBackpressureSurface;
  readonly relayfileReason: string | null;
  constructor(input: { status: number; attempts: number; surface: RelayfileBackpressureSurface; relayfileReason?: string | null }) {
    super(`Relayfile ${input.surface} server error (${input.status}) after ${input.attempts} attempt(s)`);
    this.name = "RelayfileServerError";
    this.status = input.status;
    this.attempts = input.attempts;
    this.surface = input.surface;
    this.relayfileReason = input.relayfileReason ?? null;
  }
}

export type RelayfileBackpressureFallback = { baseMs: number; maxMs: number; jitterRatio: number };

export const RELAYFILE_BACKPRESSURE_FALLBACK: RelayfileBackpressureFallback = {
  baseMs: 1_000,
  maxMs: 8_000,
  jitterRatio: 0.2,
};

/**
 * Lengthening-only jitter on a server-requested delay: the actual wait is
 * uniform in [hint, hint * (1 + ratio)]. Never shortens the hint.
 */
export const RELAYFILE_BACKPRESSURE_SERVER_HINT_JITTER_RATIO = 0.25;

/**
 * Delay before the next attempt, or null to defer to the durable queue.
 * `attempt` is the 0-based retry index (0 = the first retry).
 */
export function resolveRelayfileBackpressureDelayMs(input: {
  retryAfterHeader: string | null | undefined;
  bodyRetryAfterSeconds?: number | null;
  attempt: number;
  nowMs: number;
  deadlineMs: number;
  random?: () => number;
  fallback?: RelayfileBackpressureFallback;
}): number | null {
  const random = input.random ?? Math.random;
  const requestedMs = serverRequestedDelayMs(input.retryAfterHeader, input.bodyRetryAfterSeconds, input.nowMs);
  const delayMs = requestedMs !== null && requestedMs > 0
    ? lengthenedServerDelayMs(requestedMs, random)
    : fallbackDelayMs(input.attempt, random, input.fallback ?? RELAYFILE_BACKPRESSURE_FALLBACK);
  // `>=`: an attempt that would START at or past the deadline has no budget left.
  if (input.nowMs + delayMs >= input.deadlineMs) return null;
  return delayMs;
}

function serverRequestedDelayMs(
  retryAfterHeader: string | null | undefined,
  bodyRetryAfterSeconds: number | null | undefined,
  nowMs: number,
): number | null {
  const headerMs = parseRetryAfterMs(retryAfterHeader, nowMs);
  if (headerMs !== null) return headerMs;
  if (typeof bodyRetryAfterSeconds === "number" && Number.isFinite(bodyRetryAfterSeconds) && bodyRetryAfterSeconds >= 0) {
    const bodyMs = bodyRetryAfterSeconds * 1_000;
    return Number.isFinite(bodyMs) ? bodyMs : null;
  }
  return null;
}

/** Clamp a uniform sample so a misbehaving `random` cannot shorten or over-lengthen. */
function unitSample(random: () => number): number {
  const sample = random();
  return Number.isFinite(sample) ? Math.min(Math.max(sample, 0), 1) : 0;
}

/** `[requestedMs, requestedMs * (1 + ratio)]`: a positive hint is a hard minimum. */
function lengthenedServerDelayMs(requestedMs: number, random: () => number): number {
  const factor = 1 + RELAYFILE_BACKPRESSURE_SERVER_HINT_JITTER_RATIO * unitSample(random);
  return Math.max(requestedMs, Math.round(requestedMs * factor));
}

function fallbackDelayMs(attempt: number, random: () => number, fallback: RelayfileBackpressureFallback): number {
  const exponent = Math.max(0, attempt);
  const capped = Math.min(fallback.maxMs, fallback.baseMs * 2 ** exponent);
  // Uniform in [1 - jitterRatio, 1 + jitterRatio): de-synchronises callers that
  // were shed by the same saturated gate at the same moment.
  const factor = 1 + fallback.jitterRatio * (2 * random() - 1);
  return Math.max(0, Math.round(capped * factor));
}

function isJsonResponse(response: Response): boolean {
  const contentType = response.headers.get("content-type") ?? "";
  return /^application\/(?:[\w.+-]+\+)?json\b/i.test(contentType.trim());
}

function readRetryAfterSecondsField(value: unknown): number | null {
  if (!value || typeof value !== "object") return null;
  const field = (value as { retryAfterSeconds?: unknown }).retryAfterSeconds;
  return typeof field === "number" && Number.isFinite(field) && field >= 0 ? field : null;
}

/**
 * Upper bound on the bytes read from a 429 body to find a retry hint. Relayfile's
 * admission 429 bodies are a few hundred bytes; anything larger is not a hint
 * worth buffering under overload.
 */
export const RELAYFILE_BACKPRESSURE_HINT_MAX_BYTES = 4_096;

/** Upper bound on the wall-clock time spent waiting for those bytes. */
export const RELAYFILE_BACKPRESSURE_HINT_READ_TIMEOUT_MS = 1_000;

/**
 * Reads `details.retryAfterSeconds` / `retryAfterSeconds` from a CLONE, reading
 * at most `RELAYFILE_BACKPRESSURE_HINT_MAX_BYTES` (whatever Content-Length
 * claims) for at most `RELAYFILE_BACKPRESSURE_HINT_READ_TIMEOUT_MS`. Returns
 * null when the hint cannot be read within those bounds. Never throws, and the
 * body text never leaves this function.
 */

export type BodyHint = { retryAfterSeconds: number | null; reason: string | null };

function readReasonField(value: unknown): string | null {
  if (!value || typeof value !== "object") return null;
  const v = value as { reason?: unknown; error?: unknown; code?: unknown };
  const pick = [v.reason, v.error, v.code].find((x) => typeof x === "string" && x.length > 0) as string | undefined;
  return pick ? pick.slice(0, 120) : null;
}

/**
 * Same bounded clone read as Cloud's hint reader, additionally returning the
 * body `reason`/`error` (≤120 chars) for the §10 `relayfileReason` field. The
 * reason is a short machine code, never the body itself.
 */
export async function readBodyHint(response: Response): Promise<BodyHint> {
  const none: BodyHint = { retryAfterSeconds: null, reason: null };
  if (!isJsonResponse(response) || !response.body || response.bodyUsed) return none;
  const declaredLength = Number(response.headers.get("content-length") ?? "");
  if (Number.isFinite(declaredLength) && declaredLength > RELAYFILE_BACKPRESSURE_HINT_MAX_BYTES) return none;
  let reader: ReadableStreamDefaultReader<Uint8Array>;
  try {
    reader = response.clone().body!.getReader();
  } catch {
    return none;
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timedOut = new Promise<"timeout">((resolve) => {
    timer = setTimeout(() => resolve("timeout"), RELAYFILE_BACKPRESSURE_HINT_READ_TIMEOUT_MS);
  });
  const chunks: Uint8Array[] = [];
  let total = 0;
  let complete = false;
  try {
    while (true) {
      const next = await Promise.race([reader.read(), timedOut]);
      if (next === "timeout") break;
      if (next.done) {
        complete = true;
        break;
      }
      total += next.value.byteLength;
      if (total > RELAYFILE_BACKPRESSURE_HINT_MAX_BYTES) break;
      chunks.push(next.value);
    }
  } catch {
    // Errored body (e.g. the attempt's AbortSignal fired): no hint.
  } finally {
    clearTimeout(timer);
    if (!complete) {
      // Fire-and-forget: cancelling one tee branch settles only once the other
      // branch is cancelled too, so awaiting it here could block the decision.
      reader.cancel().catch(() => undefined);
    }
  }
  if (!complete) return none;
  try {
    const bytes = new Uint8Array(total);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    const parsed: unknown = JSON.parse(new TextDecoder().decode(bytes));
    if (!parsed || typeof parsed !== "object") return none;
    const details = (parsed as { details?: unknown }).details;
    return {
      retryAfterSeconds: readRetryAfterSecondsField(details) ?? readRetryAfterSecondsField(parsed),
      reason: readReasonField(parsed) ?? readReasonField(details),
    };
  } catch {
    return none;
  }
}

async function cancelBestEffort(response: Response): Promise<void> {
  // The body may already be errored (e.g. the attempt's own AbortSignal fired);
  // a rejecting cancel must never escape and change the retry decision.
  try {
    await response.body?.cancel();
  } catch {
    // Unusable either way.
  }
}

export type RelayfileBackpressureOptions = {
  /** Issue one attempt. Must create a fresh signature and AbortSignal per call. */
  post: () => Promise<Response>;
  surface: RelayfileBackpressureSurface;
  maxRetries: number;
  budgetMs: number;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
  /** Consult the JSON body for a retry hint when the header is absent (default true). */
  readBodyRetryAfterSeconds?: boolean;
  /** Statuses retried in place (default 429, 503, 500). */
  retryStatuses?: ReadonlySet<number>;
  /** Observes every retryable response, including ones retried in place (§10 `relayfile_backpressure`). */
  onBackpressure?: (event: {
    status: number;
    retryAfterMs: number | null;
    attempt: number;
    budgetRemainingMs: number;
    relayfileReason: string | null;
  }) => void;
};

export type RelayfileBackpressureOutcome = {
  response: Response;
  /** Number of `post()` calls made. */
  attempts: number;
};

const defaultSleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Core loop shared by both entry points. Returns the final response untouched
 * (a deferred/exhausted 429 is NOT cancelled here).
 */
export async function retryRelayfileBackpressure({
  post,
  maxRetries,
  budgetMs,
  now = () => Date.now(),
  sleep = defaultSleep,
  random = Math.random,
  readBodyRetryAfterSeconds: readBody = true,
  retryStatuses = RETRY_IN_PLACE_STATUSES,
  onBackpressure,
}: Omit<RelayfileBackpressureOptions, "surface">): Promise<RelayfileBackpressureOutcome> {
  const deadlineMs = now() + budgetMs;
  let response = await post();
  let attempts = 1;
  for (let attempt = 0; attempt < maxRetries && retryStatuses.has(response.status); attempt += 1) {
    const retryAfterHeader = response.headers.get("retry-after");
    const headerMs = parseRetryAfterMs(retryAfterHeader, now());
    const hint = readBody && (headerMs === null || onBackpressure)
      ? await readBodyHint(response)
      : { retryAfterSeconds: null, reason: null };
    const bodyRetryAfterSeconds = headerMs === null ? hint.retryAfterSeconds : null;
    onBackpressure?.({
      status: response.status,
      retryAfterMs: headerMs ?? secondsToMs(bodyRetryAfterSeconds),
      attempt: attempts,
      budgetRemainingMs: Math.max(0, deadlineMs - now()),
      relayfileReason: hint.reason,
    });
    const delayMs = resolveRelayfileBackpressureDelayMs({
      retryAfterHeader,
      bodyRetryAfterSeconds,
      attempt,
      nowMs: now(),
      deadlineMs,
      random,
    });
    if (delayMs === null) break;
    await sleep(delayMs);
    // Re-check AFTER the wait: sleep may overshoot (or land on) the deadline.
    if (now() >= deadlineMs) break;
    // Cancel only the response being replaced; the final one is the caller's.
    await cancelBestEffort(response);
    // `cancel()` can itself consume budget.
    if (now() >= deadlineMs) break;
    response = await post();
    attempts += 1;
  }
  return { response, attempts };
}

/**
 * Issue `post()`, retrying 429 in place within `maxRetries` / `budgetMs`.
 * Non-429 responses are returned untouched. A final 429 has its body cancelled
 * and is thrown as `RelayfileBackpressureError`.
 */
export async function fetchWithRelayfileBackpressure(options: RelayfileBackpressureOptions): Promise<Response> {
  const now = options.now ?? (() => Date.now());
  const retryStatuses = options.retryStatuses ?? RETRY_IN_PLACE_STATUSES;
  const { response, attempts } = await retryRelayfileBackpressure({ ...options, now, retryStatuses });
  if (!retryStatuses.has(response.status)) return response;
  const nowMs = now();
  const headerMs = parseRetryAfterMs(response.headers.get("retry-after"), nowMs);
  // Cloud rule: the body is only consulted when the header is absent/malformed.
  // With an observer attached (production), it is also read (same bounds) for `relayfileReason`.
  const hint = options.readBodyRetryAfterSeconds === false || (headerMs !== null && !options.onBackpressure)
    ? { retryAfterSeconds: null, reason: null }
    : await readBodyHint(response);
  const retryAfterMs = headerMs ?? secondsToMs(hint.retryAfterSeconds);
  options.onBackpressure?.({
    status: response.status,
    retryAfterMs,
    attempt: attempts,
    budgetRemainingMs: 0,
    relayfileReason: hint.reason,
  });
  await cancelBestEffort(response);
  if (!BACKPRESSURE_STATUSES.has(response.status)) {
    throw new RelayfileServerError({ status: response.status, attempts, surface: options.surface, relayfileReason: hint.reason });
  }
  throw new RelayfileBackpressureError({
    status: response.status,
    retryAfterMs,
    attempts,
    surface: options.surface,
    relayfileReason: hint.reason,
  });
}

function secondsToMs(seconds: number | null): number | null {
  if (seconds === null) return null;
  const ms = seconds * 1_000;
  return Number.isFinite(ms) ? ms : null;
}
