import {
  fetchWithRelayfileBackpressure,
  readBodyHint,
  type RelayfileBackpressureSurface,
} from "./backpressure.ts";
import { signedHeaders } from "./signer.ts";

/** §6.2 in-place policies. */
export type InPlacePolicy = { maxRetries: number; budgetMs: number; attemptTimeoutMs: number };
export const ENVELOPE_ADMISSION_POLICY: InPlacePolicy = { maxRetries: 1, budgetMs: 30_000, attemptTimeoutMs: 15_000 };
export const PROVIDER_WRITES_POLICY: InPlacePolicy = { maxRetries: 1, budgetMs: 30_000, attemptTimeoutMs: 30_000 };

/** A contract failure that must not be retried (400/401/403/413, redirect). */
export class TerminalHandlerError extends Error {
  readonly retryable = false;
  constructor(
    readonly code: string,
    readonly status: number | null,
    readonly relayfileReason: string | null = null,
  ) {
    super(code);
    this.name = "TerminalHandlerError";
  }
}

/** Any other unexpected Relayfile status: normal retry schedule. */
export class RelayfileUnexpectedStatusError extends Error {
  constructor(
    readonly status: number,
    readonly surface: RelayfileBackpressureSurface,
    readonly relayfileReason: string | null,
  ) {
    super(`Relayfile ${surface} unexpected status ${status}`);
    this.name = "RelayfileUnexpectedStatusError";
  }
}

/** Body of POST /v1/internal/webhook-envelopes (OSS is additionalProperties:false). */
export type WebhookEnvelopeBody = {
  envelopeId: string;
  workspaceId: string;
  provider: string;
  deliveryId: string;
  receivedAt: string;
  correlationId: string;
  payload: {
    event_type: "file.created" | "file.updated" | "file.deleted";
    path: string;
    content: string;
    contentType?: string;
    providerEventType?: string;
    resourceRef?: string;
  };
  headers?: Record<string, string>;
};

export type EnvelopeResult = { status: number; outcome: "queued" | "repaired" | "duplicate" | "accepted" };

export type BackpressureObserver = Parameters<typeof fetchWithRelayfileBackpressure>[0]["onBackpressure"];

export type RelayfileClientOptions = {
  hmacSecret: string;
  fetch?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
  random?: () => number;
  now?: () => number;
};

export class RelayfileClient {
  private readonly fetchImpl: typeof fetch;
  constructor(private readonly options: RelayfileClientOptions) {
    this.fetchImpl = options.fetch ?? fetch;
  }

  /**
   * POST one envelope with the envelope-admission in-place policy (§6.2).
   * 202 queued / 202 repaired / 409 duplicate_envelope are acks. 400/401/403/413
   * and any redirect are terminal. A final 429/503 throws
   * RelayfileBackpressureError; a final 500 throws RelayfileServerError.
   */
  async postEnvelope(
    baseUrl: string,
    body: WebhookEnvelopeBody,
    opts: { policy?: InPlacePolicy; signal?: AbortSignal; onBackpressure?: BackpressureObserver } = {},
  ): Promise<EnvelopeResult> {
    const policy = opts.policy ?? ENVELOPE_ADMISSION_POLICY;
    const raw = JSON.stringify(body);
    const url = new URL("/v1/internal/webhook-envelopes", baseUrl).toString();
    const response = await fetchWithRelayfileBackpressure({
      surface: "webhook-envelopes",
      maxRetries: policy.maxRetries,
      budgetMs: policy.budgetMs,
      ...(this.options.now ? { now: this.options.now } : {}),
      ...(this.options.sleep ? { sleep: this.options.sleep } : {}),
      ...(this.options.random ? { random: this.options.random } : {}),
      ...(opts.onBackpressure ? { onBackpressure: opts.onBackpressure } : {}),
      post: () => {
        // Fresh signature and fresh per-attempt timeout on every attempt.
        const timeout = AbortSignal.timeout(policy.attemptTimeoutMs);
        const signal = opts.signal ? AbortSignal.any([opts.signal, timeout]) : timeout;
        return this.fetchImpl(url, {
          method: "POST",
          headers: signedHeaders(this.options.hmacSecret, raw, body.correlationId),
          body: raw,
          redirect: "manual",
          signal,
        });
      },
    });
    return classifyEnvelopeResponse(response);
  }
}

async function classifyEnvelopeResponse(response: Response): Promise<EnvelopeResult> {
  const status = response.status;
  if (status >= 300 && status < 400) {
    await response.body?.cancel().catch(() => undefined);
    throw new TerminalHandlerError("relayfile_redirect", status);
  }
  if (status === 202 || status === 200 || status === 201) {
    const hint = await readStatusField(response);
    return { status, outcome: hint === "repaired" ? "repaired" : hint === "queued" ? "queued" : "accepted" };
  }
  if (status === 409) {
    await response.body?.cancel().catch(() => undefined);
    return { status, outcome: "duplicate" };
  }
  const { reason } = await readBodyHint(response);
  await response.body?.cancel().catch(() => undefined);
  if (status === 413) throw new TerminalHandlerError("relayfile_payload_too_large", status, reason);
  if (status === 400 || status === 401 || status === 403 || status === 422) {
    throw new TerminalHandlerError(`relayfile_rejected_${status}`, status, reason);
  }
  throw new RelayfileUnexpectedStatusError(status, "webhook-envelopes", reason);
}

async function readStatusField(response: Response): Promise<string | null> {
  try {
    const text = await response.text();
    if (text.length > 4096) return null;
    const parsed = JSON.parse(text) as { status?: unknown };
    return typeof parsed.status === "string" ? parsed.status : null;
  } catch {
    return null;
  }
}
