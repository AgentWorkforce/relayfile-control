import type { Config } from "../config.ts";
import type { Db } from "../db/db.ts";
import type { EnvelopeRow } from "../envelope/model.ts";
import { ulid } from "../envelope/ulid.ts";
import type { Entitlement } from "../entitlement/entitlement.ts";
import { laneHash, type Logger } from "../observability/log.ts";
import type { Metrics } from "../observability/metrics.ts";
import { backpressureCooldownSeconds, type CooldownStore } from "../queue/cooldown.ts";
import { resolveForwardRetryDelaySeconds } from "../queue/retry-delay.ts";
import { claimNext, fanOut, heartbeat, markDead, markDone, markRetry } from "../queue/store.ts";
import { type RelayfileBackpressureError, RelayfileServerError } from "../relayfile/backpressure.ts";
import { type RelayfileClient, RelayfileUnexpectedStatusError, TerminalHandlerError } from "../relayfile/client.ts";
import { type EnvelopeHandler, forwardEnvelope, RetryableHandlerError } from "./envelope-handler.ts";
import { backpressureRetryAfterSeconds, detectRelayfileBackpressure } from "./failure.ts";
import { handlerDeadlineMs } from "./policy.ts";

export class HandlerTimeoutError extends Error {
  constructor(readonly deadlineMs: number) {
    super("handler_deadline_exceeded");
    this.name = "HandlerTimeoutError";
  }
}

export type ConsumerDeps = {
  db: Db;
  config: Config;
  logger: Logger;
  metrics: Metrics;
  entitlement: Entitlement;
  cooldowns: CooldownStore;
  relayfile: RelayfileClient | null;
  handler?: EnvelopeHandler;
  random?: () => number;
  /** Override the deadline table (tests). */
  deadlineMs?: (provider: string) => number;
};

export type Disposition = "ack" | "ack_terminal" | "retry" | "dead" | "fanout" | "lease_lost";

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * §4 consumer: CONSUMER_CONCURRENCY slots per process, each claiming one
 * envelope at a time with the strict per-lane cap (§4.2), heartbeating the
 * lease, aborting at the provider deadline (§4.3), and mapping every outcome
 * to a disposition (§4.4). Nothing is ever acked without a handler success or
 * a terminal contract error, and nothing is dropped: attempts ≥ MAX_ATTEMPTS
 * goes to the DLQ.
 */
export class Consumer {
  readonly id = `consumer-${ulid()}`;
  private running = false;
  private slots: Promise<void>[] = [];
  private readonly handler: EnvelopeHandler;

  constructor(private readonly deps: ConsumerDeps) {
    this.handler = deps.handler ?? forwardEnvelope;
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    for (let i = 0; i < this.deps.config.CONSUMER_CONCURRENCY; i += 1) this.slots.push(this.slot(i));
  }

  async stop(): Promise<void> {
    this.running = false;
    await Promise.all(this.slots);
    this.slots = [];
  }

  private async slot(index: number): Promise<void> {
    const worker = `${this.id}/${index}`;
    while (this.running) {
      let worked = false;
      try {
        worked = await this.runOnce(worker);
      } catch (error) {
        this.deps.logger.event("consumer_error", { area: "consumer", worker, code: error instanceof Error ? error.name : "unknown" });
      }
      if (!worked) await sleep(this.deps.config.CONSUMER_IDLE_POLL_MS);
    }
  }

  /** Claim and process at most one envelope. False when nothing was claimable (or paused). */
  async runOnce(worker = `${this.id}/manual`): Promise<boolean> {
    if (!this.deps.entitlement.isEntitled()) return false; // §11: rows stay queued.
    const row = await claimNext(this.deps.db, {
      worker,
      leaseSeconds: this.deps.config.LEASE_SECONDS,
      perLaneMax: this.deps.config.PER_LANE_MAX_INFLIGHT,
    });
    if (!row) return false;
    await this.process(row, worker);
    return true;
  }

  /** Drain until no claimable work remains (tests / e2e). */
  async drain(maxRows = 100_000): Promise<number> {
    let n = 0;
    while (n < maxRows && (await this.runOnce())) n += 1;
    return n;
  }

  async process(row: EnvelopeRow, worker: string): Promise<Disposition> {
    const { config, metrics, logger } = this.deps;
    const started = Date.now();
    const deadline = (this.deps.deadlineMs ?? handlerDeadlineMs)(row.provider);
    const abort = new AbortController();
    const beat = setInterval(() => {
      void heartbeat(this.deps.db, row.id, worker, config.LEASE_SECONDS).catch(() => undefined);
    }, Math.max(1000, Math.floor((config.LEASE_SECONDS * 1000) / 3)));
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        abort.abort(new HandlerTimeoutError(deadline));
        reject(new HandlerTimeoutError(deadline));
      }, deadline);
    });

    let disposition: Disposition;
    let fields: Record<string, unknown> = {};
    try {
      const outcome = await Promise.race([
        this.handler(row, {
          db: this.deps.db,
          config,
          logger,
          metrics,
          relayfile: this.deps.relayfile,
          worker,
          signal: abort.signal,
        }),
        timeout,
      ]);
      if (outcome.type === "fanout") {
        const children = await fanOut(this.deps.db, row, worker, outcome.workspaceIds, { disposition: "ack", reason: outcome.reason });
        disposition = "fanout";
        fields = { reason: outcome.reason, children: children.length };
      } else {
        fields = { ...outcome, type: undefined };
        const ok = await markDone(this.deps.db, row.id, worker, { disposition: "ack", reason: outcome.reason, status: outcome.relayfileStatus });
        disposition = ok ? "ack" : "lease_lost";
      }
    } catch (error) {
      ({ disposition, fields } = await this.onError(row, worker, error));
    } finally {
      clearTimeout(timer);
      clearInterval(beat);
    }

    const code = typeof fields.code === "string" ? fields.code : "none";
    metrics.inc("consume_total", { provider: row.provider.split("-")[0] ?? row.provider, disposition, code });
    logger.event("consume_outcome", {
      area: "consumer",
      requestId: row.request_id,
      envelopeId: row.id,
      laneHash: laneHash(row.workspace_id ?? row.lane),
      provider: row.provider,
      ingress: row.ingress,
      disposition,
      attempts: row.attempts,
      durationMs: Date.now() - started,
      ...fields,
    });
    return disposition;
  }

  private async onError(row: EnvelopeRow, worker: string, error: unknown): Promise<{ disposition: Disposition; fields: Record<string, unknown> }> {
    const { config, db } = this.deps;
    const lane = row.workspace_id ?? row.lane;

    if (error instanceof TerminalHandlerError) {
      const fields = { code: error.code, status: error.status, relayfileReason: error.relayfileReason, terminal: true };
      const ok = await markDone(db, row.id, worker, { disposition: "ack_terminal", ...fields });
      return { disposition: ok ? "ack_terminal" : "lease_lost", fields };
    }

    let retryAfterSeconds: number | null = null;
    let fields: Record<string, unknown>;
    const backpressure = detectRelayfileBackpressure(error);
    if (backpressure) {
      // HARD: max(1, ceil(retryAfterMs/1000)) or default 60; never an ack, never a drop.
      retryAfterSeconds = backpressureRetryAfterSeconds(backpressure.retryAfterMs);
      const src = backpressure.source as Partial<RelayfileBackpressureError>;
      fields = {
        code: "relayfile_backpressure",
        status: src.status,
        surface: src.surface,
        relayfileReason: src.relayfileReason ?? null,
        retryAfterSeconds,
        inPlaceAttempts: src.attempts,
      };
      // #6177 arming contract: only a final backpressure error arms (never 5xx/timeouts/network).
      const window = backpressureCooldownSeconds(error);
      if (this.deps.cooldowns.enabled && window !== null) {
        const remaining = await this.deps.cooldowns.arm(lane, window, worker, src.relayfileReason ?? `status_${src.status ?? "unknown"}`);
        fields.cooldownArmedSeconds = window;
        this.deps.metrics.inc("cooldown_armed_total", { provider: row.provider.split("-")[0] ?? row.provider });
        this.deps.logger.event("cooldown_armed", { area: "consumer", laneHash: laneHash(lane), provider: row.provider, armedSeconds: window, cooldownRemainingSeconds: remaining });
      }
    } else if (error instanceof RelayfileServerError || error instanceof RelayfileUnexpectedStatusError) {
      fields = { code: "relayfile_server_error", status: error.status, surface: error.surface, relayfileReason: error.relayfileReason };
    } else if (error instanceof HandlerTimeoutError) {
      fields = { code: "handler_deadline_exceeded", deadlineMs: error.deadlineMs };
    } else if (error instanceof RetryableHandlerError) {
      fields = { code: error.code };
    } else {
      fields = { code: "handler_error", errorName: error instanceof Error ? error.name : "unknown" };
    }

    const retryDelaySeconds = resolveForwardRetryDelaySeconds({
      attempts: row.attempts,
      retryAfterSeconds,
      ...(this.deps.random ? { random: this.deps.random } : {}),
    });
    if (row.attempts >= config.MAX_ATTEMPTS) {
      const out = { ...fields, retryDelaySeconds, attempts: row.attempts };
      const ok = await markDead(db, row.id, worker, { disposition: "dead", ...out });
      return { disposition: ok ? "dead" : "lease_lost", fields: out };
    }
    const out = { ...fields, retryDelaySeconds, attempts: row.attempts };
    const ok = await markRetry(db, row.id, worker, retryDelaySeconds, { disposition: "retry", ...out });
    return { disposition: ok ? "retry" : "lease_lost", fields: out };
  }
}
