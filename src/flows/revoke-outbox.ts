import type { Db } from "../db/db.ts";
import type { Logger } from "../observability/log.ts";
import { BrokerError, type CliCredentialBroker } from "./broker.ts";
import type { SqlExecutor } from "./database.ts";

/**
 * Durable outbox for CLI-credential revocations. Finishing a run records the
 * revocation in the same transaction as the status change; this worker
 * delivers it to relay-control (CliCredentialBroker.revokeForRun) and retries
 * with capped exponential backoff until it is acknowledged. Any replica may
 * run a worker: a row is leased before delivery, so two workers never send
 * the same revocation at once (a lost lease only causes a harmless repeat:
 * revokeForRun is idempotent).
 */
export async function enqueueCredentialRevocation(tx: SqlExecutor, runId: string): Promise<void> {
  await tx.query(
    `INSERT INTO selfhost_cli_credential_revocations (run_id) VALUES ($1)
     ON CONFLICT (run_id) DO NOTHING`,
    [runId],
  );
}

export type RevokeOutboxOptions = {
  db: Db;
  broker: CliCredentialBroker;
  logger?: Logger;
  /** Rows per drain pass. */
  batchSize?: number;
  /** How long a leased row is hidden from other workers. */
  leaseMs?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  intervalMs?: number;
};

export function revokeRetryDelayMs(attempts: number, baseDelayMs: number, maxDelayMs: number): number {
  return Math.min(maxDelayMs, baseDelayMs * 2 ** Math.min(Math.max(attempts - 1, 0), 20));
}

export class RevokeOutbox {
  readonly #db: Db;
  readonly #broker: CliCredentialBroker;
  readonly #logger: Logger | undefined;
  readonly #batchSize: number;
  readonly #leaseMs: number;
  readonly #baseDelayMs: number;
  readonly #maxDelayMs: number;
  readonly #intervalMs: number;
  #timer: NodeJS.Timeout | null = null;
  #running: Promise<unknown> | null = null;

  constructor(options: RevokeOutboxOptions) {
    this.#db = options.db;
    this.#broker = options.broker;
    this.#logger = options.logger;
    this.#batchSize = options.batchSize ?? 50;
    this.#leaseMs = options.leaseMs ?? 60_000;
    this.#baseDelayMs = options.baseDelayMs ?? 5_000;
    this.#maxDelayMs = options.maxDelayMs ?? 10 * 60_000;
    this.#intervalMs = options.intervalMs ?? 5_000;
  }

  /** One pass: delivers every due revocation. Returns delivered/failed counts. */
  async drain(): Promise<{ delivered: number; failed: number }> {
    const leased = await this.#db.query<{ run_id: string; attempts: number }>(
      `UPDATE selfhost_cli_credential_revocations o
          SET next_attempt_at = now() + ($2::bigint * interval '1 millisecond'),
              attempts = o.attempts + 1
        WHERE o.run_id IN (
          SELECT run_id FROM selfhost_cli_credential_revocations
           WHERE next_attempt_at <= now()
           ORDER BY next_attempt_at, run_id
           LIMIT $1
           FOR UPDATE SKIP LOCKED)
        RETURNING o.run_id, o.attempts`,
      [this.#batchSize, this.#leaseMs],
    );
    let delivered = 0;
    let failed = 0;
    for (const row of leased.rows) {
      try {
        // Only a 2xx acknowledgement deletes the row: revokeForRun is
        // idempotent, so relay-control answers 2xx even for a run it never
        // issued a reference for. Any other status (404 included: a wrong
        // route or base URL) is retried with backoff.
        await this.#broker.revokeForRun(row.run_id);
        await this.#db.query("DELETE FROM selfhost_cli_credential_revocations WHERE run_id = $1", [row.run_id]);
        delivered += 1;
      } catch (error) {
        failed += 1;
        const reason = error instanceof BrokerError ? error.code : error instanceof Error ? error.name : "error";
        const delay = revokeRetryDelayMs(Number(row.attempts), this.#baseDelayMs, this.#maxDelayMs);
        await this.#db.query(
          `UPDATE selfhost_cli_credential_revocations
              SET last_error = $2, next_attempt_at = now() + ($3::bigint * interval '1 millisecond')
            WHERE run_id = $1`,
          [row.run_id, reason, delay],
        );
        this.#logger?.event("flows_revoke_retry", {
          area: "flows",
          runId: row.run_id,
          attempts: Number(row.attempts),
          ...(error instanceof BrokerError ? { status: error.status } : {}),
          reason,
          retryInMs: delay,
        });
      }
    }
    return { delivered, failed };
  }

  start(): void {
    if (this.#timer) return;
    const tick = () => {
      if (this.#running) return;
      this.#running = this.drain()
        .catch((error: unknown) => {
          this.#logger?.event("flows_revoke_drain_failed", {
            area: "flows",
            reason: error instanceof Error ? error.name : "error",
          });
        })
        .finally(() => {
          this.#running = null;
        });
    };
    this.#timer = setInterval(tick, this.#intervalMs);
    this.#timer.unref();
    tick();
  }

  async stop(): Promise<void> {
    if (this.#timer) clearInterval(this.#timer);
    this.#timer = null;
    await this.#running;
  }
}
