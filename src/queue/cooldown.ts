import type { Queryable } from "../db/db.ts";
import { detectRelayfileBackpressure } from "../consumer/failure.ts";

// Arming contract ported from cloud#6177 @ bda4057 (workspace-cooldown.ts); see plan §6.3
// for the two deliberate differences (key = lane, store = shared table).
export const COOLDOWN_DEFAULT_SECONDS = 60;
export const COOLDOWN_MAX_SECONDS = 86_400;
export const COOLDOWN_DEFAULT_WINDOW_SECONDS = COOLDOWN_DEFAULT_SECONDS;
export const COOLDOWN_MAX_WINDOW_SECONDS = COOLDOWN_MAX_SECONDS;
export const COOLDOWN_MAX_KEYS = 1_000;

/**
 * Cool-down key for an envelope: its lane (the resolved workspace, or the
 * `unresolved:<provider>:<id>` lane). Cloud keyed by providerConfigKey +
 * connection because its worker did not know the workspace (plan §6.3).
 */
export function cooldownKeyForEnvelope(row: { lane: string; workspace_id: string | null }): string {
  return row.workspace_id ?? row.lane;
}

/**
 * Window (seconds) a handler failure arms, or null when it arms nothing. Only a
 * final typed Relayfile backpressure error arms (Cloud: only 503 + code
 * relayfile_backpressure, which is cloud-web's translation of that error).
 * Window = the error's Retry-After (header, else body retryAfterSeconds, as
 * resolved by the backpressure module) → 60; clamped to [1, 86 400].
 */
export function backpressureCooldownSeconds(error: unknown): number | null {
  const detected = detectRelayfileBackpressure(error);
  if (!detected) return null;
  return cooldownWindowSeconds({ retryAfterMs: detected.retryAfterMs });
}

/**
 * §6.3 window: Retry-After delta > 0 → body retryAfterSeconds > 0 → 60,
 * clamped [1, 86 400].
 */
export function cooldownWindowSeconds(input: { retryAfterMs: number | null; bodyRetryAfterSeconds?: number | null }): number {
  let seconds = COOLDOWN_DEFAULT_SECONDS;
  if (input.retryAfterMs !== null && Number.isFinite(input.retryAfterMs) && input.retryAfterMs > 0) {
    seconds = Math.ceil(input.retryAfterMs / 1000);
  } else if (typeof input.bodyRetryAfterSeconds === "number" && input.bodyRetryAfterSeconds > 0) {
    seconds = Math.ceil(input.bodyRetryAfterSeconds);
  }
  return Math.min(COOLDOWN_MAX_SECONDS, Math.max(1, seconds));
}

export class CooldownStore {
  constructor(
    private readonly db: Queryable,
    private readonly opts: { enabled: boolean; maxKeys: number },
  ) {}

  get enabled(): boolean {
    return this.opts.enabled;
  }

  /** Lengthen-only arm. Returns the effective remaining seconds; null when nothing was armed. */
  async arm(lane: string | null, seconds: number, armedBy = "consumer", reason = "relayfile_backpressure"): Promise<number | null> {
    if (!this.opts.enabled || !lane || !Number.isFinite(seconds) || seconds <= 0) return null;
    const { rows } = await this.db.query<{ remaining: number }>(
      `INSERT INTO lane_cooldowns (lane, until, armed_by, reason)
       VALUES ($1, now() + make_interval(secs => $2), $3, $4)
       ON CONFLICT (lane) DO UPDATE
          SET until = GREATEST(lane_cooldowns.until, EXCLUDED.until),
              armed_by = CASE WHEN EXCLUDED.until > lane_cooldowns.until THEN EXCLUDED.armed_by ELSE lane_cooldowns.armed_by END,
              reason = CASE WHEN EXCLUDED.until > lane_cooldowns.until THEN EXCLUDED.reason ELSE lane_cooldowns.reason END
       RETURNING EXTRACT(EPOCH FROM (until - now()))::float8 AS remaining`,
      [lane, seconds, armedBy, reason],
    );
    await this.prune();
    return Math.max(0, Math.ceil(rows[0]?.remaining ?? 0));
  }

  /** Remaining seconds, 0 when not cooled. Expired rows read as absent. */
  async remaining(lane: string | null): Promise<number> {
    if (!this.opts.enabled || !lane) return 0;
    const { rows } = await this.db.query<{ remaining: number }>(
      "SELECT EXTRACT(EPOCH FROM (until - now()))::float8 AS remaining FROM lane_cooldowns WHERE lane = $1 AND until > now()",
      [lane],
    );
    return Math.max(0, Math.ceil(rows[0]?.remaining ?? 0));
  }

  async list(): Promise<{ lane: string; remainingSeconds: number; reason: string | null }[]> {
    const { rows } = await this.db.query<{ lane: string; remaining: number; reason: string | null }>(
      "SELECT lane, EXTRACT(EPOCH FROM (until - now()))::float8 AS remaining, reason FROM lane_cooldowns WHERE until > now() ORDER BY until DESC",
    );
    return rows.map((r) => ({ lane: r.lane, remainingSeconds: Math.ceil(r.remaining), reason: r.reason }));
  }

  /** Live (unexpired) keys. */
  async size(): Promise<number> {
    const { rows } = await this.db.query<{ n: number }>("SELECT count(*)::int AS n FROM lane_cooldowns WHERE until > now()");
    return rows[0]?.n ?? 0;
  }

  async clear(lane: string): Promise<boolean> {
    const r = await this.db.query("DELETE FROM lane_cooldowns WHERE lane = $1", [lane]);
    return r.rowCount > 0;
  }

  /** Expire on read, plus an LRU bound of COOLDOWN_MAX_KEYS by oldest `until`. */
  async prune(): Promise<void> {
    await this.db.query("DELETE FROM lane_cooldowns WHERE until <= now()");
    await this.db.query(
      `DELETE FROM lane_cooldowns WHERE lane IN (
         SELECT lane FROM lane_cooldowns ORDER BY until DESC OFFSET $1)`,
      [this.opts.maxKeys],
    );
  }

  /** Cooled lanes that currently hold visible queued work (for `forward_cooldown_skip`). */
  async skippedLanes(): Promise<{ lane: string; provider: string; remaining: number; queued: number }[]> {
    if (!this.opts.enabled) return [];
    const { rows } = await this.db.query<{ lane: string; provider: string; remaining: number; queued: number }>(
      `SELECT c.lane, min(e.provider) AS provider, EXTRACT(EPOCH FROM (c.until - now()))::float8 AS remaining, count(*)::int AS queued
         FROM lane_cooldowns c JOIN webhook_envelopes e ON e.lane = c.lane
        WHERE c.until > now() AND e.state = 'queued' AND e.visible_at <= now()
        GROUP BY c.lane, c.until`,
    );
    return rows;
  }
}
