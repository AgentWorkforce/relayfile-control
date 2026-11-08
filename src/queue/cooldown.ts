import type { Queryable } from "../db/db.ts";

export const COOLDOWN_DEFAULT_SECONDS = 60;
export const COOLDOWN_MAX_SECONDS = 86_400;

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

  /** Lengthen-only arm. Returns the effective remaining seconds. */
  async arm(lane: string, seconds: number, armedBy: string, reason: string): Promise<number | null> {
    if (!this.opts.enabled) return null;
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
  async remaining(lane: string): Promise<number> {
    if (!this.opts.enabled) return 0;
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
