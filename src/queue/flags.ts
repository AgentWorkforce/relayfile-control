import type { Queryable } from "../db/db.ts";

/** Operator switches shared by all pods (service_flags), cached briefly. */
export class FlagStore {
  private cache: { paused: boolean; at: number } | null = null;
  constructor(
    private readonly db: Queryable,
    private readonly cacheMs = 2000,
  ) {}

  async ingressPaused(): Promise<boolean> {
    if (this.cache && Date.now() - this.cache.at < this.cacheMs) return this.cache.paused;
    const { rows } = await this.db.query<{ value: unknown }>("SELECT value FROM service_flags WHERE key = 'ingress_paused'");
    const paused = rows[0]?.value === true;
    this.cache = { paused, at: Date.now() };
    return paused;
  }

  async setIngressPaused(paused: boolean, by: string): Promise<void> {
    await this.db.query(
      `INSERT INTO service_flags (key, value, updated_by) VALUES ('ingress_paused', $1::jsonb, $2)
       ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_by = EXCLUDED.updated_by, updated_at = now()`,
      [JSON.stringify(paused), by],
    );
    this.cache = null;
  }
}
