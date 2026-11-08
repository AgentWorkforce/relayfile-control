import type { Queryable } from "../db/db.ts";

/**
 * §3 level 2: handler claims, one implementation for every surface.
 *
 * Claim = the plan's atomic upsert. `lease_until` is cleared on completion,
 * so the plan's `lease_until < now()` predicate can only ever reclaim a
 * crashed `processing` row, never a `completed` one.
 *
 * With `coalesceSeconds` (Google-mail forward sync), a `completed` row older
 * than the window may be reclaimed; younger is a duplicate (Cloud behaviour).
 */
export type ClaimResult = { claimed: true; attempts: number } | { claimed: false };

export async function claim(
  db: Queryable,
  input: { surface: string; dedupeId: string; leaseSeconds: number; envelopeId?: string | null; coalesceSeconds?: number },
): Promise<ClaimResult> {
  const coalesce = input.coalesceSeconds !== undefined;
  const { rows } = await db.query<{ attempts: number }>(
    `INSERT INTO webhook_delivery_claims AS claims (surface, dedupe_id, status, lease_until, envelope_id, attempts)
     VALUES ($1, $2, 'processing', now() + make_interval(secs => $3), $4, 1)
     ON CONFLICT (surface, dedupe_id) DO UPDATE
        SET status = 'processing', lease_until = now() + make_interval(secs => $3),
            envelope_id = EXCLUDED.envelope_id, attempts = claims.attempts + 1, completed_at = NULL
      WHERE claims.status = 'failed' OR claims.lease_until < now()
         ${coalesce ? "OR (claims.status = 'completed' AND claims.completed_at < now() - make_interval(secs => $5))" : ""}
     RETURNING attempts`,
    coalesce
      ? [input.surface, input.dedupeId, input.leaseSeconds, input.envelopeId ?? null, input.coalesceSeconds]
      : [input.surface, input.dedupeId, input.leaseSeconds, input.envelopeId ?? null],
  );
  const row = rows[0];
  return row ? { claimed: true, attempts: row.attempts } : { claimed: false };
}

/** Handler success. */
export async function completeClaim(db: Queryable, surface: string, dedupeId: string): Promise<void> {
  await db.query(
    "UPDATE webhook_delivery_claims SET status = 'completed', lease_until = NULL, completed_at = now() WHERE surface = $1 AND dedupe_id = $2",
    [surface, dedupeId],
  );
}

/** Handler failure: release so the next attempt can reclaim. */
export async function failClaim(db: Queryable, surface: string, dedupeId: string): Promise<void> {
  await db.query(
    "UPDATE webhook_delivery_claims SET status = 'failed', lease_until = NULL WHERE surface = $1 AND dedupe_id = $2 AND status = 'processing'",
    [surface, dedupeId],
  );
}
