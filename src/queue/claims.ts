import type { Queryable } from "../db/db.ts";

/**
 * §3 level 2: handler claims, one implementation for every surface.
 *
 * Claim = the plan's atomic upsert, with the expired-lease branch narrowed to
 * `processing` rows so the §3 HARD invariant (completed → duplicate) holds
 * whatever `lease_until` a completed row carries. `lease_until` is also
 * cleared on completion.
 *
 * With `coalesceSeconds` (Google-mail forward sync), a `completed` row older
 * than the window may be reclaimed; younger is a duplicate (Cloud behaviour).
 */
/**
 * `claimed: false` distinguishes a finished delivery (`inFlight: false`: a true
 * duplicate, ack it) from one still in flight under a live lease (`inFlight:
 * true`: the holder may crash mid-delivery, so retry, never ack).
 * `attempts` is the fencing token for complete/fail.
 */
export type ClaimResult = { claimed: true; attempts: number } | { claimed: false; inFlight: boolean };

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
      WHERE claims.status = 'failed' OR (claims.status = 'processing' AND claims.lease_until < now())
         ${coalesce ? "OR (claims.status = 'completed' AND claims.completed_at < now() - make_interval(secs => $5))" : ""}
     RETURNING attempts`,
    coalesce
      ? [input.surface, input.dedupeId, input.leaseSeconds, input.envelopeId ?? null, input.coalesceSeconds]
      : [input.surface, input.dedupeId, input.leaseSeconds, input.envelopeId ?? null],
  );
  const row = rows[0];
  if (row) return { claimed: true, attempts: row.attempts };
  const existing = await db.query<{ status: string }>("SELECT status FROM webhook_delivery_claims WHERE surface = $1 AND dedupe_id = $2", [
    input.surface,
    input.dedupeId,
  ]);
  // Not reclaimed: a completed row is a true duplicate. Any other row means a transition between the
  // upsert and this read (a `failed` row cannot have blocked the upsert, which reclaims failed rows), so
  // retry. A missing row was a purged completed claim (only completed rows are swept): duplicate.
  const status = existing.rows[0]?.status;
  return { claimed: false, inFlight: status !== undefined && status !== "completed" };
}

/**
 * Handler success, fenced on the claim attempt: a late handler whose lease
 * expired and was reclaimed cannot complete the newer claim. False when fenced out.
 */
export async function completeClaim(db: Queryable, surface: string, dedupeId: string, attempts: number): Promise<boolean> {
  const r = await db.query(
    `UPDATE webhook_delivery_claims SET status = 'completed', lease_until = NULL, completed_at = now()
      WHERE surface = $1 AND dedupe_id = $2 AND status = 'processing' AND attempts = $3`,
    [surface, dedupeId, attempts],
  );
  return r.rowCount > 0;
}

/** Handler failure: release so the next attempt can reclaim. Fenced like `completeClaim`. */
export async function failClaim(db: Queryable, surface: string, dedupeId: string, attempts: number): Promise<boolean> {
  const r = await db.query(
    `UPDATE webhook_delivery_claims SET status = 'failed', lease_until = NULL
      WHERE surface = $1 AND dedupe_id = $2 AND status = 'processing' AND attempts = $3`,
    [surface, dedupeId, attempts],
  );
  return r.rowCount > 0;
}
