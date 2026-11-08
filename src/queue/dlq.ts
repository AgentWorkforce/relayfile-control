import type { Db, Queryable } from "../db/db.ts";

/** §8 preservation delay: min(300, 60·2^min(cycle,3)) seconds. */
export function preservationDelaySeconds(cycle: number): number {
  return Math.min(300, 60 * 2 ** Math.min(Math.max(0, cycle), 3));
}

/** Columns restored on replay (everything except queue state, which is reset). */
const RESTORED = [
  "id", "parent_id", "kind", "ingress", "provider", "request_id", "delivery_id", "has_provider_delivery_id",
  "event_type", "connection_id", "native_ids", "dedupe_surface", "dedupe_id", "headers", "payload",
  "payload_blob_id", "payload_sha256", "payload_bytes", "received_at", "workspace_id", "lane", "outcome_history",
] as const;

export type DlqArchiveRow = {
  id: string;
  lane: string;
  provider: string;
  received_at: Date;
  archived_at: Date;
  last_outcome: Record<string, unknown> | null;
  outcome_history: Record<string, unknown>[];
};

/**
 * Re-queue dead rows whose cycle < max (Cloud's preservation loop, as a DB
 * job). attempts reset to 0, cycle + 1.
 */
export async function preserveDead(db: Queryable, maxCycles: number): Promise<{ id: string; cycle: number; delay: number; lane: string }[]> {
  const { rows } = await db.query<{ id: string; preservation_cycle: number; lane: string }>(
    `UPDATE webhook_envelopes
        SET state = 'queued', attempts = 0, preservation_cycle = preservation_cycle + 1,
            visible_at = now() + make_interval(secs => LEAST(300, 60 * power(2, LEAST(preservation_cycle, 3)))::int),
            updated_at = now()
      WHERE state = 'dead' AND preservation_cycle < $1
      RETURNING id, preservation_cycle, lane`,
    [maxCycles],
  );
  return rows.map((r) => ({ id: r.id, cycle: r.preservation_cycle, delay: preservationDelaySeconds(r.preservation_cycle - 1), lane: r.lane }));
}

/** Exhausted dead rows move to the archive with their full row and outcome chain. */
export async function archiveExhausted(db: Db, maxCycles: number): Promise<{ id: string; lane: string; provider: string; cycle: number }[]> {
  return db.tx(async (q) => {
    const { rows } = await q.query<{ id: string; lane: string; provider: string; preservation_cycle: number }>(
      `WITH moved AS (
         DELETE FROM webhook_envelopes WHERE state = 'dead' AND preservation_cycle >= $1
         RETURNING *)
       INSERT INTO webhook_dlq_archive (id, lane, provider, received_at, payload_blob_id, envelope)
       SELECT m.id, m.lane, m.provider, m.received_at, m.payload_blob_id, to_jsonb(m) FROM moved m
       RETURNING id, lane, provider, (envelope->>'preservation_cycle')::int AS preservation_cycle`,
      [maxCycles],
    );
    return rows.map((r) => ({ id: r.id, lane: r.lane, provider: r.provider, cycle: r.preservation_cycle }));
  });
}

export type ReplaySelector = { ids?: string[]; lane?: string; provider?: string; since?: string; limit?: number };

/** Operator replay: archive → queued with attempts=0, cycle=0, replayed_by. */
export async function replayArchive(db: Db, selector: ReplaySelector, replayedBy: string): Promise<string[]> {
  if (!selector.ids?.length && !selector.lane && !selector.provider) throw new Error("replay requires ids, lane or provider");
  return db.tx(async (q) => {
    const cols = RESTORED.join(", ");
    const { rows } = await q.query<{ id: string }>(
      `WITH ids AS (
         SELECT a.id FROM webhook_dlq_archive a
          WHERE ($1::text[] IS NULL OR a.id = ANY($1::text[]))
            AND ($2::text IS NULL OR a.lane = $2)
            AND ($3::text IS NULL OR a.provider = $3)
            AND ($4::timestamptz IS NULL OR a.archived_at >= $4)
          ORDER BY a.archived_at LIMIT $5
          FOR UPDATE),
       picked AS (
         DELETE FROM webhook_dlq_archive a USING ids WHERE a.id = ids.id
         RETURNING a.envelope)
       INSERT INTO webhook_envelopes (${cols}, state, attempts, preservation_cycle, visible_at, replayed_by, last_outcome)
       SELECT ${RESTORED.map((c) => `r.${c}`).join(", ")}, 'queued', 0, 0, now(), $6,
              jsonb_build_object('disposition', 'replay', 'replayedBy', $6::text)
         FROM picked p, jsonb_populate_record(NULL::webhook_envelopes, p.envelope) r
       RETURNING id`,
      [selector.ids?.length ? selector.ids : null, selector.lane ?? null, selector.provider ?? null, selector.since ?? null, Math.min(selector.limit ?? 1000, 10_000), replayedBy],
    );
    return rows.map((r) => r.id);
  });
}

export async function listArchive(
  db: Queryable,
  filter: { lane?: string; provider?: string; limit?: number },
): Promise<DlqArchiveRow[]> {
  const { rows } = await db.query<DlqArchiveRow>(
    `SELECT id, lane, provider, received_at, archived_at, envelope->'last_outcome' AS last_outcome,
            envelope->'outcome_history' AS outcome_history
       FROM webhook_dlq_archive
      WHERE ($1::text IS NULL OR lane = $1) AND ($2::text IS NULL OR provider = $2)
      ORDER BY archived_at DESC LIMIT $3`,
    [filter.lane ?? null, filter.provider ?? null, Math.min(Math.max(filter.limit ?? 100, 1), 1000)],
  );
  return rows;
}

/** §8 retention: done envelopes + completed claims after ENVELOPE_RETENTION, archive after its own window, orphan blobs. */
export async function retentionSweep(
  db: Queryable,
  opts: { envelopeRetentionSeconds: number; archiveRetentionSeconds: number },
): Promise<{ envelopes: number; claims: number; archive: number; blobs: number }> {
  const envelopes = await db.query(
    "DELETE FROM webhook_envelopes WHERE state = 'done' AND updated_at < now() - make_interval(secs => $1)",
    [opts.envelopeRetentionSeconds],
  );
  const claims = await db.query(
    "DELETE FROM webhook_delivery_claims WHERE status = 'completed' AND completed_at < now() - make_interval(secs => $1)",
    [opts.envelopeRetentionSeconds],
  );
  const archive = await db.query("DELETE FROM webhook_dlq_archive WHERE archived_at < now() - make_interval(secs => $1)", [
    opts.archiveRetentionSeconds,
  ]);
  const blobs = await db.query(
    `DELETE FROM webhook_payload_blobs b
      WHERE NOT EXISTS (SELECT 1 FROM webhook_envelopes e WHERE e.payload_blob_id = b.id)
        AND NOT EXISTS (SELECT 1 FROM webhook_dlq_archive a WHERE a.payload_blob_id = b.id)
        AND b.created_at < now() - interval '1 hour'`,
  );
  await db.query("DELETE FROM lane_queue_depth WHERE queued = 0");
  return { envelopes: envelopes.rowCount, claims: claims.rowCount, archive: archive.rowCount, blobs: blobs.rowCount };
}

export async function deadCounts(db: Queryable): Promise<{ dead: number; archived: number }> {
  const { rows } = await db.query<{ dead: number; archived: number }>(
    `SELECT (SELECT count(*)::int FROM webhook_envelopes WHERE state = 'dead') AS dead,
            (SELECT count(*)::int FROM webhook_dlq_archive) AS archived`,
  );
  return rows[0] ?? { dead: 0, archived: 0 };
}
