import type { Db, Queryable } from "../db/db.ts";
import { dedupeKey, type EnvelopeKind, type EnvelopeRow, type Ingress, type NativeIds, sha256Hex } from "../envelope/model.ts";
import { ulid } from "../envelope/ulid.ts";

/** Keep at most this many outcomes on a row (the DLQ "reason chain"). */
const OUTCOME_HISTORY_MAX = 20;

export type NewEnvelope = {
  kind: EnvelopeKind;
  ingress: Ingress;
  provider: string;
  requestId: string;
  /** Real provider delivery id, or null (then the request id is used, never deduped at ingress). */
  providerDeliveryId: string | null;
  eventType: string | null;
  connectionId: string | null;
  nativeIds: NativeIds;
  headers: Record<string, string>;
  raw: Buffer;
  parsed: unknown;
  receivedAt: Date;
  lane: string;
  workspaceId: string | null;
  inlineMaxBytes: number;
};

export type InsertResult = { duplicate: true } | { duplicate: false; id: string; inline: boolean };

export function ingressDedupeSurface(ingress: Ingress): string {
  return `ingress:${ingress}`;
}

/**
 * §2.3 + §3 level 1: the ingress dedupe claim and the envelope row commit in
 * one transaction. A conflict on the claim means the sender is retrying a
 * delivery we already hold: nothing is re-queued.
 */
export async function insertEnvelope(db: Db, input: NewEnvelope): Promise<InsertResult> {
  const id = ulid(input.receivedAt.getTime());
  const sha = sha256Hex(input.raw);
  const inline = input.raw.length <= input.inlineMaxBytes;
  const deliveryId = input.providerDeliveryId ?? input.requestId;
  const surface = input.providerDeliveryId ? ingressDedupeSurface(input.ingress) : null;
  const dedupeId = input.providerDeliveryId ? dedupeKey([input.provider, input.providerDeliveryId]) : null;

  return db.tx(async (q) => {
    if (surface && dedupeId) {
      const claim = await q.query(
        `INSERT INTO webhook_delivery_claims (surface, dedupe_id, status, envelope_id, completed_at)
         VALUES ($1, $2, 'completed', $3, now())
         ON CONFLICT (surface, dedupe_id) DO NOTHING RETURNING surface`,
        [surface, dedupeId, id],
      );
      if (claim.rows.length === 0) return { duplicate: true } as const;
    }
    let blobId: string | null = null;
    if (!inline) {
      blobId = ulid();
      await q.query("INSERT INTO webhook_payload_blobs (id, sha256, size_bytes, body) VALUES ($1, $2, $3, $4)", [
        blobId,
        sha,
        input.raw.length,
        input.raw,
      ]);
    }
    await q.query(
      `INSERT INTO webhook_envelopes
        (id, kind, ingress, provider, request_id, delivery_id, has_provider_delivery_id, event_type, connection_id,
         native_ids, dedupe_surface, dedupe_id, headers, payload, payload_blob_id, payload_sha256, payload_bytes,
         received_at, workspace_id, lane, visible_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$18)`,
      [
        id,
        input.kind,
        input.ingress,
        input.provider,
        input.requestId,
        deliveryId,
        input.providerDeliveryId !== null,
        input.eventType,
        input.connectionId,
        JSON.stringify(input.nativeIds),
        surface,
        dedupeId,
        JSON.stringify(input.headers),
        inline ? JSON.stringify(input.parsed) : null,
        blobId,
        sha,
        input.raw.length,
        input.receivedAt,
        input.workspaceId,
        input.lane,
      ],
    );
    return { duplicate: false, id, inline } as const;
  });
}

// ------------------------------------------------------------ queue depth (§6.4)

export class QueueDepth {
  private globalCache: { value: number; at: number } | null = null;
  constructor(
    private readonly db: Queryable,
    private readonly cacheMs = 1000,
    private readonly now = () => Date.now(),
  ) {}

  async lane(lane: string): Promise<number> {
    const { rows } = await this.db.query<{ queued: number }>("SELECT queued::int AS queued FROM lane_queue_depth WHERE lane = $1", [lane]);
    return rows[0]?.queued ?? 0;
  }

  /** SUM of the per-lane counters, cached in-process for `cacheMs`. */
  async global(): Promise<number> {
    const t = this.now();
    if (this.globalCache && t - this.globalCache.at < this.cacheMs) return this.globalCache.value;
    const { rows } = await this.db.query<{ total: number }>("SELECT COALESCE(sum(queued), 0)::int AS total FROM lane_queue_depth");
    const value = rows[0]?.total ?? 0;
    this.globalCache = { value, at: t };
    return value;
  }

  invalidate(): void {
    this.globalCache = null;
  }

  async oldestAgeSeconds(): Promise<number> {
    const { rows } = await this.db.query<{ age: number | null }>(
      "SELECT EXTRACT(EPOCH FROM (now() - min(visible_at)))::float8 AS age FROM webhook_envelopes WHERE state = 'queued' AND visible_at <= now()",
    );
    return Math.max(0, Number(rows[0]?.age ?? 0));
  }

  async topLanes(limit: number): Promise<{ lane: string; queued: number }[]> {
    const { rows } = await this.db.query<{ lane: string; queued: number }>(
      "SELECT lane, queued::int AS queued FROM lane_queue_depth WHERE queued > 0 ORDER BY queued DESC LIMIT $1",
      [limit],
    );
    return rows;
  }
}

// ------------------------------------------------------------ claim (§4.2)

export type ClaimOptions = {
  worker: string;
  leaseSeconds: number;
  perLaneMax: number;
  /** How many candidate lanes to try before giving up this round. */
  maxLaneTries?: number;
};

/**
 * Claim one envelope. The candidate query is the plan's §4.2 SQL; the per-lane
 * cap is made strict (ruling 3) by taking a transaction advisory lock on the
 * lane and re-counting leased rows under it. Two workers can therefore never
 * both see "1 leased" and push a lane to 3 against a cap of 2.
 */
export async function claimNext(db: Db, opts: ClaimOptions): Promise<EnvelopeRow | null> {
  const skip: string[] = [];
  const tries = opts.maxLaneTries ?? 4;
  for (let i = 0; i < tries; i += 1) {
    const result = await db.tx(async (q): Promise<EnvelopeRow | { skipLane: string } | null> => {
      const pick = await q.query<{ id: string; lane: string }>(
        `SELECT id, lane FROM webhook_envelopes
          WHERE state = 'queued' AND visible_at <= now()
            AND lane NOT IN (SELECT lane FROM lane_cooldowns WHERE until > now())
            AND lane NOT IN (SELECT lane FROM webhook_envelopes
                              WHERE state = 'leased' GROUP BY lane
                             HAVING count(*) >= $1)
            AND NOT (lane = ANY($2::text[]))
          ORDER BY visible_at, received_at
          FOR UPDATE SKIP LOCKED LIMIT 1`,
        [opts.perLaneMax, skip],
      );
      const candidate = pick.rows[0];
      if (!candidate) return null;
      const lock = await q.query<{ ok: boolean }>("SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0)) AS ok", [candidate.lane]);
      if (!lock.rows[0]?.ok) return { skipLane: candidate.lane };
      const inflight = await q.query<{ n: number }>(
        "SELECT count(*)::int AS n FROM webhook_envelopes WHERE lane = $1 AND state = 'leased'",
        [candidate.lane],
      );
      if ((inflight.rows[0]?.n ?? 0) >= opts.perLaneMax) return { skipLane: candidate.lane };
      const leased = await q.query<EnvelopeRow>(
        `UPDATE webhook_envelopes
            SET state = 'leased', leased_by = $2, lease_until = now() + make_interval(secs => $3),
                attempts = attempts + 1, updated_at = now()
          WHERE id = $1 RETURNING *`,
        [candidate.id, opts.worker, opts.leaseSeconds],
      );
      return leased.rows[0] ?? null;
    });
    if (result === null) return null;
    if ("skipLane" in result) {
      skip.push(result.skipLane);
      continue;
    }
    return result;
  }
  return null;
}

export async function heartbeat(db: Queryable, id: string, worker: string, leaseSeconds: number): Promise<boolean> {
  const r = await db.query(
    `UPDATE webhook_envelopes SET lease_until = now() + make_interval(secs => $3), updated_at = now()
      WHERE id = $1 AND leased_by = $2 AND state = 'leased'`,
    [id, worker, leaseSeconds],
  );
  return r.rowCount > 0;
}

const APPEND_OUTCOME = `outcome_history = (CASE WHEN jsonb_array_length(outcome_history) >= ${OUTCOME_HISTORY_MAX}
                                 THEN outcome_history - 0 ELSE outcome_history END) || jsonb_build_array($3::jsonb),
                       last_outcome = $3::jsonb`;

/** Ack: `done`. Fenced on the lease holder; false means the lease was lost. */
export async function markDone(db: Queryable, id: string, worker: string, outcome: Record<string, unknown>): Promise<boolean> {
  const r = await db.query(
    `UPDATE webhook_envelopes SET state = 'done', leased_by = NULL, lease_until = NULL, updated_at = now(), ${APPEND_OUTCOME}
      WHERE id = $1 AND leased_by = $2 AND state = 'leased'`,
    [id, worker, JSON.stringify(outcome)],
  );
  return r.rowCount > 0;
}

export async function markRetry(
  db: Queryable,
  id: string,
  worker: string,
  delaySeconds: number,
  outcome: Record<string, unknown>,
): Promise<boolean> {
  const r = await db.query(
    `UPDATE webhook_envelopes SET state = 'queued', leased_by = NULL, lease_until = NULL, updated_at = now(),
            visible_at = now() + make_interval(secs => $4), ${APPEND_OUTCOME}
      WHERE id = $1 AND leased_by = $2 AND state = 'leased'`,
    [id, worker, JSON.stringify(outcome), delaySeconds],
  );
  return r.rowCount > 0;
}

export async function markDead(db: Queryable, id: string, worker: string, outcome: Record<string, unknown>): Promise<boolean> {
  const r = await db.query(
    `UPDATE webhook_envelopes SET state = 'dead', leased_by = NULL, lease_until = NULL, updated_at = now(), ${APPEND_OUTCOME}
      WHERE id = $1 AND leased_by = $2 AND state = 'leased'`,
    [id, worker, JSON.stringify(outcome)],
  );
  return r.rowCount > 0;
}

/** Move a leased row to its resolved workspace lane before handling (§4.2). */
export async function relane(db: Queryable, id: string, worker: string, workspaceId: string, lane: string): Promise<boolean> {
  const r = await db.query(
    `UPDATE webhook_envelopes SET workspace_id = $3, lane = $4, updated_at = now()
      WHERE id = $1 AND leased_by = $2 AND state = 'leased'`,
    [id, worker, workspaceId, lane],
  );
  return r.rowCount > 0;
}

/**
 * GitHub installation fan-out (ruling 5): one child per workspace and the
 * parent ack commit in ONE transaction, so a crash never leaves an acked
 * parent without children. Children carry the parent's request id.
 */
export async function fanOut(
  db: Db,
  parent: EnvelopeRow,
  worker: string,
  workspaceIds: string[],
  outcome: Record<string, unknown>,
): Promise<string[]> {
  return db.tx(async (q) => {
    const ids: string[] = [];
    for (const workspaceId of workspaceIds) {
      const childId = ulid();
      ids.push(childId);
      await q.query(
        `INSERT INTO webhook_envelopes
          (id, parent_id, kind, ingress, provider, request_id, delivery_id, has_provider_delivery_id, event_type,
           connection_id, native_ids, headers, payload, payload_blob_id, payload_sha256, payload_bytes, received_at,
           workspace_id, lane, state, visible_at)
         SELECT $1, id, kind, ingress, provider, request_id, delivery_id, has_provider_delivery_id, event_type,
                connection_id, native_ids, headers, payload, payload_blob_id, payload_sha256, payload_bytes, received_at,
                $2, $2, 'queued', now()
           FROM webhook_envelopes WHERE id = $3`,
        [childId, workspaceId, parent.id],
      );
    }
    const acked = await markDone(q, parent.id, worker, { ...outcome, children: ids.length });
    if (!acked) throw new Error("lease_lost");
    return ids;
  });
}

/** Expired leases (crashed worker) go back to queued, or dead when out of attempts. */
export async function reapExpiredLeases(db: Queryable, maxAttempts: number): Promise<{ requeued: number; dead: number }> {
  const outcome = JSON.stringify({ disposition: "retry", code: "lease_expired" });
  const { rows } = await db.query<{ state: string }>(
    `UPDATE webhook_envelopes
        SET state = CASE WHEN attempts >= $1 THEN 'dead' ELSE 'queued' END,
            leased_by = NULL, lease_until = NULL, visible_at = now(), updated_at = now(),
            outcome_history = (CASE WHEN jsonb_array_length(outcome_history) >= ${OUTCOME_HISTORY_MAX}
                                 THEN outcome_history - 0 ELSE outcome_history END) || jsonb_build_array($2::jsonb),
            last_outcome = $2::jsonb
      WHERE state = 'leased' AND lease_until < now()
      RETURNING state`,
    [maxAttempts, outcome],
  );
  return { requeued: rows.filter((r) => r.state === "queued").length, dead: rows.filter((r) => r.state === "dead").length };
}

/** Bounded payload read (§4.3 PAYLOAD_READ_TIMEOUT_MS). */
export async function readPayload(db: Queryable, row: EnvelopeRow, timeoutMs: number): Promise<unknown> {
  if (row.payload_blob_id === null) return row.payload;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error("payload_read_timeout")), timeoutMs);
  });
  try {
    const { rows } = await Promise.race([
      db.query<{ body: Uint8Array }>("SELECT body FROM webhook_payload_blobs WHERE id = $1", [row.payload_blob_id]),
      timeout,
    ]);
    const body = rows[0]?.body;
    if (!body) throw new Error("payload_blob_missing");
    return JSON.parse(Buffer.from(body).toString("utf8"));
  } finally {
    clearTimeout(timer);
  }
}
