import { createHash, timingSafeEqual } from "node:crypto";
import type { IncomingMessage } from "node:http";
import type { Db } from "../db/db.ts";

/**
 * `GET /internal/v1/telemetry/flows`: installation-wide Flows counts for
 * relay-control's telemetry. relay-control sends
 * RELAYFILE_CONTROL_TELEMETRY_SECRET as `Authorization: Bearer`; unset here
 * means the route answers 404. No workspace ids, names or bodies leave.
 */
export const TELEMETRY_FLOWS_PATH = "/internal/v1/telemetry/flows";

export type FlowsTelemetry = {
  runs_by_status: Record<string, number>;
  workers_by_status: Record<string, number>;
  flow_definitions: number;
};

/**
 * Constant-time compare on equal-length buffers. Both sides are hashed first
 * so the compare does not leak the expected length; a missing or short
 * presented value is rejected before the compare.
 */
export function telemetrySecretMatches(presented: string | null, expected: string): boolean {
  if (!presented || Buffer.byteLength(presented, "utf8") < 32) return false;
  const a = createHash("sha256").update(presented, "utf8").digest();
  const b = createHash("sha256").update(expected, "utf8").digest();
  return a.length === b.length && timingSafeEqual(a, b);
}

export function presentedBearer(request: IncomingMessage): string | null {
  const header = request.headers.authorization;
  if (!header) return null;
  const match = /^Bearer\s+(.+)$/i.exec(header);
  return match?.[1]?.trim() || null;
}

export async function flowsTelemetry(db: Db): Promise<FlowsTelemetry> {
  const counts = async (sql: string) => {
    const result = await db.query<{ status: string; n: string | number }>(sql);
    const out: Record<string, number> = {};
    for (const row of result.rows) out[row.status] = Number(row.n);
    return out;
  };
  const runs = await counts("SELECT status, count(*) AS n FROM selfhost_workflow_runs GROUP BY status");
  const workers = await counts("SELECT status, count(*) AS n FROM selfhost_workers GROUP BY status");
  const defs = await db.query<{ n: string | number }>("SELECT count(*) AS n FROM selfhost_flow_definitions");
  return { runs_by_status: runs, workers_by_status: workers, flow_definitions: Number(defs.rows[0]?.n ?? 0) };
}
