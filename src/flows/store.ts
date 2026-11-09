import { randomUUID } from "node:crypto";
import type { Database, SqlExecutor } from "./database.ts";
import {
  decryptSecret,
  encryptSecret,
  hashSecret,
  opaqueToken,
  secureEqual,
  type EncryptionKeyring,
} from "./crypto.ts";
import type { CliCredentialBroker, RelaycastCredentialSource } from "./broker.ts";
import { RelaycastBindingRefused, RelaycastCredentialsUnavailable } from "./broker.ts";
import type { Logger } from "../observability/log.ts";
import type { FlowsConfig } from "./config.ts";
import { enqueueCredentialRevocation } from "./revoke-outbox.ts";
import { summarizeDefinition, type DeclaredStep } from "./definition.ts";
import { RunObjectStore } from "./objects.ts";
import { mergeRunEnvironment, type RuntimeEnvironment } from "./runtime-env.ts";

export const ENROLLMENT_TOKEN_PREFIX = "sh_wrk_enr_";
export const WORKER_TOKEN_PREFIX = "sh_wrk_";
export const RUN_TOKEN_PREFIX = "sh_run_";
export const CALLBACK_TOKEN_PREFIX = "sh_cb_";

export type RunStatus = "pending" | "running" | "completed" | "failed" | "cancelled";
export type AssignmentStatus = "queued" | "assigned" | "running" | "completed" | "failed" | "timeout";
export type WorkerStatus = "pending" | "online" | "offline" | "revoked";
export const TERMINAL_RUN_STATUSES: RunStatus[] = ["completed", "failed", "cancelled"];

export class FlowsError extends Error {
  constructor(readonly status: number, readonly code: string, message: string) {
    super(message);
  }
}

export type WorkerRecord = {
  id: string;
  workspaceId: string;
  name: string;
  displayName: string;
  hostInfo: Record<string, unknown>;
  tags: string[];
  status: WorkerStatus;
  lastSeen: string | null;
  registeredAt: string;
  registeredBy: string | null;
  currentRunId: string | null;
};

export type WorkAssignmentRecord = {
  id: string;
  workspaceId: string;
  workerId: string | null;
  runId: string;
  workflowRef: { type: "inline"; value: string };
  status: AssignmentStatus;
  queuedAt: string;
  assignedAt: string | null;
  startedAt: string | null;
  completedAt: string | null;
  queueDeadline: string;
  result?: Record<string, unknown>;
  error?: string;
};

export type FlowRecord = {
  id: string;
  workspaceId: string;
  name: string;
  description: string | null;
  fileType: "yaml" | "ts";
  createdBy: string | null;
  createdAt: string;
  updatedAt: string;
  stepCount: number;
  lastRun: { runId: string; status: RunStatus; createdAt: string } | null;
  runCount: number;
};

export type RunRecord = {
  runId: string;
  workspaceId: string;
  flowId: string | null;
  workflow: string;
  workflowName: string;
  fileType: "yaml" | "ts";
  workflowFileName: string;
  dispatchType: "worker" | "local";
  status: RunStatus;
  trigger: string;
  inputs: unknown;
  workerId: string | null;
  workerName: string | null;
  assignmentStatus: AssignmentStatus | null;
  requestedWorkerId: string | null;
  requestedTags: string[];
  resumedFromRunId: string | null;
  resumedBy: Array<{ runId: string; status: RunStatus; createdAt: string }>;
  resumeRunId: string | null;
  startFrom: string | null;
  createdBy: string | null;
  result: unknown;
  summary: string | null;
  error: string | null;
  exitCode: number | null;
  durationMs: number | null;
  cancelRequestedAt: string | null;
  queueDeadline: string | null;
  startedAt: string | null;
  completedAt: string | null;
  createdAt: string;
  updatedAt: string;
  hasEnvSecrets: boolean;
  hasCodeArchive: boolean;
};

export type StepRecord = {
  stepName: string;
  label: string | null;
  stepType: string | null;
  agent: string | null;
  cli: string | null;
  status: string;
  source: "declared" | "snapshot" | "final";
  attempt: number | null;
  startTime: string | null;
  endTime: string | null;
  durationMs: number | null;
  exitCode: number | null;
  outputSummary: string | null;
  error: string | null;
  completionReason: string | null;
  retryCount: number | null;
  dependsOn: string[];
  detail: unknown;
};

export type RunEvent = {
  id: string;
  runId: string;
  sequence: number;
  eventType: string;
  stepName: string | null;
  payload: Record<string, unknown>;
  createdAt: string;
};

type Row = Record<string, unknown>;

const iso = (value: unknown): string | null =>
  value === null || value === undefined ? null : new Date(value as string).toISOString();
const isoRequired = (value: unknown): string => new Date(value as string).toISOString();
const num = (value: unknown): number | null =>
  value === null || value === undefined ? null : Number(value);
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const isUuid = (value: string): boolean => UUID.test(value);

function jsonValue<T>(value: unknown, fallback: T): T {
  if (value === null || value === undefined) return fallback;
  if (typeof value === "string") {
    try {
      return JSON.parse(value) as T;
    } catch {
      return fallback;
    }
  }
  return value as T;
}

function cleanName(value: string, field: string, max = 128): string {
  const cleaned = value.replace(/[\p{C}]/gu, " ").trim();
  if (!cleaned) throw new FlowsError(400, "invalid_request", `${field} is required`);
  if (cleaned.length > max) {
    throw new FlowsError(400, "invalid_request", `${field} must be ${max} characters or fewer`);
  }
  return cleaned;
}

export function cleanTags(value: unknown): string[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value) || value.some((tag) => typeof tag !== "string")) {
    throw new FlowsError(400, "invalid_request", "tags must be an array of strings");
  }
  return [...new Set((value as string[]).map((tag) => tag.trim()).filter(Boolean))]
    .slice(0, 32)
    .map((tag) => tag.slice(0, 64));
}

export function workflowFileName(name: string, fileType: "yaml" | "ts"): string {
  const base = name.replace(/[^A-Za-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "") || "workflow";
  const extension = fileType === "ts" ? ".ts" : ".yaml";
  return base.endsWith(extension) ? base : `${base}${extension}`;
}

const MAX_SOURCE_BYTES = 256 * 1024;
function checkSource(source: string): void {
  if (!source.trim()) throw new FlowsError(400, "invalid_request", "workflow source is required");
  if (Buffer.byteLength(source) > MAX_SOURCE_BYTES) {
    throw new FlowsError(413, "workflow_too_large", "workflow source exceeds 256 KiB");
  }
}

function snapshotStatus(state: string, completionReason?: string | null): string {
  switch (state) {
    case "running":
    case "backoff":
      return "running";
    case "waiting":
    case "needs_human":
      return "waiting";
    case "done":
      return !completionReason || completionReason === "success" ? "completed" : "failed";
    default:
      return state || "running";
  }
}

export class FlowsStore {
  readonly objects: RunObjectStore;
  /** relay-control's CLI-credential broker (subscription logins), over HTTP. */
  cliCredentials?: CliCredentialBroker;
  /** The workspace's Relaycast binding, owned by relay-control. */
  relaycastCredentials?: RelaycastCredentialSource;
  /** Structured events (names and ids only, never secrets or error text). */
  logger?: Logger;
  /**
   * How long a workspace's queue stays paused after relay-control refused
   * its Relaycast binding (4xx). A refusal is a configuration error, so the
   * claim is released and the workspace is not retried in a tight loop.
   */
  relaycastRefusalHoldMs = 60_000;
  /**
   * First hold after relay-control failed during assignment (5xx, network,
   * timeout or a malformed answer from the binding or credential calls).
   * Each further consecutive failure doubles it, up to `outageHoldMaxMs`; a
   * successful assignment in the workspace resets it.
   */
  outageHoldInitialMs = 5_000;
  outageHoldMaxMs = 300_000;
  /** Workspace id -> time (ms) before which its queue is not offered. */
  readonly #workspaceHeldUntil = new Map<string, number>();
  /** Workspace id -> the hold applied after its last consecutive outage. */
  readonly #outageHoldMs = new Map<string, number>();
  readonly #listeners = new Set<() => void>();

  constructor(
    private readonly database: Database,
    private readonly keyring: EncryptionKeyring,
    readonly config: FlowsConfig,
    private readonly relaycastFallbackUrl: string,
  ) {
    this.objects = new RunObjectStore(database, config.storage);
  }

  /** In-process wakeup for open queue streams; Postgres polling covers replicas. */
  onWork(listener: () => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  private notifyWork(): void {
    for (const listener of this.#listeners) listener();
  }

  // ---------------------------------------------------------------- flows

  private flowFromRow(row: Row): FlowRecord {
    const source = String(row.source ?? "");
    const fileType = row.file_type as "yaml" | "ts";
    return {
      id: String(row.id),
      workspaceId: String(row.workspace_id),
      name: String(row.name),
      description: (row.description as string | null) ?? null,
      fileType,
      createdBy: (row.created_by as string | null) ?? null,
      createdAt: isoRequired(row.created_at),
      updatedAt: isoRequired(row.updated_at),
      stepCount: summarizeDefinition(source, fileType).steps.length,
      lastRun: row.last_run_id
        ? {
          runId: String(row.last_run_id),
          status: row.last_run_status as RunStatus,
          createdAt: isoRequired(row.last_run_created_at),
        }
        : null,
      runCount: Number(row.run_count ?? 0),
    };
  }

  private readonly flowSelect = `
    SELECT f.*, lr.id AS last_run_id, lr.status AS last_run_status,
           lr.created_at AS last_run_created_at,
           (SELECT count(*) FROM selfhost_workflow_runs c WHERE c.flow_id = f.id)::int AS run_count
      FROM selfhost_flow_definitions f
      LEFT JOIN LATERAL (
        SELECT id, status, created_at FROM selfhost_workflow_runs r
         WHERE r.flow_id = f.id ORDER BY r.created_at DESC, r.id DESC LIMIT 1
      ) lr ON true`;

  async listFlows(workspaceId: string): Promise<FlowRecord[]> {
    const result = await this.database.query(
      `${this.flowSelect} WHERE f.workspace_id = $1 ORDER BY f.updated_at DESC, f.id`,
      [workspaceId],
    );
    return result.rows.map((row) => this.flowFromRow(row));
  }

  async getFlow(workspaceId: string, flowId: string): Promise<(FlowRecord & { source: string; steps: DeclaredStep[] }) | null> {
    if (!isUuid(flowId)) return null;
    const result = await this.database.query(
      `${this.flowSelect} WHERE f.workspace_id = $1 AND f.id = $2`,
      [workspaceId, flowId],
    );
    const row = result.rows[0];
    if (!row) return null;
    const source = String(row.source);
    return {
      ...this.flowFromRow(row),
      source,
      steps: summarizeDefinition(source, String(row.file_type)).steps,
    };
  }

  async createFlow(input: {
    workspaceId: string;
    userId: string;
    name: string;
    description?: string;
    fileType: "yaml" | "ts";
    source: string;
  }): Promise<FlowRecord & { source: string; steps: DeclaredStep[] }> {
    checkSource(input.source);
    const id = randomUUID();
    const name = cleanName(input.name, "name", 100);
    try {
      await this.database.query(
        `INSERT INTO selfhost_flow_definitions
           (id, workspace_id, name, description, file_type, source, created_by)
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [
          id,
          input.workspaceId,
          name,
          input.description?.trim().slice(0, 1_000) || null,
          input.fileType,
          input.source,
          input.userId,
        ],
      );
    } catch (error) {
      if (String((error as { code?: string }).code) === "23505" || /unique|duplicate/i.test(String(error))) {
        throw new FlowsError(409, "flow_name_conflict", "A flow with this name already exists");
      }
      throw error;
    }
    return (await this.getFlow(input.workspaceId, id))!;
  }

  async updateFlow(input: {
    workspaceId: string;
    flowId: string;
    name?: string;
    description?: string | null;
    fileType?: "yaml" | "ts";
    source?: string;
  }): Promise<(FlowRecord & { source: string; steps: DeclaredStep[] }) | null> {
    const existing = await this.getFlow(input.workspaceId, input.flowId);
    if (!existing) return null;
    if (input.source !== undefined) checkSource(input.source);
    const name = input.name !== undefined ? cleanName(input.name, "name", 100) : existing.name;
    try {
      await this.database.query(
        `UPDATE selfhost_flow_definitions
            SET name = $3, description = $4, file_type = $5, source = $6, updated_at = now()
          WHERE workspace_id = $1 AND id = $2`,
        [
          input.workspaceId,
          input.flowId,
          name,
          input.description === undefined
            ? existing.description
            : input.description?.trim().slice(0, 1_000) || null,
          input.fileType ?? existing.fileType,
          input.source ?? existing.source,
        ],
      );
    } catch (error) {
      if (/unique|duplicate/i.test(String(error))) {
        throw new FlowsError(409, "flow_name_conflict", "A flow with this name already exists");
      }
      throw error;
    }
    return this.getFlow(input.workspaceId, input.flowId);
  }

  async deleteFlow(workspaceId: string, flowId: string): Promise<boolean> {
    if (!isUuid(flowId)) return false;
    const result = await this.database.query(
      "DELETE FROM selfhost_flow_definitions WHERE workspace_id = $1 AND id = $2 RETURNING id",
      [workspaceId, flowId],
    );
    return result.rows.length === 1;
  }

  // ---------------------------------------------------- enrollment tokens

  async createEnrollmentToken(input: {
    workspaceId: string;
    userId: string;
    /** Snapshot of the creator's email (relay-control owns users). */
    userEmail?: string | null;
    name?: string;
    tags?: string[];
    ttlMs?: number;
  }): Promise<{ id: string; token: string; expiresAt: string; createdAt: string }> {
    const id = randomUUID();
    const token = opaqueToken(ENROLLMENT_TOKEN_PREFIX);
    const ttl = Math.min(
      Math.max(input.ttlMs ?? this.config.enrollmentTokenTtlMs, 60_000),
      7 * 86_400_000,
    );
    const expiresAt = new Date(Date.now() + ttl);
    const result = await this.database.query<{ created_at: Date | string }>(
      `INSERT INTO selfhost_worker_enrollment_tokens
         (id, workspace_id, token_hash, token_last4, name, tags, created_by, created_by_email, expires_at)
       VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
       RETURNING created_at`,
      [
        id,
        input.workspaceId,
        hashSecret(token),
        token.slice(-4),
        input.name ? cleanName(input.name, "name") : null,
        JSON.stringify(input.tags ?? []),
        input.userId,
        input.userEmail ?? null,
        expiresAt,
      ],
    );
    return {
      id,
      token,
      expiresAt: expiresAt.toISOString(),
      createdAt: isoRequired(result.rows[0]!.created_at),
    };
  }

  async listEnrollmentTokens(workspaceId: string): Promise<Array<Record<string, unknown>>> {
    const result = await this.database.query(
      `SELECT t.id, t.name, t.tags, t.token_last4, t.expires_at, t.used_at, t.revoked_at,
              t.created_at, t.used_by_worker_id, t.created_by_email
         FROM selfhost_worker_enrollment_tokens t
        WHERE t.workspace_id = $1
        ORDER BY t.created_at DESC LIMIT 100`,
      [workspaceId],
    );
    return result.rows.map((row) => {
      const expiresAt = isoRequired(row.expires_at);
      const status = row.revoked_at
        ? "revoked"
        : row.used_at
          ? "used"
          : Date.parse(expiresAt) <= Date.now()
            ? "expired"
            : "active";
      return {
        id: row.id,
        name: row.name ?? null,
        tags: jsonValue<string[]>(row.tags, []),
        tokenHint: `${ENROLLMENT_TOKEN_PREFIX}…${row.token_last4}`,
        status,
        expiresAt,
        usedAt: iso(row.used_at),
        usedByWorkerId: row.used_by_worker_id ?? null,
        revokedAt: iso(row.revoked_at),
        createdAt: isoRequired(row.created_at),
        createdBy: row.created_by_email ?? null,
      };
    });
  }

  async revokeEnrollmentToken(workspaceId: string, tokenId: string): Promise<boolean> {
    if (!isUuid(tokenId)) return false;
    const result = await this.database.query(
      `UPDATE selfhost_worker_enrollment_tokens SET revoked_at = now()
        WHERE workspace_id = $1 AND id = $2 AND revoked_at IS NULL AND used_at IS NULL
        RETURNING id`,
      [workspaceId, tokenId],
    );
    return result.rows.length === 1;
  }

  // -------------------------------------------------------------- workers

  async registerWorker(input: {
    enrollmentToken: string;
    name: string;
    hostInfo?: Record<string, unknown>;
  }): Promise<{ workerId: string; workerToken: string; heartbeatIntervalMs: number }> {
    const token = input.enrollmentToken.trim();
    if (!token.startsWith(ENROLLMENT_TOKEN_PREFIX)) {
      throw new FlowsError(401, "invalid_enrollment_token", "Invalid enrollment token");
    }
    const name = cleanName(input.name, "name");
    const hostInfo = input.hostInfo ?? {};
    if (JSON.stringify(hostInfo).length > 16_384) {
      throw new FlowsError(400, "invalid_request", "hostInfo is too large");
    }
    const workerId = randomUUID();
    const workerToken = opaqueToken(WORKER_TOKEN_PREFIX);
    return this.database.transaction(async (tx) => {
      const found = await tx.query<{
        id: string;
        workspace_id: string;
        created_by: string | null;
        created_by_email: string | null;
        tags: unknown;
      }>(
        `SELECT id, workspace_id, created_by, created_by_email, tags FROM selfhost_worker_enrollment_tokens
          WHERE token_hash = $1 AND used_at IS NULL AND revoked_at IS NULL AND expires_at > now()
          FOR UPDATE`,
        [hashSecret(token)],
      );
      const enrollment = found.rows[0];
      if (!enrollment) {
        throw new FlowsError(401, "invalid_enrollment_token", "Invalid enrollment token");
      }
      const existing = await tx.query(
        "SELECT id FROM selfhost_workers WHERE workspace_id = $1 AND name = $2",
        [enrollment.workspace_id, name],
      );
      if (existing.rows.length) {
        throw new FlowsError(409, "worker_name_conflict", "Worker name already exists");
      }
      const hostTags = Array.isArray(hostInfo.tags) ? cleanTags(hostInfo.tags) : [];
      const tags = [...new Set([...jsonValue<string[]>(enrollment.tags, []), ...hostTags])];
      await tx.query(
        `INSERT INTO selfhost_workers
           (id, workspace_id, name, display_name, host_info, tags, token_hash, status, registered_by,
            registered_by_email)
         VALUES ($1, $2, $3, $3, $4::jsonb, $5::jsonb, $6, 'pending', $7, $8)`,
        [
          workerId,
          enrollment.workspace_id,
          name,
          JSON.stringify(hostInfo),
          JSON.stringify(tags),
          hashSecret(workerToken),
          enrollment.created_by,
          enrollment.created_by_email,
        ],
      );
      await tx.query(
        `UPDATE selfhost_worker_enrollment_tokens SET used_at = now(), used_by_worker_id = $2
          WHERE id = $1`,
        [enrollment.id, workerId],
      );
      return { workerId, workerToken, heartbeatIntervalMs: this.config.heartbeatIntervalMs };
    });
  }

  async authenticateWorker(workerId: string, token: string | null): Promise<{
    id: string;
    workspaceId: string;
    name: string;
    tags: string[];
  } | null> {
    if (!token || !isUuid(workerId) || !token.startsWith(WORKER_TOKEN_PREFIX) ||
      token.startsWith(ENROLLMENT_TOKEN_PREFIX)) {
      return null;
    }
    const result = await this.database.query<{
      id: string;
      workspace_id: string;
      name: string;
      tags: unknown;
      token_hash: string;
    }>(
      `SELECT id, workspace_id, name, tags, token_hash FROM selfhost_workers
        WHERE id = $1 AND revoked_at IS NULL`,
      [workerId],
    );
    const row = result.rows[0];
    if (!row || !secureEqual(row.token_hash, hashSecret(token))) return null;
    return {
      id: row.id,
      workspaceId: row.workspace_id,
      name: row.name,
      tags: jsonValue<string[]>(row.tags, []),
    };
  }

  async touchWorker(workerId: string): Promise<void> {
    await this.database.query(
      `UPDATE selfhost_workers SET status = 'online', last_seen = now()
        WHERE id = $1 AND revoked_at IS NULL`,
      [workerId],
    );
  }

  async isWorkerRevoked(workerId: string): Promise<boolean> {
    const result = await this.database.query(
      "SELECT 1 FROM selfhost_workers WHERE id = $1 AND revoked_at IS NULL",
      [workerId],
    );
    return result.rows.length === 0;
  }

  private workerFromRow(row: Row): WorkerRecord {
    const lastSeen = iso(row.last_seen);
    let status: WorkerStatus;
    if (row.revoked_at) status = "revoked";
    else if (!lastSeen) status = "pending";
    else status = Date.now() - Date.parse(lastSeen) <= this.config.onlineWindowMs ? "online" : "offline";
    return {
      id: String(row.id),
      workspaceId: String(row.workspace_id),
      name: String(row.name),
      displayName: String(row.display_name),
      hostInfo: jsonValue<Record<string, unknown>>(row.host_info, {}),
      tags: jsonValue<string[]>(row.tags, []),
      status,
      lastSeen,
      registeredAt: isoRequired(row.registered_at),
      registeredBy: (row.registered_by_email as string | null) ?? null,
      currentRunId: (row.current_run_id as string | null) ?? null,
    };
  }

  private readonly workerSelect = `
    SELECT w.*,
           (SELECT a.run_id FROM selfhost_work_assignments a
             WHERE a.worker_id = w.id AND a.status IN ('assigned', 'running')
             ORDER BY a.assigned_at DESC NULLS LAST LIMIT 1) AS current_run_id
      FROM selfhost_workers w`;

  async listWorkers(workspaceId: string): Promise<WorkerRecord[]> {
    const result = await this.database.query(
      `${this.workerSelect} WHERE w.workspace_id = $1
        ORDER BY w.last_seen DESC NULLS LAST, w.registered_at DESC`,
      [workspaceId],
    );
    return result.rows.map((row) => this.workerFromRow(row));
  }

  async getWorker(workspaceId: string, workerId: string): Promise<WorkerRecord | null> {
    if (!isUuid(workerId)) return null;
    const result = await this.database.query(
      `${this.workerSelect} WHERE w.workspace_id = $1 AND w.id = $2`,
      [workspaceId, workerId],
    );
    return result.rows[0] ? this.workerFromRow(result.rows[0]) : null;
  }

  async revokeWorker(workspaceId: string, workerId: string): Promise<WorkerRecord | null> {
    if (!isUuid(workerId)) return null;
    const revoked = await this.database.transaction(async (tx) => {
      const result = await tx.query(
        `UPDATE selfhost_workers
            SET status = 'revoked', revoked_at = COALESCE(revoked_at, now()), token_hash = $3
          WHERE workspace_id = $1 AND id = $2 RETURNING id`,
        [workspaceId, workerId, hashSecret(opaqueToken("revoked_"))],
      );
      if (!result.rows.length) return false;
      this.notifyWork();
      const active = await tx.query<{ run_id: string }>(
        `UPDATE selfhost_work_assignments
            SET status = 'failed', error = 'worker-revoked', completed_at = now()
          WHERE worker_id = $1 AND status IN ('queued', 'assigned', 'running')
          RETURNING run_id`,
        [workerId],
      );
      for (const { run_id } of active.rows) {
        await this.finishRun(tx, run_id, "failed", { error: "worker-revoked" });
      }
      return true;
    });
    return revoked ? this.getWorker(workspaceId, workerId) : null;
  }

  // ----------------------------------------------------------------- runs

  async createRun(input: {
    workspaceId: string;
    userId: string | null;
    /** Snapshot of the creator's email (relay-control owns users). */
    userEmail?: string | null;
    flowId?: string | null;
    workflow: string;
    fileType: "yaml" | "ts";
    workflowName?: string;
    workflowFileName?: string;
    dispatchType?: "worker" | "local";
    trigger?: string;
    inputs?: unknown;
    envSecrets?: Record<string, string>;
    codeArchive?: Buffer;
    workerId?: string;
    workerName?: string;
    tags?: string[];
    resumedFromRunId?: string;
    resumeRunId?: string;
    startFrom?: string;
    previousRunId?: string;
  }): Promise<{ run: RunRecord; accessToken?: string; callbackToken?: string; assignmentId?: string }> {
    checkSource(input.workflow);
    const dispatchType = input.dispatchType ?? "worker";
    const summary = summarizeDefinition(input.workflow, input.fileType);
    const workflowName = cleanName(
      input.workflowName ?? summary.name ?? "workflow",
      "workflowName",
      200,
    );
    const fileName = input.workflowFileName
      ? workflowFileName(input.workflowFileName.replace(/\.(ya?ml|ts)$/i, ""), input.fileType)
      : workflowFileName(workflowName, input.fileType);
    const runId = randomUUID();
    const assignmentId = randomUUID();
    const accessToken = dispatchType === "local" ? opaqueToken(RUN_TOKEN_PREFIX) : undefined;
    const callbackToken = dispatchType === "local" ? opaqueToken(CALLBACK_TOKEN_PREFIX) : undefined;
    const tags = input.tags ?? [];
    let requestedWorkerId: string | null = null;
    if (input.workerId || input.workerName) {
      const worker = await this.database.query<{ id: string }>(
        input.workerId
          ? "SELECT id FROM selfhost_workers WHERE workspace_id = $1 AND id = $2 AND revoked_at IS NULL"
          : "SELECT id FROM selfhost_workers WHERE workspace_id = $1 AND name = $2 AND revoked_at IS NULL",
        [input.workspaceId, input.workerId && !isUuid(input.workerId) ? "00000000-0000-0000-0000-000000000000" : (input.workerId ?? input.workerName)],
      );
      if (!worker.rows[0]) throw new FlowsError(404, "worker_not_found", "Worker not found");
      requestedWorkerId = worker.rows[0].id;
    }
    if (input.envSecrets && Object.keys(input.envSecrets).length > 64) {
      throw new FlowsError(400, "invalid_request", "envSecrets supports at most 64 entries");
    }
    await this.database.transaction(async (tx) => {
      await tx.query(
        `INSERT INTO selfhost_workflow_runs
           (id, workspace_id, flow_id, workflow_name, workflow, file_type, workflow_file_name,
            dispatch_type, status, trigger, inputs, env_secrets_ciphertext, code_object_key,
            requested_worker_id, requested_tags, resumed_from_run_id, resume_run_id, start_from,
            previous_run_id, created_by, access_token_hash, callback_token_hash, started_at,
            created_by_email)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $13, $14, $15::jsonb,
                 $16, $17, $18, $19, $20, $21, $22, $23, $24)`,
        [
          runId,
          input.workspaceId,
          input.flowId ?? null,
          workflowName,
          input.workflow,
          input.fileType,
          fileName,
          dispatchType,
          dispatchType === "local" ? "running" : "pending",
          input.trigger ?? "manual",
          input.inputs === undefined ? null : JSON.stringify(input.inputs),
          input.envSecrets && Object.keys(input.envSecrets).length
            ? encryptSecret(JSON.stringify(input.envSecrets), this.keyring.active)
            : null,
          input.codeArchive ? "code.tar.gz" : null,
          requestedWorkerId,
          JSON.stringify(tags),
          input.resumedFromRunId ?? null,
          input.resumeRunId ?? null,
          input.startFrom ?? null,
          input.previousRunId ?? null,
          input.userId,
          accessToken ? hashSecret(accessToken) : null,
          callbackToken ? hashSecret(callbackToken) : null,
          dispatchType === "local" ? new Date() : null,
          input.userEmail ?? null,
        ],
      );
      if (dispatchType === "worker") {
        await tx.query(
          `INSERT INTO selfhost_work_assignments
             (id, workspace_id, worker_id, run_id, status, queue_deadline)
           VALUES ($1, $2, $3, $4, 'queued', $5)`,
          [
            assignmentId,
            input.workspaceId,
            requestedWorkerId,
            runId,
            new Date(Date.now() + this.config.queueWaitMs),
          ],
        );
      }
      await this.appendEventTx(tx, runId, dispatchType === "worker" ? "run.queued" : "run.started", null, {
        dispatchType,
        ...(requestedWorkerId ? { workerId: requestedWorkerId } : {}),
        ...(tags.length ? { tags } : {}),
        ...(input.resumedFromRunId ? { resumedFromRunId: input.resumedFromRunId } : {}),
      });
    });
    if (input.codeArchive) {
      await this.objects.put(runId, "code.tar.gz", input.codeArchive, "application/gzip");
    }
    if (dispatchType === "worker") this.notifyWork();
    const run = (await this.getRun(input.workspaceId, runId))!;
    return { run, accessToken, callbackToken, ...(dispatchType === "worker" ? { assignmentId } : {}) };
  }

  private runFromRow(row: Row, resumedBy: RunRecord["resumedBy"] = []): RunRecord {
    return {
      runId: String(row.id),
      workspaceId: String(row.workspace_id),
      flowId: (row.flow_id as string | null) ?? null,
      workflow: String(row.workflow_name),
      workflowName: String(row.workflow_name),
      fileType: row.file_type as "yaml" | "ts",
      workflowFileName: String(row.workflow_file_name),
      dispatchType: row.dispatch_type as "worker" | "local",
      status: row.status as RunStatus,
      trigger: String(row.trigger),
      inputs: jsonValue(row.inputs, null),
      workerId: (row.worker_id as string | null) ?? null,
      workerName: (row.worker_name as string | null) ?? null,
      assignmentStatus: (row.assignment_status as AssignmentStatus | null) ?? null,
      requestedWorkerId: (row.requested_worker_id as string | null) ?? null,
      requestedTags: jsonValue<string[]>(row.requested_tags, []),
      resumedFromRunId: (row.resumed_from_run_id as string | null) ?? null,
      resumedBy,
      resumeRunId: (row.resume_run_id as string | null) ?? null,
      startFrom: (row.start_from as string | null) ?? null,
      createdBy: (row.created_by_email as string | null) ?? null,
      result: jsonValue(row.result, null),
      summary: (row.summary as string | null) ?? null,
      error: (row.error as string | null) ?? null,
      exitCode: num(row.exit_code),
      durationMs: num(row.duration_ms),
      cancelRequestedAt: iso(row.cancel_requested_at),
      queueDeadline: iso(row.queue_deadline),
      startedAt: iso(row.started_at),
      completedAt: iso(row.completed_at),
      createdAt: isoRequired(row.created_at),
      updatedAt: isoRequired(row.updated_at),
      hasEnvSecrets: Boolean(row.env_secrets_ciphertext),
      hasCodeArchive: Boolean(row.code_object_key),
    };
  }

  private readonly runSelect = `
    SELECT r.*, a.worker_id, a.status AS assignment_status, a.queue_deadline,
           w.name AS worker_name
      FROM selfhost_workflow_runs r
      LEFT JOIN selfhost_work_assignments a ON a.run_id = r.id
      LEFT JOIN selfhost_workers w ON w.id = a.worker_id`;

  async getRun(workspaceId: string, runId: string): Promise<RunRecord | null> {
    if (!isUuid(runId)) return null;
    const result = await this.database.query(
      `${this.runSelect} WHERE r.workspace_id = $1 AND r.id = $2`,
      [workspaceId, runId],
    );
    const row = result.rows[0];
    if (!row) return null;
    const children = await this.database.query(
      `SELECT id, status, created_at FROM selfhost_workflow_runs
        WHERE resumed_from_run_id = $1 ORDER BY created_at`,
      [runId],
    );
    return this.runFromRow(row, children.rows.map((child) => ({
      runId: String(child.id),
      status: child.status as RunStatus,
      createdAt: isoRequired(child.created_at),
    })));
  }

  async getRunSource(workspaceId: string, runId: string): Promise<string | null> {
    if (!isUuid(runId)) return null;
    const result = await this.database.query<{ workflow: string }>(
      "SELECT workflow FROM selfhost_workflow_runs WHERE workspace_id = $1 AND id = $2",
      [workspaceId, runId],
    );
    return result.rows[0]?.workflow ?? null;
  }

  async listRuns(workspaceId: string, options: {
    cursor?: string;
    flowId?: string;
    status?: string;
    limit?: number;
  } = {}): Promise<{
    runs: RunRecord[];
    nextCursor: string | null;
    totalRuns: number;
    activeRuns: number;
    failedRuns: number;
    needsReviewRuns: number;
  }> {
    const limit = Math.min(Math.max(options.limit ?? 50, 1), 100);
    const where = ["r.workspace_id = $1"];
    const params: unknown[] = [workspaceId];
    if (options.flowId) {
      if (!isUuid(options.flowId)) throw new FlowsError(400, "invalid_request", "Invalid flowId");
      params.push(options.flowId);
      where.push(`r.flow_id = $${params.length}`);
    }
    if (options.status) {
      params.push(options.status);
      where.push(`r.status = $${params.length}`);
    }
    const filterWhere = where.join(" AND ");
    const pageWhere = [...where];
    const pageParams = [...params];
    if (options.cursor) {
      let cursor: { createdAt?: string; runId?: string };
      try {
        cursor = JSON.parse(Buffer.from(options.cursor, "base64url").toString("utf8"));
      } catch {
        throw new FlowsError(400, "invalid_request", "Invalid cursor");
      }
      if (!cursor.createdAt || !cursor.runId || !isUuid(cursor.runId) ||
        !Number.isFinite(Date.parse(cursor.createdAt))) {
        throw new FlowsError(400, "invalid_request", "Invalid cursor");
      }
      pageParams.push(cursor.createdAt, cursor.runId);
      pageWhere.push(
        `(r.created_at, r.id) < ($${pageParams.length - 1}::timestamptz, $${pageParams.length}::uuid)`,
      );
    }
    pageParams.push(limit + 1);
    const result = await this.database.query(
      `${this.runSelect} WHERE ${pageWhere.join(" AND ")}
        ORDER BY r.created_at DESC, r.id DESC LIMIT $${pageParams.length}`,
      pageParams,
    );
    const rows = result.rows.slice(0, limit);
    const last = rows[rows.length - 1];
    const counts = await this.database.query<{
      total: number;
      active: number;
      failed: number;
    }>(
      `SELECT count(*)::int AS total,
              count(*) FILTER (WHERE r.status IN ('pending', 'running'))::int AS active,
              count(*) FILTER (WHERE r.status = 'failed')::int AS failed
         FROM selfhost_workflow_runs r WHERE ${filterWhere}`,
      params,
    );
    return {
      runs: rows.map((row) => this.runFromRow(row)),
      nextCursor: result.rows.length > limit && last
        ? Buffer.from(JSON.stringify({
          createdAt: isoRequired(last.created_at),
          runId: String(last.id),
        })).toString("base64url")
        : null,
      totalRuns: Number(counts.rows[0]?.total ?? 0),
      activeRuns: Number(counts.rows[0]?.active ?? 0),
      failedRuns: Number(counts.rows[0]?.failed ?? 0),
      needsReviewRuns: 0,
    };
  }

  /** Marks a run terminal unless it already is. Returns true when it changed. */
  private async finishRun(
    tx: SqlExecutor,
    runId: string,
    status: "completed" | "failed" | "cancelled",
    detail: {
      error?: string | null;
      summary?: string | null;
      exitCode?: number | null;
      durationMs?: number | null;
      result?: unknown;
    } = {},
  ): Promise<boolean> {
    const updated = await tx.query(
      `UPDATE selfhost_workflow_runs
          SET status = $2, error = COALESCE($3, error), summary = COALESCE($4, summary),
              exit_code = COALESCE($5, exit_code), duration_ms = COALESCE($6, duration_ms),
              result = COALESCE($7::jsonb, result), completed_at = now(), updated_at = now()
        WHERE id = $1 AND status NOT IN ('completed', 'failed', 'cancelled')
        RETURNING id`,
      [
        runId,
        status,
        detail.error ?? null,
        detail.summary ?? null,
        detail.exitCode ?? null,
        detail.durationMs ?? null,
        detail.result === undefined || detail.result === null ? null : JSON.stringify(detail.result),
      ],
    );
    if (!updated.rows.length) return false;
    // The run's CLI-credential references stop working with the run. They live
    // in relay-control, so the revocation is recorded here in the same
    // transaction and delivered by the revoke outbox (CliCredentialBroker
    // revokeForRun), retried until relay-control acknowledges it.
    await enqueueCredentialRevocation(tx, runId);
    await this.appendEventTx(tx, runId, `run.${status}`, null, {
      ...(detail.error ? { error: detail.error } : {}),
      ...(detail.exitCode !== undefined && detail.exitCode !== null ? { exitCode: detail.exitCode } : {}),
      ...(detail.durationMs ? { durationMs: detail.durationMs } : {}),
      ...(detail.summary ? { summary: detail.summary } : {}),
    });
    return true;
  }

  async cancelRun(workspaceId: string, runId: string, userEmail?: string): Promise<RunRecord | null> {
    const run = await this.getRun(workspaceId, runId);
    if (!run) return null;
    if (TERMINAL_RUN_STATUSES.includes(run.status)) {
      throw new FlowsError(409, "run_already_terminal", `Run already ${run.status}`);
    }
    await this.database.transaction(async (tx) => {
      await tx.query(
        "UPDATE selfhost_workflow_runs SET cancel_requested_at = now() WHERE id = $1",
        [runId],
      );
      // Work that a worker has not picked up yet is withdrawn. Work already on a
      // worker cannot be interrupted over the v1 assignment protocol; its later
      // status report is recorded on the assignment but the run stays cancelled.
      await tx.query(
        `UPDATE selfhost_work_assignments
            SET status = 'failed', error = 'cancelled', completed_at = now()
          WHERE run_id = $1 AND status = 'queued'`,
        [runId],
      );
      await this.finishRun(tx, runId, "cancelled", {
        error: userEmail ? `Cancelled by ${userEmail}` : "Cancelled",
      });
    });
    return this.getRun(workspaceId, runId);
  }

  async resumeRun(input: {
    workspaceId: string;
    userId: string;
    userEmail?: string | null;
    runId: string;
    mode: "resume" | "retry";
    startFrom?: string;
    workerId?: string;
  }): Promise<{ run: RunRecord; assignmentId?: string }> {
    const original = await this.getRun(input.workspaceId, input.runId);
    if (!original) throw new FlowsError(404, "run_not_found", "Run not found");
    if (!TERMINAL_RUN_STATUSES.includes(original.status)) {
      throw new FlowsError(409, "run_not_terminal", "Only finished runs can be resumed or retried");
    }
    if (original.dispatchType !== "worker") {
      throw new FlowsError(409, "run_not_resumable", "Locally mirrored runs are resumed from the CLI");
    }
    const row = await this.database.query<{ workflow: string; env_secrets_ciphertext: string | null; requested_tags: unknown }>(
      "SELECT workflow, env_secrets_ciphertext, requested_tags FROM selfhost_workflow_runs WHERE id = $1",
      [input.runId],
    );
    const source = row.rows[0]!;
    const envSecrets = source.env_secrets_ciphertext
      ? JSON.parse(decryptSecret(source.env_secrets_ciphertext, this.keyring)) as Record<string, string>
      : undefined;
    const code = original.hasCodeArchive
      ? (await this.objects.get(input.runId, "code.tar.gz"))?.body
      : undefined;
    const created = await this.createRun({
      workspaceId: input.workspaceId,
      userId: input.userId,
      userEmail: input.userEmail,
      flowId: original.flowId,
      workflow: source.workflow,
      fileType: original.fileType,
      workflowName: original.workflowName,
      workflowFileName: original.workflowFileName,
      trigger: input.mode,
      inputs: original.inputs ?? undefined,
      envSecrets,
      codeArchive: code,
      workerId: input.workerId ?? original.requestedWorkerId ?? undefined,
      tags: jsonValue<string[]>(source.requested_tags, []),
      resumedFromRunId: original.runId,
      // The v1 runtime keys `--resume`/`--previous-run-id` by its own internal
      // run id, which the worker protocol never reports, so a resume re-queues
      // the definition and may skip ahead with `--start-from <step>`.
      ...(input.mode === "resume" && input.startFrom ? { startFrom: input.startFrom } : {}),
    });
    return { run: created.run, assignmentId: created.assignmentId };
  }

  // ---------------------------------------------------------- assignments

  private assignmentFromRow(row: Row, workflowRef: string): WorkAssignmentRecord {
    const result = jsonValue<Record<string, unknown> | null>(row.result, null);
    return {
      id: String(row.id),
      workspaceId: String(row.workspace_id),
      workerId: (row.worker_id as string | null) ?? null,
      runId: String(row.run_id),
      workflowRef: { type: "inline", value: workflowRef },
      status: row.status as AssignmentStatus,
      queuedAt: isoRequired(row.queued_at),
      assignedAt: iso(row.assigned_at),
      startedAt: iso(row.started_at),
      completedAt: iso(row.completed_at),
      queueDeadline: isoRequired(row.queue_deadline),
      ...(result ? { result } : {}),
      ...(row.error ? { error: String(row.error) } : {}),
    };
  }

  /** Builds the WorkerWorkflowPayload the relay CLI validates, at delivery time. */
  private async workflowPayload(runId: string): Promise<string> {
    const result = await this.database.query<{
      id: string;
      workspace_id: string;
      workflow: string;
      file_type: "yaml" | "ts";
      workflow_file_name: string;
      flow_id: string | null;
      env_secrets_ciphertext: string | null;
      code_object_key: string | null;
      resume_run_id: string | null;
      start_from: string | null;
      previous_run_id: string | null;
    }>(
      `SELECT r.id, r.workspace_id, r.workflow, r.file_type, r.workflow_file_name, r.flow_id,
              r.env_secrets_ciphertext, r.code_object_key, r.resume_run_id, r.start_from,
              r.previous_run_id
         FROM selfhost_workflow_runs r
        WHERE r.id = $1`,
      [runId],
    );
    const row = result.rows[0];
    if (!row) throw new FlowsError(404, "run_not_found", "Run not found");
    // The workspace's Relaycast binding lives in relay-control. Throws when it
    // is unreachable; the claim is released and retried, as for the runtime
    // environment below.
    if (!this.relaycastCredentials) {
      throw new RelaycastCredentialsUnavailable("relaycast credential source is not configured");
    }
    const relaycast = await this.relaycastCredentials(row.workspace_id);
    const runSecrets = row.env_secrets_ciphertext
      ? JSON.parse(decryptSecret(row.env_secrets_ciphertext, this.keyring)) as Record<string, string>
      : undefined;
    let workspaceEnvironment: RuntimeEnvironment | null = null;
    if (this.config.runtimeEnvironment) {
      // Throws when the store is unreachable; the claim is released and retried.
      workspaceEnvironment = await this.config.runtimeEnvironment(row.workspace_id);
      const skipped = [
        ...workspaceEnvironment.skipped.map((entry) => entry.name),
        ...workspaceEnvironment.integrationErrors.map((entry) => `integration:${entry.provider}`),
      ];
      if (skipped.length) {
        // Names only, never values.
        await this.appendEvent(row.id, {
          eventType: "run.environment_warnings",
          payload: { skipped: skipped.slice(0, 50) },
        });
      }
    }
    const envSecrets = mergeRunEnvironment(workspaceEnvironment, runSecrets);
    // A reference, never the auth files: the worker fetches them with its own
    // worker credential, and the reference dies with the run.
    const cliReference = this.cliCredentials
      ? await this.cliCredentials.issueRunReference(row.workspace_id, row.id)
      : null;
    return JSON.stringify({
      runId: row.id,
      workspaceId: row.workspace_id,
      relayWorkspaceId: relaycast.relaycastWorkspaceId,
      relaycastApiKey: relaycast.relaycastApiKey,
      relaycastBaseUrl: this.config.workerRelaycastUrl ?? this.relaycastFallbackUrl,
      relayfileUrl: this.config.workerRelayfileUrl,
      relayfileToken: "",
      workflow: row.workflow,
      fileType: row.file_type,
      sourceFileType: row.file_type,
      workflowFileName: row.workflow_file_name,
      ...(envSecrets ? { envSecrets } : {}),
      ...(cliReference
        ? {
            cliCredentials: {
              ref: cliReference.ref,
              expiresAt: cliReference.expiresAt,
              path: `/api/v1/workers/{workerId}/assignments/${row.id}/cli-credentials`,
              credentials: cliReference.credentials,
            },
          }
        : {}),
      metadata: {
        controlPlane: "relaycontrol",
        ...(row.flow_id ? { flowId: row.flow_id } : {}),
      },
      ...(row.code_object_key ? { s3CodeKey: row.code_object_key } : {}),
      ...(row.resume_run_id ? { resumeRunId: row.resume_run_id } : {}),
      ...(row.start_from ? { startFrom: row.start_from } : {}),
      ...(row.previous_run_id ? { previousRunId: row.previous_run_id } : {}),
    });
  }

  /** Assigned but never acknowledged work for this worker (redelivered on reconnect). */
  async unackedAssignments(workerId: string): Promise<WorkAssignmentRecord[]> {
    const result = await this.database.query(
      `SELECT a.* FROM selfhost_work_assignments a
         JOIN selfhost_workflow_runs r ON r.id = a.run_id
        WHERE a.worker_id = $1 AND a.status = 'assigned' AND a.acked_at IS NULL
          AND r.status = 'pending'
        ORDER BY a.assigned_at`,
      [workerId],
    );
    const records: WorkAssignmentRecord[] = [];
    for (const row of result.rows) {
      try {
        records.push(this.assignmentFromRow(row, await this.workflowPayload(String(row.run_id))));
      } catch (error) {
        this.logger?.event("flows_assignment_redelivery_deferred", {
          area: "flows",
          runId: String(row.run_id),
          error: error instanceof Error ? error.name : "unknown",
        });
      }
    }
    return records;
  }

  /**
   * Claims the next queued assignment this worker may run. A worker runs one
   * assignment at a time, matching the serial CLI loop.
   */
  async claimNext(worker: { id: string; workspaceId: string; tags: string[] }): Promise<WorkAssignmentRecord | null> {
    const heldUntil = this.#workspaceHeldUntil.get(worker.workspaceId);
    if (heldUntil !== undefined) {
      if (heldUntil > Date.now()) return null;
      this.#workspaceHeldUntil.delete(worker.workspaceId);
    }
    const claimed = await this.database.transaction(async (tx) => {
      const busy = await tx.query(
        `SELECT 1 FROM selfhost_work_assignments
          WHERE worker_id = $1 AND status IN ('assigned', 'running') LIMIT 1`,
        [worker.id],
      );
      if (busy.rows.length) return null;
      const candidate = await tx.query<{ id: string }>(
        `SELECT a.id FROM selfhost_work_assignments a
           JOIN selfhost_workflow_runs r ON r.id = a.run_id
          WHERE a.workspace_id = $1 AND a.status = 'queued' AND a.queue_deadline > now()
            AND (a.worker_id IS NULL OR a.worker_id = $2)
            AND r.status = 'pending'
            AND r.requested_tags <@ $3::jsonb
          ORDER BY a.queued_at, a.id
          LIMIT 1
          FOR UPDATE OF a SKIP LOCKED`,
        [worker.workspaceId, worker.id, JSON.stringify(worker.tags)],
      );
      const id = candidate.rows[0]?.id;
      if (!id) return null;
      const updated = await tx.query(
        `UPDATE selfhost_work_assignments a
            SET status = 'assigned', worker_id = $2, assigned_at = now()
           FROM selfhost_workflow_runs r
          WHERE a.id = $1 AND a.status = 'queued' AND r.id = a.run_id
          RETURNING a.*, r.requested_worker_id`,
        [id, worker.id],
      );
      return updated.rows[0] ?? null;
    });
    if (!claimed) return null;
    try {
      const assignment = this.assignmentFromRow(claimed, await this.workflowPayload(String(claimed.run_id)));
      // Only a delivered assignment is recorded, so failed attempts do not
      // pile up `run.assigned` events.
      await this.appendEvent(String(claimed.run_id), { eventType: "run.assigned", payload: { workerId: worker.id } });
      this.#outageHoldMs.delete(worker.workspaceId);
      return assignment;
    } catch (error) {
      // Release the claim so the run is retried rather than stranded.
      await this.database.query(
        `UPDATE selfhost_work_assignments
            SET status = 'queued', worker_id = $2, assigned_at = NULL
          WHERE id = $1 AND status = 'assigned' AND acked_at IS NULL`,
        [claimed.id, claimed.requested_worker_id ?? null],
      );
      if (!(error instanceof RelaycastBindingRefused) && !(error instanceof FlowsError)) {
        // relay-control (or the database) failed: hold the workspace with a
        // growing delay instead of re-claiming on every poll of every worker.
        // Time-based, so the queue always resumes.
        const previous = this.#outageHoldMs.get(worker.workspaceId);
        const holdMs = previous === undefined
          ? this.outageHoldInitialMs
          : Math.min(this.outageHoldMaxMs, previous * 2);
        this.#outageHoldMs.set(worker.workspaceId, holdMs);
        this.#workspaceHeldUntil.set(worker.workspaceId, Date.now() + holdMs);
      }
      if (error instanceof RelaycastBindingRefused) {
        // A configuration error, not an outage: one event, then the
        // workspace's queue waits a fixed time instead of re-claiming every poll.
        this.#workspaceHeldUntil.set(worker.workspaceId, Date.now() + this.relaycastRefusalHoldMs);
        this.logger?.event("flows_relaycast_binding_refused", {
          area: "flows",
          workspaceId: worker.workspaceId,
          runId: String(claimed.run_id),
          status: error.status,
          code: error.code,
          retryInMs: this.relaycastRefusalHoldMs,
        });
        return null;
      }
      throw error;
    }
  }

  /**
   * `runtimeCredentialVerifier` for the admin module: lets a worker token read
   * its workspace's runtime environment and integration tokens.
   */
  async verifyRuntimeCredential(token: string): Promise<{
    subject: string;
    workspaceId: string;
    scopes: string[];
  } | null> {
    if (!token.startsWith(WORKER_TOKEN_PREFIX) || token.startsWith(ENROLLMENT_TOKEN_PREFIX)) return null;
    const result = await this.database.query<{ id: string; workspace_id: string }>(
      "SELECT id, workspace_id FROM selfhost_workers WHERE token_hash = $1 AND revoked_at IS NULL",
      [hashSecret(token)],
    );
    const row = result.rows[0];
    if (!row) return null;
    return {
      subject: `worker:${row.id}`,
      workspaceId: row.workspace_id,
      scopes: ["runtime:env:read", "runtime:integrations:read"],
    };
  }

  /**
   * Deletes everything this service holds for a workspace relay-control
   * deleted: flows, runs (with their encrypted envSecrets, events, steps,
   * assignments and stored objects), workers and enrollment tokens. Every
   * run's CLI-credential references are queued for revocation in the same
   * transaction. Idempotent: a second call deletes nothing.
   */
  async deleteWorkspace(workspaceId: string): Promise<{
    flows: number;
    runs: number;
    workers: number;
    enrollmentTokens: number;
    revocationsQueued: number;
  }> {
    if (!isUuid(workspaceId)) throw new FlowsError(400, "invalid_request", "workspace id must be a UUID");
    const counts = await this.database.transaction(async (tx) => {
      // Revocations are queued for exactly the runs this statement deleted, so
      // a run committed concurrently cannot be deleted without one.
      const runs = await tx.query<{ id: string }>(
        "DELETE FROM selfhost_workflow_runs WHERE workspace_id = $1 RETURNING id",
        [workspaceId],
      );
      const revocations = runs.rows.length
        ? await tx.query(
          `INSERT INTO selfhost_cli_credential_revocations (run_id)
           SELECT unnest($1::uuid[])
           ON CONFLICT (run_id) DO NOTHING
           RETURNING run_id`,
          [runs.rows.map((row) => row.id)],
        )
        : { rows: [] };
      // Assignments cascade with their runs; this catches any left without one.
      await tx.query("DELETE FROM selfhost_work_assignments WHERE workspace_id = $1", [workspaceId]);
      const flows = await tx.query("DELETE FROM selfhost_flow_definitions WHERE workspace_id = $1 RETURNING id", [workspaceId]);
      const workers = await tx.query("DELETE FROM selfhost_workers WHERE workspace_id = $1 RETURNING id", [workspaceId]);
      const tokens = await tx.query(
        "DELETE FROM selfhost_worker_enrollment_tokens WHERE workspace_id = $1 RETURNING id",
        [workspaceId],
      );
      return {
        flows: flows.rows.length,
        runs: runs.rows.length,
        workers: workers.rows.length,
        enrollmentTokens: tokens.rows.length,
        revocationsQueued: revocations.rows.length,
      };
    });
    this.#workspaceHeldUntil.delete(workspaceId);
    this.#outageHoldMs.delete(workspaceId);
    // Open queue streams of the deleted workers see them gone and end.
    this.notifyWork();
    return counts;
  }

  private async workerAssignment(tx: SqlExecutor, workerId: string, runId: string): Promise<Row | null> {
    if (!isUuid(runId)) return null;
    const result = await tx.query(
      `SELECT a.*, r.status AS run_status FROM selfhost_work_assignments a
         JOIN selfhost_workflow_runs r ON r.id = a.run_id
        WHERE a.worker_id = $1 AND a.run_id = $2 FOR UPDATE OF a`,
      [workerId, runId],
    );
    return result.rows[0] ?? null;
  }

  async ackAssignment(workerId: string, runId: string): Promise<{ ok: true; status: AssignmentStatus; assignedAt: string | null }> {
    return this.database.transaction(async (tx) => {
      const row = await this.workerAssignment(tx, workerId, runId);
      if (!row) throw new FlowsError(404, "assignment_not_found", "Assignment not found");
      if (row.run_status === "cancelled") {
        throw new FlowsError(409, "run_cancelled", "Run was cancelled");
      }
      if (row.status === "queued" || row.status === "assigned") {
        const updated = await tx.query(
          `UPDATE selfhost_work_assignments
              SET status = 'assigned', assigned_at = COALESCE(assigned_at, now()),
                  acked_at = COALESCE(acked_at, now())
            WHERE id = $1 RETURNING status, assigned_at`,
          [row.id],
        );
        if (!row.acked_at) {
          await this.appendEventTx(tx, runId, "run.acknowledged", null, { workerId });
        }
        return {
          ok: true as const,
          status: updated.rows[0]!.status as AssignmentStatus,
          assignedAt: iso(updated.rows[0]!.assigned_at),
        };
      }
      return { ok: true as const, status: row.status as AssignmentStatus, assignedAt: iso(row.assigned_at) };
    });
  }

  async reportStatus(workerId: string, runId: string, detail: {
    phase: "running" | "completed" | "failed";
    exitCode?: number;
    durationMs?: number;
    summary?: string;
    error?: string;
    result?: Record<string, unknown>;
  }): Promise<{ ok: true; status: AssignmentStatus }> {
    return this.database.transaction(async (tx) => {
      const row = await this.workerAssignment(tx, workerId, runId);
      if (!row) throw new FlowsError(404, "assignment_not_found", "Assignment not found");
      const current = row.status as AssignmentStatus;
      if (current === "completed" || current === "failed" || current === "timeout") {
        return { ok: true as const, status: current };
      }
      if (detail.phase === "running") {
        await tx.query(
          `UPDATE selfhost_work_assignments
              SET status = 'running', started_at = COALESCE(started_at, now()),
                  assigned_at = COALESCE(assigned_at, now()), acked_at = COALESCE(acked_at, now())
            WHERE id = $1`,
          [row.id],
        );
        const run = await tx.query(
          `UPDATE selfhost_workflow_runs
              SET status = 'running', started_at = COALESCE(started_at, now()), updated_at = now()
            WHERE id = $1 AND status = 'pending' RETURNING id`,
          [runId],
        );
        if (run.rows.length) {
          await this.appendEventTx(tx, runId, "run.running", null, { workerId });
        }
        return { ok: true as const, status: "running" as const };
      }
      const assignmentResult = detail.result ?? {
        ...(detail.exitCode !== undefined ? { exitCode: detail.exitCode } : {}),
        ...(detail.durationMs !== undefined ? { durationMs: detail.durationMs } : {}),
        ...(detail.summary ? { summary: detail.summary } : {}),
      };
      await tx.query(
        `UPDATE selfhost_work_assignments
            SET status = $2, completed_at = now(), result = $3::jsonb, error = $4
          WHERE id = $1`,
        [row.id, detail.phase, JSON.stringify(assignmentResult), detail.error ?? null],
      );
      const changed = await this.finishRun(tx, runId, detail.phase, {
        error: detail.phase === "failed" ? (detail.error ?? "worker-failed") : null,
        summary: detail.summary ?? null,
        exitCode: detail.exitCode ?? null,
        durationMs: detail.durationMs ?? null,
        result: detail.result ?? null,
      });
      if (!changed) {
        await this.appendEventTx(tx, runId, "worker.reported_after_finish", null, {
          phase: detail.phase,
          ...(detail.exitCode !== undefined ? { exitCode: detail.exitCode } : {}),
        });
      }
      return { ok: true as const, status: detail.phase };
    });
  }

  /** Assignment storage readable by the worker that holds the assignment. */
  async workerCanAccessRun(workerId: string, runId: string): Promise<boolean> {
    if (!isUuid(runId)) return false;
    const result = await this.database.query(
      `SELECT 1 FROM selfhost_work_assignments
        WHERE worker_id = $1 AND run_id = $2 AND status IN ('assigned', 'running')`,
      [workerId, runId],
    );
    return result.rows.length === 1;
  }

  /** Expires stale queue entries and fails work orphaned by a vanished worker. */
  async maintenance(): Promise<{ timedOut: WorkAssignmentRecord[] }> {
    const timedOut: WorkAssignmentRecord[] = [];
    await this.database.transaction(async (tx) => {
      const expired = await tx.query(
        `UPDATE selfhost_work_assignments
            SET status = 'timeout', error = 'queue-timeout', completed_at = now()
          WHERE status = 'queued' AND queue_deadline <= now()
          RETURNING *`,
      );
      for (const row of expired.rows) {
        await this.finishRun(tx, String(row.run_id), "failed", { error: "queue-timeout" });
        timedOut.push(this.assignmentFromRow(row, "{}"));
      }
      const orphaned = await tx.query(
        `UPDATE selfhost_work_assignments a
            SET status = 'failed', error = 'worker-disconnected', completed_at = now()
           FROM selfhost_workers w
          WHERE w.id = a.worker_id AND a.status IN ('assigned', 'running')
            AND (w.last_seen IS NULL OR w.last_seen <= now() - ($1::text || ' milliseconds')::interval)
          RETURNING a.run_id`,
        [String(this.config.disconnectTimeoutMs)],
      );
      for (const row of orphaned.rows) {
        await this.finishRun(tx, String(row.run_id), "failed", { error: "worker-disconnected" });
      }
    });
    return { timedOut };
  }

  // ------------------------------------------------- events, steps, logs

  private async appendEventTx(
    tx: SqlExecutor,
    runId: string,
    eventType: string,
    stepName: string | null,
    payload: Record<string, unknown>,
  ): Promise<number> {
    await tx.query("SELECT id FROM selfhost_workflow_runs WHERE id = $1 FOR UPDATE", [runId]);
    const next = await tx.query<{ sequence: number }>(
      "SELECT COALESCE(max(sequence), 0)::int + 1 AS sequence FROM selfhost_run_events WHERE run_id = $1",
      [runId],
    );
    const sequence = Number(next.rows[0]!.sequence);
    await tx.query(
      `INSERT INTO selfhost_run_events (id, run_id, sequence, event_type, step_name, payload)
       VALUES ($1, $2, $3, $4, $5, $6::jsonb)`,
      [randomUUID(), runId, sequence, eventType, stepName, JSON.stringify(payload)],
    );
    return sequence;
  }

  async appendEvent(runId: string, input: {
    eventType: string;
    stepName?: string;
    payload?: Record<string, unknown>;
  }): Promise<number> {
    return this.database.transaction((tx) =>
      this.appendEventTx(tx, runId, input.eventType, input.stepName ?? null, input.payload ?? {}));
  }

  async listEvents(runId: string, options: { after?: number; limit?: number; sort?: "asc" | "desc" } = {}): Promise<RunEvent[]> {
    const limit = Math.min(Math.max(options.limit ?? 100, 1), 1_000);
    const sort = options.sort === "desc" ? "DESC" : "ASC";
    const result = await this.database.query(
      `SELECT * FROM selfhost_run_events
        WHERE run_id = $1 AND sequence > $2
        ORDER BY sequence ${sort} LIMIT $3`,
      [runId, options.after ?? 0, limit],
    );
    return result.rows.map((row) => ({
      id: String(row.id),
      runId: String(row.run_id),
      sequence: Number(row.sequence),
      eventType: String(row.event_type),
      stepName: (row.step_name as string | null) ?? null,
      payload: jsonValue<Record<string, unknown>>(row.payload, {}),
      createdAt: isoRequired(row.created_at),
    }));
  }

  async upsertSnapshotSteps(runId: string, steps: Array<{
    stepName: string;
    stepType?: string;
    state: string;
    attempt?: number;
    elapsedMs?: number;
    completionReason?: string;
    label?: string;
    dependsOn?: string[];
    detail?: Record<string, unknown>;
  }>): Promise<number> {
    await this.database.transaction(async (tx) => {
      let position = 0;
      for (const step of steps) {
        await tx.query(
          `INSERT INTO selfhost_run_steps
             (run_id, step_name, position, source, label, step_type, status, attempt, elapsed_ms,
              completion_reason, depends_on, detail)
           VALUES ($1, $2, $3, 'snapshot', $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb)
           ON CONFLICT (run_id, step_name) DO UPDATE
             SET position = EXCLUDED.position, label = EXCLUDED.label,
                 step_type = EXCLUDED.step_type, status = EXCLUDED.status,
                 attempt = EXCLUDED.attempt, elapsed_ms = EXCLUDED.elapsed_ms,
                 completion_reason = EXCLUDED.completion_reason,
                 depends_on = EXCLUDED.depends_on, detail = EXCLUDED.detail, updated_at = now()
             WHERE selfhost_run_steps.source = 'snapshot'`,
          [
            runId,
            step.stepName,
            position++,
            step.label ?? null,
            step.stepType ?? null,
            snapshotStatus(step.state, step.completionReason),
            step.attempt ?? null,
            step.elapsedMs ?? null,
            step.completionReason ?? null,
            JSON.stringify(step.dependsOn ?? []),
            step.detail ? JSON.stringify(step.detail) : null,
          ],
        );
      }
    });
    return steps.length;
  }

  async replaceFinalSteps(runId: string, steps: Array<Record<string, unknown>>): Promise<number> {
    await this.database.transaction(async (tx) => {
      await tx.query("DELETE FROM selfhost_run_steps WHERE run_id = $1", [runId]);
      let position = 0;
      for (const step of steps) {
        const exitCode = typeof step.exitCode === "number" ? step.exitCode : null;
        const status = typeof step.status === "string"
          ? step.status
          : step.error || (exitCode !== null && exitCode !== 0) ? "failed" : "completed";
        await tx.query(
          `INSERT INTO selfhost_run_steps
             (run_id, step_name, position, source, label, step_type, agent, cli, status,
              started_at, ended_at, elapsed_ms, exit_code, output_summary, error,
              completion_reason, retry_count, depends_on, detail)
           VALUES ($1, $2, $3, 'final', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                   $17::jsonb, $18::jsonb)`,
          [
            runId,
            String(step.stepName),
            position++,
            step.label ?? null,
            step.stepType ?? null,
            step.agent ?? null,
            step.cli ?? null,
            status,
            step.startTime ?? null,
            step.endTime ?? null,
            typeof step.durationMs === "number" ? step.durationMs : null,
            exitCode,
            typeof step.outputSummary === "string" ? step.outputSummary.slice(0, 16_384) : null,
            typeof step.error === "string" ? step.error.slice(0, 16_384) : null,
            step.completionReason ?? null,
            typeof step.retryCount === "number" ? step.retryCount : null,
            JSON.stringify(Array.isArray(step.dependsOn) ? step.dependsOn : []),
            step.detail && typeof step.detail === "object" ? JSON.stringify(step.detail) : null,
          ],
        );
      }
    });
    return steps.length;
  }

  /**
   * Reported steps when the runtime mirrors them; otherwise the steps declared
   * in the definition, with a status inferred from the run (`declared`).
   */
  async listSteps(workspaceId: string, runId: string): Promise<{
    steps: StepRecord[];
    source: "reported" | "declared" | "none";
    live: boolean;
  } | null> {
    const run = await this.getRun(workspaceId, runId);
    if (!run) return null;
    const result = await this.database.query(
      "SELECT * FROM selfhost_run_steps WHERE run_id = $1 ORDER BY position, step_name",
      [runId],
    );
    if (result.rows.length) {
      return {
        source: "reported",
        live: result.rows.some((row) => row.source === "snapshot") && !TERMINAL_RUN_STATUSES.includes(run.status),
        steps: result.rows.map((row) => ({
          stepName: String(row.step_name),
          label: (row.label as string | null) ?? null,
          stepType: (row.step_type as string | null) ?? null,
          agent: (row.agent as string | null) ?? null,
          cli: (row.cli as string | null) ?? null,
          status: String(row.status),
          source: row.source as "snapshot" | "final",
          attempt: num(row.attempt),
          startTime: (row.started_at as string | null) ?? null,
          endTime: (row.ended_at as string | null) ?? null,
          durationMs: num(row.elapsed_ms),
          exitCode: num(row.exit_code),
          outputSummary: (row.output_summary as string | null) ?? null,
          error: (row.error as string | null) ?? null,
          completionReason: (row.completion_reason as string | null) ?? null,
          retryCount: num(row.retry_count),
          dependsOn: jsonValue<string[]>(row.depends_on, []),
          detail: jsonValue(row.detail, null),
        })),
      };
    }
    const source = await this.getRunSource(workspaceId, runId);
    const declared = summarizeDefinition(source ?? "", run.fileType).steps;
    if (!declared.length) return { steps: [], source: "none", live: false };
    const inferred = run.status === "completed"
      ? "completed"
      : run.status === "pending" || run.status === "running"
        ? "pending"
        : "unknown";
    return {
      source: "declared",
      live: false,
      steps: declared.map((step) => ({
        stepName: step.stepName,
        label: null,
        stepType: step.stepType,
        agent: step.agent,
        cli: null,
        status: inferred,
        source: "declared",
        attempt: null,
        startTime: null,
        endTime: null,
        durationMs: null,
        exitCode: null,
        outputSummary: null,
        error: null,
        completionReason: null,
        retryCount: null,
        dependsOn: step.dependsOn,
        detail: null,
      })),
    };
  }

  // ---------------------------------------------------- run-token ingest

  async authenticateRunToken(runId: string, token: string | null): Promise<{ workspaceId: string; status: RunStatus } | null> {
    if (!token || !token.startsWith(RUN_TOKEN_PREFIX) || !isUuid(runId)) return null;
    const result = await this.database.query<{ workspace_id: string; status: RunStatus; access_token_hash: string | null }>(
      "SELECT workspace_id, status, access_token_hash FROM selfhost_workflow_runs WHERE id = $1",
      [runId],
    );
    const row = result.rows[0];
    if (!row?.access_token_hash || !secureEqual(row.access_token_hash, hashSecret(token))) return null;
    return { workspaceId: row.workspace_id, status: row.status };
  }

  async callback(input: {
    runId: string;
    accessToken: string | null;
    callbackToken: string | null;
    status: string;
    result?: Record<string, unknown>;
    error?: string;
  }): Promise<{ runId: string; status: RunStatus }> {
    const run = await this.authenticateRunToken(input.runId, input.accessToken);
    if (!run) throw new FlowsError(401, "unauthorized", "Invalid run token");
    const stored = await this.database.query<{ callback_token_hash: string | null }>(
      "SELECT callback_token_hash FROM selfhost_workflow_runs WHERE id = $1",
      [input.runId],
    );
    const hash = stored.rows[0]?.callback_token_hash;
    if (hash && (!input.callbackToken || !secureEqual(hash, hashSecret(input.callbackToken)))) {
      throw new FlowsError(401, "unauthorized", "Invalid callback token");
    }
    if (!["completed", "failed", "cancelled"].includes(input.status)) {
      throw new FlowsError(409, "local_run_status_not_reportable", "Only terminal statuses are reportable");
    }
    let status = input.status as "completed" | "failed" | "cancelled";
    if (status === "completed" && input.result && input.result.ok === false) status = "failed";
    const changed = await this.database.transaction((tx) =>
      this.finishRun(tx, input.runId, status, {
        error: status === "failed" ? input.error ?? String(input.result?.completionReason ?? "failed") : null,
        result: input.result ?? null,
        summary: typeof input.result?.completionReason === "string" ? String(input.result.completionReason) : null,
      }));
    if (!changed && run.status !== status) {
      throw new FlowsError(409, "run_already_terminal", `Run already ${run.status}`);
    }
    return { runId: input.runId, status };
  }
}
