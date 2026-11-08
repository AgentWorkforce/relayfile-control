import type { Queryable } from "../db/db.ts";
import type { NativeIds } from "../envelope/model.ts";
import { ulid } from "../envelope/ulid.ts";

export type Binding = {
  id: string;
  workspace_id: string | null;
  provider: string;
  connection_id: string;
  provider_config_key: string | null;
  native_ids: NativeIds;
  relayfile_base_url: string | null;
  status: "active" | "pending" | "disabled";
};

export class InvalidWorkspaceIdError extends Error {
  readonly code = "invalid_workspace_id";
  constructor() {
    super("invalid_workspace_id");
  }
}

export type BindingInput = {
  workspaceId: string;
  provider: string;
  connectionId: string;
  providerConfigKey?: string | null;
  nativeIds?: NativeIds;
  relayfileBaseUrl?: string | null;
  status?: "active" | "disabled";
};

/** Workspace ids must start with `rw_` or the binding is refused (§5). */
export function assertWorkspaceId(id: unknown): asserts id is string {
  if (typeof id !== "string" || !/^rw_[A-Za-z0-9_-]{1,128}$/.test(id)) throw new InvalidWorkspaceIdError();
}

export async function upsertBinding(db: Queryable, input: BindingInput): Promise<Binding> {
  assertWorkspaceId(input.workspaceId);
  const { rows } = await db.query<Binding>(
    `INSERT INTO integration_bindings (id, workspace_id, provider, connection_id, provider_config_key, native_ids, relayfile_base_url, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
     ON CONFLICT (provider, connection_id) DO UPDATE
        SET workspace_id = EXCLUDED.workspace_id, provider_config_key = EXCLUDED.provider_config_key,
            native_ids = EXCLUDED.native_ids, relayfile_base_url = EXCLUDED.relayfile_base_url,
            status = EXCLUDED.status, updated_at = now()
     RETURNING *`,
    [
      ulid(),
      input.workspaceId,
      input.provider,
      input.connectionId,
      input.providerConfigKey ?? null,
      JSON.stringify(input.nativeIds ?? {}),
      input.relayfileBaseUrl ?? null,
      input.status ?? "active",
    ],
  );
  return rows[0]!;
}

/** Nango auth / connection.created: a pending binding so forwards lane by connection (§5). Never downgrades an active one. */
export async function upsertPendingBinding(db: Queryable, provider: string, connectionId: string, providerConfigKey: string | null): Promise<void> {
  await db.query(
    `INSERT INTO integration_bindings (id, provider, connection_id, provider_config_key, status)
     VALUES ($1, $2, $3, $4, 'pending')
     ON CONFLICT (provider, connection_id) DO NOTHING`,
    [ulid(), provider, connectionId, providerConfigKey],
  );
}

export async function listBindings(db: Queryable, filter: { workspaceId?: string; provider?: string; limit?: number }): Promise<Binding[]> {
  const { rows } = await db.query<Binding>(
    `SELECT * FROM integration_bindings
      WHERE ($1::text IS NULL OR workspace_id = $1) AND ($2::text IS NULL OR provider = $2)
      ORDER BY created_at DESC LIMIT $3`,
    [filter.workspaceId ?? null, filter.provider ?? null, Math.min(Math.max(filter.limit ?? 100, 1), 1000)],
  );
  return rows;
}

export async function deleteBinding(db: Queryable, provider: string, connectionId: string): Promise<boolean> {
  const r = await db.query("DELETE FROM integration_bindings WHERE provider = $1 AND connection_id = $2", [provider, connectionId]);
  return r.rowCount > 0;
}

export type Resolution =
  | { kind: "matched"; bindings: Binding[]; via: "connection" | "native" }
  | { kind: "pending" }
  | { kind: "unmatched" };

const NATIVE_KEYS: (keyof NativeIds)[] = ["installation_id", "team_id", "enterprise_id", "organization_id", "cloud_id", "email"];

/**
 * Resolution order (§5 HARD): connection id → provider-native id → unmatched.
 * Native lookups return every active match (GitHub installation fans out).
 */
export async function resolveBindings(
  db: Queryable,
  input: { connectionId: string | null; nativeIds: NativeIds },
): Promise<Resolution> {
  if (input.connectionId) {
    const { rows } = await db.query<Binding>(
      "SELECT * FROM integration_bindings WHERE connection_id = $1 AND status <> 'disabled' ORDER BY created_at",
      [input.connectionId],
    );
    const active = rows.filter((b) => b.status === "active");
    if (active.length > 0) return { kind: "matched", bindings: active, via: "connection" };
    if (rows.some((b) => b.status === "pending")) return { kind: "pending" };
  }
  for (const key of NATIVE_KEYS) {
    const value = input.nativeIds[key];
    if (!value) continue;
    const { rows } = await db.query<Binding>(
      "SELECT * FROM integration_bindings WHERE native_ids @> $1::jsonb AND status = 'active' ORDER BY created_at",
      [JSON.stringify({ [key]: value })],
    );
    if (rows.length > 0) return { kind: "matched", bindings: rows, via: "native" };
  }
  return { kind: "unmatched" };
}

/** Small TTL cache for the ingress lane lookup (§4.2 "cached binding lookup"). */
export class BindingLaneCache {
  private cache = new Map<string, { lane: string | null; at: number }>();
  constructor(
    private readonly db: Queryable,
    private readonly ttlMs = 30_000,
    private readonly max = 10_000,
  ) {}

  /** The single workspace lane, or null when unresolved/ambiguous (the consumer decides). */
  async lane(connectionId: string | null, nativeIds: NativeIds): Promise<string | null> {
    const key = JSON.stringify([connectionId, nativeIds]);
    const hit = this.cache.get(key);
    if (hit && Date.now() - hit.at < this.ttlMs) return hit.lane;
    const r = await resolveBindings(this.db, { connectionId, nativeIds });
    const workspaces = r.kind === "matched" ? [...new Set(r.bindings.map((b) => b.workspace_id))] : [];
    const lane = workspaces.length === 1 ? (workspaces[0] ?? null) : null;
    if (this.cache.size >= this.max) this.cache.clear();
    this.cache.set(key, { lane, at: Date.now() });
    return lane;
  }

  /** Whether a GitHub installation is known at all (§5 unknown-installation rule). */
  async knownInstallation(installationId: string): Promise<boolean> {
    const r = await resolveBindings(this.db, { connectionId: null, nativeIds: { installation_id: installationId } });
    return r.kind === "matched";
  }

  clear(): void {
    this.cache.clear();
  }
}
