import { createHash } from "node:crypto";
import type { Database } from "./database.ts";

/**
 * Run object storage (code archives, runner logs, step logs, artifacts).
 *
 * The object index always lives in Postgres (`selfhost_run_objects`), so
 * listing and authorization never depend on the blob backend. Bytes live
 * either inline in Postgres (default; fine for logs and small artifacts) or in
 * a GCS bucket when `SELFHOST_FLOWS_STORAGE=gcs`.
 */
export interface BlobBackend {
  readonly kind: "postgres" | "gcs";
  /** Returns an external location, or null when bytes are stored inline. */
  put(location: string, body: Buffer, contentType: string): Promise<string | null>;
  get(location: string): Promise<Buffer | null>;
}

export type RunObjectMeta = {
  key: string;
  contentType: string;
  size: number;
  etag: string;
  updatedAt: string;
};

export const MAX_OBJECT_BYTES = 8 * 1024 * 1024;

export class InlinePostgresBackend implements BlobBackend {
  readonly kind = "postgres" as const;
  async put(): Promise<string | null> {
    return null;
  }
  async get(): Promise<Buffer | null> {
    return null;
  }
}

type FetchLike = typeof fetch;

/**
 * Minimal GCS JSON-API backend. Credentials come from the GKE metadata server
 * (Workload Identity), so no key material is configured or stored.
 */
export class GcsBackend implements BlobBackend {
  readonly kind = "gcs" as const;
  #token: { value: string; expiresAt: number } | null = null;

  constructor(
    private readonly bucket: string,
    private readonly prefix = "relayflows",
    private readonly fetchImpl: FetchLike = fetch,
    private readonly tokenUrl =
      "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
  ) {
    if (!/^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$/.test(bucket)) {
      throw new Error("SELFHOST_FLOWS_GCS_BUCKET is not a valid bucket name");
    }
  }

  private async accessToken(): Promise<string> {
    if (this.#token && this.#token.expiresAt > Date.now() + 60_000) return this.#token.value;
    const response = await this.fetchImpl(this.tokenUrl, {
      headers: { "metadata-flavor": "Google" },
    });
    if (!response.ok) throw new Error(`GCS token request failed: ${response.status}`);
    const body = await response.json() as { access_token?: string; expires_in?: number };
    if (!body.access_token) throw new Error("GCS token response missing access_token");
    this.#token = {
      value: body.access_token,
      expiresAt: Date.now() + (body.expires_in ?? 300) * 1_000,
    };
    return this.#token.value;
  }

  private objectName(location: string): string {
    return `${this.prefix.replace(/\/+$/, "")}/${location}`;
  }

  async put(location: string, body: Buffer, contentType: string): Promise<string> {
    const name = this.objectName(location);
    const url = `https://storage.googleapis.com/upload/storage/v1/b/${encodeURIComponent(this.bucket)}/o?uploadType=media&name=${encodeURIComponent(name)}`;
    const response = await this.fetchImpl(url, {
      method: "POST",
      headers: {
        authorization: `Bearer ${await this.accessToken()}`,
        "content-type": contentType,
      },
      body: new Uint8Array(body),
    });
    if (!response.ok) throw new Error(`GCS upload failed: ${response.status}`);
    return `gs://${this.bucket}/${name}`;
  }

  async get(location: string): Promise<Buffer | null> {
    const name = this.objectName(location);
    const url = `https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(this.bucket)}/o/${encodeURIComponent(name)}?alt=media`;
    const response = await this.fetchImpl(url, {
      headers: { authorization: `Bearer ${await this.accessToken()}` },
    });
    if (response.status === 404) return null;
    if (!response.ok) throw new Error(`GCS download failed: ${response.status}`);
    return Buffer.from(await response.arrayBuffer());
  }
}

export function validateObjectKey(raw: string): string | null {
  if (!raw || raw.length > 1024 || raw.startsWith("/") || /[\\\u0000]/u.test(raw)) return null;
  const segments = raw.split("/");
  if (segments.some((segment) => !segment || segment === "." || segment === "..")) return null;
  if (/[\u0000-\u001f\u007f]/u.test(raw)) return null;
  return raw;
}

export class RunObjectStore {
  constructor(
    private readonly database: Database,
    readonly backend: BlobBackend = new InlinePostgresBackend(),
  ) {}

  async put(
    runId: string,
    key: string,
    body: Buffer,
    contentType = "application/octet-stream",
  ): Promise<RunObjectMeta> {
    if (body.length > MAX_OBJECT_BYTES) throw new Error("object too large");
    const etag = `"${createHash("sha256").update(body).digest("hex").slice(0, 32)}"`;
    const external = await this.backend.put(`${runId}/${key}`, body, contentType);
    const result = await this.database.query<{ updated_at: Date | string }>(
      `INSERT INTO selfhost_run_objects
         (run_id, object_key, content_type, size_bytes, etag, content, external_location)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (run_id, object_key) DO UPDATE
         SET content_type = EXCLUDED.content_type, size_bytes = EXCLUDED.size_bytes,
             etag = EXCLUDED.etag, content = EXCLUDED.content,
             external_location = EXCLUDED.external_location, updated_at = now()
       RETURNING updated_at`,
      [runId, key, contentType, body.length, etag, external ? null : body, external],
    );
    return {
      key,
      contentType,
      size: body.length,
      etag,
      updatedAt: new Date(result.rows[0]!.updated_at).toISOString(),
    };
  }

  async head(runId: string, key: string): Promise<RunObjectMeta | null> {
    const result = await this.database.query<{
      object_key: string;
      content_type: string;
      size_bytes: number;
      etag: string;
      updated_at: Date | string;
    }>(
      `SELECT object_key, content_type, size_bytes, etag, updated_at
         FROM selfhost_run_objects WHERE run_id = $1 AND object_key = $2`,
      [runId, key],
    );
    const row = result.rows[0];
    return row ? toMeta(row) : null;
  }

  async get(runId: string, key: string): Promise<(RunObjectMeta & { body: Buffer }) | null> {
    const result = await this.database.query<{
      object_key: string;
      content_type: string;
      size_bytes: number;
      etag: string;
      updated_at: Date | string;
      content: Uint8Array | null;
      external_location: string | null;
    }>(
      `SELECT object_key, content_type, size_bytes, etag, updated_at, content, external_location
         FROM selfhost_run_objects WHERE run_id = $1 AND object_key = $2`,
      [runId, key],
    );
    const row = result.rows[0];
    if (!row) return null;
    const body = row.content
      ? Buffer.from(row.content)
      : row.external_location
        ? await this.backend.get(`${runId}/${key}`)
        : Buffer.alloc(0);
    if (!body) return null;
    return { ...toMeta(row), body };
  }

  async list(runId: string): Promise<RunObjectMeta[]> {
    const result = await this.database.query<{
      object_key: string;
      content_type: string;
      size_bytes: number;
      etag: string;
      updated_at: Date | string;
    }>(
      `SELECT object_key, content_type, size_bytes, etag, updated_at
         FROM selfhost_run_objects WHERE run_id = $1 ORDER BY object_key`,
      [runId],
    );
    return result.rows.map(toMeta);
  }
}

function toMeta(row: {
  object_key: string;
  content_type: string;
  size_bytes: number;
  etag: string;
  updated_at: Date | string;
}): RunObjectMeta {
  return {
    key: row.object_key,
    contentType: row.content_type,
    size: Number(row.size_bytes),
    etag: row.etag,
    updatedAt: new Date(row.updated_at).toISOString(),
  };
}
