import { readFileSync } from "node:fs";
import { GcsBackend, InlinePostgresBackend, type BlobBackend } from "./objects.ts";
import { httpRuntimeEnvironmentSource, type RuntimeEnvironmentSource } from "./runtime-env.ts";

type Env = Record<string, string | undefined>;

/**
 * Subscription logins (Codex/Grok auth files) for a run, provided in-process by
 * the admin module's SubscriptionLoginStore. The assignment payload carries
 * only a short-lived reference; the worker exchanges it, with its own worker
 * credential, for the files at GET .../assignments/{runId}/cli-credentials.
 */
export type CliCredentialBroker = {
  issueRunReference(workspaceId: string, runId: string): Promise<{
    ref: string;
    expiresAt: string;
    credentials: Array<{ credentialId: string; provider: string; cli: string }>;
  } | null>;
  readForRun(ref: string | null, runId: string): Promise<{
    files: Array<{
      credentialId: string;
      provider: string;
      cli: string;
      homeEnv: string;
      directory: string;
      file: string;
      content: string;
    }>;
    skipped: Array<{ credentialId: string; reason: string }>;
  }>;
  writeBack(ref: string | null, runId: string, credentialId: string, content: unknown): Promise<{ updated: boolean }>;
};

export type FlowsConfig = {
  /** Relaycast origin handed to workers in the assignment payload. */
  workerRelaycastUrl?: string;
  /** Relayfile origin handed to workers (empty when not deployed). */
  workerRelayfileUrl: string;
  /** Optional https dashboard origin used to build runUrl for mirrored runs. */
  dashboardUrl?: string;
  queueWaitMs: number;
  enrollmentTokenTtlMs: number;
  heartbeatIntervalMs: number;
  /** SSE keepalive cadence. Must stay under proxy idle timeouts. */
  queuePingMs: number;
  /** How often the queue stream checks Postgres for new work (replica safe). */
  queuePollMs: number;
  /** A worker is online when seen within this window. */
  onlineWindowMs: number;
  /** Running work on a worker unseen for this long is failed. */
  disconnectTimeoutMs: number;
  storage: BlobBackend;
  /**
   * Workspace env vars and integration tokens merged into each assignment
   * payload at delivery time (admin lane internal runtime contract).
   */
  runtimeEnvironment?: RuntimeEnvironmentSource;
};

function secret(env: Env, name: string): string | undefined {
  const direct = env[name]?.trim();
  const file = env[`${name}_FILE`]?.trim();
  if (direct && file) throw new Error(`${name} and ${name}_FILE are mutually exclusive`);
  if (direct) return direct;
  return file ? readFileSync(file, "utf8").trim() || undefined : undefined;
}

function origin(env: Env, name: string): string | undefined {
  const value = env[name]?.trim();
  if (!value) return undefined;
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${name} must be an absolute URL`);
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new Error(`${name} must use http or https`);
  }
  if (url.username || url.password || url.search || url.hash) {
    throw new Error(`${name} must not contain credentials, query, or fragment`);
  }
  return value.replace(/\/+$/, "");
}

function duration(env: Env, name: string, fallback: number, min: number, max: number): number {
  const raw = env[name]?.trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer from ${min} to ${max}`);
  }
  return value;
}

export function defaultFlowsConfig(overrides: Partial<FlowsConfig> = {}): FlowsConfig {
  return {
    workerRelayfileUrl: "",
    queueWaitMs: 10 * 60 * 1_000,
    enrollmentTokenTtlMs: 60 * 60 * 1_000,
    heartbeatIntervalMs: 30_000,
    queuePingMs: 20_000,
    queuePollMs: 1_000,
    onlineWindowMs: 90_000,
    disconnectTimeoutMs: 5 * 60 * 1_000,
    storage: new InlinePostgresBackend(),
    ...overrides,
  };
}

export function loadFlowsConfig(env: Env = process.env): FlowsConfig {
  const storageKind = env.SELFHOST_FLOWS_STORAGE?.trim() || "postgres";
  let storage: BlobBackend;
  if (storageKind === "postgres") {
    storage = new InlinePostgresBackend();
  } else if (storageKind === "gcs") {
    const bucket = env.SELFHOST_FLOWS_GCS_BUCKET?.trim();
    if (!bucket) throw new Error("SELFHOST_FLOWS_GCS_BUCKET is required when SELFHOST_FLOWS_STORAGE=gcs");
    storage = new GcsBackend(bucket, env.SELFHOST_FLOWS_GCS_PREFIX?.trim() || "relayflows");
  } else {
    throw new Error("SELFHOST_FLOWS_STORAGE must be postgres or gcs");
  }
  const dashboardUrl = origin(env, "SELFHOST_DASHBOARD_URL");
  const internalToken = secret(env, "SELFHOST_INTERNAL_TOKEN");
  const runtimeEnvironment = internalToken
    ? httpRuntimeEnvironmentSource({
      baseUrl: origin(env, "SELFHOST_FLOWS_RUNTIME_ENV_URL") ??
        `http://127.0.0.1:${env.PORT?.trim() || "3000"}`,
      token: internalToken,
    })
    : undefined;
  return defaultFlowsConfig({
    workerRelaycastUrl: origin(env, "SELFHOST_FLOWS_WORKER_RELAYCAST_URL"),
    workerRelayfileUrl: origin(env, "SELFHOST_FLOWS_WORKER_RELAYFILE_URL") ?? "",
    dashboardUrl,
    queueWaitMs: duration(env, "SELFHOST_FLOWS_QUEUE_WAIT_MS", 600_000, 10_000, 86_400_000),
    enrollmentTokenTtlMs: duration(
      env,
      "SELFHOST_FLOWS_ENROLLMENT_TTL_MS",
      3_600_000,
      60_000,
      7 * 86_400_000,
    ),
    queuePingMs: duration(env, "SELFHOST_FLOWS_QUEUE_PING_MS", 20_000, 1_000, 120_000),
    storage,
    runtimeEnvironment,
  });
}
