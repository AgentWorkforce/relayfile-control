import { GcsBackend, InlinePostgresBackend, type BlobBackend } from "./objects.ts";
import type { RuntimeEnvironmentSource } from "./runtime-env.ts";

type Env = Record<string, string | undefined>;

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
   * payload at delivery time (admin lane internal runtime contract). Not
   * wired yet: the relay-control route behind it is authenticated with a
   * secret this service must not hold; unset means runs carry only their
   * own envSecrets, as on an installation without the admin module.
   */
  runtimeEnvironment?: RuntimeEnvironmentSource;
};

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
  });
}
