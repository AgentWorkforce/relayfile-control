import { z } from "zod";

const MiB = 1024 * 1024;

const int = (def: number, min: number, max: number) =>
  z.coerce.number().int().min(min).max(max).default(def);
const bool = (def: boolean) =>
  z
    .enum(["1", "0", "true", "false", "yes", "no", "on", "off", ""])
    .default(def ? "true" : "false")
    .transform((v) => ["1", "true", "yes", "on"].includes(v));
/** Comma/whitespace separated secret list (rotation). Empty means "not configured". */
const secrets = z
  .string()
  .default("")
  .transform((v) => v.split(/[\s,]+/).filter((s) => s.length > 0));
const optional = z
  .string()
  .optional()
  .transform((v) => (v && v.trim().length > 0 ? v.trim() : undefined));

/**
 * Every knob in docs/design/webhook-ingest.md, all finite, all env-driven.
 * Secrets are parsed here and never logged (see `redactedConfig`).
 */
export const ConfigSchema = z.object({
  ROLE: z.enum(["ingress", "consumer", "all"]).default("all"),
  LISTEN: z.string().default("0.0.0.0:3100"),
  ADMIN_LISTEN: z.string().default("127.0.0.1:3101"),
  DATABASE_URL: optional,
  DATABASE_POOL_MAX: int(20, 1, 500),
  MIGRATE_ON_START: bool(false),

  // §2.2 secrets (lists allow rotation).
  GITHUB_WEBHOOK_SECRET: secrets,
  NANGO_WEBHOOK_SECRET: secrets,
  HOOKDECK_WEBHOOK_SECRET: secrets,
  DROPBOX_APP_SECRET: secrets,
  SLACK_SIGNING_SECRET: secrets,
  LINEAR_WEBHOOK_SECRET: secrets,

  // §2.2 bounds.
  INGRESS_MAX_BODY_BYTES: int(25 * MiB, 1024, 100 * MiB),
  INGRESS_MAX_BODY_BYTES_OTHER: int(1 * MiB, 1024, 100 * MiB),
  INLINE_PAYLOAD_MAX_BYTES: int(96 * 1024, 1024, 1 * MiB),
  INGRESS_REJECT_UNKNOWN_INSTALLATION: bool(false),
  INGRESS_PAUSED: bool(false),

  // §6.4 admission gate.
  LANE_MAX_QUEUED: int(5000, 1, 10_000_000),
  GLOBAL_MAX_QUEUED: int(100_000, 1, 100_000_000),
  MAX_QUEUE_AGE_SECONDS: int(3600, 1, 7 * 86_400),
  ADMISSION_RETRY_AFTER_SECONDS: int(30, 1, 3600),

  // §3 claims.
  CLAIM_LEASE_SECONDS: int(900, 1, 86_400),
  GMAIL_SYNC_COALESCE_SECONDS: int(60, 0, 86_400),

  // §4 consumer.
  CONSUMER_CONCURRENCY: int(10, 1, 256),
  PER_LANE_MAX_INFLIGHT: int(2, 1, 256),
  MAX_ATTEMPTS: int(5, 1, 100),
  LEASE_SECONDS: int(180, 5, 3600),
  PAYLOAD_READ_TIMEOUT_MS: int(15_000, 100, 120_000),
  CONSUMER_IDLE_POLL_MS: int(500, 10, 60_000),

  // §6.3 cool-down.
  /** #6177 per-workspace cool-down: ships as a default-off option (Khaliq, plan §6.3). */
  COOLDOWN_ENABLED: bool(false),
  COOLDOWN_MAX_KEYS: int(1000, 1, 1_000_000),

  // §8 DLQ / retention.
  DLQ_MAX_PRESERVATION_CYCLES: int(3, 0, 10),
  DLQ_ARCHIVE_RETENTION_SECONDS: int(1_209_600, 3600, 365 * 86_400),
  ENVELOPE_RETENTION_SECONDS: int(7 * 86_400, 3600, 365 * 86_400),
  SWEEP_INTERVAL_MS: int(60_000, 100, 3_600_000),

  // §6.1 Relayfile.
  RELAYFILE_BASE_URL: optional,
  RELAYFILE_INTERNAL_HMAC_SECRET: optional,
  RELAYFILE_FLAVOR: z.enum(["oss", "cloud"]).default("oss"),
  /** Retry 503 (and OSS 500) in place like 429; default off = identical to cloud#6175 final (§6.2). */
  RELAYFILE_BACKPRESSURE_RETRY_503: bool(false),
  /** Provider-writes client is gated on this, not on flavour (plan §11, f9d0241). PR 2 consumes it. */
  RELAYFILE_PROVIDER_WRITES: bool(false),

  // §9 admin.
  RELAYFILE_CONTROL_ADMIN_TOKEN: secrets,

  // §11 entitlement.
  RELAYCONTROL_BASE_URL: optional,
  RELAYCONTROL_SERVICE_TOKEN: optional,
  RELAYCONTROL_LICENSE_DOC: optional,
  RELAYCONTROL_LICENSE_FILE: optional,
  RELAYCONTROL_LICENSE_PUBLIC_KEY: optional,
  ENTITLEMENT_REFRESH_SECONDS: int(300, 5, 86_400),
  ENTITLEMENT_GRACE_SECONDS: int(86_400, 0, 30 * 86_400),
});

export type Config = z.infer<typeof ConfigSchema>;

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = ConfigSchema.safeParse(env);
  if (!parsed.success) {
    const fields = parsed.error.issues.map((i) => i.path.join(".")).join(", ");
    throw new Error(`invalid configuration: ${fields}`);
  }
  return parsed.data;
}

/** Test/dev helper: defaults plus overrides, without reading process.env. */
export function testConfig(overrides: Partial<Record<keyof Config, string>> = {}): Config {
  return ConfigSchema.parse(overrides);
}

const SECRET_KEYS = new Set<keyof Config>([
  "GITHUB_WEBHOOK_SECRET",
  "NANGO_WEBHOOK_SECRET",
  "HOOKDECK_WEBHOOK_SECRET",
  "DROPBOX_APP_SECRET",
  "SLACK_SIGNING_SECRET",
  "LINEAR_WEBHOOK_SECRET",
  "RELAYFILE_INTERNAL_HMAC_SECRET",
  "RELAYFILE_CONTROL_ADMIN_TOKEN",
  "RELAYCONTROL_SERVICE_TOKEN",
  "RELAYCONTROL_LICENSE_DOC",
  "DATABASE_URL",
]);

/** Config safe to log: secrets reduced to "set"/"unset". */
export function redactedConfig(config: Config): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(config)) {
    if (SECRET_KEYS.has(key as keyof Config)) {
      const set = Array.isArray(value) ? value.length > 0 : value !== undefined;
      out[key] = set ? "set" : "unset";
    } else {
      out[key] = value;
    }
  }
  return out;
}

/** `host:port` → listen options. */
export function parseListen(value: string): { host: string; port: number } {
  const idx = value.lastIndexOf(":");
  const host = idx > 0 ? value.slice(0, idx) : "0.0.0.0";
  const port = Number(idx >= 0 ? value.slice(idx + 1) : value);
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error(`invalid listen address ${value}`);
  return { host, port };
}
