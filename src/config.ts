import { readFileSync } from "node:fs";
import { z } from "zod";

const MiB = 1024 * 1024;

/** An empty env value means "unset" (use the default), never 0/false. */
const blankToUndefined = (v: unknown) => (typeof v === "string" && v.trim() === "" ? undefined : v);
const int = (def: number, min: number, max: number) =>
  z.preprocess(blankToUndefined, z.coerce.number().int().min(min).max(max).default(def));
const bool = (def: boolean) =>
  z.preprocess(
    blankToUndefined,
    z
      .enum(["1", "0", "true", "false", "yes", "no", "on", "off"])
      .default(def ? "true" : "false")
      .transform((v) => ["1", "true", "yes", "on"].includes(v)),
  );
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
 * An absolute http(s) origin: scheme, host and optional port only (no
 * credentials, path, query or fragment). Normalized to `URL.origin`.
 */
export function parseOrigin(value: string): string | null {
  let url: URL;
  try {
    url = new URL(value.trim());
  } catch {
    return null;
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") return null;
  if (url.username || url.password || url.search || url.hash || url.pathname !== "/") return null;
  if (/[?#]$/.test(value.trim())) return null;
  return url.origin;
}

/** True for an absolute http(s) URL (a path, query or trailing slash is allowed). */
function isHttpUrl(value: string): boolean {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

/** An optional http(s) origin (see parseOrigin); blank means unset. */
const optionalOrigin = z
  .string()
  .optional()
  .transform((v, ctx) => {
    if (v === undefined || v.trim() === "") return undefined;
    const parsed = parseOrigin(v);
    if (!parsed) {
      ctx.addIssue({ code: "custom", message: "must be an absolute http(s) origin" });
      return z.NEVER;
    }
    return parsed;
  });

/** Shared secrets verified with a constant-time compare must carry at least 32 bytes. */
const longSecret = optional.refine((v) => v === undefined || Buffer.byteLength(v, "utf8") >= 32, {
  message: "must be at least 32 bytes",
});

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
  /** Server-side statement_timeout for every pooled connection (bounds hung sweeps and blob reads). */
  DATABASE_STATEMENT_TIMEOUT_MS: int(30_000, 1000, 3_600_000),
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
  /**
   * relay-control's URL (entitlement, introspection and the Flows credential
   * broker). An absolute http(s) URL; with FLOWS_ENABLED it must be a bare
   * origin (see parseOrigin) and is normalized to it. Checked in superRefine.
   */
  RELAYCONTROL_BASE_URL: optional,
  RELAYCONTROL_SERVICE_TOKEN: optional,
  RELAYCONTROL_LICENSE_DOC: optional,
  RELAYCONTROL_LICENSE_FILE: optional,
  RELAYCONTROL_LICENSE_PUBLIC_KEY: optional,
  ENTITLEMENT_REFRESH_SECONDS: int(300, 5, 86_400),
  ENTITLEMENT_GRACE_SECONDS: int(86_400, 0, 30 * 86_400),

  // Flows API (README "Flows API").
  /**
   * This service's public origin. Sent as `aud` when introspecting
   * relay-control access tokens; relay-control must list it in
   * SELFHOST_INTROSPECT_AUDIENCES. Required when FLOWS_ENABLED.
   */
  RELAYFILE_CONTROL_PUBLIC_ORIGIN: optionalOrigin,
  /**
   * The public URL users and workers reach the Flows routes at (worker
   * register/start commands, local-run `apiUrl`). The ingress routes the
   * Flows paths here, so this is normally the installation's shared public
   * URL. Defaults to RELAYFILE_CONTROL_PUBLIC_ORIGIN.
   */
  FLOWS_PUBLIC_BASE_URL: z
    .string()
    .optional()
    .transform((v, ctx) => {
      if (v === undefined || v.trim() === "") return undefined;
      try {
        const url = new URL(v.trim());
        if ((url.protocol === "http:" || url.protocol === "https:") && !url.username && !url.password && !url.search && !url.hash) {
          return url.toString().replace(/\/+$/, "");
        }
      } catch {
        // fall through
      }
      ctx.addIssue({ code: "custom", message: "must be an absolute http(s) URL" });
      return z.NEVER;
    }),
  /** Mounts the Flows routes and starts their background workers. */
  FLOWS_ENABLED: bool(false),
  /** Bearer for relay-control POST /internal/v1/auth/introspect (at least 32 bytes). */
  RELAYCONTROL_INTROSPECT_SECRET: longSecret,
  /**
   * Sent as the bearer on every call to relay-control's Flows credential
   * broker, and verified on DELETE /internal/v1/flows/workspaces/{id}
   * (at least 32 bytes).
   */
  RELAYFILE_CONTROL_BROKER_SECRET: longSecret,
  /** Verified on GET /internal/v1/telemetry/flows. Unset disables the route (404). */
  RELAYFILE_CONTROL_TELEMETRY_SECRET: longSecret,
  /** Verified on POST /internal/v1/flows/runtime-credentials/verify. Unset disables the route (404). */
  RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET: longSecret,
  /** Keyring for run envSecrets at rest (AES-256-GCM; 32 bytes as hex or base64). */
  RELAYFILE_CONTROL_ENCRYPTION_KEY_ID: optional,
  RELAYFILE_CONTROL_ENCRYPTION_KEY: optional,
  RELAYFILE_CONTROL_ENCRYPTION_PREVIOUS_KEY_ID: optional,
  RELAYFILE_CONTROL_ENCRYPTION_PREVIOUS_KEY: optional,
  /** Introspection answers are cached per token hash for at most this long. */
  FLOWS_INTROSPECTION_CACHE_SECONDS: int(30, 0, 30),
  FLOWS_REVOKE_OUTBOX_INTERVAL_MS: int(5_000, 100, 3_600_000),
}).superRefine((c, ctx) => {
  // Each static shared secret authorizes a different caller; two equal values
  // would let one caller act as the other.
  const distinct = [
    "RELAYCONTROL_INTROSPECT_SECRET",
    "RELAYFILE_CONTROL_BROKER_SECRET",
    "RELAYFILE_CONTROL_TELEMETRY_SECRET",
    "RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET",
  ] as const;
  distinct.forEach((key, index) => {
    const value = c[key];
    if (value === undefined) return;
    const twin = distinct.slice(0, index).find((other) => c[other] === value);
    if (twin) ctx.addIssue({ code: "custom", path: [key], message: `must differ from ${twin}` });
  });
  // Flows join route paths onto RELAYCONTROL_BASE_URL, so they need a bare
  // origin. Without Flows only the entitlement client uses it, and a path
  // there was always tolerated.
  if (c.RELAYCONTROL_BASE_URL !== undefined) {
    if (c.FLOWS_ENABLED) {
      if (!parseOrigin(c.RELAYCONTROL_BASE_URL)) {
        ctx.addIssue({ code: "custom", path: ["RELAYCONTROL_BASE_URL"], message: "must be an absolute http(s) origin when FLOWS_ENABLED" });
      }
    } else if (!isHttpUrl(c.RELAYCONTROL_BASE_URL)) {
      ctx.addIssue({ code: "custom", path: ["RELAYCONTROL_BASE_URL"], message: "must be an absolute http(s) URL" });
    }
  }
  if (!c.FLOWS_ENABLED) return;
  const required: Array<keyof typeof c> = [
    "RELAYFILE_CONTROL_PUBLIC_ORIGIN",
    "RELAYCONTROL_BASE_URL",
    "RELAYCONTROL_INTROSPECT_SECRET",
    "RELAYFILE_CONTROL_BROKER_SECRET",
    "RELAYFILE_CONTROL_ENCRYPTION_KEY_ID",
    "RELAYFILE_CONTROL_ENCRYPTION_KEY",
  ];
  for (const key of required) {
    if (c[key] === undefined) ctx.addIssue({ code: "custom", path: [key], message: "required when FLOWS_ENABLED" });
  }
}).transform((c) =>
  c.FLOWS_ENABLED && c.RELAYCONTROL_BASE_URL !== undefined
    ? { ...c, RELAYCONTROL_BASE_URL: parseOrigin(c.RELAYCONTROL_BASE_URL) ?? c.RELAYCONTROL_BASE_URL }
    : c,
);

export type Config = z.infer<typeof ConfigSchema>;

/** Secrets that may also be given as a file path in `<NAME>_FILE`. */
export const FILE_SECRETS = [
  "RELAYCONTROL_INTROSPECT_SECRET",
  "RELAYFILE_CONTROL_BROKER_SECRET",
  "RELAYFILE_CONTROL_TELEMETRY_SECRET",
  "RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET",
  "RELAYFILE_CONTROL_ENCRYPTION_KEY",
  "RELAYFILE_CONTROL_ENCRYPTION_PREVIOUS_KEY",
] as const;

/** Resolves `<NAME>_FILE` into `<NAME>`; setting both is a configuration error. */
export function resolveFileSecrets(env: NodeJS.ProcessEnv, read: (path: string) => string = (path) => readFileSync(path, "utf8")): NodeJS.ProcessEnv {
  const out: NodeJS.ProcessEnv = { ...env };
  const conflicts: string[] = [];
  for (const name of FILE_SECRETS) {
    const file = env[`${name}_FILE`]?.trim();
    if (!file) continue;
    if (env[name]?.trim()) {
      conflicts.push(name);
      continue;
    }
    out[name] = read(file).trim();
  }
  if (conflicts.length) throw new Error(`invalid configuration: ${conflicts.join(", ")} (set the value or _FILE, not both)`);
  return out;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = ConfigSchema.safeParse(resolveFileSecrets(env));
  if (!parsed.success) {
    const fields = parsed.error.issues.map((i) => i.path.join(".")).join(", ");
    throw new Error(`invalid configuration: ${fields}`);
  }
  return parsed.data;
}

/** Test/dev helper: defaults plus overrides, without reading process.env. */
export function testConfig(overrides: Partial<Record<keyof Config, string>> = {}): Config {
  return ConfigSchema.parse({ RELAYFILE_CONTROL_PUBLIC_ORIGIN: "http://relayfile-control.test", ...overrides });
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
  "RELAYCONTROL_INTROSPECT_SECRET",
  "RELAYFILE_CONTROL_BROKER_SECRET",
  "RELAYFILE_CONTROL_TELEMETRY_SECRET",
  "RELAYFILE_CONTROL_RUNTIME_VERIFY_SECRET",
  "RELAYFILE_CONTROL_ENCRYPTION_KEY",
  "RELAYFILE_CONTROL_ENCRYPTION_PREVIOUS_KEY",
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
  const portText = idx >= 0 ? value.slice(idx + 1) : value;
  if (!/^\d{1,5}$/.test(portText)) throw new Error(`invalid listen address ${value}`);
  const port = Number(portText);
  if (port < 1 || port > 65535) throw new Error(`invalid listen address ${value}`);
  return { host, port };
}
