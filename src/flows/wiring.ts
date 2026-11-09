import type { FlowsDeps } from "../app.ts";
import type { Config } from "../config.ts";
import type { Db } from "../db/db.ts";
import type { Logger } from "../observability/log.ts";
import { httpCliCredentialBroker, httpRelaycastCredentialSource } from "./broker.ts";
import { loadFlowsConfig } from "./config.ts";
import type { EncryptionKey, EncryptionKeyring } from "./crypto.ts";
import { flowsDatabase } from "./database.ts";
import { createIntrospector, introspectionUserAccess } from "./introspection.ts";
import { RevokeOutbox } from "./revoke-outbox.ts";
import { FlowsStore } from "./store.ts";

function encryptionKey(value: string, name: string): Buffer {
  const decoded = /^[a-f0-9]{64}$/i.test(value) ? Buffer.from(value, "hex") : Buffer.from(value, "base64");
  if (decoded.length !== 32) throw new Error(`${name} must encode exactly 32 bytes`);
  return decoded;
}

function encryptionKeyId(value: string, name: string): string {
  if (!/^[A-Za-z0-9_-]{1,64}$/.test(value)) throw new Error(`${name} must contain 1-64 URL-safe characters`);
  return value;
}

/** The run envSecrets keyring; same rules as relay-control's encryption keyring. */
export function flowsKeyring(config: Config): EncryptionKeyring {
  if (!config.RELAYFILE_CONTROL_ENCRYPTION_KEY_ID || !config.RELAYFILE_CONTROL_ENCRYPTION_KEY) {
    throw new Error("RELAYFILE_CONTROL_ENCRYPTION_KEY_ID and RELAYFILE_CONTROL_ENCRYPTION_KEY are required");
  }
  const active: EncryptionKey = {
    id: encryptionKeyId(config.RELAYFILE_CONTROL_ENCRYPTION_KEY_ID, "RELAYFILE_CONTROL_ENCRYPTION_KEY_ID"),
    key: encryptionKey(config.RELAYFILE_CONTROL_ENCRYPTION_KEY, "RELAYFILE_CONTROL_ENCRYPTION_KEY"),
  };
  const previousId = config.RELAYFILE_CONTROL_ENCRYPTION_PREVIOUS_KEY_ID;
  const previousKey = config.RELAYFILE_CONTROL_ENCRYPTION_PREVIOUS_KEY;
  if (Boolean(previousId) !== Boolean(previousKey)) {
    throw new Error("RELAYFILE_CONTROL_ENCRYPTION_PREVIOUS_KEY_ID and RELAYFILE_CONTROL_ENCRYPTION_PREVIOUS_KEY must be set together");
  }
  if (!previousId || !previousKey) return { active };
  const previous: EncryptionKey = {
    id: encryptionKeyId(previousId, "RELAYFILE_CONTROL_ENCRYPTION_PREVIOUS_KEY_ID"),
    key: encryptionKey(previousKey, "RELAYFILE_CONTROL_ENCRYPTION_PREVIOUS_KEY"),
  };
  if (previous.id === active.id) throw new Error("Active and previous encryption key IDs must differ");
  if (previous.key.equals(active.key)) throw new Error("Active and previous encryption key material must differ");
  return { active, previous };
}

/**
 * Origins a cookie-authenticated browser request may come from (relay-control's
 * browserOriginsFor): this service's public origin, the Flows public URL's
 * origin and the dashboard's origin.
 */
export function browserOrigins(...urls: Array<string | undefined>): string[] {
  const out = new Set<string>();
  for (const value of urls) {
    if (!value) continue;
    try {
      out.add(new URL(value).origin);
    } catch {
      // not a URL; config validation already refused it
    }
  }
  return [...out];
}

export type FlowsRuntime = { deps: FlowsDeps; outbox: RevokeOutbox };

/**
 * Builds the Flows store and its relay-control seams from configuration
 * (FLOWS_ENABLED=true). Throws when a required value is missing.
 */
export function buildFlows(config: Config, db: Db, logger: Logger, env: NodeJS.ProcessEnv = process.env): FlowsRuntime {
  const baseUrl = config.RELAYCONTROL_BASE_URL;
  const introspectSecret = config.RELAYCONTROL_INTROSPECT_SECRET;
  const brokerSecret = config.RELAYFILE_CONTROL_BROKER_SECRET;
  const publicOrigin = config.RELAYFILE_CONTROL_PUBLIC_ORIGIN;
  if (!baseUrl || !introspectSecret || !brokerSecret || !publicOrigin) {
    throw new Error(
      "Flows require RELAYCONTROL_BASE_URL, RELAYCONTROL_INTROSPECT_SECRET, RELAYFILE_CONTROL_BROKER_SECRET and RELAYFILE_CONTROL_PUBLIC_ORIGIN",
    );
  }
  const flowsConfig = loadFlowsConfig(env);
  const relaycastFallback = env.RELAYCAST_PUBLIC_URL?.trim() || "";
  if (!flowsConfig.workerRelaycastUrl && !relaycastFallback) {
    throw new Error("Flows require SELFHOST_FLOWS_WORKER_RELAYCAST_URL or RELAYCAST_PUBLIC_URL");
  }
  const store = new FlowsStore(flowsDatabase(db), flowsKeyring(config), flowsConfig, relaycastFallback);
  const seam = { baseUrl, secret: brokerSecret };
  store.cliCredentials = httpCliCredentialBroker(seam);
  store.relaycastCredentials = httpRelaycastCredentialSource(seam);
  store.logger = logger;
  const introspect = createIntrospector({
    baseUrl,
    secret: introspectSecret,
    audience: publicOrigin,
    cacheSeconds: config.FLOWS_INTROSPECTION_CACHE_SECONDS,
  });
  const outbox = new RevokeOutbox({ db, broker: store.cliCredentials, logger, intervalMs: config.FLOWS_REVOKE_OUTBOX_INTERVAL_MS });
  const publicBaseUrl = config.FLOWS_PUBLIC_BASE_URL ?? publicOrigin;
  return {
    deps: {
      store,
      userAccess: introspectionUserAccess(introspect, {
        allowedOrigins: browserOrigins(publicOrigin, publicBaseUrl, flowsConfig.dashboardUrl),
      }),
      publicBaseUrl,
    },
    outbox,
  };
}
