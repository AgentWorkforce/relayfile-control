import { createServer, type Server } from "node:http";
import { createApp } from "./app.ts";
import { loadConfig, parseListen, redactedConfig } from "./config.ts";
import { Consumer } from "./consumer/consumer.ts";
import { Sweeper } from "./consumer/sweeper.ts";
import { pgDb } from "./db/db.ts";
import { migrate } from "./db/migrate.ts";
import { RefreshingEntitlement } from "./entitlement/entitlement.ts";
import { jsonLogger } from "./observability/log.ts";
import { RelayfileClient } from "./relayfile/client.ts";

async function main(): Promise<void> {
  const logger = jsonLogger();
  const config = loadConfig();
  if (!config.DATABASE_URL) throw new Error("DATABASE_URL is required");
  logger.event("starting", { area: "server", config: redactedConfig(config) });

  const db = pgDb(config.DATABASE_URL, { max: config.DATABASE_POOL_MAX, statementTimeoutMs: config.DATABASE_STATEMENT_TIMEOUT_MS });
  if (config.MIGRATE_ON_START) {
    const applied = await migrate(db);
    logger.event("migrated", { area: "server", applied });
  }

  const entitlement = new RefreshingEntitlement({
    ...(config.RELAYCONTROL_BASE_URL ? { relaycontrolBaseUrl: config.RELAYCONTROL_BASE_URL } : {}),
    ...(config.RELAYCONTROL_SERVICE_TOKEN ? { serviceToken: config.RELAYCONTROL_SERVICE_TOKEN } : {}),
    ...(config.RELAYCONTROL_LICENSE_DOC ? { licenseDocument: config.RELAYCONTROL_LICENSE_DOC } : {}),
    ...(config.RELAYCONTROL_LICENSE_FILE ? { licenseFile: config.RELAYCONTROL_LICENSE_FILE } : {}),
    ...(config.RELAYCONTROL_LICENSE_PUBLIC_KEY ? { publicKey: config.RELAYCONTROL_LICENSE_PUBLIC_KEY } : {}),
    graceSeconds: config.ENTITLEMENT_GRACE_SECONDS,
  });
  const logEntitlement = async () => {
    const s = await entitlement.refresh();
    logger.event("entitlement", { area: "entitlement", entitled: s.entitled, source: s.source, reason: s.reason });
  };
  await logEntitlement();
  const entitlementTimer = setInterval(() => void logEntitlement().catch(() => undefined), config.ENTITLEMENT_REFRESH_SECONDS * 1000);

  const app = createApp({ config, db, logger, entitlement });
  const servers: Server[] = [];
  const listen = (handler: Parameters<typeof createServer>[1], address: string, name: string) => {
    const { host, port } = parseListen(address);
    const server = createServer({ requestTimeout: 60_000, headersTimeout: 15_000 }, handler);
    server.listen(port, host, () => logger.event("listening", { area: "server", name, host, port }));
    servers.push(server);
  };
  listen(app.publicHandler, config.LISTEN, "public");
  listen(app.adminHandler, config.ADMIN_LISTEN, "admin");

  let consumer: Consumer | null = null;
  let sweeper: Sweeper | null = null;
  if (config.ROLE !== "ingress") {
    // A consumer without the Relayfile secret would claim and dead-letter every envelope.
    if (!config.RELAYFILE_INTERNAL_HMAC_SECRET) throw new Error("RELAYFILE_INTERNAL_HMAC_SECRET is required for ROLE=consumer|all");
    const relayfile = new RelayfileClient({ hmacSecret: config.RELAYFILE_INTERNAL_HMAC_SECRET, retry503: config.RELAYFILE_BACKPRESSURE_RETRY_503 });
    consumer = new Consumer({ db, config, logger, metrics: app.metrics, entitlement, cooldowns: app.cooldowns, relayfile });
    sweeper = new Sweeper({ db, config, logger, metrics: app.metrics, cooldowns: app.cooldowns, depth: app.depth });
    consumer.start();
    sweeper.start();
  }

  const shutdown = async (signal: string) => {
    logger.event("stopping", { area: "server", signal });
    clearInterval(entitlementTimer);
    sweeper?.stop();
    await Promise.all(servers.map((s) => new Promise<void>((resolve) => s.close(() => resolve()))));
    await consumer?.stop();
    await db.close();
    process.exit(0);
  };
  process.once("SIGTERM", () => void shutdown("SIGTERM"));
  process.once("SIGINT", () => void shutdown("SIGINT"));
}

main().catch((error) => {
  process.stderr.write(`fatal: ${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
});
