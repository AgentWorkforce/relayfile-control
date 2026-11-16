import pg from "pg";
import type { Logger } from "../observability/log.ts";
import type { Metrics } from "../observability/metrics.ts";

const STARTUP_CHECK_TIMEOUT_MS = 5_000;

/** Bound both connection establishment and the RESET/SHOW query. */
export function statementBoundCheckPoolConfig(connectionString: string): pg.PoolConfig {
  return {
    connectionString,
    max: 1,
    connectionTimeoutMillis: STARTUP_CHECK_TIMEOUT_MS,
    query_timeout: STARTUP_CHECK_TIMEOUT_MS,
  };
}

/**
 * Read the role/database default without pgDb's session override. This runs at
 * startup, before the application pool can set or leak a session value.
 */
export async function readStatementTimeoutDefault(connectionString: string): Promise<string> {
  const pool = new pg.Pool(statementBoundCheckPoolConfig(connectionString));
  pool.on("error", () => undefined);
  try {
    const client = await pool.connect();
    try {
      // One simple-query message keeps RESET and SHOW on the same backend even
      // when a transaction pooler releases the backend after each transaction.
      const raw = await client.query<{ statement_timeout: string }>(
        "RESET statement_timeout; SHOW statement_timeout",
      );
      const result = Array.isArray(raw) ? raw.at(-1) : raw;
      const setting = result.rows[0]?.statement_timeout;
      if (typeof setting !== "string" || setting.trim() === "") {
        throw new Error("statement_timeout default was not returned");
      }
      return setting.trim();
    } finally {
      client.release(true);
    }
  } finally {
    await pool.end();
  }
}

const POSTGRES_DURATION_MULTIPLIERS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  min: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

/** Parse the single-value duration format returned by SHOW statement_timeout. */
export function parsePostgresDurationMs(setting: string): number | null {
  const match = /^(\d+(?:\.\d+)?)\s*(ms|s|min|h|d)?$/i.exec(setting.trim());
  if (!match) return null;
  const value = Number(match[1]);
  const multiplier = POSTGRES_DURATION_MULTIPLIERS[(match[2] ?? "ms").toLowerCase()];
  if (!Number.isFinite(value) || multiplier === undefined) return null;
  const milliseconds = value * multiplier;
  return Number.isFinite(milliseconds) ? milliseconds : null;
}

export function statementTimeoutIsBound(setting: string, configuredBoundMs: number): boolean {
  const observedMs = parsePostgresDurationMs(setting);
  return observedMs !== null && observedMs > 0 && observedMs <= configuredBoundMs;
}

export type StatementBoundStatus = {
  bounded: boolean | null;
  observedMs: number | null;
};

export async function observeRoleDatabaseStatementBound(args: {
  connectionString: string;
  configuredBoundMs: number;
  logger: Logger;
  metrics: Metrics;
  readDefault?: (connectionString: string) => Promise<string>;
}): Promise<StatementBoundStatus> {
  let bounded = false;
  let observedMs: number | null = null;
  let observedSetting: string | null = null;
  let checkFailed = false;
  try {
    observedSetting = await (args.readDefault ?? readStatementTimeoutDefault)(args.connectionString);
    observedMs = parsePostgresDurationMs(observedSetting);
    bounded = statementTimeoutIsBound(observedSetting, args.configuredBoundMs);
  } catch {
    checkFailed = true;
  }

  args.metrics.set("database_statement_bound", bounded ? 1 : 0);
  if (!bounded) {
    args.logger.event(
      checkFailed ? "database_statement_bound_check_failed" : "database_statement_bound_out_of_range",
      {
        area: "database",
        severity: "error",
        configuredBoundMs: args.configuredBoundMs,
        ...(observedSetting === null ? {} : { observedSetting }),
      },
    );
  }
  return { bounded: checkFailed ? null : bounded, observedMs };
}
