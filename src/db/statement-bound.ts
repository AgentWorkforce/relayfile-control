import pg from "pg";
import type { Logger } from "../observability/log.ts";
import type { Metrics } from "../observability/metrics.ts";

export type TransactionPoolHint = "supabase_6543" | "pgbouncer" | "port_6543";

/** Return a bounded-cardinality reason when the URL likely uses transaction pooling. */
export function transactionPoolHint(connectionString: string): TransactionPoolHint | null {
  let url: URL;
  try {
    url = new URL(connectionString);
  } catch {
    return null;
  }
  const hostname = url.hostname.toLowerCase().replace(/\.+$/, "");
  if (
    url.port === "6543" &&
    (hostname === "pooler.supabase.com" || hostname.endsWith(".pooler.supabase.com"))
  ) {
    return "supabase_6543";
  }
  const pgbouncerValue = [...url.searchParams.entries()].find(
    ([key]) => key.toLowerCase() === "pgbouncer",
  )?.[1];
  if (
    pgbouncerValue !== undefined &&
    ["1", "true", "yes", "on"].includes(pgbouncerValue.trim().toLowerCase())
  ) {
    return "pgbouncer";
  }
  return url.port === "6543" ? "port_6543" : null;
}

/**
 * Read the role/database default without pgDb's session override. This runs at
 * startup, before the application pool can set or leak a session value.
 */
export async function readStatementTimeoutDefault(connectionString: string): Promise<string> {
  const pool = new pg.Pool({ connectionString, max: 1 });
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

export function statementTimeoutIsBound(setting: string): boolean {
  return setting.trim() !== "0";
}

export type StatementBoundStatus = {
  checked: boolean;
  hint: TransactionPoolHint | null;
  bounded: boolean | null;
};

export async function verifyLikelyTransactionPoolBound(args: {
  connectionString: string;
  required: boolean;
  logger: Logger;
  metrics: Metrics;
  readDefault?: (connectionString: string) => Promise<string>;
}): Promise<StatementBoundStatus> {
  const hint = transactionPoolHint(args.connectionString);
  if (!hint) return { checked: false, hint: null, bounded: null };

  let bounded = false;
  let checkFailed = false;
  try {
    const setting = await (args.readDefault ?? readStatementTimeoutDefault)(args.connectionString);
    bounded = statementTimeoutIsBound(setting);
  } catch {
    checkFailed = true;
  }

  args.metrics.set("database_statement_bound", bounded ? 1 : 0, { pool_hint: hint });
  if (!bounded) {
    args.logger.event(
      checkFailed ? "database_statement_bound_check_failed" : "database_statement_bound_missing",
      {
        area: "database",
        severity: "error",
        poolHint: hint,
        required: args.required,
      },
    );
    if (args.required) {
      throw new Error(
        checkFailed
          ? "database statement timeout bound could not be verified"
          : "database statement timeout default is unbounded",
      );
    }
  }
  return { checked: true, hint, bounded: checkFailed ? null : bounded };
}
