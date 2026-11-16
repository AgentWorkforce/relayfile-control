import pg from "pg";
import { parse as parseConnectionString } from "pg-connection-string";

export type QueryResult<T> = { rows: T[]; rowCount: number };

/** The query surface shared by a pool, a transaction, and PGlite. */
export interface Queryable {
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;
}

/** A transaction handle: queries plus multi-statement scripts (migrations). */
export interface Tx extends Queryable {
  exec(sql: string): Promise<void>;
}

export interface Db extends Queryable {
  /** Run `fn` in one transaction. Rolls back on throw. */
  tx<T>(fn: (q: Tx) => Promise<T>): Promise<T>;
  /** Execute a multi-statement script (migrations). */
  exec(sql: string): Promise<void>;
  close(): Promise<void>;
}

type SessionClient = { query(sql: string, params?: unknown[]): Promise<unknown> };

async function setStatementTimeout(
  client: SessionClient,
  statementTimeoutMs: number | undefined,
  local: boolean,
): Promise<void> {
  if (!statementTimeoutMs) return;
  await client.query(`SELECT set_config($1, $2, ${local ? "true" : "false"})`, [
    "statement_timeout",
    `${statementTimeoutMs}ms`,
  ]);
}

/** Apply the server-side bound for the lifetime of this database session. */
export async function setSessionStatementTimeout(
  client: { query(sql: string, params?: unknown[]): Promise<unknown> },
  statementTimeoutMs: number | undefined,
): Promise<void> {
  await setStatementTimeout(client, statementTimeoutMs, false);
}

/** Apply the server-side bound locally to the current transaction. */
export async function setTransactionStatementTimeout(
  client: SessionClient,
  statementTimeoutMs: number | undefined,
): Promise<void> {
  await setStatementTimeout(client, statementTimeoutMs, true);
}

/**
 * Session-scoped settings are unsafe through transaction poolers: the setting
 * can miss the next query and leak to another client that gets the backend.
 */
export function assertSessionPoolMode(connectionString: string): void {
  let url: URL;
  try {
    url = new URL(connectionString);
  } catch {
    // Leave malformed or non-URL connection strings to pg's own validation.
    return;
  }
  // Resolve host and port with the parser pg itself uses, so query-string
  // overrides (including duplicates) are read exactly as the driver will.
  let parsed: ReturnType<typeof parseConnectionString>;
  try {
    parsed = parseConnectionString(connectionString);
  } catch {
    return;
  }
  const hostname = String(parsed.host ?? "").trim().toLowerCase().replace(/\.+$/, "");
  const port = String(parsed.port ?? "").trim();
  const supabaseTransactionPort =
    (hostname === "pooler.supabase.com" || hostname.endsWith(".pooler.supabase.com")) &&
    port === "6543";
  // pg ignores `pgbouncer`; it is an operator's declaration, so match any case.
  const pgbouncerValue = [...url.searchParams.entries()].find(
    ([key]) => key.toLowerCase() === "pgbouncer",
  )?.[1];
  const explicitTransactionMode =
    pgbouncerValue !== undefined &&
    ["1", "true", "yes", "on"].includes(pgbouncerValue.trim().toLowerCase());
  if (supabaseTransactionPort || explicitTransactionMode) {
    throw new Error(
      "transaction-mode PostgreSQL poolers are unsupported; use a session-mode URL",
    );
  }
}

export function pgDb(connectionString: string, options: { max?: number; statementTimeoutMs?: number } = {}): Db {
  assertSessionPoolMode(connectionString);
  const pool = new pg.Pool({
    connectionString,
    max: options.max ?? 20,
  });
  // An idle client error (e.g. the server restarted) must not crash the process.
  pool.on("error", () => undefined);
  // Nor may one on a checked-out client: pg-pool only listens while a client is
  // idle (or inside pool.query). The failed query still rejects, and the pool
  // discards the client on release because it is no longer queryable.
  pool.on("connect", (client) => {
    client.on("error", () => undefined);
  });
  const configuredClients = new WeakSet<pg.PoolClient>();
  // Supavisor session mode drops libpq startup options. Apply the bound once to
  // each physical client. A transaction-mode pool can swap its server session,
  // so plain query()/exec() there also require an ALTER ROLE default; tx() sets
  // the bound locally after BEGIN and does not rely on session affinity.
  const checkout = async (): Promise<pg.PoolClient> => {
    const client = await pool.connect();
    if (configuredClients.has(client)) return client;
    try {
      await setSessionStatementTimeout(client, options.statementTimeoutMs);
      configuredClients.add(client);
      return client;
    } catch (error) {
      client.release(true);
      throw error;
    }
  };
  return {
    async query<T>(sql: string, params?: unknown[]) {
      const client = await checkout();
      try {
        const result = await client.query(sql, params as unknown[]);
        return { rows: result.rows as T[], rowCount: result.rowCount ?? 0 };
      } finally {
        client.release();
      }
    },
    async tx<T>(fn: (q: Tx) => Promise<T>): Promise<T> {
      const client = await checkout();
      let broken = false;
      try {
        await client.query("BEGIN");
        await setTransactionStatementTimeout(client, options.statementTimeoutMs);
        const q: Tx = {
          async query<R>(sql: string, params?: unknown[]) {
            const result = await client.query(sql, params as unknown[]);
            return { rows: result.rows as R[], rowCount: result.rowCount ?? 0 };
          },
          async exec(sql: string) {
            await client.query(sql);
          },
        };
        const value = await fn(q);
        await client.query("COMMIT");
        return value;
      } catch (error) {
        // A connection whose ROLLBACK failed may still be inside a transaction:
        // destroy it rather than return it to the pool.
        broken = await client.query("ROLLBACK").then(() => false, () => true);
        throw error;
      } finally {
        client.release(broken);
      }
    },
    async exec(sql: string) {
      const client = await checkout();
      try {
        await client.query(sql);
      } finally {
        client.release();
      }
    },
    async close() {
      await pool.end();
    },
  };
}

type PGliteTx = {
  query<R>(sql: string, params?: unknown[]): Promise<{ rows: R[]; affectedRows?: number }>;
  exec(sql: string): Promise<unknown>;
};

/** Minimal structural type for PGlite so the runtime import stays test-only. */
export interface PGliteLike {
  query<T>(sql: string, params?: unknown[]): Promise<{ rows: T[]; affectedRows?: number }>;
  exec(sql: string): Promise<unknown>;
  transaction<T>(fn: (tx: PGliteTx) => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

/**
 * PGlite is single-connection: transactions are serialised by a local mutex so
 * concurrent callers (consumer slots in unit tests) never interleave inside one.
 */
export function pgliteDb(db: PGliteLike): Db {
  let chain: Promise<unknown> = Promise.resolve();
  const serial = <T>(fn: () => Promise<T>): Promise<T> => {
    const next = chain.then(fn, fn);
    chain = next.catch(() => undefined);
    return next;
  };
  const wrap = (q: PGliteTx): Tx => ({
    async query<R>(sql: string, params?: unknown[]) {
      const result = await q.query<R>(sql, params);
      return { rows: result.rows, rowCount: result.affectedRows ?? result.rows.length };
    },
    async exec(sql: string) {
      await q.exec(sql);
    },
  });
  return {
    query: <T>(sql: string, params?: unknown[]) => serial(() => wrap(db).query<T>(sql, params)),
    tx: <T>(fn: (q: Tx) => Promise<T>) => serial(() => db.transaction((tx) => fn(wrap(tx)))),
    exec: (sql: string) => serial(async () => { await db.exec(sql); }),
    close: () => db.close(),
  };
}
