import pg from "pg";

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

export function pgDb(connectionString: string, options: { max?: number } = {}): Db {
  const pool = new pg.Pool({ connectionString, max: options.max ?? 20 });
  // An idle client error (e.g. the server restarted) must not crash the process.
  pool.on("error", () => undefined);
  return {
    async query<T>(sql: string, params?: unknown[]) {
      const result = await pool.query(sql, params as unknown[]);
      return { rows: result.rows as T[], rowCount: result.rowCount ?? 0 };
    },
    async tx<T>(fn: (q: Tx) => Promise<T>): Promise<T> {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
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
        await client.query("ROLLBACK").catch(() => undefined);
        throw error;
      } finally {
        client.release();
      }
    },
    async exec(sql: string) {
      await pool.query(sql);
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
