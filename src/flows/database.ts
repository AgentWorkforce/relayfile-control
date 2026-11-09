import type { Db } from "../db/db.ts";

/**
 * The query surface the Flows store was written against. `flowsDatabase`
 * adapts this service's `Db` to it so the ported store stays line-for-line
 * comparable with its origin.
 */
export type SqlResult<T> = {
  rows: T[];
};

export interface SqlExecutor {
  query<T extends Record<string, unknown> = Record<string, unknown>>(
    sql: string,
    params?: unknown[],
  ): Promise<SqlResult<T>>;
}

export interface Database extends SqlExecutor {
  transaction<T>(run: (transaction: SqlExecutor) => Promise<T>): Promise<T>;
}

export function flowsDatabase(db: Db): Database {
  return {
    async query<T extends Record<string, unknown>>(sql: string, params: unknown[] = []) {
      const result = await db.query<T>(sql, params);
      return { rows: result.rows };
    },
    async transaction<T>(run: (transaction: SqlExecutor) => Promise<T>): Promise<T> {
      return db.tx((tx) => run({
        async query<R extends Record<string, unknown>>(sql: string, params: unknown[] = []) {
          const result = await tx.query<R>(sql, params);
          return { rows: result.rows };
        },
      }));
    },
  };
}
