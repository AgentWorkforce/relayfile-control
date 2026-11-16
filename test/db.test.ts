import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  assertSessionPoolMode,
  pgDb,
  setSessionStatementTimeout,
  setTransactionStatementTimeout,
} from "../src/db/db.ts";

describe("database session settings", () => {
  it("uses an in-session set_config instead of a startup option", async () => {
    const calls: Array<{ sql: string; params: unknown[] | undefined }> = [];
    await setSessionStatementTimeout(
      {
        query: async (sql: string, params?: unknown[]) => {
          calls.push({ sql, params });
          return { rows: [], rowCount: 1 } as never;
        },
      },
      15_000,
    );
    assert.deepEqual(calls, [
      {
        sql: "SELECT set_config($1, $2, false)",
        params: ["statement_timeout", "15000ms"],
      },
    ]);
  });

  it("uses SET LOCAL semantics inside a transaction", async () => {
    const calls: Array<{ sql: string; params: unknown[] | undefined }> = [];
    await setTransactionStatementTimeout(
      {
        query: async (sql: string, params?: unknown[]) => {
          calls.push({ sql, params });
          return { rows: [], rowCount: 1 } as never;
        },
      },
      15_000,
    );
    assert.deepEqual(calls, [
      {
        sql: "SELECT set_config($1, $2, true)",
        params: ["statement_timeout", "15000ms"],
      },
    ]);
  });

  it("does not issue a session statement when the timeout is disabled", async () => {
    let queried = false;
    await setSessionStatementTimeout(
      {
        query: async () => {
          queried = true;
          return { rows: [], rowCount: 1 } as never;
        },
      },
      undefined,
    );
    assert.equal(queried, false);
  });

  it("refuses transaction-mode pooler URLs before a connection is opened", () => {
    assert.throws(
      () => pgDb("postgres://user:secret@aws-1-us-east-1.pooler.supabase.com:6543/db"),
      /transaction-mode PostgreSQL poolers are unsupported/,
    );
    assert.throws(
      () => pgDb("postgres://user:secret@aws-1-us-west-2.pooler.supabase.com.:6543/db"),
      /transaction-mode PostgreSQL poolers are unsupported/,
    );
    assert.throws(
      () => pgDb("postgres://user:secret@aws-1-us-west-2.pooler.supabase.com:5432/db?port=6543"),
      /transaction-mode PostgreSQL poolers are unsupported/,
    );
    assert.throws(
      () => pgDb("postgres://user:secret@localhost:6543/db?host=aws-1-us-west-2.pooler.supabase.com"),
      /transaction-mode PostgreSQL poolers are unsupported/,
    );
    for (const url of [
      "postgres://user:secret@aws-1-us-west-2.pooler.supabase.com:5432/db?port=5432&port=6543",
      "postgres://user:secret@aws-1-us-west-2.pooler.supabase.com:5432/db?PORT=5432&port=6543",
    ]) {
      assert.throws(() => pgDb(url), /transaction-mode PostgreSQL poolers are unsupported/, url);
    }
    assert.throws(
      () => pgDb("postgres://user:secret@db.example.com:5432/db?PgBouncer=TRUE"),
      /transaction-mode PostgreSQL poolers are unsupported/,
    );
  });

  it("accepts direct and Supabase session-mode URLs", () => {
    assert.doesNotThrow(() => assertSessionPoolMode("postgres://user:secret@db.example.com:5432/db"));
    assert.doesNotThrow(() =>
      assertSessionPoolMode(
        "postgres://user:secret@aws-1-us-west-2.pooler.supabase.com:5432/db?sslmode=require",
      ),
    );
    assert.doesNotThrow(() =>
      assertSessionPoolMode("postgres://user:secret@db.example.com:5432/db?pgbouncer=false"),
    );
  });
});
