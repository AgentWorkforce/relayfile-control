import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
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

});
