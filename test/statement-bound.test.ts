import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  observeRoleDatabaseStatementBound,
  parsePostgresDurationMs,
  readStatementTimeoutDefault,
  statementBoundCheckPoolConfig,
  statementTimeoutIsBound,
} from "../src/db/statement-bound.ts";
import { MemoryLogger } from "../src/observability/log.ts";
import { Metrics } from "../src/observability/metrics.ts";

describe("database role/default statement bound startup check", () => {
  it("parses the PostgreSQL SHOW duration units", () => {
    assert.equal(parsePostgresDurationMs("15000"), 15_000);
    assert.equal(parsePostgresDurationMs("15000ms"), 15_000);
    assert.equal(parsePostgresDurationMs("30s"), 30_000);
    assert.equal(parsePostgresDurationMs("2min"), 120_000);
    assert.equal(parsePostgresDurationMs("1h"), 3_600_000);
    assert.equal(parsePostgresDurationMs("1d"), 86_400_000);
    assert.equal(parsePostgresDurationMs("not-a-duration"), null);
  });

  it("requires a positive default no looser than the configured bound", () => {
    assert.equal(statementTimeoutIsBound("2min", 30_000), false);
    assert.equal(statementTimeoutIsBound("30s", 30_000), true);
    assert.equal(statementTimeoutIsBound("0", 30_000), false);
  });

  it("bounds both startup-check connection and query waits", () => {
    assert.deepEqual(statementBoundCheckPoolConfig("postgres://u:p@db.example.com/db"), {
      connectionString: "postgres://u:p@db.example.com/db",
      max: 1,
      connectionTimeoutMillis: 5_000,
      query_timeout: 5_000,
    });
  });

  it("exports one when the role/database default is within the application bound", async () => {
    const logger = new MemoryLogger();
    const metrics = new Metrics();
    assert.deepEqual(
      await observeRoleDatabaseStatementBound({
        connectionString: "postgres://u:p@db.example.com:5432/db",
        configuredBoundMs: 30_000,
        logger,
        metrics,
        readDefault: async () => "30s",
      }),
      { bounded: true, observedMs: 30_000 },
    );
    assert.equal(metrics.gauge("database_statement_bound"), 1);
    assert.equal(logger.named("database_statement_bound_out_of_range").length, 0);
  });

  it("logs and exports zero for a role/database default looser than the application bound", async () => {
    const logger = new MemoryLogger();
    const metrics = new Metrics();
    assert.deepEqual(
      await observeRoleDatabaseStatementBound({
        connectionString: "postgres://u:p@db.example.com:5432/db",
        configuredBoundMs: 30_000,
        logger,
        metrics,
        readDefault: async () => "2min",
      }),
      { bounded: false, observedMs: 120_000 },
    );
    assert.equal(metrics.gauge("database_statement_bound"), 0);
    assert.deepEqual(logger.named("database_statement_bound_out_of_range"), [
      {
        event: "database_statement_bound_out_of_range",
        area: "database",
        severity: "error",
        configuredBoundMs: 30_000,
        observedSetting: "2min",
      },
    ]);
  });

  it("logs check failure, exports zero, and does not gate startup", async () => {
    const logger = new MemoryLogger();
    const metrics = new Metrics();
    assert.deepEqual(
      await observeRoleDatabaseStatementBound({
        connectionString: "postgres://u:p@db.example.com:5432/db",
        configuredBoundMs: 30_000,
        logger,
        metrics,
        readDefault: async () => {
          throw new Error("fixture timeout");
        },
      }),
      { bounded: null, observedMs: null },
    );
    assert.equal(metrics.gauge("database_statement_bound"), 0);
    assert.deepEqual(logger.named("database_statement_bound_check_failed"), [
      {
        event: "database_statement_bound_check_failed",
        area: "database",
        severity: "error",
        configuredBoundMs: 30_000,
      },
    ]);
  });
});

const PG_URL = process.env.RELAYFILE_CONTROL_PG_TEST_URL;
describe(
  "real Postgres statement timeout default",
  { skip: PG_URL ? false : "RELAYFILE_CONTROL_PG_TEST_URL not set" },
  () => {
    it("reads the fresh role/database default after RESET", async () => {
      assert.equal(await readStatementTimeoutDefault(PG_URL!), "0");
    });
  },
);
