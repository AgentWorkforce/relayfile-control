import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { loadConfig } from "../src/config.ts";
import {
  readStatementTimeoutDefault,
  statementTimeoutIsBound,
  transactionPoolHint,
  verifyLikelyTransactionPoolBound,
} from "../src/db/statement-bound.ts";
import { MemoryLogger } from "../src/observability/log.ts";
import { Metrics } from "../src/observability/metrics.ts";

describe("database statement bound startup check", () => {
  it("detects likely transaction poolers without flagging the production session URL shape", () => {
    assert.equal(transactionPoolHint("postgres://u:p@db.example.com:6543/db"), "port_6543");
    assert.equal(
      transactionPoolHint("postgres://u:p@aws-1-us-west-2.pooler.supabase.com.:6543/db"),
      "supabase_6543",
    );
    assert.equal(
      transactionPoolHint("postgres://u:p@db.example.com:5432/db?PgBouncer=TRUE"),
      "pgbouncer",
    );
    assert.equal(
      transactionPoolHint(
        "postgres://u:p@aws-1-us-west-2.pooler.supabase.com:5432/db?sslmode=require",
      ),
      null,
    );
  });

  it("keeps fail-closed enforcement default-off", () => {
    assert.equal(loadConfig({}).DATABASE_REQUIRE_STATEMENT_BOUND, false);
    assert.equal(
      loadConfig({ DATABASE_REQUIRE_STATEMENT_BOUND: "true" }).DATABASE_REQUIRE_STATEMENT_BOUND,
      true,
    );
  });

  it("logs loudly and exports zero without refusing by default", async () => {
    const logger = new MemoryLogger();
    const metrics = new Metrics();
    assert.deepEqual(
      await verifyLikelyTransactionPoolBound({
        connectionString: "postgres://u:p@db.example.com:6543/db",
        required: false,
        logger,
        metrics,
        readDefault: async () => "0",
      }),
      { checked: true, hint: "port_6543", bounded: false },
    );
    assert.equal(metrics.gauge("database_statement_bound", { pool_hint: "port_6543" }), 0);
    assert.deepEqual(logger.named("database_statement_bound_missing"), [
      {
        event: "database_statement_bound_missing",
        area: "database",
        severity: "error",
        poolHint: "port_6543",
        required: false,
      },
    ]);
  });

  it("fails closed only when the flag is enabled", async () => {
    await assert.rejects(
      verifyLikelyTransactionPoolBound({
        connectionString: "postgres://u:p@db.example.com:5432/db?pgbouncer=true",
        required: true,
        logger: new MemoryLogger(),
        metrics: new Metrics(),
        readDefault: async () => "0",
      }),
      /database statement timeout default is unbounded/,
    );
  });

  it("reports a verified role/database default and skips ordinary session URLs", async () => {
    const metrics = new Metrics();
    let reads = 0;
    const readDefault = async () => {
      reads += 1;
      return "30s";
    };
    assert.deepEqual(
      await verifyLikelyTransactionPoolBound({
        connectionString: "postgres://u:p@db.example.com:6543/db",
        required: true,
        logger: new MemoryLogger(),
        metrics,
        readDefault,
      }),
      { checked: true, hint: "port_6543", bounded: true },
    );
    assert.equal(metrics.gauge("database_statement_bound", { pool_hint: "port_6543" }), 1);
    assert.deepEqual(
      await verifyLikelyTransactionPoolBound({
        connectionString: "postgres://u:p@db.example.com:5432/db",
        required: true,
        logger: new MemoryLogger(),
        metrics,
        readDefault,
      }),
      { checked: false, hint: null, bounded: null },
    );
    assert.equal(reads, 1);
  });

  it("treats an unverifiable default as unbounded for metrics and optional enforcement", async () => {
    const logger = new MemoryLogger();
    const metrics = new Metrics();
    assert.deepEqual(
      await verifyLikelyTransactionPoolBound({
        connectionString: "postgres://u:p@db.example.com:6543/db",
        required: false,
        logger,
        metrics,
        readDefault: async () => {
          throw new Error("fixture outage");
        },
      }),
      { checked: true, hint: "port_6543", bounded: null },
    );
    assert.equal(metrics.gauge("database_statement_bound", { pool_hint: "port_6543" }), 0);
    assert.equal(logger.named("database_statement_bound_check_failed").length, 1);
    await assert.rejects(
      verifyLikelyTransactionPoolBound({
        connectionString: "postgres://u:p@db.example.com:6543/db",
        required: true,
        logger: new MemoryLogger(),
        metrics: new Metrics(),
        readDefault: async () => {
          throw new Error("fixture outage");
        },
      }),
      /database statement timeout bound could not be verified/,
    );
  });

  it("recognizes Postgres's zero and non-zero SHOW formats", () => {
    assert.equal(statementTimeoutIsBound("0"), false);
    assert.equal(statementTimeoutIsBound("30s"), true);
    assert.equal(statementTimeoutIsBound("15000ms"), true);
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
