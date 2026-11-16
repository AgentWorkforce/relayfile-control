import assert from "node:assert/strict";
import net, { type AddressInfo, type Socket } from "node:net";
import { describe, it } from "node:test";
import pg from "pg";
import { pgDb } from "../src/db/db.ts";

const PG_URL = process.env.RELAYFILE_CONTROL_PG_TEST_URL;
const opts = { skip: PG_URL ? false : "RELAYFILE_CONTROL_PG_TEST_URL not set" };
const PROTOCOL_V3 = 196_608;

function stripStartupParameters(message: Buffer): Buffer {
  assert.equal(message.readInt32BE(4), PROTOCOL_V3, "expected a PostgreSQL v3 StartupMessage");
  const values = new Map<string, string>();
  let offset = 8;
  while (offset < message.length - 1) {
    const keyEnd = message.indexOf(0, offset);
    assert.notEqual(keyEnd, -1, "startup key is NUL terminated");
    if (keyEnd === offset) break;
    const valueEnd = message.indexOf(0, keyEnd + 1);
    assert.notEqual(valueEnd, -1, "startup value is NUL terminated");
    values.set(
      message.toString("utf8", offset, keyEnd),
      message.toString("utf8", keyEnd + 1, valueEnd),
    );
    offset = valueEnd + 1;
  }
  const user = values.get("user");
  const database = values.get("database");
  assert.ok(user, "StartupMessage includes user");
  assert.ok(database, "StartupMessage includes database");
  const fields = Buffer.from(`user\0${user}\0database\0${database}\0\0`);
  const rewritten = Buffer.alloc(8 + fields.length);
  rewritten.writeInt32BE(rewritten.length, 0);
  rewritten.writeInt32BE(PROTOCOL_V3, 4);
  fields.copy(rewritten, 8);
  return rewritten;
}

async function startupStrippingProxy(targetUrl: string): Promise<{
  url: string;
  close(): Promise<void>;
}> {
  const target = new URL(targetUrl);
  const sockets = new Set<Socket>();
  const server = net.createServer((client) => {
    sockets.add(client);
    client.on("close", () => sockets.delete(client));
    client.pause();
    const upstream = net.createConnection({
      host: target.hostname,
      port: Number(target.port || 5432),
    });
    sockets.add(upstream);
    upstream.on("close", () => sockets.delete(upstream));
    client.on("error", () => upstream.destroy());
    upstream.on("error", () => client.destroy());
    upstream.pipe(client);

    let startup = Buffer.alloc(0);
    const onData = (chunk: Buffer) => {
      startup = Buffer.concat([startup, chunk]);
      if (startup.length < 4) return;
      const length = startup.readInt32BE(0);
      if (startup.length < length) return;
      try {
        upstream.write(stripStartupParameters(startup.subarray(0, length)));
        if (startup.length > length) upstream.write(startup.subarray(length));
        client.off("data", onData);
        client.pipe(upstream);
      } catch {
        client.destroy();
        upstream.destroy();
      }
    };
    upstream.once("connect", () => {
      client.on("data", onData);
      client.resume();
    });
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve());
  });
  const { port } = server.address() as AddressInfo;
  const proxied = new URL(targetUrl);
  proxied.hostname = "127.0.0.1";
  proxied.port = String(port);
  return {
    url: proxied.toString(),
    close: async () => {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    },
  };
}

function isQueryCancelled(error: unknown): boolean {
  return typeof error === "object" && error !== null && "code" in error && error.code === "57014";
}

describe("PostgreSQL startup parameters are not a timeout guarantee", opts, () => {
  it("sets and enforces the timeout after a proxy strips the StartupMessage", async () => {
    const proxy = await startupStrippingProxy(PG_URL!);
    const startupOnly = new pg.Pool({
      connectionString: proxy.url,
      max: 1,
      statement_timeout: 50,
    });
    try {
      const stripped = await startupOnly.query<{ statement_timeout: string }>("SHOW statement_timeout");
      assert.deepEqual(stripped.rows, [{ statement_timeout: "0" }]);
    } finally {
      await startupOnly.end();
    }

    const db = pgDb(proxy.url, { max: 1, statementTimeoutMs: 50 });
    try {
      assert.deepEqual(
        (await db.query<{ statement_timeout: string }>("SHOW statement_timeout")).rows,
        [{ statement_timeout: "50ms" }],
      );
      const unrelated = pgDb(proxy.url, { max: 1 });
      try {
        assert.deepEqual(
          (await unrelated.query<{ statement_timeout: string }>("SHOW statement_timeout")).rows,
          [{ statement_timeout: "0" }],
          "the session setting does not leak into a second pool",
        );
      } finally {
        await unrelated.close();
      }
      await assert.rejects(db.query("SELECT pg_sleep(0.25)"), isQueryCancelled);

      // Prove checkout does not repeat the session SET, while tx() still gets
      // its own bound even when a transaction pooler swaps server sessions.
      await db.query("SET statement_timeout = 0");
      await db.tx(async (tx) => {
        assert.deepEqual(
          (await tx.query<{ statement_timeout: string }>("SHOW statement_timeout")).rows,
          [{ statement_timeout: "50ms" }],
        );
        await assert.rejects(tx.query("SELECT pg_sleep(0.25)"), isQueryCancelled);
      });
      assert.deepEqual(
        (await db.query<{ statement_timeout: string }>("SHOW statement_timeout")).rows,
        [{ statement_timeout: "0" }],
      );
    } finally {
      await db.close();
      await proxy.close();
    }
  });
  it("rejects, rather than crashing the process, when a checked-out connection drops", async () => {
    const proxy = await startupStrippingProxy(PG_URL!);
    const db = pgDb(proxy.url, { max: 1, statementTimeoutMs: 10_000 });
    try {
      await db.query("SELECT 1");
      const running = db.query("SELECT pg_sleep(5)");
      await new Promise((resolve) => setTimeout(resolve, 200));
      await proxy.close();
      await assert.rejects(running);
      await assert.rejects(db.exec("SELECT 1"));
    } finally {
      await db.close();
    }
  });
});
