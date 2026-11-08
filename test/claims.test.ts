import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { claim, completeClaim, failClaim } from "../src/queue/claims.ts";
import { newPglite } from "./support/harness.ts";

describe("handler claims state machine (§3, PGlite)", () => {
  it("claim → in-flight while processing → completed is duplicate forever", async () => {
    const db = await newPglite();
    const c = { surface: "nango-forward", dedupeId: "r1", leaseSeconds: 900 };
    assert.deepEqual(await claim(db, c), { claimed: true, attempts: 1 });
    assert.deepEqual(await claim(db, c), { claimed: false, inFlight: true }, "a live lease is in flight, not a duplicate");
    assert.equal(await completeClaim(db, c.surface, c.dedupeId, 1), true);
    assert.deepEqual(await claim(db, c), { claimed: false, inFlight: false });
    // Even with a lease_until in the past, a completed claim stays a duplicate.
    await db.query("UPDATE webhook_delivery_claims SET lease_until = now() - interval '1 second'");
    assert.deepEqual(await claim(db, c), { claimed: false, inFlight: false });
    await db.close();
  });
  it("failed → reclaim (release on handler failure)", async () => {
    const db = await newPglite();
    const c = { surface: "slack", dedupeId: "e1", leaseSeconds: 900 };
    await claim(db, c);
    assert.equal(await failClaim(db, c.surface, c.dedupeId, 1), true);
    assert.deepEqual(await claim(db, c), { claimed: true, attempts: 2 });
    await db.close();
  });
  it("expired lease takeover (crashed worker)", async () => {
    const db = await newPglite();
    const c = { surface: "jira", dedupeId: "j1", leaseSeconds: 900 };
    await claim(db, c);
    await db.query("UPDATE webhook_delivery_claims SET lease_until = now() - interval '1 second'");
    assert.deepEqual(await claim(db, c), { claimed: true, attempts: 2 });
    await db.close();
  });
  it("completion and failure are fenced: a late handler cannot finish the newer claim", async () => {
    const db = await newPglite();
    const c = { surface: "jira", dedupeId: "late", leaseSeconds: 900 };
    await claim(db, c); // attempt 1
    await db.query("UPDATE webhook_delivery_claims SET lease_until = now() - interval '1 second'");
    await claim(db, c); // attempt 2 takes over
    assert.equal(await completeClaim(db, c.surface, c.dedupeId, 1), false, "stale attempt fenced out");
    assert.equal(await failClaim(db, c.surface, c.dedupeId, 1), false);
    const { rows } = await db.query<{ status: string }>("SELECT status FROM webhook_delivery_claims");
    assert.equal(rows[0]!.status, "processing");
    assert.equal(await completeClaim(db, c.surface, c.dedupeId, 2), true);
    await db.close();
  });
  it("Google-mail TTL coalescing: completed younger than window is a duplicate, older is reclaimable", async () => {
    const db = await newPglite();
    const c = { surface: "google-mail-forward-sync", dedupeId: "u@x", leaseSeconds: 900, coalesceSeconds: 60 };
    await claim(db, c);
    await completeClaim(db, c.surface, c.dedupeId, 1);
    assert.deepEqual(await claim(db, c), { claimed: false, inFlight: false });
    await db.query("UPDATE webhook_delivery_claims SET completed_at = now() - interval '61 seconds'");
    assert.deepEqual(await claim(db, c), { claimed: true, attempts: 2 });
    await db.close();
  });
  it("a holder that fails between our upsert and the read-back is in flight (retry), never a duplicate", async () => {
    const db = await newPglite();
    const c = { surface: "nango-forward", dedupeId: "race", leaseSeconds: 900 };
    await claim(db, c); // holder A, live lease
    // B's upsert loses to A's live claim; A then fails before B reads the row back.
    const racing = {
      async query<T>(sql: string, params?: unknown[]) {
        const r = await db.query<T>(sql, params);
        if (sql.includes("INSERT INTO webhook_delivery_claims")) await failClaim(db, c.surface, c.dedupeId, 1);
        return r;
      },
    };
    assert.deepEqual(await claim(racing, c), { claimed: false, inFlight: true });
    // A completed claim swept by retention between the upsert and the read-back reads as a duplicate.
    const d = { surface: "nango-forward", dedupeId: "swept", leaseSeconds: 900 };
    await claim(db, d);
    await completeClaim(db, d.surface, d.dedupeId, 1);
    const sweeping = {
      async query<T>(sql: string, params?: unknown[]) {
        const r = await db.query<T>(sql, params);
        if (sql.includes("INSERT INTO webhook_delivery_claims")) await db.query("DELETE FROM webhook_delivery_claims WHERE dedupe_id = 'swept'");
        return r;
      },
    };
    assert.deepEqual(await claim(sweeping, d), { claimed: false, inFlight: false });
    await db.close();
  });

  it("surfaces are independent", async () => {
    const db = await newPglite();
    assert.equal((await claim(db, { surface: "a", dedupeId: "x", leaseSeconds: 1 })).claimed, true);
    assert.equal((await claim(db, { surface: "b", dedupeId: "x", leaseSeconds: 1 })).claimed, true);
    await db.close();
  });
});
