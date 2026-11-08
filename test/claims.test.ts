import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { claim, completeClaim, failClaim } from "../src/queue/claims.ts";
import { newPglite } from "./support/harness.ts";

describe("handler claims state machine (§3, PGlite)", () => {
  it("claim → duplicate while processing → completed is duplicate forever", async () => {
    const db = await newPglite();
    const c = { surface: "nango-forward", dedupeId: "r1", leaseSeconds: 900 };
    assert.deepEqual(await claim(db, c), { claimed: true, attempts: 1 });
    assert.deepEqual(await claim(db, c), { claimed: false });
    await completeClaim(db, c.surface, c.dedupeId);
    assert.deepEqual(await claim(db, c), { claimed: false });
    // Even after the original lease would have expired, a completed claim stays a duplicate.
    await db.query("UPDATE webhook_delivery_claims SET created_at = now() - interval '1 day'");
    assert.deepEqual(await claim(db, c), { claimed: false });
    await db.close();
  });
  it("failed → reclaim (release on handler failure)", async () => {
    const db = await newPglite();
    const c = { surface: "slack", dedupeId: "e1", leaseSeconds: 900 };
    await claim(db, c);
    await failClaim(db, c.surface, c.dedupeId);
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
  it("Google-mail TTL coalescing: completed younger than window is a duplicate, older is reclaimable", async () => {
    const db = await newPglite();
    const c = { surface: "google-mail-forward-sync", dedupeId: "u@x", leaseSeconds: 900, coalesceSeconds: 60 };
    await claim(db, c);
    await completeClaim(db, c.surface, c.dedupeId);
    assert.deepEqual(await claim(db, c), { claimed: false });
    await db.query("UPDATE webhook_delivery_claims SET completed_at = now() - interval '61 seconds'");
    assert.deepEqual(await claim(db, c), { claimed: true, attempts: 2 });
    await db.close();
  });
  it("surfaces are independent", async () => {
    const db = await newPglite();
    assert.equal((await claim(db, { surface: "a", dedupeId: "x", leaseSeconds: 1 })).claimed, true);
    assert.equal((await claim(db, { surface: "b", dedupeId: "x", leaseSeconds: 1 })).claimed, true);
    await db.close();
  });
});
