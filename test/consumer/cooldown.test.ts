// Ported from cloud packages/webhook-worker/test/workspace-cooldown.test.ts @ bda4057 (cloud#6177, 12 tests).
// Test names are Cloud's. Deliberate differences (plan §6.3), marked PORT where they change an assertion:
//  - key = the envelope's lane (workspace known here), not providerConfigKey + connection;
//  - arming input = the final typed RelayfileBackpressureError (Cloud: cloud-web's 503 +
//    code relayfile_backpressure, which is that error translated over HTTP);
//  - store = the shared `lane_cooldowns` table (Postgres clock); the test clock is moved by
//    shifting `until`, and the LRU bound prunes by oldest `until` (= least recently armed for
//    equal windows).
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import type { Db } from "../../src/db/db.ts";
import {
  COOLDOWN_DEFAULT_WINDOW_SECONDS,
  COOLDOWN_MAX_KEYS,
  COOLDOWN_MAX_WINDOW_SECONDS,
  CooldownStore,
  backpressureCooldownSeconds,
  cooldownKeyForEnvelope,
} from "../../src/queue/cooldown.ts";
import { fetchWithRelayfileBackpressure, RelayfileBackpressureError, RelayfileServerError } from "../../src/relayfile/backpressure.ts";
import { HandlerTimeoutError } from "../../src/consumer/consumer.ts";
import { RelayfileUnexpectedStatusError } from "../../src/relayfile/client.ts";
import { newPglite } from "../support/harness.ts";

/** The error a Relayfile response turns into after the in-place policy (0 retries). */
async function errorFor(status: number, headers: Record<string, string>, body: string): Promise<unknown> {
  const res = () => new Response(body, { status, headers: { "content-type": "application/json", ...headers } });
  try {
    const r = await fetchWithRelayfileBackpressure({ post: async () => res(), surface: "webhook-envelopes", maxRetries: 0, budgetMs: 0, now: () => Date.parse("2026-10-21T07:27:15Z") });
    return new RelayfileUnexpectedStatusError(r.status, "webhook-envelopes", null);
  } catch (e) {
    return e;
  }
}
const busyBody = (extra: Record<string, unknown> = {}) => JSON.stringify({ code: "workspace_busy", ...extra });

describe("cooldownKeyForMessage", () => {
  it("keys forward and sync messages by provider config key + connection, shared across both", () => {
    // PORT: keyed by lane; forward and sync rows for the same workspace share it.
    const forward = cooldownKeyForEnvelope({ lane: "rw_1", workspace_id: "rw_1" });
    const sync = cooldownKeyForEnvelope({ lane: "rw_1", workspace_id: "rw_1" });
    assert.equal(forward, "rw_1");
    assert.equal(sync, forward);
  });

  it("distinguishes connections and provider config keys", () => {
    // PORT: distinct workspaces (or unresolved connections) get distinct keys.
    const key = cooldownKeyForEnvelope({ lane: "rw_1", workspace_id: "rw_1" });
    assert.notEqual(cooldownKeyForEnvelope({ lane: "rw_2", workspace_id: "rw_2" }), key);
    assert.notEqual(cooldownKeyForEnvelope({ lane: "unresolved:notion:conn-2", workspace_id: null }), key);
    assert.equal(cooldownKeyForEnvelope({ lane: "unresolved:notion:conn-1", workspace_id: null }), "unresolved:notion:conn-1");
  });

  it("returns null (no cool-down) without a connection, for github-raw and for other ingresses", async () => {
    // PORT: every envelope has a lane, so there is always a key; what Cloud expressed with a null key
    // (messages whose route never answers backpressure) is expressed by `arm` ignoring a null key and
    // by non-backpressure failures never arming.
    const db = await newPglite();
    const store = new CooldownStore(db, { enabled: true, maxKeys: 10 });
    assert.equal(await store.arm(null, 60), null);
    assert.equal(await store.remaining(null), 0);
    assert.equal(await store.size(), 0);
    await db.close();
  });
});

describe("backpressureCooldownSeconds", () => {
  it("arms only for 503 + code relayfile_backpressure", async () => {
    // PORT: arms only for the final typed backpressure error (a final 429 here).
    assert.equal(backpressureCooldownSeconds(await errorFor(429, { "retry-after": "120" }, busyBody({ retryAfterSeconds: 120 }))), 120);
    assert.equal(backpressureCooldownSeconds(await errorFor(502, { "retry-after": "120" }, busyBody())), null);
    assert.equal(backpressureCooldownSeconds(await errorFor(503, { "retry-after": "120" }, busyBody())), null, "503 without RELAYFILE_BACKPRESSURE_RETRY_503");
    assert.equal(backpressureCooldownSeconds(new RelayfileServerError({ status: 500, attempts: 2, surface: "webhook-envelopes" })), null);
    assert.equal(backpressureCooldownSeconds(new HandlerTimeoutError(15_000)), null);
    assert.equal(backpressureCooldownSeconds(new TypeError("fetch failed")), null);
    assert.equal(backpressureCooldownSeconds(null), null);
  });

  it("prefers the Retry-After header, then the body, then 60 s", async () => {
    assert.equal(backpressureCooldownSeconds(await errorFor(429, { "retry-after": "30" }, busyBody({ retryAfterSeconds: 90 }))), 30);
    assert.equal(backpressureCooldownSeconds(await errorFor(429, {}, busyBody({ retryAfterSeconds: 90 }))), 90);
    assert.equal(backpressureCooldownSeconds(await errorFor(429, { "retry-after": "Wed, 21 Oct 2026 07:28:00 GMT" }, busyBody({ retryAfterSeconds: 45.2 }))), 45);
    assert.equal(backpressureCooldownSeconds(await errorFor(429, { "retry-after": "0" }, busyBody())), COOLDOWN_DEFAULT_WINDOW_SECONDS);
    assert.equal(backpressureCooldownSeconds(await errorFor(429, {}, busyBody({ retryAfterSeconds: "90" }))), 60);
    // PORT: Cloud parses delta-seconds only, so its HTTP-date case falls through to the body (46 s);
    // this module also honours an IMF-fixdate header (45 s to 07:28:00), still header-first.
  });

  it("honours a long Retry-After in full and clamps the window at the Cloudflare 24 h delay limit", async () => {
    assert.equal(backpressureCooldownSeconds(await errorFor(429, { "retry-after": "3600" }, busyBody())), 3600);
    assert.equal(backpressureCooldownSeconds(await errorFor(429, { "retry-after": "172800" }, busyBody())), COOLDOWN_MAX_WINDOW_SECONDS);
    assert.equal(COOLDOWN_MAX_WINDOW_SECONDS, 86_400);
    assert.ok(backpressureCooldownSeconds(new RelayfileBackpressureError({ status: 429, retryAfterMs: 1, attempts: 1, surface: "provider-writes" }))! >= 1);
  });
});

describe("WorkspaceCooldownStore", () => {
  async function storeAt(maxKeys = COOLDOWN_MAX_KEYS) {
    const db = await newPglite();
    return { db, store: new CooldownStore(db, { enabled: true, maxKeys }) };
  }
  /** Advance the store's clock by moving every window back (PG now() is the clock). */
  const advance = (db: Db, ms: number) => db.query("UPDATE lane_cooldowns SET until = until - make_interval(secs => $1)", [ms / 1000]);

  it("arms a key and reports the remaining window", async () => {
    const { db, store } = await storeAt();
    assert.equal(await store.remaining("k"), 0);
    assert.equal(await store.arm("k", 60), 60);
    await advance(db, 15_000);
    assert.equal(await store.remaining("k"), 45);
    assert.equal(await store.remaining("other"), 0);
    await db.close();
  });

  it("a longer window extends, a shorter one never shortens", async () => {
    const { db, store } = await storeAt();
    await store.arm("k", 60);
    await advance(db, 10_000);
    await store.arm("k", 120);
    assert.equal(await store.remaining("k"), 120);
    await store.arm("k", 5);
    assert.equal(await store.remaining("k"), 120);
    await db.close();
  });

  it("expires on read", async () => {
    const { db, store } = await storeAt();
    await store.arm("k", 60);
    await advance(db, 60_000);
    assert.equal(await store.remaining("k"), 0);
    assert.equal(await store.size(), 0);
    await db.close();
  });

  it("ignores a null key and non-positive windows", async () => {
    const { db, store } = await storeAt();
    assert.equal(await store.arm(null, 60), null);
    assert.equal(await store.arm("k", 0), null);
    assert.equal(await store.arm("k", Number.NaN), null);
    assert.equal(await store.remaining(null), 0);
    assert.equal(await store.size(), 0);
    await db.close();
  });

  it("caps the map: evicts expired entries first, then the least recently armed", async () => {
    const { db, store } = await storeAt(3);
    await store.arm("expired", 1);
    await store.arm("a", 600);
    await store.arm("b", 600);
    await advance(db, 2_000);
    await store.arm("c", 600);
    assert.equal(await store.size(), 3);
    assert.ok((await store.remaining("a")) > 0);
    assert.equal(await store.remaining("expired"), 0);

    await store.arm("a", 600); // re-arm moves "a" to most recent
    await store.arm("d", 600);
    assert.equal(await store.size(), 3);
    assert.equal(await store.remaining("b"), 0);
    assert.ok((await store.remaining("a")) > 0);
    assert.ok((await store.remaining("c")) > 0);
    assert.ok((await store.remaining("d")) > 0);
    await db.close();
  });

  it("never exceeds the default 1000-key cap", async () => {
    const { db, store } = await storeAt();
    // PORT: 1 500 single-row arms through PGlite are slow; arm in bulk then prune through the store.
    await db.query(
      "INSERT INTO lane_cooldowns (lane, until) SELECT 'k' || g, now() + make_interval(secs => 600 + g) FROM generate_series(0, 1498) g",
    );
    await store.arm("k1499", 3000);
    assert.equal(await store.size(), 1_000);
    assert.ok((await store.remaining("k1499")) > 0);
    assert.equal(await store.remaining("k0"), 0);
    await db.close();
  });
});
