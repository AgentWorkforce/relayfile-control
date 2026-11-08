import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { classify, resolveDeliveryId } from "../src/envelope/classify.ts";
import { allowListHeaders, dedupeKey, relayfileEnvelopeId } from "../src/envelope/model.ts";
import { ulid } from "../src/envelope/ulid.ts";
import { defaultEnvelopePath } from "../src/consumer/envelope-handler.ts";
import { handlerDeadlineMs, inPlaceRetryPolicy, envelopePolicy } from "../src/consumer/policy.ts";

describe("delivery-id precedence (§2.3)", () => {
  const base = { ingress: "nango" as const, inner: {}, nangoEnvelope: true };
  it("github: github:<x-github-delivery>", () => {
    assert.equal(resolveDeliveryId({ ...base, ingress: "github", provider: "github", headers: { "x-github-delivery": "d1" }, nangoEnvelope: false }), "github:d1");
  });
  it("nango headers: x-nango-delivery-id → x-nango-webhook-id → webhook-id", () => {
    const h = { "x-nango-delivery-id": "a", "x-nango-webhook-id": "b", "webhook-id": "c" };
    assert.equal(resolveDeliveryId({ ...base, provider: "notion", headers: h }), "a");
    assert.equal(resolveDeliveryId({ ...base, provider: "notion", headers: { "x-nango-webhook-id": "b", "webhook-id": "c" } }), "b");
    assert.equal(resolveDeliveryId({ ...base, provider: "notion", headers: { "webhook-id": "c" } }), "c");
  });
  it("provider-specific fallbacks", () => {
    const p = { ...base, nangoEnvelope: false, headers: {} };
    assert.equal(resolveDeliveryId({ ...p, provider: "gitlab", headers: { "x-gitlab-event-uuid": "g" } }), "g");
    assert.equal(resolveDeliveryId({ ...p, provider: "slack", inner: { event_id: "Ev1" } }), "Ev1");
    assert.equal(resolveDeliveryId({ ...p, provider: "linear", headers: { "linear-delivery": "L" } }), "L");
    assert.equal(resolveDeliveryId({ ...p, provider: "linear", inner: { _connection: { deliveryId: "LC" } } }), "LC");
    assert.equal(resolveDeliveryId({ ...p, provider: "jira", headers: { "x-atlassian-webhook-identifier": "J" } }), "J");
    assert.equal(resolveDeliveryId({ ...p, provider: "notion", inner: { id: "N" } }), "N");
  });
  it("generic: x-github-delivery → webhook-id → x-hookdeck-eventid, else null (request id)", () => {
    const p = { ...base, nangoEnvelope: false, provider: "other" };
    assert.equal(resolveDeliveryId({ ...p, headers: { "webhook-id": "w", "x-hookdeck-eventid": "h" } }), "w");
    assert.equal(resolveDeliveryId({ ...p, headers: { "x-hookdeck-eventid": "h" } }), "h");
    assert.equal(resolveDeliveryId({ ...p, headers: {} }), null);
  });
});

describe("classify", () => {
  it("nango body without provider identifier is dropped (Cloud rule)", () => {
    const c = classify({ ingress: "nango", headers: {}, body: { type: "forward", connectionId: "c1", payload: {} } });
    assert.deepEqual(c, { drop: true, reason: "missing_nango_provider_identifier", connectionId: "c1" });
  });
  it("nango forward infers slack-relay / github-relay from headers", () => {
    const s = classify({ ingress: "nango", headers: { "x-slack-signature": "v0=x" }, body: { type: "forward", payload: {} } });
    assert.equal(s.drop === false && s.provider, "slack-relay");
    const g = classify({ ingress: "nango", headers: { "x-github-delivery": "d", "x-github-event": "push" }, body: { type: "forward", payload: { installation: { id: 5 } } } });
    assert.equal(g.drop === false && g.provider, "github-relay");
    assert.equal(g.drop === false && g.deliveryId, "github:d");
  });
  it("kinds: forward/sync/auth", () => {
    for (const [type, kind] of [["forward", "nango-forward"], ["sync", "nango-sync"], ["auth", "nango-auth"]] as const) {
      const c = classify({ ingress: "nango", headers: {}, body: { type, from: "notion", connectionId: "c" } });
      assert.equal(c.drop === false && c.kind, kind);
    }
  });
  it("direct github: native installation id and event.action", () => {
    const c = classify({ ingress: "github", headers: { "x-github-delivery": "d", "x-github-event": "issues" }, body: { action: "opened", installation: { id: 42 } } });
    assert.ok(!c.drop);
    assert.equal(c.provider, "github");
    assert.equal(c.eventType, "issues.opened");
    assert.deepEqual(c.nativeIds, { installation_id: "42" });
  });
  it("slack team id", () => {
    const c = classify({ ingress: "slack", headers: {}, body: { team_id: "T1", event_id: "Ev", event: { type: "message" } } });
    assert.ok(!c.drop);
    assert.equal(c.deliveryId, "Ev");
    assert.deepEqual(c.nativeIds, { team_id: "T1" });
  });
});

describe("dedupe key / envelope id / headers", () => {
  it("length-prefixed tuples cannot collide", () => {
    assert.notEqual(dedupeKey(["a|1:b", "c"]), dedupeKey(["a", "b|1:c"]));
    assert.equal(dedupeKey(["github", "github:d1"]), "6:github|9:github:d1");
    assert.equal(dedupeKey(["é"]), "2:é");
  });
  it("envelopeId is stable and always includes deliveryId (HARD)", () => {
    const a = relayfileEnvelopeId({ provider: "github", workspaceId: "rw_1", deliveryId: "d1", eventType: "push" });
    assert.equal(a, relayfileEnvelopeId({ provider: "github", workspaceId: "rw_1", deliveryId: "d1", eventType: "push" }));
    assert.notEqual(a, relayfileEnvelopeId({ provider: "github", workspaceId: "rw_1", deliveryId: "d2", eventType: "push" }));
    assert.match(a, /^github_[a-f0-9]{64}$/);
  });
  it("header allow-list drops Authorization and anything not listed", () => {
    const h = allowListHeaders({ authorization: "Bearer x", "x-github-delivery": "d", "x-hub-signature-256": "s", cookie: "c", "x-nango-delivery-id": "n" });
    assert.deepEqual(h, { "x-github-delivery": "d", "x-nango-delivery-id": "n" });
  });
  it("ulid is 26 chars and time-ordered", () => {
    const a = ulid(1_000);
    const b = ulid(2_000);
    assert.equal(a.length, 26);
    assert.ok(a < b);
  });
  it("PR 1 default paths", () => {
    assert.equal(defaultEnvelopePath({ provider: "github", event_type: "issues.opened", delivery_id: "github:abc", native_ids: {} }), "/github/webhooks/issues.opened/github_abc.json");
    assert.equal(defaultEnvelopePath({ provider: "slack-relay", event_type: "message", delivery_id: "Ev1", native_ids: { team_id: "T1" } }), "/slack/teams/T1/events/Ev1.json");
  });
});

describe("deadlines and in-place policy (§4.3, §6.2)", () => {
  it("120 s for github/google-mail/linear/notion (prefix on -), else 15 s", () => {
    for (const p of ["github", "github-relay", "google-mail", "linear", "notion"]) assert.equal(handlerDeadlineMs(p), 120_000, p);
    for (const p of ["slack", "jira", "slack-relay", "gitlab"]) assert.equal(handlerDeadlineMs(p), 15_000, p);
  });
  it("in-place retry only for long-deadline providers", () => {
    assert.deepEqual(inPlaceRetryPolicy("github"), { maxRetries: 1, budgetMs: 30_000, attemptTimeoutMs: 15_000 });
    assert.deepEqual(inPlaceRetryPolicy("slack"), { maxRetries: 0, budgetMs: 0, attemptTimeoutMs: 15_000 });
    assert.equal(envelopePolicy("slack").maxRetries, 0);
    assert.equal(envelopePolicy("github").maxRetries, 1);
  });
});
