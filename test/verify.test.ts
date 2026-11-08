import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { describe, it } from "node:test";
import {
  detectSkipHookdeckProvider,
  linearTimestampFresh,
  verifyDropbox,
  verifyGitHub,
  verifyHookdeck,
  verifyLinear,
  verifyNango,
  verifySlack,
  verifyStandardWebhook,
} from "../src/ingress/verify.ts";
import { FIXTURE, sign } from "./support/harness.ts";

const body = '{"hello":"world","n":1}';
const raw = Buffer.from(body);

describe("GitHub x-hub-signature-256", () => {
  it("known vector: HMAC-SHA256 hex with sha256= prefix", () => {
    // RFC-style fixed vector so the scheme itself is pinned, not just round-tripped.
    const secret = "It's a Secret to Everybody";
    const payload = Buffer.from("Hello, World!");
    const headers = { "x-hub-signature-256": "sha256=757107ea0eb2509fc211221cce984b8a37570b6d7586c22c46f4379c8b043e17" };
    assert.equal(verifyGitHub(payload, headers, [secret]), true);
  });
  it("good, bad, malformed, missing secret, rotation", () => {
    assert.equal(verifyGitHub(raw, sign.github(body), [FIXTURE.github]), true);
    assert.equal(verifyGitHub(raw, sign.github(body, "other"), [FIXTURE.github]), false);
    assert.equal(verifyGitHub(raw, { "x-hub-signature-256": "sha1=abc" }, [FIXTURE.github]), false);
    assert.equal(verifyGitHub(raw, { "x-hub-signature-256": "sha256=" + "a".repeat(63) }, [FIXTURE.github]), false);
    assert.equal(verifyGitHub(raw, {}, [FIXTURE.github]), false);
    assert.equal(verifyGitHub(raw, sign.github(body), []), false);
    assert.equal(verifyGitHub(raw, sign.github(body), ["old-secret", FIXTURE.github]), true);
    assert.equal(verifyGitHub(Buffer.from(body + " "), sign.github(body), [FIXTURE.github]), false);
  });
  it("accepts an upper-case hex digest (pattern is case-insensitive)", () => {
    const h = sign.github(body)["x-hub-signature-256"];
    assert.equal(verifyGitHub(raw, { "x-hub-signature-256": "sha256=" + h.slice(7).toUpperCase() }, [FIXTURE.github]), true);
  });
});

describe("Nango", () => {
  it("x-nango-hmac-sha256", () => {
    assert.equal(verifyNango(raw, sign.nango(body), [FIXTURE.nango]), true);
    assert.equal(verifyNango(raw, sign.nango(body, "x"), [FIXTURE.nango]), false);
  });
  it("legacy x-nango-signature sha256(secret+raw)", () => {
    assert.equal(verifyNango(raw, sign.nangoLegacy(body), [FIXTURE.nango]), true);
  });
  it("legacy normalized form sha256(secret + JSON.stringify(JSON.parse(raw)))", () => {
    const spaced = '{ "hello": "world", "n": 1 }';
    const headers = sign.nangoLegacy(JSON.stringify(JSON.parse(spaced)));
    assert.equal(verifyNango(Buffer.from(spaced), headers, [FIXTURE.nango]), true);
  });
  it("hmac header takes precedence: a bad hmac is not rescued by a good legacy header", () => {
    const headers = { ...sign.nango(body, "wrong"), ...sign.nangoLegacy(body) };
    assert.equal(verifyNango(raw, headers, [FIXTURE.nango]), false);
  });
  it("no headers / no secret", () => {
    assert.equal(verifyNango(raw, {}, [FIXTURE.nango]), false);
    assert.equal(verifyNango(raw, sign.nango(body), []), false);
  });
});

describe("Hookdeck", () => {
  const secrets = [FIXTURE.hookdeckLegacy, FIXTURE.hookdeckStandard];
  it("Standard Webhooks within ±300 s, rotation list in the header", () => {
    assert.equal(verifyHookdeck(raw, sign.hookdeckStandard(body), secrets), "standard");
    const h = sign.hookdeckStandard(body);
    const rotated = { ...h, "webhook-signature": `v1,AAAA ${h["webhook-signature"]}` };
    assert.equal(verifyHookdeck(raw, rotated, secrets), "standard");
  });
  it("Standard Webhooks outside the window fails", () => {
    const old = Math.floor(Date.now() / 1000) - 301;
    assert.equal(verifyStandardWebhook(raw, sign.hookdeckStandard(body, "m", old), FIXTURE.hookdeckStandard), false);
    const future = Math.floor(Date.now() / 1000) + 301;
    assert.equal(verifyStandardWebhook(raw, sign.hookdeckStandard(body, "m", future), FIXTURE.hookdeckStandard), false);
  });
  it("legacy x-hookdeck-signature and -2", () => {
    assert.equal(verifyHookdeck(raw, sign.hookdeckLegacy(body), secrets), "legacy");
    assert.equal(verifyHookdeck(raw, { "x-hookdeck-signature-2": sign.hookdeckLegacy(body)["x-hookdeck-signature"] }, secrets), "legacy");
    assert.equal(verifyHookdeck(raw, sign.hookdeckLegacy(body, "x"), secrets), null);
  });
  it("Dropbox branch: HMAC hex with the app secret", () => {
    assert.equal(verifyDropbox(raw, sign.dropbox(body), [FIXTURE.dropbox]), true);
    assert.equal(verifyDropbox(raw, sign.dropbox(body, "x"), [FIXTURE.dropbox]), false);
  });
  it("skip detection for gitlab / bitbucket / shortcut / granola (Cloud predicates)", () => {
    assert.equal(detectSkipHookdeckProvider({ "x-gitlab-event": "Push Hook" }, raw), "gitlab");
    assert.equal(detectSkipHookdeckProvider({ "x-hub-signature": "sha256=x", "x-event-key": "repo:push" }, raw), "bitbucket");
    assert.equal(detectSkipHookdeckProvider({}, Buffer.from(JSON.stringify({ actions: [{ id: 1, entity_type: "story", action: "update" }] }))), "shortcut");
    assert.equal(detectSkipHookdeckProvider({}, Buffer.from(JSON.stringify({ note_id: "n1" }))), "granola");
    assert.equal(detectSkipHookdeckProvider({}, raw), null);
  });
});

describe("Slack", () => {
  it("v0 signature with ±300 s timestamp", () => {
    assert.equal(verifySlack(raw, sign.slack(body), [FIXTURE.slack]), true);
    assert.equal(verifySlack(raw, sign.slack(body, Math.floor(Date.now() / 1000) - 301), [FIXTURE.slack]), false);
    assert.equal(verifySlack(raw, sign.slack(body, undefined, "x"), [FIXTURE.slack]), false);
    assert.equal(verifySlack(raw, {}, [FIXTURE.slack]), false);
  });
  it("Slack's documented example vector", () => {
    const secret = "8f742231b10e8888abcd99yyyzzz85a5";
    const b = "token=xyzz0WbapA4vBCDEFasx0q6G&team_id=T1DC2JH3J&team_domain=testteamnow&channel_id=G8PSS9T3V&channel_name=foobar&user_id=U2CERLKJA&user_name=roadrunner&command=%2Fwebhook-collect&text=&response_url=https%3A%2F%2Fhooks.slack.com%2Fcommands%2FT1DC2JH3J%2F397700885554%2F96rGlfmibIGlgcZRskXaIFfN&trigger_id=398738663015.47445629121.803a0bc887a14d10d2c447fce8b6703c";
    const ts = 1531420618;
    const headers = { "x-slack-request-timestamp": String(ts), "x-slack-signature": "v0=a2114d57b48eac39b9ad189dd8316235a7b4a8d21a10bd27519666489c69b503" };
    assert.equal(verifySlack(Buffer.from(b), headers, [secret], ts), true);
  });
});

describe("Linear", () => {
  it("HMAC hex + webhookTimestamp ±60 s", () => {
    assert.equal(verifyLinear(raw, sign.linear(body), [FIXTURE.linear]), true);
    assert.equal(verifyLinear(raw, sign.linear(body, "x"), [FIXTURE.linear]), false);
    assert.equal(linearTimestampFresh({ webhookTimestamp: Date.now() }), true);
    assert.equal(linearTimestampFresh({ webhookTimestamp: Date.now() - 61_000 }), false);
    assert.equal(linearTimestampFresh({}), false);
  });
});

describe("timing-safe on equal-length buffers", () => {
  it("different-length candidates are rejected without throwing", () => {
    const short = { "x-nango-hmac-sha256": createHmac("sha256", FIXTURE.nango).update(raw).digest("hex").slice(0, 10) };
    assert.equal(verifyNango(raw, short, [FIXTURE.nango]), false);
    assert.equal(verifyHookdeck(raw, { "x-hookdeck-signature": "short" }, [FIXTURE.hookdeckLegacy]), null);
  });
});
