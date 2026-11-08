import assert from "node:assert/strict";
import { generateKeyPairSync, sign as edSign } from "node:crypto";
import { describe, it } from "node:test";
import { LICENSE_SIGNATURE_PREFIX, RefreshingEntitlement, parseLicensePublicKey, verifyLicenseForRelayfile } from "../src/entitlement/entitlement.ts";
import { listen } from "./support/harness.ts";

// Key pair generated at test time only; never committed.
const { publicKey, privateKey } = generateKeyPairSync("ed25519");
const rawPub = publicKey.export({ format: "der", type: "spki" }).subarray(12).toString("base64");
const pem = publicKey.export({ format: "pem", type: "spki" }).toString();

function licence(claims: Record<string, unknown>, key = privateKey): string {
  const payload = Buffer.from(JSON.stringify(claims));
  const sig = edSign(null, Buffer.concat([Buffer.from(LICENSE_SIGNATURE_PREFIX), payload]), key);
  return `${payload.toString("base64url")}.${sig.toString("base64url")}`;
}
const good = { v: 1, license_id: "00000000-0000-4000-8000-000000000001", customer: "fixture", issued_at: "2026-01-01T00:00:00Z", expires_at: "2099-01-01T00:00:00Z", features: ["relayfile"], seats: 5 };

describe("licence verifier (§11)", () => {
  const key = parseLicensePublicKey(rawPub);
  it("good", () => assert.deepEqual(verifyLicenseForRelayfile(licence(good), key), { ok: true, expiresAt: good.expires_at }));
  it("expired", () => assert.deepEqual(verifyLicenseForRelayfile(licence({ ...good, expires_at: "2020-01-01T00:00:00Z" }), key), { ok: false, reason: "license_expired" }));
  it("wrong feature", () => assert.deepEqual(verifyLicenseForRelayfile(licence({ ...good, features: ["flows"] }), key), { ok: false, reason: "license_missing_feature" }));
  it("bad signature", () => {
    const other = generateKeyPairSync("ed25519").privateKey;
    assert.deepEqual(verifyLicenseForRelayfile(licence(good, other), key), { ok: false, reason: "license_bad_signature" });
  });
  it("tampered payload", () => {
    const [, sig] = licence(good).split(".");
    const tampered = Buffer.from(JSON.stringify({ ...good, seats: 500 })).toString("base64url");
    assert.deepEqual(verifyLicenseForRelayfile(`${tampered}.${sig}`, key), { ok: false, reason: "license_bad_signature" });
  });
  it("malformed", () => assert.deepEqual(verifyLicenseForRelayfile("nope", key), { ok: false, reason: "license_malformed" }));
  it("PEM and raw base64 keys both parse", () => {
    assert.equal(parseLicensePublicKey(pem).asymmetricKeyType, "ed25519");
    assert.throws(() => parseLicensePublicKey("AAAA"));
  });
});

describe("RefreshingEntitlement", () => {
  it("offline document source", async () => {
    const e = new RefreshingEntitlement({ licenseDocument: licence(good), publicKey: rawPub, graceSeconds: 60 });
    assert.equal((await e.refresh()).entitled, true);
    assert.equal(e.status().source, "license_document");
  });
  it("nothing configured → not entitled", async () => {
    const e = new RefreshingEntitlement({ graceSeconds: 60 });
    const s = await e.refresh();
    assert.equal(s.entitled, false);
    assert.equal(s.reason, "no_entitlement_source_configured");
  });
  it("relaycontrol capability (fake server), bearer sent, outage covered by grace, definitive no ends it", async () => {
    let mode: "yes" | "down" | "no" = "yes";
    const auth: string[] = [];
    const srv = await listen((req, res) => {
      auth.push(String(req.headers.authorization));
      if (mode === "down") {
        res.writeHead(503);
        return res.end();
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ status: "valid", capabilities: { relayfile: mode === "yes" } }));
    });
    let now = 1_000_000;
    const e = new RefreshingEntitlement({ relaycontrolBaseUrl: srv.url, serviceToken: "fixture-service-token", graceSeconds: 100, now: () => now });
    assert.equal((await e.refresh()).source, "relaycontrol");
    assert.equal(auth[0], "Bearer fixture-service-token");
    mode = "down";
    now += 50_000;
    assert.deepEqual([(await e.refresh()).entitled, e.status().source], [true, "grace"]);
    now += 60_000;
    assert.equal((await e.refresh()).entitled, false, "grace expired");
    mode = "yes";
    await e.refresh();
    mode = "no";
    assert.equal((await e.refresh()).entitled, false, "definitive not-entitled ends grace");
    await srv.close();
  });
});
