import { createPublicKey, verify, type KeyObject } from "node:crypto";
import { readFile } from "node:fs/promises";

/**
 * §11 entitlement: relayfile is a licence add-on. Two sources, OR-ed:
 *  1. relaycontrol `GET /api/v1/license` with a service bearer, requiring
 *     `capabilities.relayfile === true`;
 *  2. an offline licence document verified locally (relaycontrol's format:
 *     `<base64url(payload JSON)>.<base64url(Ed25519 sig)>`, signature over
 *     `"relaycontrol-license-v1\n" + payload`), requiring `features ∋ relayfile`
 *     and `expires_at` in the future.
 * The last good result is honoured for ENTITLEMENT_GRACE_SECONDS so a
 * relaycontrol outage does not stop ingest.
 */

export const LICENSE_SIGNATURE_PREFIX = "relaycontrol-license-v1\n";
const ED25519_SPKI_PREFIX = Buffer.from("302a300506032b6570032100", "hex");
const MAX_DOCUMENT_BYTES = 16 * 1024;

export type EntitlementStatus = {
  entitled: boolean;
  source: "relaycontrol" | "license_document" | "grace" | null;
  reason: string;
  checkedAt: string | null;
  lastGoodAt: string | null;
};

export interface Entitlement {
  isEntitled(): boolean;
  status(): EntitlementStatus;
}

/** Fixed entitlement (tests, and the e2e harness). */
export class StaticEntitlement implements Entitlement {
  constructor(private entitled = true, private reason = entitled ? "static" : "not_entitled") {}
  set(entitled: boolean, reason = entitled ? "static" : "not_entitled"): void {
    this.entitled = entitled;
    this.reason = reason;
  }
  isEntitled(): boolean {
    return this.entitled;
  }
  status(): EntitlementStatus {
    return { entitled: this.entitled, source: this.entitled ? "license_document" : null, reason: this.reason, checkedAt: null, lastGoodAt: null };
  }
}

/** PEM (SPKI) or base64 of a raw 32-byte Ed25519 key. Throws when malformed. */
export function parseLicensePublicKey(value: string): KeyObject {
  const trimmed = value.trim();
  if (trimmed.startsWith("-----BEGIN")) {
    const key = createPublicKey(trimmed);
    if (key.asymmetricKeyType !== "ed25519") throw new Error("licence public key must be Ed25519");
    return key;
  }
  const raw = Buffer.from(trimmed, "base64");
  if (raw.length !== 32) throw new Error("licence public key must be base64 of exactly 32 bytes");
  return createPublicKey({ key: Buffer.concat([ED25519_SPKI_PREFIX, raw]), format: "der", type: "spki" });
}

export type LicenseCheck = { ok: true; expiresAt: string } | { ok: false; reason: string };

/** Verify a licence document for the `relayfile` feature at `nowMs`. */
export function verifyLicenseForRelayfile(document: string, publicKey: KeyObject, nowMs = Date.now()): LicenseCheck {
  const trimmed = document.trim();
  if (Buffer.byteLength(trimmed) > MAX_DOCUMENT_BYTES) return { ok: false, reason: "license_too_large" };
  const parts = trimmed.split(".");
  const b64 = (s: string | undefined) => (s && /^[A-Za-z0-9_-]+$/.test(s) ? Buffer.from(s, "base64url") : null);
  const payload = parts.length === 2 ? b64(parts[0]) : null;
  const signature = parts.length === 2 ? b64(parts[1]) : null;
  if (!payload || !signature || signature.length !== 64) return { ok: false, reason: "license_malformed" };
  let signed = false;
  try {
    signed = verify(null, Buffer.concat([Buffer.from(LICENSE_SIGNATURE_PREFIX), payload]), publicKey, signature);
  } catch {
    signed = false;
  }
  if (!signed) return { ok: false, reason: "license_bad_signature" };
  let claims: Record<string, unknown>;
  try {
    const parsed: unknown = JSON.parse(payload.toString("utf8"));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("not an object");
    claims = parsed as Record<string, unknown>;
  } catch {
    return { ok: false, reason: "license_malformed" };
  }
  if (claims.v !== 1) return { ok: false, reason: "license_malformed" };
  const expiresAt = typeof claims.expires_at === "string" ? Date.parse(claims.expires_at) : Number.NaN;
  if (!Number.isFinite(expiresAt)) return { ok: false, reason: "license_malformed" };
  if (expiresAt <= nowMs) return { ok: false, reason: "license_expired" };
  const features = Array.isArray(claims.features) ? claims.features : [];
  if (!features.includes("relayfile")) return { ok: false, reason: "license_missing_feature" };
  return { ok: true, expiresAt: claims.expires_at as string };
}

export type RefreshingEntitlementOptions = {
  relaycontrolBaseUrl?: string;
  serviceToken?: string;
  licenseDocument?: string;
  licenseFile?: string;
  publicKey?: string;
  graceSeconds: number;
  fetch?: typeof fetch;
  now?: () => number;
};

export class RefreshingEntitlement implements Entitlement {
  private state: EntitlementStatus = { entitled: false, source: null, reason: "not_checked", checkedAt: null, lastGoodAt: null };
  private lastGoodMs: number | null = null;
  private readonly fetchImpl: typeof fetch;
  private readonly now: () => number;
  private key: KeyObject | null = null;
  private keyError: string | null = null;

  constructor(private readonly opts: RefreshingEntitlementOptions) {
    this.fetchImpl = opts.fetch ?? fetch;
    this.now = opts.now ?? (() => Date.now());
    if (opts.publicKey) {
      try {
        this.key = parseLicensePublicKey(opts.publicKey);
      } catch {
        this.keyError = "license_public_key_invalid";
      }
    }
  }

  isEntitled(): boolean {
    return this.state.entitled;
  }

  status(): EntitlementStatus {
    return { ...this.state };
  }

  private async checkRemote(): Promise<{ ok: boolean; reason: string } | null> {
    if (!this.opts.relaycontrolBaseUrl) return null;
    try {
      const response = await this.fetchImpl(new URL("/api/v1/license", this.opts.relaycontrolBaseUrl), {
        headers: this.opts.serviceToken ? { authorization: `Bearer ${this.opts.serviceToken}` } : {},
        redirect: "manual",
        signal: AbortSignal.timeout(10_000),
      });
      if (response.status !== 200) {
        await response.body?.cancel().catch(() => undefined);
        return { ok: false, reason: `relaycontrol_status_${response.status}` };
      }
      const text = await response.text();
      if (text.length > 64 * 1024) return { ok: false, reason: "relaycontrol_response_too_large" };
      const body = JSON.parse(text) as { capabilities?: { relayfile?: unknown } };
      return body.capabilities?.relayfile === true ? { ok: true, reason: "relaycontrol_capability" } : { ok: false, reason: "relaycontrol_not_entitled" };
    } catch {
      return { ok: false, reason: "relaycontrol_unreachable" };
    }
  }

  private async checkDocument(): Promise<{ ok: boolean; reason: string } | null> {
    let doc = this.opts.licenseDocument;
    if (!doc && this.opts.licenseFile) {
      try {
        doc = await readFile(this.opts.licenseFile, "utf8");
      } catch {
        return { ok: false, reason: "license_file_unreadable" };
      }
    }
    if (!doc) return null;
    if (!this.key) return { ok: false, reason: this.keyError ?? "license_public_key_missing" };
    const check = verifyLicenseForRelayfile(doc, this.key, this.now());
    return check.ok ? { ok: true, reason: "license_document_valid" } : { ok: false, reason: check.reason };
  }

  async refresh(): Promise<EntitlementStatus> {
    const nowMs = this.now();
    const [remote, document] = await Promise.all([this.checkRemote(), this.checkDocument()]);
    const checkedAt = new Date(nowMs).toISOString();
    if (remote?.ok || document?.ok) {
      this.lastGoodMs = nowMs;
      this.state = {
        entitled: true,
        source: remote?.ok ? "relaycontrol" : "license_document",
        reason: (remote?.ok ? remote.reason : document?.reason) ?? "entitled",
        checkedAt,
        lastGoodAt: checkedAt,
      };
      return this.status();
    }
    const reason = [remote?.reason, document?.reason].filter(Boolean).join(",") || "no_entitlement_source_configured";
    // A definitive "not entitled" answer ends grace; only unavailability is covered.
    const definitive = remote?.reason === "relaycontrol_not_entitled" || document?.reason === "license_expired" || document?.reason === "license_missing_feature";
    const inGrace = !definitive && this.lastGoodMs !== null && nowMs - this.lastGoodMs <= this.opts.graceSeconds * 1000;
    this.state = {
      entitled: inGrace,
      source: inGrace ? "grace" : null,
      reason,
      checkedAt,
      lastGoodAt: this.lastGoodMs === null ? null : new Date(this.lastGoodMs).toISOString(),
    };
    return this.status();
  }
}
