import { createHash, createHmac, timingSafeEqual } from "node:crypto";

/**
 * §2.2 verification, per ingress, over raw bytes before any parse (HARD).
 * Every comparison is `timingSafeEqual` on equal-length buffers. Every
 * function returns a boolean only: callers answer `401 invalid_signature`
 * with no further detail.
 */

type Headers = Record<string, string>;

function safeEqual(a: Buffer, b: Buffer): boolean {
  return a.length === b.length && a.length > 0 && timingSafeEqual(a, b);
}

function safeEqualString(a: string, b: string): boolean {
  return safeEqual(Buffer.from(a, "utf8"), Buffer.from(b, "utf8"));
}

function isHex(value: string): boolean {
  return value.length > 0 && value.length % 2 === 0 && /^[a-f0-9]+$/i.test(value);
}

function safeEqualHex(expectedHex: string, receivedHex: string): boolean {
  if (!isHex(expectedHex) || !isHex(receivedHex) || expectedHex.length !== receivedHex.length) return false;
  return safeEqual(Buffer.from(expectedHex, "hex"), Buffer.from(receivedHex.toLowerCase(), "hex"));
}

const hmacHex = (secret: string | Buffer, data: Buffer | string) => createHmac("sha256", secret).update(data).digest("hex");
const hmacB64 = (secret: string | Buffer, data: Buffer | string) => createHmac("sha256", secret).update(data).digest("base64");

const nowSeconds = () => Math.floor(Date.now() / 1000);

// ---------------------------------------------------------------- GitHub

const GITHUB_SIGNATURE = /^sha256=[a-f0-9]{64}$/i;

export function verifyGitHub(raw: Buffer, headers: Headers, secrets: readonly string[]): boolean {
  const signature = headers["x-hub-signature-256"]?.trim();
  if (!signature || !GITHUB_SIGNATURE.test(signature)) return false;
  const received = signature.slice("sha256=".length).toLowerCase();
  return secrets.some((secret) => safeEqualHex(hmacHex(secret, raw), received));
}

// ---------------------------------------------------------------- Nango

export function hasNangoSignature(headers: Headers): boolean {
  return Boolean(headers["x-nango-hmac-sha256"] || headers["x-nango-signature"]);
}

/**
 * `x-nango-hmac-sha256` = HMAC-SHA256(secret, raw) hex, else legacy
 * `x-nango-signature` = sha256(secret + raw) hex, else
 * sha256(secret + JSON.stringify(JSON.parse(raw))). No timestamp window.
 */
export function verifyNango(raw: Buffer, headers: Headers, secrets: readonly string[]): boolean {
  const hmac = headers["x-nango-hmac-sha256"]?.trim();
  if (hmac) return secrets.some((secret) => safeEqualHex(hmacHex(secret, raw), hmac));
  const legacy = headers["x-nango-signature"]?.trim();
  if (!legacy) return false;
  const rawText = raw.toString("utf8");
  let normalized: string | null = null;
  try {
    normalized = JSON.stringify(JSON.parse(rawText));
  } catch {
    normalized = null;
  }
  return secrets.some((secret) => {
    if (safeEqualHex(createHash("sha256").update(secret + rawText).digest("hex"), legacy)) return true;
    return normalized !== null && safeEqualHex(createHash("sha256").update(secret + normalized).digest("hex"), legacy);
  });
}

// ---------------------------------------------------------------- Hookdeck

const STANDARD_PREFIX = "whsec_";
const STANDARD_TOLERANCE_SECONDS = 300;

export function splitHookdeckSecrets(secrets: readonly string[]): { standard: string[]; legacy: string[] } {
  return {
    standard: secrets.filter((s) => s.startsWith(STANDARD_PREFIX)),
    legacy: secrets.filter((s) => !s.startsWith(STANDARD_PREFIX)),
  };
}

function standardKey(secret: string): Buffer | null {
  const encoded = secret.slice(STANDARD_PREFIX.length);
  if (!/^[A-Za-z0-9+/]+={0,2}$/.test(encoded)) return null;
  const key = Buffer.from(encoded, "base64");
  // An empty (or trivially short) key would make the HMAC computable by anyone.
  return key.length >= 16 ? key : null;
}

/** Standard Webhooks: `v1,<base64 HMAC(key, "${id}.${ts}.${body}")>`, ±300 s, space-separated rotation list. */
export function verifyStandardWebhook(raw: Buffer, headers: Headers, secret: string, now = nowSeconds()): boolean {
  const id = headers["webhook-id"]?.trim();
  const ts = headers["webhook-timestamp"]?.trim();
  const sig = headers["webhook-signature"]?.trim();
  if (!id || !ts || !sig || !/^\d{1,12}$/.test(ts)) return false;
  if (Math.abs(now - Number(ts)) > STANDARD_TOLERANCE_SECONDS) return false;
  const key = standardKey(secret);
  if (!key) return false;
  const expected = hmacB64(key, Buffer.concat([Buffer.from(`${id}.${ts}.`), raw]));
  return sig.split(" ").some((candidate) => {
    const [version, value] = candidate.split(",", 2);
    return version === "v1" && Boolean(value) && safeEqualString(value ?? "", expected);
  });
}

function verifyHookdeckLegacy(raw: Buffer, headers: Headers, secret: string): boolean {
  const expected = hmacB64(secret, raw);
  return ["x-hookdeck-signature", "x-hookdeck-signature-2"].some((name) => {
    const v = headers[name]?.trim();
    return v ? safeEqualString(v, expected) : false;
  });
}

export function verifyHookdeck(raw: Buffer, headers: Headers, secrets: readonly string[], now = nowSeconds()): "standard" | "legacy" | null {
  const { standard, legacy } = splitHookdeckSecrets(secrets);
  if (standard.some((s) => verifyStandardWebhook(raw, headers, s, now))) return "standard";
  if (legacy.some((s) => verifyHookdeckLegacy(raw, headers, s))) return "legacy";
  return null;
}

/** `x-dropbox-signature` = HMAC-SHA256(DROPBOX_APP_SECRET, raw) hex. */
export function verifyDropbox(raw: Buffer, headers: Headers, secrets: readonly string[]): boolean {
  const sig = headers["x-dropbox-signature"]?.trim();
  if (!sig) return false;
  return secrets.some((s) => safeEqualHex(hmacHex(s, raw), sig));
}

/**
 * Raw providers whose Hookdeck delivery skips the Hookdeck signature in Cloud
 * (they verify a provider-native signature in their own handler). Detection
 * mirrors Cloud's predicates closely enough to decide the skip; PR 1 has no
 * handler for them, so they fall through to "Nango signature required" (401).
 */
export function detectSkipHookdeckProvider(headers: Headers, raw: Buffer): "gitlab" | "bitbucket" | "shortcut" | "granola" | null {
  if (headers["x-gitlab-event"] || headers["x-gitlab-event-uuid"] || headers["x-gitlab-token"]) return "gitlab";
  // Cloud also checks x-event-key against @relayfile/adapter-bitbucket's list; any key suffices here.
  if (headers["x-hub-signature"] && headers["x-event-key"]) return "bitbucket";
  let body: unknown;
  try {
    body = JSON.parse(raw.toString("utf8"));
  } catch {
    return null;
  }
  if (body && typeof body === "object" && !Array.isArray(body)) {
    const actions = (body as { actions?: unknown }).actions;
    if (
      Array.isArray(actions) &&
      actions.length > 0 &&
      actions.every((a) => a && typeof a === "object" && "id" in a && "entity_type" in a && "action" in a)
    ) {
      return "shortcut";
    }
    // Conservative subset of Cloud's marker: the explicit Granola object-id fields.
    const b = body as Record<string, unknown>;
    const idLike = (v: unknown) => (typeof v === "string" && v.length > 0) || (typeof v === "number" && Number.isFinite(v));
    if (idLike(b.note_id) || idLike(b.noteId) || idLike(b.folder_id) || idLike(b.folderId)) return "granola";
  }
  return null;
}

// ---------------------------------------------------------------- Slack

/** `v0=` + HMAC(secret, `v0:${ts}:${body}`) hex; timestamp within ±300 s. */
export function verifySlack(raw: Buffer, headers: Headers, secrets: readonly string[], now = nowSeconds()): boolean {
  const sig = headers["x-slack-signature"]?.trim();
  const ts = headers["x-slack-request-timestamp"]?.trim();
  if (!sig || !ts || !/^\d{1,12}$/.test(ts) || !sig.startsWith("v0=")) return false;
  if (Math.abs(now - Number(ts)) > 300) return false;
  const received = sig.slice(3);
  const base = Buffer.concat([Buffer.from(`v0:${ts}:`), raw]);
  return secrets.some((s) => safeEqualHex(hmacHex(s, base), received));
}

// ---------------------------------------------------------------- Linear

/**
 * `linear-signature` = HMAC(secret, raw) hex. The ±60 s `webhookTimestamp`
 * window is checked after parse by `linearTimestampFresh` (it lives in the body).
 */
export function verifyLinear(raw: Buffer, headers: Headers, secrets: readonly string[]): boolean {
  const sig = headers["linear-signature"]?.trim();
  if (!sig) return false;
  return secrets.some((s) => safeEqualHex(hmacHex(s, raw), sig));
}

export function linearTimestampFresh(body: unknown, nowMs = Date.now()): boolean {
  if (!body || typeof body !== "object") return false;
  const ts = (body as { webhookTimestamp?: unknown }).webhookTimestamp;
  if (typeof ts !== "number" || !Number.isFinite(ts)) return false;
  return Math.abs(nowMs - ts) <= 60_000;
}
