/**
 * Secret helpers for the Flows store, ported unchanged from relay-control so
 * ciphertexts and token hashes keep the same format (AES-256-GCM `v2.<keyId>.`
 * envelopes, SHA-256 hex token hashes).
 */
import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
  timingSafeEqual,
} from "node:crypto";

const ENCRYPTED_SECRET_VERSION = "v2";

export type EncryptionKey = {
  id: string;
  key: Buffer;
};

export type EncryptionKeyring = {
  active: EncryptionKey;
  previous?: EncryptionKey;
};

export type CiphertextKeyReference =
  | { kind: "key"; keyId: string }
  | { kind: "legacy" }
  | { kind: "malformed" };

export class EncryptedSecretError extends Error {
  constructor(readonly code: "malformed" | "unknown_key" | "decrypt_failed") {
    super("Encrypted secret is not readable with the configured keyring");
    this.name = "EncryptedSecretError";
  }
}

export function hashSecret(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function opaqueToken(prefix: string): string {
  return `${prefix}${randomBytes(32).toString("base64url")}`;
}

export function secureEqual(left: string, right: string): boolean {
  const a = Buffer.from(left);
  const b = Buffer.from(right);
  return a.length === b.length && timingSafeEqual(a, b);
}



function decodePart(value: string, expectedLength?: number): Buffer {
  if (!value || !/^[A-Za-z0-9_-]+$/.test(value)) {
    throw new EncryptedSecretError("malformed");
  }
  const decoded = Buffer.from(value, "base64url");
  if (
    decoded.toString("base64url") !== value ||
    (expectedLength !== undefined && decoded.length !== expectedLength)
  ) {
    throw new EncryptedSecretError("malformed");
  }
  return decoded;
}

function decryptAesGcm(
  rawIv: string,
  rawTag: string,
  rawCiphertext: string,
  key: Buffer,
  associatedData?: string,
): string {
  try {
    const decipher = createDecipheriv("aes-256-gcm", key, decodePart(rawIv, 12));
    if (associatedData) decipher.setAAD(Buffer.from(associatedData, "utf8"));
    decipher.setAuthTag(decodePart(rawTag, 16));
    return Buffer.concat([
      decipher.update(decodePart(rawCiphertext)),
      decipher.final(),
    ]).toString("utf8");
  } catch (error) {
    if (error instanceof EncryptedSecretError) throw error;
    throw new EncryptedSecretError("decrypt_failed");
  }
}

export function ciphertextKeyReference(value: string): CiphertextKeyReference {
  const parts = value.split(".");
  if (parts.length === 4 && parts[0] === "v1" && parts.slice(1).every(Boolean)) {
    return { kind: "legacy" };
  }
  if (
    parts.length === 5 &&
    parts[0] === ENCRYPTED_SECRET_VERSION &&
    /^[A-Za-z0-9_-]{1,64}$/.test(parts[1] ?? "") &&
    parts.slice(2).every(Boolean)
  ) {
    return { kind: "key", keyId: parts[1]! };
  }
  return { kind: "malformed" };
}

export function encryptSecret(value: string, key: EncryptionKey): string {
  const iv = randomBytes(12);
  const associatedData = `${ENCRYPTED_SECRET_VERSION}.${key.id}`;
  const cipher = createCipheriv("aes-256-gcm", key.key, iv);
  cipher.setAAD(Buffer.from(associatedData, "utf8"));
  const ciphertext = Buffer.concat([cipher.update(value, "utf8"), cipher.final()]);
  return [
    ENCRYPTED_SECRET_VERSION,
    key.id,
    iv.toString("base64url"),
    cipher.getAuthTag().toString("base64url"),
    ciphertext.toString("base64url"),
  ].join(".");
}

export function decryptSecret(value: string, keyring: EncryptionKeyring): string {
  const reference = ciphertextKeyReference(value);
  const parts = value.split(".");
  if (reference.kind === "malformed") {
    throw new EncryptedSecretError("malformed");
  }
  if (reference.kind === "legacy") {
    const candidates = keyring.previous
      ? [keyring.previous, keyring.active]
      : [keyring.active];
    for (const candidate of candidates) {
      try {
        return decryptAesGcm(parts[1]!, parts[2]!, parts[3]!, candidate.key);
      } catch (error) {
        if (
          !(error instanceof EncryptedSecretError) ||
          error.code === "malformed"
        ) {
          throw error;
        }
      }
    }
    throw new EncryptedSecretError("decrypt_failed");
  }

  const key = reference.keyId === keyring.active.id
    ? keyring.active
    : reference.keyId === keyring.previous?.id
      ? keyring.previous
      : undefined;
  if (!key) throw new EncryptedSecretError("unknown_key");
  return decryptAesGcm(
    parts[2]!,
    parts[3]!,
    parts[4]!,
    key.key,
    `${ENCRYPTED_SECRET_VERSION}.${reference.keyId}`,
  );
}
