import { createHmac } from "node:crypto";

let lastMs = 0;

/**
 * RFC3339 UTC timestamp with millisecond precision, strictly increasing per
 * process. Second precision made an in-place retry within the same second
 * byte-identical (same body, same timestamp ⇒ same signature), which the OSS
 * replay guard (`markInternalReplaySeen`) rejects as a replay. Go's
 * `time.Parse(time.RFC3339, …)` accepts the fractional second.
 */
export function relayTimestamp(nowMs = Date.now()): string {
  const ms = Math.max(nowMs, lastMs + 1);
  lastMs = ms;
  return new Date(ms).toISOString();
}

/** §6.1: lowercase hex HMAC-SHA256(secret, `${timestamp}\n${rawBody}`). */
export function signRelayRequest(secret: string, timestamp: string, rawBody: string): string {
  return createHmac("sha256", secret).update(`${timestamp}\n${rawBody}`).digest("hex");
}

/**
 * Headers for one attempt. Called inside every `post()`, so each retry gets a
 * fresh timestamp and signature (the OSS replay guard rejects identical
 * bytes+headers).
 */
export function signedHeaders(secret: string, rawBody: string, correlationId: string, nowMs = Date.now()): Record<string, string> {
  const timestamp = relayTimestamp(nowMs);
  return {
    "content-type": "application/json",
    "content-length": String(Buffer.byteLength(rawBody, "utf8")),
    "x-relay-timestamp": timestamp,
    "x-relay-signature": signRelayRequest(secret, timestamp, rawBody),
    "x-correlation-id": correlationId,
  };
}
