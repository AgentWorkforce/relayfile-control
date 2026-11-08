import type { IncomingMessage } from "node:http";

export class BodyTooLargeError extends Error {
  constructor() {
    super("payload_too_large");
  }
}

/**
 * Read a request body with a hard cap (§2.2). A declared Content-Length over
 * the cap fails before reading; otherwise chunks are counted as they arrive
 * and the read stops the moment the cap is crossed. Nothing past the cap is
 * buffered: further chunks are discarded, not stored.
 */
export function readCappedBody(req: IncomingMessage, maxBytes: number): Promise<Buffer> {
  const declared = req.headers["content-length"];
  if (declared !== undefined) {
    const n = Number(declared);
    if (!/^\d+$/.test(declared) || !Number.isSafeInteger(n) || n > maxBytes) {
      req.resume();
      return Promise.reject(new BodyTooLargeError());
    }
  }
  return new Promise<Buffer>((resolve, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    let over = false;
    const onData = (chunk: Buffer) => {
      if (over) return;
      total += chunk.length;
      if (total > maxBytes) {
        over = true;
        chunks.length = 0;
        reject(new BodyTooLargeError());
        return;
      }
      chunks.push(chunk);
    };
    req.on("data", onData);
    req.once("end", () => {
      if (!over) resolve(Buffer.concat(chunks, total));
    });
    req.once("error", (err) => {
      if (!over) reject(err);
    });
  });
}

/** Node lower-cases header names; flatten repeated values. */
export function headerRecord(req: IncomingMessage): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [name, value] of Object.entries(req.headers)) {
    if (value === undefined) continue;
    out[name.toLowerCase()] = Array.isArray(value) ? value.join(", ") : value;
  }
  return out;
}
