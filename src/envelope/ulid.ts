import { randomBytes } from "node:crypto";

const ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/** 26-char Crockford base32 ULID (48-bit ms time + 80 random bits). */
export function ulid(nowMs = Date.now()): string {
  let time = "";
  let t = nowMs;
  for (let i = 0; i < 10; i += 1) {
    time = ALPHABET[t % 32] + time;
    t = Math.floor(t / 32);
  }
  const bytes = randomBytes(16);
  let rand = "";
  for (let i = 0; i < 16; i += 1) rand += ALPHABET[(bytes[i] ?? 0) % 32];
  return time + rand;
}
