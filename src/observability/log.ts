import { createHash } from "node:crypto";

export type LogFields = Record<string, unknown>;

export interface Logger {
  event(event: string, fields?: LogFields): void;
}

/** Field names that must never appear in a log line (§10: never payloads, never secrets). Case-insensitive. */
const FORBIDDEN = /^(payload|body|rawbody|headers|secret|.*secret.*|authorization|cookie|signature|.*token.*|password)$/i;

function scrubValue(value: unknown, depth: number): unknown {
  if (Array.isArray(value)) return depth > 4 ? "[truncated]" : value.map((v) => scrubValue(v, depth + 1));
  if (value && typeof value === "object" && Object.getPrototypeOf(value) === Object.prototype) {
    if (depth > 4) return "[truncated]";
    return scrub(value as LogFields, depth + 1);
  }
  return value;
}

function scrub(fields: LogFields, depth = 0): LogFields {
  const out: LogFields = {};
  for (const [key, value] of Object.entries(fields)) {
    if (value === undefined) continue;
    // Redacted config values are already "set"/"unset" strings; any other value under a secret-like key is dropped.
    if (FORBIDDEN.test(key) && !(value === "set" || value === "unset")) continue;
    out[key] = scrubValue(value, depth);
  }
  return out;
}

/** One JSON line per event on stdout. */
export function jsonLogger(write: (line: string) => void = (line) => process.stdout.write(line + "\n")): Logger {
  return {
    event(event, fields = {}) {
      write(JSON.stringify({ ts: new Date().toISOString(), event, ...scrub(fields) }));
    },
  };
}

/** Captures events in memory; used by tests and the e2e storm report. */
export class MemoryLogger implements Logger {
  readonly events: (LogFields & { event: string })[] = [];
  event(event: string, fields: LogFields = {}): void {
    this.events.push({ event, ...scrub(fields) });
  }
  named(event: string): (LogFields & { event: string })[] {
    return this.events.filter((e) => e.event === event);
  }
}

/** Stable, non-reversible lane label for logs and metrics (sha256, 16 hex). */
export function laneHash(lane: string): string {
  return createHash("sha256").update(lane).digest("hex").slice(0, 16);
}
