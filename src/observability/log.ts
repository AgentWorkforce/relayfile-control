import { createHash } from "node:crypto";

export type LogFields = Record<string, unknown>;

export interface Logger {
  event(event: string, fields?: LogFields): void;
}

/** Field names that must never appear in a log line (§10: never payloads, never secrets). */
const FORBIDDEN = new Set(["payload", "body", "rawBody", "secret", "authorization", "signature", "token"]);

function scrub(fields: LogFields): LogFields {
  const out: LogFields = {};
  for (const [key, value] of Object.entries(fields)) {
    if (FORBIDDEN.has(key)) continue;
    if (value === undefined) continue;
    out[key] = value;
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
