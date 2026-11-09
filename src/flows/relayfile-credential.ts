import { parseOrigin } from "../config.ts";
import type { RelayfileCredential } from "./broker.ts";
import type { Introspection } from "./introspection.ts";
import { relayfileScopesFor } from "./relayfile-mount.ts";

/**
 * Checks on a freshly issued per-run Relayfile credential before it is handed
 * to a worker (docs/design/selfhost-flows-relayfile-token.md §2.4).
 */

/** `exp` must lie at least this far in the future. */
export const MIN_REMAINING_MS = 60_000;
/** Tolerance between the issuer's `relayfileTokenExpiresAt` and `exp`. */
export const EXPIRY_TOLERANCE_MS = 1_000;

/** Codes a run event may carry when no Relayfile credential was handed out. */
export const KNOWN_ISSUER_REFUSALS: readonly string[] = [
  "invalid_request",
  "invalid_mount_paths",
  "relayfile_not_configured",
  "workspace_not_found",
  "relayfile_audience_not_accepted",
];

/** The worker-facing Relayfile origin (`SELFHOST_FLOWS_WORKER_RELAYFILE_URL` normalized). */
export function relayfileOrigin(url: string): string | null {
  const parsed = parseOrigin(url);
  if (parsed) return parsed;
  try {
    const value = new URL(url);
    return value.protocol === "http:" || value.protocol === "https:" ? value.origin : null;
  } catch {
    return null;
  }
}

function sameSet(a: readonly string[], b: readonly string[]): boolean {
  const left = new Set(a);
  const right = new Set(b);
  if (left.size !== right.size) return false;
  for (const value of left) if (!right.has(value)) return false;
  return true;
}

/**
 * Returns null when the introspection answer vouches for the credential, else
 * the reason: the inactive reason, `principal`, `credential_id`, `workspace`,
 * `audience`, `expiry`, `scopes` or `mount_paths`.
 */
export function verifyRelayfileCredential(input: {
  credential: RelayfileCredential;
  answer: Introspection;
  runId: string;
  workspaceId: string;
  relayfileOrigin: string;
  requestedMountPaths: readonly string[];
  /** What was asked of the issuer; the grant must be exactly this shape. */
  readOnly: boolean;
  nowMs: number;
}): string | null {
  const { credential, answer } = input;
  if (!answer.active) return answer.reason || "inactive";
  if (answer.principal_type !== "agent" || answer.agent_name !== `flows-run-${input.runId}`) return "principal";
  if (answer.token_id !== credential.credentialId) return "credential_id";
  if (answer.workspace !== input.workspaceId) return "workspace";
  const answered = answer.aud === undefined ? null : parseOrigin(answer.aud);
  if (answered === null || answered !== input.relayfileOrigin) return "audience";
  const expMs = answer.exp * 1000;
  const issuedExpiry = Date.parse(credential.relayfileTokenExpiresAt);
  if (
    expMs < input.nowMs + MIN_REMAINING_MS
    || !Number.isFinite(issuedExpiry)
    || Math.abs(expMs - issuedExpiry) > EXPIRY_TOLERANCE_MS
  ) {
    return "expiry";
  }
  // The grant must be exactly the set derived from the issued mount paths
  // for the requested mode: the worker derives its expectations from
  // `relayfileMountPaths`, so a narrower (read-only) or wider grant would
  // only surface at Relayfile.
  const expectedScopes = relayfileScopesFor(credential.relayfileMountPaths, input.readOnly);
  if (!sameSet(answer.scopes, credential.scopes) || !sameSet(answer.scopes, expectedScopes)) return "scopes";
  // Compared as sets of the same size: the issuer may not keep the order.
  const issued = credential.relayfileMountPaths;
  if (issued.length !== input.requestedMountPaths.length || !sameSet(issued, input.requestedMountPaths)) return "mount_paths";
  return null;
}
