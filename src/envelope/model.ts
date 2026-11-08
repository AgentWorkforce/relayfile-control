import { createHash } from "node:crypto";

export type EnvelopeKind = "nango-forward" | "nango-sync" | "nango-auth" | "provider-webhook" | "hookdeck-raw";
export type Ingress = "github" | "nango" | "hookdeck" | "slack" | "linear";
export type EnvelopeState = "queued" | "leased" | "done" | "dead";

/** Native provider ids usable for workspace resolution (§5). */
export type NativeIds = {
  installation_id?: string;
  team_id?: string;
  enterprise_id?: string;
  organization_id?: string;
  cloud_id?: string;
  email?: string;
};

export type EnvelopeRow = {
  id: string;
  parent_id: string | null;
  kind: EnvelopeKind;
  ingress: Ingress;
  provider: string;
  request_id: string;
  delivery_id: string;
  has_provider_delivery_id: boolean;
  event_type: string | null;
  connection_id: string | null;
  native_ids: NativeIds;
  dedupe_surface: string | null;
  dedupe_id: string | null;
  headers: Record<string, string>;
  payload: unknown | null;
  payload_blob_id: string | null;
  payload_sha256: string;
  payload_bytes: number;
  received_at: Date;
  workspace_id: string | null;
  state: EnvelopeState;
  attempts: number;
  visible_at: Date;
  leased_by: string | null;
  lease_until: Date | null;
  lane: string;
  last_outcome: Record<string, unknown> | null;
  outcome_history: Record<string, unknown>[];
  preservation_cycle: number;
  replayed_by: string | null;
};

export function sha256Hex(input: string | Uint8Array): string {
  return createHash("sha256").update(input).digest("hex");
}

/**
 * Length-prefixed tuple encoding (`len:value|len:value`), so no choice of
 * values can make two different tuples collide (§3).
 */
export function dedupeKey(parts: readonly string[]): string {
  return parts.map((p) => `${Buffer.byteLength(p, "utf8")}:${p}`).join("|");
}

/** Headers kept on the envelope: lower-cased, allow-listed, never Authorization. */
const HEADER_ALLOW = [
  /^x-github-(delivery|event|hook-id|hook-installation-target-id|hook-installation-target-type)$/,
  /^x-nango-(delivery-id|webhook-id)$/,
  /^webhook-id$/,
  /^x-hookdeck-(eventid|event-id|source-name|source-id|attempt-count)$/,
  /^x-gitlab-(event|event-uuid|instance)$/,
  /^x-atlassian-webhook-identifier$/,
  /^linear-(delivery|event)$/,
  /^x-slack-retry-(num|reason)$/,
  /^user-agent$/,
  /^content-type$/,
];

export function allowListHeaders(headers: Record<string, string | string[] | undefined>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [rawName, rawValue] of Object.entries(headers)) {
    const name = rawName.toLowerCase();
    if (name === "authorization" || name === "cookie") continue;
    if (!HEADER_ALLOW.some((re) => re.test(name))) continue;
    const value = Array.isArray(rawValue) ? rawValue.join(", ") : rawValue;
    if (typeof value === "string" && value.length > 0) out[name] = value.slice(0, 512);
  }
  return out;
}

/**
 * Stable Relayfile envelope id (§6.1): identical across retries and pods.
 * The envelope-only path has no file path, so path is "" (ruling 6, pending
 * layered-relayfile confirmation) and providerEventType = eventType.
 */
export function relayfileEnvelopeId(input: {
  provider: string;
  workspaceId: string;
  deliveryId: string;
  path?: string;
  providerEventType?: string;
  eventType?: string;
}): string {
  const providerSlug = input.provider.replace(/[^a-z0-9_-]/gi, "-").slice(0, 64) || "unknown";
  const digest = sha256Hex(
    JSON.stringify([input.workspaceId, input.deliveryId, input.path ?? "", input.providerEventType ?? "", input.eventType ?? ""]),
  );
  return `${providerSlug}_${digest}`;
}
