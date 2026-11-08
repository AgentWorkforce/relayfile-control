import type { EnvelopeKind, Ingress, NativeIds } from "./model.ts";

/** What ingress learns about one verified, parsed request (§2.3). */
export type Classified =
  | {
      drop: false;
      kind: EnvelopeKind;
      provider: string;
      /** A real provider delivery id, or null when only the request id identifies it. */
      deliveryId: string | null;
      eventType: string | null;
      connectionId: string | null;
      nativeIds: NativeIds;
    }
  | { drop: true; reason: "missing_nango_provider_identifier"; connectionId: string | null };

type Headers = Record<string, string>;

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function str(value: unknown): string | undefined {
  if (typeof value === "string" && value.length > 0) return value;
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return undefined;
}

function header(headers: Headers, name: string): string | undefined {
  const v = headers[name];
  return v && v.trim().length > 0 ? v.trim() : undefined;
}

/** GitHub sends both on every webhook; requiring the pair avoids claiming unrelated forwards. */
function hasGitHubHeaders(headers: Headers): boolean {
  return Boolean(header(headers, "x-github-delivery") && header(headers, "x-github-event"));
}

function nangoProviderIdentifier(body: Record<string, unknown> | null, headers: Headers): string | undefined {
  const payload = isObject(body?.payload) ? body.payload : {};
  const pick = (v: unknown) => (typeof v === "string" || typeof v === "number" ? String(v).trim() : "") || undefined;
  const explicit = pick(body?.from) ?? pick(body?.provider) ?? pick(payload.from) ?? pick(payload.provider);
  if (explicit) return explicit;
  if (str(body?.type) === "forward") {
    if (header(headers, "x-slack-signature") || header(headers, "x-slack-request-timestamp")) return "slack-relay";
    if (hasGitHubHeaders(headers)) return "github-relay";
  }
  // Raw GitHub forward (no Nango envelope at all).
  if (hasGitHubHeaders(headers) && isObject(body) && (body.repository !== undefined || body.installation !== undefined)) {
    return "github-relay";
  }
  return undefined;
}

/** Provider family used for precedence and deadline tables: prefix before the first `-`. */
export function providerFamily(provider: string): string {
  return provider.toLowerCase().split("-")[0] ?? provider.toLowerCase();
}

/**
 * Delivery id precedence (§2.3, Cloud's): github `github:<x-github-delivery>`;
 * nango headers; gitlab; slack `event_id`; linear header → `_connection.deliveryId`;
 * jira; notion `id`; generic `x-github-delivery` → `webhook-id` → `x-hookdeck-eventid`.
 * Null means "only the minted request id identifies this request".
 */
export function resolveDeliveryId(input: {
  ingress: Ingress;
  provider: string;
  headers: Headers;
  /** The provider-shaped object (the Nango `payload` for Nango envelopes). */
  inner: Record<string, unknown>;
  nangoEnvelope: boolean;
}): string | null {
  const { headers, inner } = input;
  if (input.ingress === "github" || (hasGitHubHeaders(headers) && providerFamily(input.provider) === "github")) {
    const id = header(headers, "x-github-delivery");
    return id ? `github:${id}` : null;
  }
  if (input.nangoEnvelope) {
    const id = header(headers, "x-nango-delivery-id") ?? header(headers, "x-nango-webhook-id") ?? header(headers, "webhook-id");
    if (id) return id;
  }
  switch (providerFamily(input.provider)) {
    case "gitlab": {
      const id = header(headers, "x-gitlab-event-uuid");
      if (id) return id;
      break;
    }
    case "slack": {
      const id = str(inner.event_id);
      if (id) return id;
      break;
    }
    case "linear": {
      const conn = isObject(inner._connection) ? inner._connection : {};
      const id = header(headers, "linear-delivery") ?? str(conn.deliveryId);
      if (id) return id;
      break;
    }
    case "jira": {
      const id = header(headers, "x-atlassian-webhook-identifier");
      if (id) return id;
      break;
    }
    case "notion": {
      const id = str(inner.id);
      if (id) return id;
      break;
    }
    default:
      break;
  }
  return header(headers, "x-github-delivery") ?? header(headers, "webhook-id") ?? header(headers, "x-hookdeck-eventid") ?? null;
}

function nativeIdsFrom(provider: string, inner: Record<string, unknown>): NativeIds {
  const ids: NativeIds = {};
  const family = providerFamily(provider);
  if (family === "github") {
    const installation = isObject(inner.installation) ? str(inner.installation.id) : undefined;
    if (installation) ids.installation_id = installation;
  }
  if (family === "slack") {
    const team = str(inner.team_id) ?? (isObject(inner.team) ? str(inner.team.id) : undefined);
    const enterprise = str(inner.enterprise_id) ?? (isObject(inner.enterprise) ? str(inner.enterprise.id) : undefined);
    if (team) ids.team_id = team;
    if (enterprise) ids.enterprise_id = enterprise;
  }
  if (family === "linear") {
    const org = str(inner.organizationId);
    if (org) ids.organization_id = org;
  }
  if (family === "jira") {
    const cloud = str(inner.cloudId) ?? str(inner.cloud_id);
    if (cloud) ids.cloud_id = cloud;
  }
  if (family === "google") {
    const email = str(inner.emailAddress) ?? str(inner.email);
    if (email) ids.email = email.toLowerCase();
  }
  return ids;
}

function eventTypeFrom(ingress: Ingress, provider: string, headers: Headers, inner: Record<string, unknown>): string | null {
  const family = providerFamily(provider);
  if (family === "github") {
    const event = header(headers, "x-github-event");
    const action = str(inner.action);
    if (event) return action ? `${event}.${action}` : event;
  }
  if (family === "slack") {
    const ev = isObject(inner.event) ? str(inner.event.type) : undefined;
    return ev ?? str(inner.type) ?? null;
  }
  if (family === "linear") {
    const type = str(inner.type);
    const action = str(inner.action);
    if (type) return action ? `${type}.${action}` : type;
  }
  if (ingress === "hookdeck") return header(headers, "x-gitlab-event") ?? null;
  return str(inner.type) ?? null;
}

const NANGO_KINDS: Record<string, EnvelopeKind> = { forward: "nango-forward", sync: "nango-sync", auth: "nango-auth" };

/**
 * Classify a verified, parsed body. `rawProvider` is set for the Hookdeck
 * Dropbox branch, the only raw-provider branch PR 1 accepts.
 */
export function classify(input: {
  ingress: Ingress;
  headers: Headers;
  body: unknown;
  rawProvider?: string;
}): Classified {
  const { ingress, headers } = input;
  const body = isObject(input.body) ? input.body : null;

  if (input.rawProvider) {
    return {
      drop: false,
      kind: "hookdeck-raw",
      provider: input.rawProvider,
      deliveryId: resolveDeliveryId({ ingress, provider: input.rawProvider, headers, inner: body ?? {}, nangoEnvelope: false }),
      eventType: null,
      connectionId: null,
      nativeIds: {},
    };
  }

  if (ingress === "github" || ingress === "slack" || ingress === "linear") {
    const provider = ingress;
    const inner = body ?? {};
    return {
      drop: false,
      kind: "provider-webhook",
      provider,
      deliveryId: resolveDeliveryId({ ingress, provider, headers, inner, nangoEnvelope: false }),
      eventType: eventTypeFrom(ingress, provider, headers, inner),
      connectionId: null,
      nativeIds: nativeIdsFrom(provider, inner),
    };
  }

  // nango or hookdeck-forwarded Nango envelope.
  const payload = isObject(body?.payload) ? body.payload : {};
  const connectionId = str(body?.connectionId) ?? str(payload.connectionId) ?? null;
  const provider = nangoProviderIdentifier(body, headers);
  if (!provider) return { drop: true, reason: "missing_nango_provider_identifier", connectionId };

  const type = str(body?.type);
  const rawGitHub = hasGitHubHeaders(headers) && !(type && NANGO_KINDS[type]);
  const kind: EnvelopeKind = rawGitHub ? "nango-forward" : (type && NANGO_KINDS[type]) || "nango-forward";
  const inner = rawGitHub ? (body ?? {}) : payload;
  return {
    drop: false,
    kind,
    provider: provider.slice(0, 64),
    deliveryId: resolveDeliveryId({ ingress, provider, headers, inner, nangoEnvelope: !rawGitHub }),
    eventType: kind === "nango-forward" ? eventTypeFrom(ingress, provider, headers, inner) : kind === "nango-sync" ? (str(body?.syncName) ?? str(payload.syncName) ?? null) : (str(body?.operation) ?? null),
    connectionId: rawGitHub ? null : connectionId,
    nativeIds: nativeIdsFrom(provider, inner),
  };
}
