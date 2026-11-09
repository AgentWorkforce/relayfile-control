import { createHash } from "node:crypto";
import type { IncomingMessage } from "node:http";
import { joinUrl } from "./http.ts";
import type { FlowsUserAccess, Principal } from "./routes.ts";

/**
 * Client for relay-control `POST /internal/v1/auth/introspect`.
 *
 * Request `{token, aud, workspace?}` with `Authorization: Bearer
 * <RELAYCONTROL_INTROSPECT_SECRET>`; `aud` is this service's public origin
 * (RELAYFILE_CONTROL_PUBLIC_ORIGIN), which relay-control must accept as an
 * introspection audience. relay-control answers 200 for every well-formed
 * authenticated request: `{active:false, reason}` or an active answer.
 *
 * relay-control's answer is not trusted blindly: an active answer whose
 * `aud` is not this service's audience, whose `exp` has passed, or whose
 * `iat` lies more than 60 s in the future is treated as inactive (reason
 * `audience`, `expired` or `issued_in_future`).
 *
 * Answers are cached for at most `cacheSeconds` (never past the token's
 * `exp`), keyed by sha256 of the token, the audience and the workspace. Raw
 * tokens are never stored or logged. Transport failures are not cached.
 */

export const INTROSPECT_PATH = "/internal/v1/auth/introspect";
export const MAX_INTROSPECTION_CACHE_MS = 30_000;
/** Clock skew tolerated for `iat`. */
export const IAT_SKEW_SECONDS = 60;

export type InactiveIntrospection = { active: false; reason: string };

export type ActiveIntrospection = {
  active: true;
  sub: string;
  email: string | null;
  principal_type: string;
  org: string;
  workspace: string;
  role?: Principal["role"];
  scopes: string[];
  exp: number;
  /** Audience relay-control checked the token against. */
  aud?: string;
  /** Issue time (seconds); absent when relay-control does not report it. */
  iat?: number;
  token_id: string;
};

export type Introspection = InactiveIntrospection | ActiveIntrospection;

export class IntrospectionUnavailable extends Error {
  override name = "IntrospectionUnavailable";
}

export type Introspector = (token: string, workspace?: string) => Promise<Introspection>;

type Cached = { value: Introspection; until: number };

function parseAnswer(body: unknown): Introspection {
  if (!body || typeof body !== "object" || Array.isArray(body)) throw new IntrospectionUnavailable("malformed introspection answer");
  const b = body as Record<string, unknown>;
  if (b.active === false) return { active: false, reason: typeof b.reason === "string" ? b.reason : "unknown" };
  if (b.active !== true) throw new IntrospectionUnavailable("malformed introspection answer");
  const str = (k: string) => (typeof b[k] === "string" && b[k] ? (b[k] as string) : null);
  const sub = str("sub");
  const org = str("org");
  const workspace = str("workspace");
  const principalType = str("principal_type");
  const tokenId = str("token_id");
  if (!sub || !org || !workspace || !principalType || !tokenId || typeof b.exp !== "number" || !Number.isFinite(b.exp)) {
    throw new IntrospectionUnavailable("malformed introspection answer");
  }
  const role = b.role === "owner" || b.role === "admin" || b.role === "member" ? b.role : undefined;
  return {
    active: true,
    sub,
    email: str("email"),
    principal_type: principalType,
    org,
    workspace,
    ...(role ? { role } : {}),
    scopes: Array.isArray(b.scopes) ? b.scopes.filter((s): s is string => typeof s === "string") : [],
    exp: b.exp,
    ...(str("aud") ? { aud: str("aud")! } : {}),
    ...(typeof b.iat === "number" && Number.isFinite(b.iat) ? { iat: b.iat } : {}),
    token_id: tokenId,
  };
}

/** Local claim checks on an active answer; returns the inactive reason or null. */
export function claimFailure(answer: ActiveIntrospection, audience: string, nowMs: number): string | null {
  if (answer.aud !== audience) return "audience";
  if (answer.exp * 1000 <= nowMs) return "expired";
  if (answer.iat !== undefined && answer.iat * 1000 > nowMs + IAT_SKEW_SECONDS * 1000) return "issued_in_future";
  return null;
}

export function createIntrospector(input: {
  baseUrl: string;
  secret: string;
  audience: string;
  cacheSeconds?: number;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  now?: () => number;
  maxEntries?: number;
}): Introspector {
  const fetchImpl = input.fetchImpl ?? fetch;
  const now = input.now ?? Date.now;
  const ttlMs = Math.min(MAX_INTROSPECTION_CACHE_MS, Math.max(0, (input.cacheSeconds ?? 30) * 1000));
  const maxEntries = input.maxEntries ?? 10_000;
  const cache = new Map<string, Cached>();
  const endpoint = joinUrl(input.baseUrl, INTROSPECT_PATH);

  return async (token, workspace) => {
    const key = createHash("sha256")
      .update(token)
      .update("\0")
      .update(input.audience)
      .update("\0")
      .update(workspace ?? "")
      .digest("hex");
    const at = now();
    const hit = cache.get(key);
    if (hit && hit.until > at) return hit.value;
    if (hit) cache.delete(key);

    let response: Response;
    try {
      response = await fetchImpl(endpoint, {
        method: "POST",
        headers: { authorization: `Bearer ${input.secret}`, "content-type": "application/json" },
        body: JSON.stringify({ token, aud: input.audience, ...(workspace !== undefined ? { workspace } : {}) }),
        signal: AbortSignal.timeout(input.timeoutMs ?? 5_000),
      });
    } catch (error) {
      throw new IntrospectionUnavailable(`introspection request failed: ${error instanceof Error ? error.name : "unknown"}`);
    }
    if (response.status !== 200) {
      await response.body?.cancel().catch(() => undefined);
      throw new IntrospectionUnavailable(`introspection answered ${response.status}`);
    }
    const parsed = parseAnswer(await response.json().catch(() => null));
    const failure = parsed.active ? claimFailure(parsed, input.audience, at) : null;
    const value: Introspection = failure ? { active: false, reason: failure } : parsed;
    let until = at + ttlMs;
    if (value.active) until = Math.min(until, value.exp * 1000);
    if (until > at) {
      if (cache.size >= maxEntries) {
        for (const [k, v] of cache) if (v.until <= at) cache.delete(k);
        if (cache.size >= maxEntries) cache.delete(cache.keys().next().value!);
      }
      cache.set(key, { value, until });
    }
    return value;
  };
}

/** The relay-control error a Flows user route answers with (`{error: code, message}`). */
export class FlowsAuthError extends Error {
  constructor(readonly status: number, readonly code: string, message: string) {
    super(message);
  }
}

export const SESSION_COOKIE = "agent_relay_session";

function bearer(request: IncomingMessage): string | null {
  const header = request.headers.authorization;
  if (!header) return null;
  const match = /^Bearer\s+(.+)$/i.exec(header);
  return match?.[1]?.trim() || null;
}

function cookie(request: IncomingMessage, name: string): string | null {
  const header = request.headers.cookie;
  if (!header) return null;
  for (const part of header.split(";")) {
    const index = part.indexOf("=");
    if (index < 0) continue;
    if (part.slice(0, index).trim() !== name) continue;
    const value = part.slice(index + 1).trim();
    try {
      return decodeURIComponent(value) || null;
    } catch {
      return null;
    }
  }
  return null;
}

/**
 * relay-control's CSRF rule for cookie-authenticated, state-changing
 * requests (src/auth/device-approval.ts): a JSON body, and an allowed
 * `Origin` (never `null`), or `Sec-Fetch-Site: same-origin` when the browser
 * sent no Origin.
 */
export function cookieRequestAllowed(request: IncomingMessage, allowedOrigins: readonly string[]): boolean {
  const contentType = request.headers["content-type"]?.split(";")[0]?.trim().toLowerCase();
  if (contentType !== "application/json") return false;
  const origin = request.headers.origin;
  if (typeof origin === "string" && origin) {
    return origin !== "null" && allowedOrigins.includes(origin);
  }
  return request.headers["sec-fetch-site"] === "same-origin";
}

/**
 * Flows user authentication with relay-control's semantics: a bearer, or the
 * session cookie only under `/cloud`; a desktop-user token whose workspace
 * is the caller's current workspace; owner/admin for admin routes. A
 * cookie-authenticated request other than GET must pass
 * `cookieRequestAllowed` against `allowedOrigins` (403 `csrf_rejected`).
 * When the route names a workspace it is sent to relay-control, which
 * answers inactive `workspace` (404 here) unless it is the token's.
 */
export function introspectionUserAccess(
  introspect: Introspector,
  options: { allowedOrigins?: readonly string[] } = {},
): FlowsUserAccess {
  const allowedOrigins = options.allowedOrigins ?? [];
  return async (request, admin = false, workspace) => {
    const headerToken = bearer(request);
    const requestPath = new URL(request.url ?? "/", "http://localhost").pathname;
    const cookieToken = requestPath === "/cloud" || requestPath.startsWith("/cloud/") ? cookie(request, SESSION_COOKIE) : null;
    const token = headerToken ?? cookieToken;
    if (!token) throw new FlowsAuthError(401, "unauthorized", "Authentication required");
    if (!headerToken && request.method !== "GET" && !cookieRequestAllowed(request, allowedOrigins)) {
      throw new FlowsAuthError(403, "csrf_rejected", "Cross-origin request refused");
    }
    let answer: Introspection;
    try {
      answer = await introspect(token, workspace);
    } catch (error) {
      if (error instanceof IntrospectionUnavailable) {
        throw new FlowsAuthError(503, "auth_unavailable", "Authentication is temporarily unavailable");
      }
      throw error;
    }
    if (!answer.active) {
      if (answer.reason === "workspace") throw new FlowsAuthError(404, "workspace_not_found", "Workspace not found");
      throw new FlowsAuthError(401, "unauthorized", "Invalid or expired token");
    }
    // relay-control's Flows routes accepted only user access tokens; agent
    // and service tokens never authenticated there.
    if (answer.principal_type !== "desktop-user" || !answer.email || !answer.role) {
      throw new FlowsAuthError(401, "unauthorized", "Invalid or expired token");
    }
    if (admin && answer.role !== "owner" && answer.role !== "admin") {
      throw new FlowsAuthError(403, "forbidden", "Workspace admin role required");
    }
    const principal: Principal = {
      userId: answer.sub,
      email: answer.email,
      organizationId: answer.org,
      workspaceId: answer.workspace,
      role: answer.role,
      scopes: answer.scopes,
      tokenId: answer.token_id,
      expiresAt: answer.exp,
    };
    return { principal, workspace: { id: answer.workspace, organizationId: answer.org, role: answer.role } };
  };
}
