import { joinUrl } from "./http.ts";

/**
 * Seams to relay-control for data that stays there after the Flows move:
 * subscription logins (CLI credentials) and each workspace's Relaycast
 * binding. Both are reached over HTTP with RELAYFILE_CONTROL_BROKER_SECRET as
 * the bearer; relay-control verifies it and enforces the run_id + ref_hash
 * binding itself, this side always sends both.
 */

/**
 * Subscription logins (Codex/Grok auth files) for a run. The assignment
 * payload carries only a short-lived reference; the worker exchanges it, with
 * its own worker credential, for the files at
 * GET .../assignments/{runId}/cli-credentials.
 */
export type CliCredentialBroker = {
  issueRunReference(workspaceId: string, runId: string): Promise<{
    ref: string;
    expiresAt: string;
    credentials: Array<{ credentialId: string; provider: string; cli: string }>;
  } | null>;
  readForRun(ref: string | null, runId: string): Promise<{
    files: Array<{
      credentialId: string;
      provider: string;
      cli: string;
      homeEnv: string;
      directory: string;
      file: string;
      content: string;
    }>;
    skipped: Array<{ credentialId: string; reason: string }>;
  }>;
  writeBack(ref: string | null, runId: string, credentialId: string, content: unknown): Promise<{ updated: boolean }>;
  /** Every reference issued for the run stops working. Idempotent. */
  revokeForRun(runId: string): Promise<void>;
};

/** The Relaycast workspace a run's agents join, and its API key. */
export type RelaycastCredentials = { relaycastWorkspaceId: string; relaycastApiKey: string };
export type RelaycastCredentialSource = (workspaceId: string) => Promise<RelaycastCredentials>;

/**
 * relay-control's Flows credential broker routes (the contract): issue a run
 * reference, read for a run, write back, revoke for a run (POST, JSON
 * bodies) and the workspace Relaycast binding (GET). Every call carries
 * `Authorization: Bearer <RELAYFILE_CONTROL_BROKER_SECRET>`.
 */
export const BROKER_PATHS = {
  issueRunReference: "/internal/v1/flows/credentials/references",
  readForRun: "/internal/v1/flows/credentials/read",
  writeBack: "/internal/v1/flows/credentials/write-back",
  revokeForRun: "/internal/v1/flows/credentials/revoke",
  relaycastCredentials: (workspaceId: string) =>
    `/internal/v1/flows/credentials/workspaces/${encodeURIComponent(workspaceId)}/relaycast`,
} as const;

/**
 * A refusal relay-control answered (4xx with `{error: code, message}`). It
 * carries `status` and `code`, so the worker routes surface it unchanged.
 */
export class BrokerError extends Error {
  constructor(readonly status: number, readonly code: string, message: string) {
    super(message);
    this.name = "BrokerError";
  }
}

/**
 * relay-control refused the workspace's Relaycast binding (4xx): the
 * workspace has no binding or this service is not authorized. Retrying does
 * not help until an operator fixes the configuration.
 */
export class RelaycastBindingRefused extends Error {
  constructor(readonly status: number, readonly code: string) {
    super(`relay-control refused the relaycast binding (${status} ${code})`);
    this.name = "RelaycastBindingRefused";
  }
}

/** relay-control unreachable or failing (network, timeout, 5xx). Retryable. */
export class BrokerUnavailable extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BrokerUnavailable";
  }
}

export class RelaycastCredentialsUnavailable extends Error {
  constructor(message: string) {
    super(message);
    this.name = "RelaycastCredentialsUnavailable";
  }
}

type HttpSeamOptions = {
  /** relay-control base URL (RELAYCONTROL_BASE_URL). */
  baseUrl: string;
  /** RELAYFILE_CONTROL_BROKER_SECRET. Never logged. */
  secret: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
  paths?: typeof BROKER_PATHS;
};

function seamCaller(options: HttpSeamOptions) {
  const fetcher = options.fetchImpl ?? fetch;
  return async (method: "GET" | "POST", path: string, body?: unknown): Promise<{ status: number; body: unknown }> => {
    let response: Response;
    try {
      response = await fetcher(joinUrl(options.baseUrl, path), {
        method,
        headers: {
          authorization: `Bearer ${options.secret}`,
          accept: "application/json",
          ...(body !== undefined ? { "content-type": "application/json" } : {}),
        },
        ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
        signal: AbortSignal.timeout(options.timeoutMs ?? 10_000),
      });
    } catch (error) {
      throw new BrokerUnavailable(
        `relay-control unreachable: ${error instanceof Error ? error.name : "error"}`,
      );
    }
    const text = await response.text().catch(() => "");
    let parsed: unknown = null;
    try {
      parsed = text ? JSON.parse(text) : null;
    } catch {
      parsed = null;
    }
    if (response.status >= 500) throw new BrokerUnavailable(`relay-control answered ${response.status}`);
    if (response.status >= 400) {
      const coded = (parsed ?? {}) as { error?: unknown; message?: unknown };
      const code = typeof coded.error === "string" ? coded.error : "credential_broker_refused";
      const message = typeof coded.message === "string" ? coded.message : code;
      throw new BrokerError(response.status, code, message);
    }
    return { status: response.status, body: parsed };
  };
}

export function httpCliCredentialBroker(options: HttpSeamOptions): CliCredentialBroker {
  const call = seamCaller(options);
  const paths = options.paths ?? BROKER_PATHS;
  return {
    async issueRunReference(workspaceId, runId) {
      const { status, body } = await call("POST", paths.issueRunReference, { workspaceId, runId });
      // 204 or a null body: the workspace has no subscription logins.
      if (status === 204 || body === null) return null;
      return body as Awaited<ReturnType<CliCredentialBroker["issueRunReference"]>>;
    },
    async readForRun(ref, runId) {
      const { body } = await call("POST", paths.readForRun, { ref, runId });
      return body as Awaited<ReturnType<CliCredentialBroker["readForRun"]>>;
    },
    async writeBack(ref, runId, credentialId, content) {
      const { body } = await call("POST", paths.writeBack, { ref, runId, credentialId, content });
      return body as { updated: boolean };
    },
    async revokeForRun(runId) {
      await call("POST", paths.revokeForRun, { runId });
    },
  };
}

export function httpRelaycastCredentialSource(options: HttpSeamOptions): RelaycastCredentialSource {
  const call = seamCaller(options);
  const paths = options.paths ?? BROKER_PATHS;
  return async (workspaceId) => {
    let result: { body: unknown };
    try {
      result = await call("GET", paths.relaycastCredentials(workspaceId));
    } catch (error) {
      if (error instanceof BrokerError) throw new RelaycastBindingRefused(error.status, error.code);
      throw new RelaycastCredentialsUnavailable(
        `relaycast credentials unavailable: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    const body = (result.body ?? {}) as Partial<RelaycastCredentials>;
    if (typeof body.relaycastWorkspaceId !== "string" || typeof body.relaycastApiKey !== "string") {
      throw new RelaycastCredentialsUnavailable("relaycast credentials response is malformed");
    }
    return { relaycastWorkspaceId: body.relaycastWorkspaceId, relaycastApiKey: body.relaycastApiKey };
  };
}
