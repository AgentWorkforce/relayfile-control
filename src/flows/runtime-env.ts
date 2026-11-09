/**
 * Workspace runtime environment for flow runs, from the admin lane's internal
 * runtime contract:
 *   GET /internal/v1/workspaces/{id}/environment?includeIntegrations=1
 * authenticated with SELFHOST_INTERNAL_TOKEN.
 *
 * The stock `agent-relay cloud worker` cannot call this itself, so the control
 * plane resolves it just in time when it builds an assignment payload for the
 * authenticated worker that claimed the run. Values are never persisted and
 * never written to run events or logs.
 */
export type RuntimeEnvironment = {
  env: Record<string, string>;
  secretEnvNames: string[];
  skipped: Array<{ name: string; reason: string }>;
  integrationErrors: Array<{ provider: string; error: string }>;
};

export type RuntimeEnvironmentSource = (workspaceId: string) => Promise<RuntimeEnvironment>;

export class RuntimeEnvironmentUnavailable extends Error {}

export function httpRuntimeEnvironmentSource(input: {
  baseUrl: string;
  token: string;
  fetchImpl?: typeof fetch;
  timeoutMs?: number;
}): RuntimeEnvironmentSource {
  const fetcher = input.fetchImpl ?? fetch;
  const base = input.baseUrl.replace(/\/+$/, "");
  return async (workspaceId) => {
    let response: Response;
    try {
      response = await fetcher(
        `${base}/internal/v1/workspaces/${encodeURIComponent(workspaceId)}/environment?includeIntegrations=1`,
        {
          headers: { authorization: `Bearer ${input.token}`, accept: "application/json" },
          signal: AbortSignal.timeout(input.timeoutMs ?? 10_000),
        },
      );
    } catch (error) {
      throw new RuntimeEnvironmentUnavailable(
        `runtime environment unreachable: ${error instanceof Error ? error.message : String(error)}`,
      );
    }
    // An installation without the admin module has no environment route:
    // runs proceed with their own envSecrets only.
    if (response.status === 404) {
      const body = await response.json().catch(() => null) as { error?: string } | null;
      if (body?.error === "not_found") return { env: {}, secretEnvNames: [], skipped: [], integrationErrors: [] };
    }
    if (!response.ok) {
      throw new RuntimeEnvironmentUnavailable(`runtime environment request failed: ${response.status}`);
    }
    const body = await response.json() as Partial<RuntimeEnvironment>;
    const env: Record<string, string> = {};
    for (const [name, value] of Object.entries(body.env ?? {})) {
      if (typeof value === "string") env[name] = value;
    }
    return {
      env,
      secretEnvNames: Array.isArray(body.secretEnvNames)
        ? body.secretEnvNames.filter((name): name is string => typeof name === "string")
        : Object.keys(env),
      skipped: Array.isArray(body.skipped) ? body.skipped : [],
      integrationErrors: Array.isArray(body.integrationErrors) ? body.integrationErrors : [],
    };
  };
}

/** Run-managed values win over workspace values (hosted precedence). */
export function mergeRunEnvironment(
  workspace: RuntimeEnvironment | null,
  runSecrets: Record<string, string> | undefined,
): Record<string, string> | undefined {
  const merged: Record<string, string> = { ...(workspace?.env ?? {}), ...(runSecrets ?? {}) };
  delete merged.RELAY_SECRET_ENV_NAMES;
  const names = Object.keys(merged);
  if (!names.length) return undefined;
  const secretNames = [...new Set([...(workspace?.secretEnvNames ?? []), ...Object.keys(runSecrets ?? {})])]
    .filter((name) => name in merged)
    .sort();
  return { ...merged, RELAY_SECRET_ENV_NAMES: JSON.stringify(secretNames) };
}
