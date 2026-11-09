import { parse } from "yaml";

/**
 * Per-run Relayfile mount paths and scopes (Cloud's
 * workflowRelayfileMountConfig semantics; docs/design/selfhost-flows-relayfile-token.md §2.2).
 */

/** Integration providers whose adapter root `/<provider>` a run may mount. */
export const MOUNTABLE_PROVIDERS: readonly string[] = ["github", "gitlab", "jira", "linear", "shortcut", "slack"];
/** Top-level `integrations:` keys that are not providers. */
export const RESERVED_INTEGRATION_KEYS: readonly string[] = ["relayfile", "subscriptions"];

export type MountRefusal = { code: "invalid_mount_paths"; reason: string };

export function runMountRoot(runId: string): string {
  return `/workflows/runs/${runId}`;
}

function integrationKeys(workflow: string): string[] {
  let document: unknown;
  try {
    document = parse(workflow, { uniqueKeys: false, logLevel: "silent" });
  } catch {
    return [];
  }
  if (!document || typeof document !== "object" || Array.isArray(document)) return [];
  const integrations = (document as Record<string, unknown>).integrations;
  if (!integrations || typeof integrations !== "object" || Array.isArray(integrations)) return [];
  return Object.keys(integrations);
}

/**
 * `[runRoot, ...providerRoots]`. YAML workflows mount the adapter root of
 * each provider in their top-level `integrations:` map (reserved keys
 * dropped, deduped, sorted); an unknown provider is a refusal, not a silent
 * drop. TS workflows, unparseable YAML and a non-map `integrations` mount the
 * run root only.
 */
export function relayfileMountPaths(input: {
  runId: string;
  workflow: string;
  fileType: "yaml" | "ts";
}): { mountPaths: string[] } | { refusal: MountRefusal } {
  const runRoot = runMountRoot(input.runId);
  if (input.fileType !== "yaml") return { mountPaths: [runRoot] };
  const providers = [...new Set(integrationKeys(input.workflow))]
    .filter((key) => !RESERVED_INTEGRATION_KEYS.includes(key))
    .sort();
  const unknown = providers.find((key) => !MOUNTABLE_PROVIDERS.includes(key));
  if (unknown !== undefined) {
    return { refusal: { code: "invalid_mount_paths", reason: `unmountable_provider:${unknown}` } };
  }
  return { mountPaths: [runRoot, ...providers.map((provider) => `/${provider}`)] };
}

/** The scopes the issuer grants for these mount roots (§1). */
export function relayfileScopesFor(mountPaths: readonly string[], readOnly: boolean): string[] {
  const actions = readOnly ? ["read"] : ["read", "write"];
  return mountPaths.flatMap((path) =>
    actions.flatMap((action) => [`relayfile:fs:${action}:${path}`, `relayfile:fs:${action}:${path}/*`]));
}
