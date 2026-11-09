import assert from "node:assert/strict";
import { randomBytes, randomUUID } from "node:crypto";
import type { Db } from "../../src/db/db.ts";
import type { CliCredentialBroker, RelaycastCredentials, RelaycastCredentialSource } from "../../src/flows/broker.ts";
import { defaultFlowsConfig, type FlowsConfig } from "../../src/flows/config.ts";
import { flowsDatabase } from "../../src/flows/database.ts";
import { introspectionUserAccess, type Introspection, type Introspector } from "../../src/flows/introspection.ts";
import { FlowsStore } from "../../src/flows/store.ts";
import { fixtureConfig, newPglite, startApp } from "./harness.ts";

export const PUBLIC_BASE_URL = "https://control.selfhosted.company";
export const RELAYCAST_PUBLIC_URL = "https://relaycast.selfhosted.company";

/**
 * Fake relay-control introspection: tokens minted by `mint` answer as active
 * desktop-user tokens; anything else is inactive `unknown`.
 */
export class FakeIntrospection {
  readonly tokens = new Map<string, Introspection>();
  readonly calls: Array<{ token: string; workspace?: string }> = [];

  mint(input: { userId: string; email: string; organizationId: string; workspaceId: string; role: "owner" | "admin" | "member" }): string {
    const token = `sh_at_${randomBytes(16).toString("hex")}`;
    this.tokens.set(token, {
      active: true,
      sub: input.userId,
      email: input.email,
      principal_type: "desktop-user",
      org: input.organizationId,
      workspace: input.workspaceId,
      role: input.role,
      scopes: [],
      exp: Math.floor(Date.now() / 1000) + 3600,
      token_id: randomBytes(16).toString("hex"),
    });
    return token;
  }

  readonly introspect: Introspector = async (token, workspace) => {
    this.calls.push({ token, ...(workspace !== undefined ? { workspace } : {}) });
    return this.tokens.get(token) ?? { active: false, reason: "unknown" };
  };
}

/** Fake relay-control credential broker: records every call. */
export class FakeBroker implements CliCredentialBroker {
  readonly calls: Array<{ op: string; args: unknown[] }> = [];
  revokeFailures = 0;
  /** workspaceId -> the reference issueRunReference answers with. */
  references = new Map<string, NonNullable<Awaited<ReturnType<CliCredentialBroker["issueRunReference"]>>>>();

  async issueRunReference(workspaceId: string, runId: string) {
    this.calls.push({ op: "issueRunReference", args: [workspaceId, runId] });
    return this.references.get(workspaceId) ?? null;
  }

  async readForRun(ref: string | null, runId: string): ReturnType<CliCredentialBroker["readForRun"]> {
    this.calls.push({ op: "readForRun", args: [ref, runId] });
    return { files: [], skipped: [] };
  }

  async writeBack(ref: string | null, runId: string, credentialId: string, content: unknown) {
    this.calls.push({ op: "writeBack", args: [ref, runId, credentialId, content] });
    return { updated: true };
  }

  async revokeForRun(runId: string) {
    this.calls.push({ op: "revokeForRun", args: [runId] });
    if (this.revokeFailures > 0) {
      this.revokeFailures -= 1;
      throw new Error("broker down");
    }
  }
}

/** workspaceId -> the workspace's Relaycast binding (as relay-control holds it). */
export function fakeRelaycast(bindings: Map<string, RelaycastCredentials>): RelaycastCredentialSource {
  return async (workspaceId) => {
    const binding = bindings.get(workspaceId);
    if (!binding) throw new Error("no relaycast binding");
    return { ...binding };
  };
}

export type Api = (
  path: string,
  options?: { method?: string; token?: string; body?: unknown; raw?: string; headers?: Record<string, string> },
) => Promise<{ status: number; body: any; headers: Headers; text: string }>;

export function apiClient(origin: string): Api {
  return async (path, options = {}) => {
    const response = await fetch(`${origin}${path}`, {
      method: options.method ?? "GET",
      headers: {
        ...(options.token ? { authorization: `Bearer ${options.token}` } : {}),
        ...(options.body !== undefined ? { "content-type": "application/json" } : {}),
        ...options.headers,
      },
      ...(options.body !== undefined
        ? { body: JSON.stringify(options.body) }
        : options.raw !== undefined
          ? { body: options.raw }
          : {}),
    });
    const text = await response.text();
    let body: any = {};
    try {
      body = text ? JSON.parse(text) : {};
    } catch {
      body = { raw: text };
    }
    return { status: response.status, body, headers: response.headers, text };
  };
}

export const ENCRYPTION_KEYRING = { active: { id: "test-key", key: Buffer.alloc(32, 7) } };

/**
 * relayfile-control with the Flows API mounted, a fake introspection, a fake
 * credential broker and a fake Relaycast binding source.
 */
export async function startControlPlane(flowsOverrides: Partial<FlowsConfig> = {}) {
  const db: Db = await newPglite();
  const introspection = new FakeIntrospection();
  const broker = new FakeBroker();
  const relaycastKeys = new Map<string, RelaycastCredentials>();
  const flows = new FlowsStore(
    flowsDatabase(db),
    ENCRYPTION_KEYRING,
    defaultFlowsConfig({ queuePollMs: 25, queuePingMs: 200, ...flowsOverrides }),
    RELAYCAST_PUBLIC_URL,
  );
  flows.cliCredentials = broker;
  flows.relaycastCredentials = fakeRelaycast(relaycastKeys);
  const app = await startApp({
    db,
    config: fixtureConfig(),
    flows: { store: flows, userAccess: introspectionUserAccess(introspection.introspect), publicBaseUrl: PUBLIC_BASE_URL },
  });
  return {
    database: db,
    flows,
    introspection,
    broker,
    relaycastKeys,
    origin: app.url,
    api: apiClient(app.url),
    app,
    async stop() {
      await app.close();
      await db.close();
    },
  };
}

/**
 * The tenant relay-control would own: an organization, two workspaces with
 * Relaycast bindings, an owner and a member, and their access tokens.
 */
export async function seedTenant(cp: Awaited<ReturnType<typeof startControlPlane>>) {
  const organizationId = randomUUID();
  const workspaceId = randomUUID();
  const otherWorkspaceId = randomUUID();
  const ownerId = randomUUID();
  const memberId = randomUUID();
  cp.relaycastKeys.set(workspaceId, { relaycastWorkspaceId: "rc-primary", relaycastApiKey: "rk_live_primary" });
  cp.relaycastKeys.set(otherWorkspaceId, { relaycastWorkspaceId: "rc-other", relaycastApiKey: "rk_live_other" });
  const mint = cp.introspection.mint.bind(cp.introspection);
  const tenant = {
    organizationId,
    workspaceId,
    otherWorkspaceId,
    ownerToken: mint({ userId: ownerId, email: "owner@acme.test", organizationId, workspaceId, role: "owner" }),
    otherOwnerToken: mint({ userId: ownerId, email: "owner@acme.test", organizationId, workspaceId: otherWorkspaceId, role: "owner" }),
    memberToken: mint({ userId: memberId, email: "member@acme.test", organizationId, workspaceId, role: "member" }),
  };
  assert.notEqual(tenant.ownerToken, tenant.memberToken);
  return tenant;
}

export type SseEvent = { event: string; data: any };

/** Minimal SSE reader mirroring relay packages/cloud/src/worker.ts parsing. */
export function openQueue(origin: string, workerId: string, workerToken: string) {
  const controller = new AbortController();
  const events: SseEvent[] = [];
  const waiters: Array<() => void> = [];
  let ended = false;
  const ready = fetch(`${origin}/api/v1/workers/${workerId}/queue`, {
    headers: { authorization: `Bearer ${workerToken}` },
    signal: controller.signal,
  }).then(async (response) => {
    assert.equal(response.status, 200);
    assert.match(response.headers.get("content-type") ?? "", /text\/event-stream/);
    const reader = response.body!.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    void (async () => {
      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let index: number;
          while ((index = buffer.indexOf("\n\n")) !== -1) {
            const frame = buffer.slice(0, index);
            buffer = buffer.slice(index + 2);
            let event = "message";
            let data = "";
            for (const line of frame.split("\n")) {
              if (line.startsWith("event:")) event = line.slice(6).trim();
              else if (line.startsWith("data:")) data += line.slice(5).trim();
            }
            events.push({ event, data: data ? JSON.parse(data) : {} });
            waiters.splice(0).forEach((wake) => wake());
          }
        }
      } catch {
        // aborted
      } finally {
        ended = true;
        waiters.splice(0).forEach((wake) => wake());
      }
    })();
  });
  return {
    events,
    ready,
    get ended() {
      return ended;
    },
    async next(event: string, timeoutMs = 5_000): Promise<SseEvent> {
      const deadline = Date.now() + timeoutMs;
      let seen = 0;
      while (Date.now() < deadline) {
        for (; seen < events.length; seen += 1) {
          if (events[seen]!.event === event) return events.splice(seen, 1)[0]!;
        }
        if (ended) break;
        await new Promise<void>((resolve) => {
          waiters.push(resolve);
          setTimeout(resolve, 50);
        });
      }
      throw new Error(`timed out waiting for SSE ${event}; saw ${events.map((e) => e.event).join(",")}`);
    },
    close() {
      controller.abort();
    },
  };
}
