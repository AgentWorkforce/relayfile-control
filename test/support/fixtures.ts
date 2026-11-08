import { ulid } from "../../src/envelope/ulid.ts";
import type { NewEnvelope } from "../../src/queue/store.ts";

export function envelope(lane: string, over: Partial<NewEnvelope> = {}): NewEnvelope {
  const body = { n: Math.random() };
  return {
    kind: "provider-webhook",
    ingress: "github",
    provider: "github",
    requestId: ulid(),
    providerDeliveryId: null,
    eventType: "push",
    connectionId: null,
    nativeIds: {},
    headers: {},
    raw: Buffer.from(JSON.stringify(body)),
    parsed: body,
    receivedAt: new Date(),
    lane,
    workspaceId: lane.startsWith("rw_") ? lane : null,
    inlineMaxBytes: 96 * 1024,
    ...over,
  };
}

