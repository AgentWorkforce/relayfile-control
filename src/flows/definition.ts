import { parse } from "yaml";

export type DeclaredStep = {
  stepName: string;
  stepType: string | null;
  agent: string | null;
  dependsOn: string[];
  position: number;
};

export type DefinitionSummary = {
  name: string | null;
  description: string | null;
  format: "relayflows-v1" | "relayflows-v2" | "unknown";
  steps: DeclaredStep[];
};

function record(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function stringList(value: unknown): string[] {
  if (typeof value === "string") return [value];
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];
}

/**
 * Best-effort, read-only inspection of a YAML flow definition. Supports the
 * v1 shape (`workflows[].steps[].name`) the cloud worker executes and the v2
 * shape (`steps[].id`). TypeScript flows are opaque: their steps are only
 * known once the runtime reports them.
 */
export function summarizeDefinition(source: string, fileType: string): DefinitionSummary {
  const empty: DefinitionSummary = { name: null, description: null, format: "unknown", steps: [] };
  if (fileType !== "yaml") return empty;
  let document: unknown;
  try {
    document = parse(source, { maxAliasCount: 50 });
  } catch {
    return empty;
  }
  const root = record(document);
  if (!root) return empty;
  const name = text(root.name);
  const description = text(root.description);
  const steps: DeclaredStep[] = [];
  let format: DefinitionSummary["format"] = "unknown";
  if (Array.isArray(root.workflows)) {
    format = "relayflows-v1";
    for (const workflow of root.workflows) {
      const steps1 = record(workflow)?.steps;
      if (!Array.isArray(steps1)) continue;
      for (const raw of steps1) {
        const step = record(raw);
        const stepName = text(step?.name);
        if (!step || !stepName) continue;
        steps.push({
          stepName,
          stepType: text(step.type) ?? (text(step.agent) ? "agent" : null),
          agent: text(step.agent),
          dependsOn: stringList(step.dependsOn),
          position: steps.length,
        });
      }
    }
  } else if (Array.isArray(root.steps)) {
    format = "relayflows-v2";
    for (const raw of root.steps) {
      const step = record(raw);
      const stepName = text(step?.id) ?? text(step?.name);
      if (!step || !stepName) continue;
      steps.push({
        stepName,
        stepType: text(step.type),
        agent: text(step.agent),
        dependsOn: stringList(step.dependsOn),
        position: steps.length,
      });
    }
  }
  return { name, description, format, steps: steps.slice(0, 500) };
}
