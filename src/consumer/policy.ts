import { ADMISSION_ATTEMPT_TIMEOUT_MS } from "../relayfile/admission-retry.ts";
import { ENVELOPE_ADMISSION_POLICY, type InPlacePolicy } from "../relayfile/client.ts";

const LONG_DEADLINE_PROVIDERS = ["github", "google-mail", "linear", "notion"];

/** `provider` is a long-deadline name, or one followed by `-…` (e.g. github-relay). */
function isLongDeadline(provider: string): boolean {
  const p = provider.toLowerCase();
  return LONG_DEADLINE_PROVIDERS.some((name) => p === name || p.startsWith(`${name}-`));
}

/** §4.3 HARD: 120 s for github / google-mail / linear / notion (prefix match on `-`), else 15 s. */
export function handlerDeadlineMs(provider: string): number {
  return isLongDeadline(provider) ? 120_000 : 15_000;
}

/** §6.2: one in-place retry for long-deadline providers, none under the 15 s deadline. */
export function inPlaceRetryPolicy(provider: string): InPlacePolicy {
  return isLongDeadline(provider)
    ? { ...ENVELOPE_ADMISSION_POLICY }
    : { maxRetries: 0, budgetMs: 0, attemptTimeoutMs: ADMISSION_ATTEMPT_TIMEOUT_MS };
}

/** Envelope admission policy bounded by the provider's in-place policy (Slack/Jira get no in-place retry). */
export function envelopePolicy(provider: string): InPlacePolicy {
  const inPlace = inPlaceRetryPolicy(provider);
  return {
    maxRetries: Math.min(ENVELOPE_ADMISSION_POLICY.maxRetries, inPlace.maxRetries),
    budgetMs: Math.min(ENVELOPE_ADMISSION_POLICY.budgetMs, inPlace.budgetMs),
    attemptTimeoutMs: Math.min(ENVELOPE_ADMISSION_POLICY.attemptTimeoutMs, inPlace.attemptTimeoutMs),
  };
}
