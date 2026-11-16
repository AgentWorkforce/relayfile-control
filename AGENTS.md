# Working in relayfile-control

```sh
npm ci
npm run typecheck
npm run test
npm run build
```

- Own provider ingestion and queues plus Flow definitions, validation, runs, events, artifacts and worker lifecycle. Canonical identity, sessions and credentials stay in relay-control.
- Authenticate through relay-control introspection and consume `/internal/v1/flows/credentials/*`; preserve workspace, run and reference binding, scopes, audiences, lifetime, revocation and refusal-versus-outage semantics.
- Flow enrollment tokens and encrypted per-run caches are local domain state, not a second user or workspace credential authority.
- Use HTTP seams; do not import sibling applications, query the identity database or copy migrations. Update consumers and seam tests with contract changes.
- Read the README Seams section and `test/flows-seams.test.ts`. Do not automatically mirror to legacy `relay-control/src/flows` or copy Cloud-only UI and readiness.

## Agency and guidance

- Act autonomously on routine assigned work; ask forgiveness rather than permission. Preserve explicit production, release and credential gates.
- For guidance, ask the engineering leads, the agent that spawned you, or the layered owner on Agent Relay.
- Only a truly pressing or blocking question goes to `khaliq-gant` or `will-washburn` (CODEOWNERS) on Agent Relay.

## Cross-repo context

- Before and during changes, understand how this repo fits the wider system; consult its repo map and related-repo docs where available.
- Check relevant agents’ summaries on Agent Relay and proactively contact owning agents about dependencies, adjacent changes and work in flight.

## Pull requests

- Open PRs ready for review, never as drafts. If merging requires Khaliq’s OK, use `merge-train:hold`.
- Immediately subscribe to PR feedback through Agent Relay using [this skill](https://github.com/AgentWorkforce/skills/blob/main/skills/subscribing-relay-webhooks-and-writebacks/SKILL.md), and verify the subscription.
- Evaluate every finding. Address applicable feedback and explain declined changes; a PR with unaddressed feedback or conflicts is not ready.
- When ready, loudly alert the CODEOWNERS and the agent that spawned you on Agent Relay.
- Keep related older PRs moving by contacting their author agent; if unavailable, escalate to the CODEOWNERS or layered owner.
