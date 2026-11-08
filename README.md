# relayfile-control

Integration webhook ingest for Relayfile: provider ingress (GitHub, Nango,
Hookdeck, Slack, Linear), durable envelope intake with idempotency, a Postgres
queue with per-workspace fairness and end-to-end backpressure, dead-letter
preservation and replay, and the writer that delivers envelopes and provider
records into Relayfile. Node 22 + Postgres, same conventions as
`AgentWorkforce/relay-control`.

Design: `docs/design/webhook-ingest.md`.

Status: design stage. No code yet; nothing deployed.
