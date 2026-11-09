-- Per-run Relayfile credential (docs/design/selfhost-flows-relayfile-token.md §2.3).
--
-- relayfile_credential_ciphertext holds relay-control's issue answer
-- (POST /internal/v1/flows/credentials/relayfile, 200 body, including the
-- sh_fr_ bearer) encrypted with the Flows keyring, like env_secrets_ciphertext.
-- relayfile_credential_refusal records `{code, reason?}` once the run was
-- refused a credential, so redelivery reuses the decision instead of asking
-- again. All four are cleared when the run finishes; the run's revocation
-- outbox row then revokes the credential in relay-control.

ALTER TABLE selfhost_workflow_runs
  ADD COLUMN IF NOT EXISTS relayfile_credential_ciphertext text,
  ADD COLUMN IF NOT EXISTS relayfile_credential_id text,
  ADD COLUMN IF NOT EXISTS relayfile_credential_expires_at timestamptz,
  ADD COLUMN IF NOT EXISTS relayfile_credential_refusal jsonb;
