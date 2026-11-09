-- Flows API storage (flow definitions, runs, the worker assignment protocol,
-- step/event ingestion, Postgres-backed run object storage), moved here from
-- relay-control with the same table and column names.
--
-- Identity columns hold relay-control ids and are deliberately NOT foreign
-- keys: users, workspaces and CLI credential references live in
-- relay-control's database. workspace_id, created_by and registered_by are
-- the `workspace` and `sub` claims of a relay-control access token as
-- answered by relay-control token introspection
-- (POST /internal/v1/auth/introspect); they are never taken from a request
-- body. Worker and run tokens are minted here and bound to those rows.
-- created_by_email / registered_by_email snapshot the introspected email at
-- write time (relay-control's users table cannot be joined from here).
-- Deleting a workspace in relay-control therefore does not cascade here.
--
-- Every secret-bearing token is stored only as a SHA-256 hash.

CREATE TABLE IF NOT EXISTS selfhost_flow_definitions (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL,
  name text NOT NULL,
  description text,
  file_type text NOT NULL,
  source text NOT NULL,
  created_by uuid,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (workspace_id, name),
  CONSTRAINT selfhost_flow_definitions_file_type CHECK (file_type IN ('yaml', 'ts'))
);

CREATE TABLE IF NOT EXISTS selfhost_worker_enrollment_tokens (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL,
  token_hash text NOT NULL UNIQUE,
  token_last4 text NOT NULL,
  name text,
  tags jsonb NOT NULL DEFAULT '[]'::jsonb,
  created_by uuid,
  created_by_email text,
  expires_at timestamptz NOT NULL,
  used_at timestamptz,
  used_by_worker_id uuid,
  revoked_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS selfhost_worker_enrollment_tokens_workspace_idx
  ON selfhost_worker_enrollment_tokens (workspace_id, created_at DESC);

CREATE TABLE IF NOT EXISTS selfhost_workers (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL,
  name text NOT NULL,
  display_name text NOT NULL,
  host_info jsonb NOT NULL DEFAULT '{}'::jsonb,
  tags jsonb NOT NULL DEFAULT '[]'::jsonb,
  token_hash text NOT NULL UNIQUE,
  status text NOT NULL DEFAULT 'pending',
  last_seen timestamptz,
  registered_at timestamptz NOT NULL DEFAULT now(),
  registered_by uuid,
  registered_by_email text,
  revoked_at timestamptz,
  UNIQUE (workspace_id, name),
  CONSTRAINT selfhost_workers_status CHECK (status IN ('pending', 'online', 'offline', 'revoked'))
);

CREATE TABLE IF NOT EXISTS selfhost_workflow_runs (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL,
  flow_id uuid REFERENCES selfhost_flow_definitions(id) ON DELETE SET NULL,
  workflow_name text NOT NULL,
  workflow text NOT NULL,
  file_type text NOT NULL,
  workflow_file_name text NOT NULL,
  dispatch_type text NOT NULL,
  status text NOT NULL DEFAULT 'pending',
  trigger text NOT NULL DEFAULT 'manual',
  inputs jsonb,
  env_secrets_ciphertext text,
  code_object_key text,
  requested_worker_id uuid,
  requested_tags jsonb NOT NULL DEFAULT '[]'::jsonb,
  resumed_from_run_id uuid REFERENCES selfhost_workflow_runs(id) ON DELETE SET NULL,
  resume_run_id text,
  start_from text,
  previous_run_id text,
  created_by uuid,
  created_by_email text,
  access_token_hash text UNIQUE,
  callback_token_hash text,
  result jsonb,
  summary text,
  error text,
  exit_code integer,
  duration_ms bigint,
  cancel_requested_at timestamptz,
  started_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT selfhost_workflow_runs_status
    CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
  CONSTRAINT selfhost_workflow_runs_dispatch CHECK (dispatch_type IN ('worker', 'local')),
  CONSTRAINT selfhost_workflow_runs_file_type CHECK (file_type IN ('yaml', 'ts'))
);

CREATE INDEX IF NOT EXISTS selfhost_workflow_runs_workspace_idx
  ON selfhost_workflow_runs (workspace_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS selfhost_workflow_runs_flow_idx
  ON selfhost_workflow_runs (flow_id, created_at DESC);

CREATE TABLE IF NOT EXISTS selfhost_work_assignments (
  id uuid PRIMARY KEY,
  workspace_id uuid NOT NULL,
  worker_id uuid REFERENCES selfhost_workers(id) ON DELETE SET NULL,
  run_id uuid NOT NULL UNIQUE REFERENCES selfhost_workflow_runs(id) ON DELETE CASCADE,
  status text NOT NULL DEFAULT 'queued',
  queued_at timestamptz NOT NULL DEFAULT now(),
  assigned_at timestamptz,
  acked_at timestamptz,
  started_at timestamptz,
  completed_at timestamptz,
  queue_deadline timestamptz NOT NULL,
  result jsonb,
  error text,
  CONSTRAINT selfhost_work_assignments_status
    CHECK (status IN ('queued', 'assigned', 'running', 'completed', 'failed', 'timeout'))
);

CREATE INDEX IF NOT EXISTS selfhost_work_assignments_queue_idx
  ON selfhost_work_assignments (workspace_id, queued_at)
  WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS selfhost_work_assignments_worker_idx
  ON selfhost_work_assignments (worker_id)
  WHERE status IN ('assigned', 'running');

CREATE TABLE IF NOT EXISTS selfhost_run_events (
  id uuid PRIMARY KEY,
  run_id uuid NOT NULL REFERENCES selfhost_workflow_runs(id) ON DELETE CASCADE,
  sequence integer NOT NULL,
  event_type text NOT NULL,
  step_name text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (run_id, sequence)
);

CREATE TABLE IF NOT EXISTS selfhost_run_steps (
  run_id uuid NOT NULL REFERENCES selfhost_workflow_runs(id) ON DELETE CASCADE,
  step_name text NOT NULL,
  position integer NOT NULL DEFAULT 0,
  source text NOT NULL,
  label text,
  step_type text,
  agent text,
  cli text,
  status text NOT NULL,
  attempt integer,
  started_at text,
  ended_at text,
  elapsed_ms bigint,
  exit_code integer,
  output_summary text,
  error text,
  completion_reason text,
  retry_count integer,
  depends_on jsonb NOT NULL DEFAULT '[]'::jsonb,
  detail jsonb,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, step_name),
  CONSTRAINT selfhost_run_steps_source CHECK (source IN ('snapshot', 'final'))
);

CREATE TABLE IF NOT EXISTS selfhost_run_objects (
  run_id uuid NOT NULL REFERENCES selfhost_workflow_runs(id) ON DELETE CASCADE,
  object_key text NOT NULL,
  content_type text NOT NULL,
  size_bytes integer NOT NULL,
  etag text NOT NULL,
  content bytea,
  external_location text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, object_key)
);

-- Revocations of a finished run's CLI-credential references, owed to
-- relay-control (CliCredentialBroker.revokeForRun). Written in the same
-- transaction that finishes the run; drained and retried by the revoke
-- outbox worker; deleted once relay-control acknowledges.
CREATE TABLE IF NOT EXISTS selfhost_cli_credential_revocations (
  run_id uuid PRIMARY KEY,
  enqueued_at timestamptz NOT NULL DEFAULT now(),
  attempts integer NOT NULL DEFAULT 0,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  last_error text
);

CREATE INDEX IF NOT EXISTS selfhost_cli_credential_revocations_due_idx
  ON selfhost_cli_credential_revocations (next_attempt_at);
