-- relayfile-control foundation schema (docs/design/webhook-ingest.md §2-§9).
-- Postgres is the only state. Every id is minted by the application (ULID text).

CREATE TABLE webhook_payload_blobs (
  id          text PRIMARY KEY,
  sha256      text NOT NULL,
  size_bytes  integer NOT NULL CHECK (size_bytes >= 0),
  body        bytea NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now()
);

-- §2.3 envelope, §4.1 queue state. One table doubles as the queue.
CREATE TABLE webhook_envelopes (
  id                  text PRIMARY KEY,
  parent_id           text,
  kind                text NOT NULL CHECK (kind IN ('nango-forward','nango-sync','nango-auth','provider-webhook','hookdeck-raw')),
  ingress             text NOT NULL CHECK (ingress IN ('github','nango','hookdeck','slack','linear')),
  provider            text NOT NULL CHECK (length(provider) BETWEEN 1 AND 64),
  request_id          text NOT NULL,
  delivery_id         text NOT NULL,
  has_provider_delivery_id boolean NOT NULL,
  event_type          text,
  connection_id       text,
  native_ids          jsonb NOT NULL DEFAULT '{}'::jsonb,
  dedupe_surface      text,
  dedupe_id           text,
  headers             jsonb NOT NULL DEFAULT '{}'::jsonb,
  payload             jsonb,
  payload_blob_id     text REFERENCES webhook_payload_blobs(id),
  payload_sha256      text NOT NULL,
  payload_bytes       integer NOT NULL CHECK (payload_bytes >= 0),
  received_at         timestamptz NOT NULL,
  workspace_id        text CHECK (workspace_id IS NULL OR workspace_id LIKE 'rw\_%'),
  state               text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued','leased','done','dead')),
  attempts            integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  visible_at          timestamptz NOT NULL DEFAULT now(),
  leased_by           text,
  lease_until         timestamptz,
  lane                text NOT NULL,
  last_outcome        jsonb,
  outcome_history     jsonb NOT NULL DEFAULT '[]'::jsonb,
  preservation_cycle  integer NOT NULL DEFAULT 0 CHECK (preservation_cycle >= 0),
  replayed_by         text,
  updated_at          timestamptz NOT NULL DEFAULT now(),
  CHECK ((payload IS NULL) <> (payload_blob_id IS NULL))
);

CREATE INDEX webhook_envelopes_queued_idx ON webhook_envelopes (state, visible_at) WHERE state = 'queued';
CREATE INDEX webhook_envelopes_lane_state_idx ON webhook_envelopes (lane, state);
CREATE INDEX webhook_envelopes_received_idx ON webhook_envelopes (received_at);
CREATE INDEX webhook_envelopes_leased_idx ON webhook_envelopes (lease_until) WHERE state = 'leased';
CREATE INDEX webhook_envelopes_dead_idx ON webhook_envelopes (updated_at) WHERE state = 'dead';
CREATE INDEX webhook_envelopes_queued_received_idx ON webhook_envelopes (received_at) WHERE state = 'queued';

-- §3 idempotency: ingress dedupe and handler claims share one table.
CREATE TABLE webhook_delivery_claims (
  surface       text NOT NULL,
  dedupe_id     text NOT NULL,
  status        text NOT NULL CHECK (status IN ('processing','completed','failed')),
  lease_until   timestamptz,
  envelope_id   text,
  attempts      integer NOT NULL DEFAULT 1,
  created_at    timestamptz NOT NULL DEFAULT now(),
  completed_at  timestamptz,
  PRIMARY KEY (surface, dedupe_id)
);
CREATE INDEX webhook_delivery_claims_completed_idx ON webhook_delivery_claims (completed_at) WHERE status = 'completed';

-- §6.3 lane cool-down.
CREATE TABLE lane_cooldowns (
  lane      text PRIMARY KEY,
  until     timestamptz NOT NULL,
  armed_by  text,
  reason    text
);
CREATE INDEX lane_cooldowns_until_idx ON lane_cooldowns (until);

-- §6.4 per-lane queued counters, maintained by trigger (no count(*) per request).
-- One row per lane rather than one global row, so ingress for different lanes
-- never contends on the same counter.
CREATE TABLE lane_queue_depth (
  lane    text PRIMARY KEY,
  queued  bigint NOT NULL DEFAULT 0
);

CREATE FUNCTION webhook_envelopes_queue_depth() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND OLD.state = 'queued' AND NEW.state = 'queued' AND OLD.lane = NEW.lane THEN
    RETURN NULL;
  END IF;
  IF TG_OP IN ('UPDATE', 'DELETE') AND OLD.state = 'queued' THEN
    UPDATE lane_queue_depth SET queued = queued - 1 WHERE lane = OLD.lane;
  END IF;
  IF TG_OP IN ('INSERT', 'UPDATE') AND NEW.state = 'queued' THEN
    INSERT INTO lane_queue_depth (lane, queued) VALUES (NEW.lane, 1)
    ON CONFLICT (lane) DO UPDATE SET queued = lane_queue_depth.queued + 1;
  END IF;
  RETURN NULL;
END
$$;

CREATE TRIGGER webhook_envelopes_queue_depth_trg
  AFTER INSERT OR DELETE OR UPDATE OF state, lane ON webhook_envelopes
  FOR EACH ROW EXECUTE FUNCTION webhook_envelopes_queue_depth();

-- §5 integration bindings. A pending binding (from a Nango auth event) has no
-- workspace yet; deliveries for it wait (retry) instead of being acked unmatched.
CREATE TABLE integration_bindings (
  id                   text PRIMARY KEY,
  workspace_id         text CHECK (workspace_id IS NULL OR workspace_id LIKE 'rw\_%'),
  provider             text NOT NULL,
  connection_id        text NOT NULL,
  provider_config_key  text,
  native_ids           jsonb NOT NULL DEFAULT '{}'::jsonb,
  relayfile_base_url   text,
  status               text NOT NULL CHECK (status IN ('active','pending','disabled')),
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),
  UNIQUE (provider, connection_id),
  CHECK (status <> 'active' OR workspace_id IS NOT NULL)
);
CREATE INDEX integration_bindings_native_ids_idx ON integration_bindings USING gin (native_ids);
CREATE INDEX integration_bindings_connection_idx ON integration_bindings (connection_id);

-- §8 DLQ archive: the full envelope row as jsonb, so replay restores it exactly.
CREATE TABLE webhook_dlq_archive (
  id               text PRIMARY KEY,
  lane             text NOT NULL,
  provider         text NOT NULL,
  received_at      timestamptz NOT NULL,
  archived_at      timestamptz NOT NULL DEFAULT now(),
  payload_blob_id  text REFERENCES webhook_payload_blobs(id),
  envelope         jsonb NOT NULL
);
CREATE INDEX webhook_dlq_archive_lane_idx ON webhook_dlq_archive (lane, archived_at);
CREATE INDEX webhook_dlq_archive_provider_idx ON webhook_dlq_archive (provider, archived_at);

-- Operator switches shared by every pod (ingress pause, §6.4 / §9).
CREATE TABLE service_flags (
  key         text PRIMARY KEY,
  value       jsonb NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  updated_by  text
);
