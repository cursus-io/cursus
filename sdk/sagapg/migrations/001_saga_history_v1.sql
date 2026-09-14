CREATE TABLE IF NOT EXISTS cursus_saga_state (
  saga_type TEXT NOT NULL, saga_id TEXT NOT NULL, association_key TEXT NOT NULL,
  correlation_id TEXT NOT NULL DEFAULT '', run_id UUID NOT NULL,
  next_sequence BIGINT NOT NULL DEFAULT 0, status TEXT NOT NULL, outcome TEXT NOT NULL DEFAULT '',
  step_id TEXT NOT NULL DEFAULT '', data JSONB NOT NULL DEFAULT '{}'::jsonb,
  retry_count INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
  effects JSONB NOT NULL DEFAULT '{}'::jsonb, compensation JSONB, updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (saga_type, saga_id)
);
CREATE TABLE IF NOT EXISTS cursus_saga_inbox (
  consumer_name TEXT NOT NULL, event_id TEXT NOT NULL, status TEXT NOT NULL,
  last_error TEXT NOT NULL DEFAULT '', updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (consumer_name, event_id)
);
CREATE TABLE IF NOT EXISTS cursus_saga_outbox (
  command_id TEXT PRIMARY KEY, saga_type TEXT NOT NULL, saga_id TEXT NOT NULL,
  effect_id TEXT NOT NULL DEFAULT '', command_type TEXT NOT NULL, correlation_id TEXT NOT NULL DEFAULT '',
  causation_id TEXT NOT NULL DEFAULT '', payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'PENDING', created_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS cursus_saga_history (
  history_event_id UUID PRIMARY KEY, history_schema_version INTEGER NOT NULL,
  environment_id TEXT NOT NULL, service_name TEXT NOT NULL, saga_type TEXT NOT NULL, saga_id TEXT NOT NULL,
  run_id UUID NOT NULL, sequence BIGINT NOT NULL, event_type TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL, recorded_at TIMESTAMPTZ NOT NULL,
  step_id TEXT NOT NULL DEFAULT '', attempt INTEGER NOT NULL DEFAULT 0,
  command_id TEXT NOT NULL DEFAULT '', effect_id TEXT NOT NULL DEFAULT '', source_event_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '', causation_id TEXT NOT NULL DEFAULT '', payload TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '',
  UNIQUE (saga_type, saga_id, run_id, sequence)
);
CREATE TABLE IF NOT EXISTS cursus_saga_history_outbox (
  history_event_id UUID PRIMARY KEY REFERENCES cursus_saga_history(history_event_id),
  topic_name TEXT NOT NULL, payload JSONB NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING',
  attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  lease_expires_at TIMESTAMPTZ, published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS cursus_saga_history_outbox_pending_idx
  ON cursus_saga_history_outbox (status, created_at);
