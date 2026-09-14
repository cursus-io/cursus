CREATE TABLE IF NOT EXISTS cursus_saga_state (
  saga_type VARCHAR(255) NOT NULL, saga_id VARCHAR(255) NOT NULL, association_key VARCHAR(255) NOT NULL,
  correlation_id VARCHAR(255) NOT NULL DEFAULT '', run_id CHAR(36) NOT NULL, next_sequence BIGINT UNSIGNED NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL, outcome VARCHAR(32) NOT NULL DEFAULT '', step_id VARCHAR(255) NOT NULL DEFAULT '',
  data JSON NOT NULL, retry_count INT NOT NULL DEFAULT 0, last_error TEXT NOT NULL, effects JSON NOT NULL,
  compensation JSON NULL, updated_at DATETIME(6) NOT NULL, PRIMARY KEY (saga_type, saga_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS cursus_saga_inbox (
  consumer_name VARCHAR(255) NOT NULL, event_id VARCHAR(255) NOT NULL, status VARCHAR(32) NOT NULL,
  last_error TEXT NOT NULL, updated_at DATETIME(6) NOT NULL, PRIMARY KEY (consumer_name,event_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS cursus_saga_outbox (
  command_id VARCHAR(255) NOT NULL, saga_type VARCHAR(255) NOT NULL, saga_id VARCHAR(255) NOT NULL,
  effect_id VARCHAR(255) NOT NULL, command_type VARCHAR(255) NOT NULL, correlation_id VARCHAR(255) NOT NULL,
  causation_id VARCHAR(255) NOT NULL, payload JSON NOT NULL, status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
  created_at DATETIME(6) NOT NULL, published_at DATETIME(6) NULL, PRIMARY KEY(command_id),
  UNIQUE KEY cursus_saga_outbox_effect_key (saga_type,saga_id,effect_id)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS cursus_saga_history (
  history_event_id CHAR(36) NOT NULL, history_schema_version INT NOT NULL, environment_id VARCHAR(255) NOT NULL,
  service_name VARCHAR(255) NOT NULL, saga_type VARCHAR(255) NOT NULL, saga_id VARCHAR(255) NOT NULL,
  run_id CHAR(36) NOT NULL, sequence BIGINT UNSIGNED NOT NULL, event_type VARCHAR(64) NOT NULL,
  occurred_at DATETIME(6) NOT NULL, recorded_at DATETIME(6) NOT NULL, step_id VARCHAR(255) NOT NULL DEFAULT '', attempt INT NULL,
  command_id VARCHAR(255) NOT NULL DEFAULT '', effect_id VARCHAR(255) NOT NULL DEFAULT '', source_event_id VARCHAR(255) NOT NULL DEFAULT '',
  correlation_id VARCHAR(255) NOT NULL DEFAULT '', causation_id VARCHAR(255) NOT NULL DEFAULT '', source_topic VARCHAR(255) NOT NULL DEFAULT '',
  source_partition INT NULL, source_offset BIGINT UNSIGNED NULL, aggregate_type VARCHAR(255) NOT NULL DEFAULT '',
  aggregate_id VARCHAR(255) NOT NULL DEFAULT '', aggregate_version BIGINT UNSIGNED NULL, payload TEXT NOT NULL, error TEXT NOT NULL,
  PRIMARY KEY(history_event_id), UNIQUE KEY cursus_saga_history_run_sequence(saga_type,saga_id,run_id,sequence),
  CONSTRAINT cursus_saga_history_v1 CHECK (history_schema_version = 1), CONSTRAINT cursus_saga_history_positive_sequence CHECK (sequence > 0)
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS cursus_saga_history_outbox (
  history_event_id CHAR(36) NOT NULL, topic_name VARCHAR(255) NOT NULL, payload JSON NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'PENDING', attempts INT NOT NULL DEFAULT 0, last_error TEXT NOT NULL,
  created_at DATETIME(6) NOT NULL, lease_expires_at DATETIME(6) NULL, published_at DATETIME(6) NULL,
  PRIMARY KEY(history_event_id), KEY cursus_saga_history_outbox_pending_idx(status,created_at),
  CONSTRAINT cursus_saga_history_outbox_fk FOREIGN KEY(history_event_id) REFERENCES cursus_saga_history(history_event_id)
) ENGINE=InnoDB;
