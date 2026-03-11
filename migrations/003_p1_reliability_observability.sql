ALTER TABLE jobs
ADD COLUMN failed_reason TEXT NULL;

CREATE TABLE IF NOT EXISTS node_attempts (
  job_id       VARCHAR(64) NOT NULL,
  node_id      VARCHAR(64) NOT NULL,
  attempt      INT NOT NULL,
  status       VARCHAR(16) NOT NULL, -- RUNNING/SUCCEEDED/FAILED
  worker_id    VARCHAR(64) NOT NULL,
  error_msg    TEXT NULL,
  started_at   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  finished_at  TIMESTAMP(3) NULL,
  PRIMARY KEY (job_id, node_id, attempt),
  INDEX idx_attempt_status (job_id, node_id, status)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS node_events (
  id           BIGINT AUTO_INCREMENT PRIMARY KEY,
  event_id     VARCHAR(64) NOT NULL,
  job_id       VARCHAR(64) NOT NULL,
  node_id      VARCHAR(64) NULL,
  status       VARCHAR(32) NOT NULL,
  attempt      INT NULL,
  ts_ms        BIGINT NOT NULL,
  worker_id    VARCHAR(64) NULL,
  output_text  MEDIUMTEXT NULL,
  artifact_uri TEXT NULL,
  error_msg    TEXT NULL,
  created_at   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uniq_event_id (event_id),
  INDEX idx_job_id_id (job_id, id),
  INDEX idx_job_ts (job_id, ts_ms)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS node_edge_unlocks (
  job_id     VARCHAR(64) NOT NULL,
  from_node  VARCHAR(64) NOT NULL,
  to_node    VARCHAR(64) NOT NULL,
  unlocked_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (job_id, from_node, to_node)
) ENGINE=InnoDB;
