CREATE TABLE IF NOT EXISTS jobs (
  job_id        VARCHAR(64) PRIMARY KEY,
  trace_id      VARCHAR(64) NOT NULL,
  status        VARCHAR(16) NOT NULL, -- PENDING/RUNNING/SUCCEEDED/FAILED
  user_request  TEXT NOT NULL,
  created_at    TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at    TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS job_nodes (
  job_id             VARCHAR(64) NOT NULL,
  node_id            VARCHAR(64) NOT NULL,
  node_type          VARCHAR(32) NOT NULL,
  params_json        JSON NOT NULL,
  status             VARCHAR(16) NOT NULL, -- PENDING/READY/RUNNING/SUCCEEDED/FAILED
  indegree_remaining INT NOT NULL DEFAULT 0,
  created_at         TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at         TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (job_id, node_id),
  INDEX idx_job_status (job_id, status),
  INDEX idx_job_ready (job_id, indegree_remaining, status)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS job_edges (
  job_id    VARCHAR(64) NOT NULL,
  from_node VARCHAR(64) NOT NULL,
  to_node   VARCHAR(64) NOT NULL,
  PRIMARY KEY (job_id, from_node, to_node),
  INDEX idx_to (job_id, to_node),
  INDEX idx_from (job_id, from_node)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS node_results (
  job_id       VARCHAR(64) NOT NULL,
  node_id      VARCHAR(64) NOT NULL,
  output_text  MEDIUMTEXT NULL,
  artifact_uri TEXT NULL,
  error_msg    TEXT NULL,
  updated_at   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (job_id, node_id)
) ENGINE=InnoDB;