ALTER TABLE job_nodes
  ADD COLUMN current_attempt INT NOT NULL DEFAULT 1 AFTER indegree_remaining,
  ADD COLUMN not_before_ms BIGINT NULL AFTER current_attempt;

CREATE INDEX idx_job_status_attempt ON job_nodes(job_id, status, current_attempt);
CREATE INDEX idx_job_retry_due ON job_nodes(job_id, status, not_before_ms);
