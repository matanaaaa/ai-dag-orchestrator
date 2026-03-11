ALTER TABLE jobs
  ADD COLUMN parent_job_id VARCHAR(64) NULL AFTER job_id;

CREATE INDEX idx_jobs_parent_job_id ON jobs(parent_job_id);