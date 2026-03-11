ALTER TABLE jobs
ADD COLUMN planner_model VARCHAR(64),
ADD COLUMN planner_latency_ms BIGINT,
ADD COLUMN repair_rounds INT;