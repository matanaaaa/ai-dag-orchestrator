package store

import (
	"context"
	"database/sql"
)

type Job struct {
	JobID       string
	TraceID     string
	Status      string
	UserRequest string
	FailedReason string

	PlannerModel     string
	PlannerLatencyMs int64
	RepairRounds     int

	ParentJobID string
}

func (s *Store) InsertJob(ctx context.Context, j Job) error {
	_, err := s.DB.ExecContext(ctx,
		`INSERT INTO jobs(
			job_id, trace_id, status, user_request,
			failed_reason, planner_model, planner_latency_ms, repair_rounds,
			parent_job_id
		)
		VALUES(?,?,?,?,?,?,?,?,?)
		ON DUPLICATE KEY UPDATE
			trace_id=VALUES(trace_id),
			status=VALUES(status),
			user_request=VALUES(user_request),
			failed_reason=VALUES(failed_reason),
			planner_model=VALUES(planner_model),
			planner_latency_ms=VALUES(planner_latency_ms),
			repair_rounds=VALUES(repair_rounds),
			parent_job_id=VALUES(parent_job_id)`,
		j.JobID, j.TraceID, j.Status, j.UserRequest,
		nullIfEmpty(j.FailedReason),
		j.PlannerModel, j.PlannerLatencyMs, j.RepairRounds,
		nullIfEmpty(j.ParentJobID),
	)
	return err
}

func (s *Store) UpdateJobStatus(ctx context.Context, jobID, status string) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE jobs SET status=? WHERE job_id=?`,
		status, jobID,
	)
	return err
}

func (s *Store) UpdateJobFailed(ctx context.Context, jobID, reason string) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE jobs SET status='FAILED', failed_reason=? WHERE job_id=?`,
		nullIfEmpty(reason), jobID,
	)
	return err
}

func (s *Store) UpdateJobStatusWithReason(ctx context.Context, jobID, status, reason string) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE jobs SET status=?, failed_reason=? WHERE job_id=?`,
		status, nullIfEmpty(reason), jobID,
	)
	return err
}

func (s *Store) GetJob(ctx context.Context, jobID string) (Job, error) {
	var j Job
	var failed sql.NullString
	var parent sql.NullString
	err := s.DB.QueryRowContext(ctx,
		`SELECT job_id, trace_id, status, user_request, failed_reason, planner_model, planner_latency_ms, repair_rounds, parent_job_id
		 FROM jobs WHERE job_id=?`,
		jobID,
	).Scan(
		&j.JobID, &j.TraceID, &j.Status, &j.UserRequest, &failed,
		&j.PlannerModel, &j.PlannerLatencyMs, &j.RepairRounds, &parent,
	)
	if failed.Valid {
		j.FailedReason = failed.String
	}
	if parent.Valid {
		j.ParentJobID = parent.String
	}
	return j, err
}

func (s *Store) ListChildJobs(ctx context.Context, parentJobID string) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT job_id FROM jobs WHERE parent_job_id=? ORDER BY job_id`, parentJobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
