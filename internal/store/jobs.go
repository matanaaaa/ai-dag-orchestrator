package store

import (
	"context"
	"database/sql"
)

type Job struct {
	JobID        string
	TraceID      string
	Status       string
	UserRequest  string
	FailedReason string

	PlannerModel     string
	PlannerLatencyMs int64
	RepairRounds     int

	ParentJobID string
}

func (s *Store) InsertJob(ctx context.Context, j Job) error {
	return insertJob(ctx, s.DB, j)
}

func (s *Store) InsertJobTx(ctx context.Context, q Querier, j Job) error {
	return insertJob(ctx, q, j)
}

func insertJob(ctx context.Context, q Querier, j Job) error {
	_, err := q.ExecContext(ctx,
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
	return updateJobStatus(ctx, s.DB, jobID, status)
}

func (s *Store) UpdateJobStatusTx(ctx context.Context, q Querier, jobID, status string) error {
	return updateJobStatus(ctx, q, jobID, status)
}

func updateJobStatus(ctx context.Context, q Querier, jobID, status string) error {
	_, err := q.ExecContext(ctx,
		`UPDATE jobs SET status=? WHERE job_id=?`,
		status, jobID,
	)
	return err
}

func (s *Store) UpdateJobFailed(ctx context.Context, jobID, reason string) error {
	return updateJobFailed(ctx, s.DB, jobID, reason)
}

func (s *Store) UpdateJobFailedTx(ctx context.Context, q Querier, jobID, reason string) error {
	return updateJobFailed(ctx, q, jobID, reason)
}

func updateJobFailed(ctx context.Context, q Querier, jobID, reason string) error {
	_, err := q.ExecContext(ctx,
		`UPDATE jobs SET status='FAILED', failed_reason=? WHERE job_id=?`,
		nullIfEmpty(reason), jobID,
	)
	return err
}

func (s *Store) UpdateJobStatusWithReason(ctx context.Context, jobID, status, reason string) error {
	return updateJobStatusWithReason(ctx, s.DB, jobID, status, reason)
}

func (s *Store) UpdateJobStatusWithReasonTx(ctx context.Context, q Querier, jobID, status, reason string) error {
	return updateJobStatusWithReason(ctx, q, jobID, status, reason)
}

func updateJobStatusWithReason(ctx context.Context, q Querier, jobID, status, reason string) error {
	_, err := q.ExecContext(ctx,
		`UPDATE jobs SET status=?, failed_reason=? WHERE job_id=?`,
		status, nullIfEmpty(reason), jobID,
	)
	return err
}

func (s *Store) GetJob(ctx context.Context, jobID string) (Job, error) {
	return getJob(ctx, s.DB, jobID)
}

func (s *Store) GetJobTx(ctx context.Context, q Querier, jobID string) (Job, error) {
	return getJob(ctx, q, jobID)
}

func getJob(ctx context.Context, q Querier, jobID string) (Job, error) {
	var j Job
	var failed sql.NullString
	var parent sql.NullString
	err := q.QueryRowContext(ctx,
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

func (s *Store) TryClaimJobForReplan(ctx context.Context, q Querier, jobID, reason string) (bool, error) {
	res, err := q.ExecContext(ctx,
		`UPDATE jobs
		 SET status='NEEDS_REPLAN', failed_reason=?
		 WHERE job_id=? AND status NOT IN ('NEEDS_REPLAN','REPLANNED','SUCCEEDED','FAILED')`,
		nullIfEmpty(reason), jobID,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *Store) TryClaimJobForReplanNow(ctx context.Context, jobID, reason string) (bool, error) {
	return s.TryClaimJobForReplan(ctx, s.DB, jobID, reason)
}

func (s *Store) TryMarkJobSucceeded(ctx context.Context, q Querier, jobID string) (bool, error) {
	res, err := q.ExecContext(ctx,
		`UPDATE jobs
		 SET status='SUCCEEDED'
		 WHERE job_id=? AND status='RUNNING'
		   AND NOT EXISTS (
		     SELECT 1 FROM job_nodes
		     WHERE job_id=? AND status<>'SUCCEEDED'
		   )`,
		jobID, jobID,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *Store) TryMarkJobSucceededNow(ctx context.Context, jobID string) (bool, error) {
	return s.TryMarkJobSucceeded(ctx, s.DB, jobID)
}

func (s *Store) TryStartJobPlanInitialization(ctx context.Context, q Querier, j Job) (bool, error) {
	res, err := q.ExecContext(ctx,
		`UPDATE jobs
		 SET status='RUNNING',
		     trace_id=?,
		     planner_model=?,
		     planner_latency_ms=?,
		     repair_rounds=?,
		     failed_reason=NULL
		 WHERE job_id=? AND status='PENDING'`,
		j.TraceID, j.PlannerModel, j.PlannerLatencyMs, j.RepairRounds,
		j.JobID,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *Store) TryMarkJobReplanned(ctx context.Context, q Querier, jobID, reason string) (bool, error) {
	res, err := q.ExecContext(ctx,
		`UPDATE jobs
		 SET status='REPLANNED', failed_reason=?
		 WHERE job_id=? AND status='NEEDS_REPLAN'`,
		nullIfEmpty(reason), jobID,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *Store) TryMarkJobReplannedNow(ctx context.Context, jobID, reason string) (bool, error) {
	return s.TryMarkJobReplanned(ctx, s.DB, jobID, reason)
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
