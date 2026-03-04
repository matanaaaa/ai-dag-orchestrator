package store

import "context"

type Job struct {
	JobID       string
	TraceID     string
	Status      string
	UserRequest string
}

func (s *Store) InsertJob(ctx context.Context, j Job) error {
	_, err := s.DB.ExecContext(ctx,
		`INSERT INTO jobs(job_id, trace_id, status, user_request) VALUES(?,?,?,?)`,
		j.JobID, j.TraceID, j.Status, j.UserRequest,
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

func (s *Store) GetJob(ctx context.Context, jobID string) (Job, error) {
	var j Job
	err := s.DB.QueryRowContext(ctx,
		`SELECT job_id, trace_id, status, user_request FROM jobs WHERE job_id=?`,
		jobID,
	).Scan(&j.JobID, &j.TraceID, &j.Status, &j.UserRequest)
	return j, err
}