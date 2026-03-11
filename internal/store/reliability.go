package store

import (
	"context"
	"database/sql"
	"errors"
)

type NodeEvent struct {
	ID          int64
	EventID     string
	JobID       string
	NodeID      string
	Status      string
	Attempt     int
	TSMs        int64
	WorkerID    string
	OutputText  string
	ArtifactURI string
	ErrorMsg    string
}

func (s *Store) TryClaimNodeAttempt(ctx context.Context, jobID, nodeID string, attempt int, workerID string) (bool, error) {
	res, err := s.DB.ExecContext(ctx,
		`INSERT IGNORE INTO node_attempts(job_id,node_id,attempt,status,worker_id)
		 VALUES(?,?,?,?,?)`,
		jobID, nodeID, attempt, "RUNNING", workerID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *Store) UpdateNodeAttemptStatus(ctx context.Context, jobID, nodeID string, attempt int, status, errorMsg string) error {
	var finished any
	if status == "RUNNING" {
		finished = nil
	} else {
		finished = "CURRENT_TIMESTAMP(3)"
	}

	if finished == nil {
		_, err := s.DB.ExecContext(ctx,
			`UPDATE node_attempts SET status=?, error_msg=? WHERE job_id=? AND node_id=? AND attempt=?`,
			status, nullIfEmpty(errorMsg), jobID, nodeID, attempt)
		return err
	}

	_, err := s.DB.ExecContext(ctx,
		`UPDATE node_attempts
		 SET status=?, error_msg=?, finished_at=CURRENT_TIMESTAMP(3)
		 WHERE job_id=? AND node_id=? AND attempt=?`,
		status, nullIfEmpty(errorMsg), jobID, nodeID, attempt)
	return err
}

func (s *Store) CountNodeAttempts(ctx context.Context, jobID, nodeID string) (int, error) {
	var c int
	err := s.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM node_attempts WHERE job_id=? AND node_id=?`,
		jobID, nodeID,
	).Scan(&c)
	return c, err
}

func (s *Store) TryMarkNodeReady(ctx context.Context, jobID, nodeID string) (bool, error) {
	res, err := s.DB.ExecContext(ctx,
		`UPDATE job_nodes
		 SET status='READY'
		 WHERE job_id=? AND node_id=? AND status='PENDING' AND indegree_remaining=0`,
		jobID, nodeID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *Store) UnlockDownstreamEdge(ctx context.Context, jobID, fromNode, toNode string) (bool, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	res, err := tx.ExecContext(ctx,
		`INSERT IGNORE INTO node_edge_unlocks(job_id,from_node,to_node) VALUES(?,?,?)`,
		jobID, fromNode, toNode)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}

	// exactly 1 -> 0, this node has just become ready
	res, err = tx.ExecContext(ctx,
		`UPDATE job_nodes
		 SET indegree_remaining=indegree_remaining-1
		 WHERE job_id=? AND node_id=? AND status='PENDING' AND indegree_remaining=1`,
		jobID, toNode)
	if err != nil {
		return false, err
	}
	readyRows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	if readyRows == 0 {
		_, err = tx.ExecContext(ctx,
			`UPDATE job_nodes
			 SET indegree_remaining=indegree_remaining-1
			 WHERE job_id=? AND node_id=? AND status='PENDING' AND indegree_remaining>1`,
			jobID, toNode)
		if err != nil {
			return false, err
		}
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}
	return readyRows == 1, nil
}

func (s *Store) InsertNodeEvent(ctx context.Context, e NodeEvent) error {
	_, err := s.DB.ExecContext(ctx,
		`INSERT INTO node_events(event_id,job_id,node_id,status,attempt,ts_ms,worker_id,output_text,artifact_uri,error_msg)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		e.EventID, e.JobID, nullIfEmpty(e.NodeID), e.Status, nullIntIfZero(e.Attempt), e.TSMs,
		nullIfEmpty(e.WorkerID), nullIfEmpty(e.OutputText), nullIfEmpty(e.ArtifactURI), nullIfEmpty(e.ErrorMsg),
	)
	return err
}

func (s *Store) ListNodeEventsAfter(ctx context.Context, jobID string, afterID int64, limit int) ([]NodeEvent, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT id,event_id,job_id,COALESCE(node_id,''),status,COALESCE(attempt,0),ts_ms,
		        COALESCE(worker_id,''),COALESCE(output_text,''),COALESCE(artifact_uri,''),COALESCE(error_msg,'')
		 FROM node_events
		 WHERE job_id=? AND id>? ORDER BY id ASC LIMIT ?`,
		jobID, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []NodeEvent
	for rows.Next() {
		var e NodeEvent
		if err := rows.Scan(&e.ID, &e.EventID, &e.JobID, &e.NodeID, &e.Status, &e.Attempt, &e.TSMs,
			&e.WorkerID, &e.OutputText, &e.ArtifactURI, &e.ErrorMsg); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Store) GetNodeStatus(ctx context.Context, jobID, nodeID string) (string, error) {
	var st string
	err := s.DB.QueryRowContext(ctx,
		`SELECT status FROM job_nodes WHERE job_id=? AND node_id=?`,
		jobID, nodeID,
	).Scan(&st)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return st, err
}

func nullIntIfZero(v int) any {
	if v == 0 {
		return nil
	}
	return v
}
