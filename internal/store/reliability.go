package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

type DispatchCandidate struct {
	JobID          string
	NodeID         string
	CurrentAttempt int
}

func (s *Store) TryClaimNodeAttempt(ctx context.Context, jobID, nodeID string, attempt int, workerID string) (bool, error) {
	res, err := s.DB.ExecContext(ctx,
		`INSERT IGNORE INTO node_attempts(job_id,node_id,attempt,status,worker_id)
		 SELECT ?,?,?,?,?
		 FROM job_nodes
		 WHERE job_id=? AND node_id=? AND current_attempt=? AND status IN ('DISPATCHING','RETRY_PENDING')`,
		jobID, nodeID, attempt, "RUNNING", workerID,
		jobID, nodeID, attempt,
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

func (s *Store) UpdateNodeAttemptStatus(ctx context.Context, jobID, nodeID string, attempt int, status, errorMsg string) error {
	return updateNodeAttemptStatus(ctx, s.DB, jobID, nodeID, attempt, status, errorMsg)
}

func (s *Store) UpdateNodeAttemptStatusTx(ctx context.Context, q Querier, jobID, nodeID string, attempt int, status, errorMsg string) error {
	return updateNodeAttemptStatus(ctx, q, jobID, nodeID, attempt, status, errorMsg)
}

func updateNodeAttemptStatus(ctx context.Context, q Querier, jobID, nodeID string, attempt int, status, errorMsg string) error {
	var finished any
	if status == "RUNNING" {
		finished = nil
	} else {
		finished = "CURRENT_TIMESTAMP(3)"
	}

	if finished == nil {
		res, err := q.ExecContext(ctx,
			`UPDATE node_attempts SET status=?, error_msg=? WHERE job_id=? AND node_id=? AND attempt=?`,
			status, nullIfEmpty(errorMsg), jobID, nodeID, attempt)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("node_attempts update affected %d rows for job=%s node=%s attempt=%d", n, jobID, nodeID, attempt)
		}
		return nil
	}

	res, err := q.ExecContext(ctx,
		`UPDATE node_attempts
		 SET status=?, error_msg=?, finished_at=CURRENT_TIMESTAMP(3)
		 WHERE job_id=? AND node_id=? AND attempt=?`,
		status, nullIfEmpty(errorMsg), jobID, nodeID, attempt)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("node_attempts update affected %d rows for job=%s node=%s attempt=%d", n, jobID, nodeID, attempt)
	}
	return nil
}

func (s *Store) CountNodeAttempts(ctx context.Context, jobID, nodeID string) (int, error) {
	var c int
	err := s.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM node_attempts WHERE job_id=? AND node_id=?`,
		jobID, nodeID,
	).Scan(&c)
	return c, err
}

func (s *Store) TryStartNodeDispatch(ctx context.Context, jobID, nodeID string) (bool, error) {
	res, err := s.DB.ExecContext(ctx,
		`UPDATE job_nodes
		 SET status='DISPATCHING', not_before_ms=NULL
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

func (s *Store) TryMarkNodeReady(ctx context.Context, jobID, nodeID string) (bool, error) {
	return s.TryStartNodeDispatch(ctx, jobID, nodeID)
}

func (s *Store) TryMarkNodeRunningAttempt(ctx context.Context, q Querier, jobID, nodeID string, attempt int) (bool, error) {
	res, err := q.ExecContext(ctx,
		`UPDATE job_nodes
		 SET status='RUNNING', not_before_ms=NULL
		 WHERE job_id=? AND node_id=? AND current_attempt=? AND status IN ('DISPATCHING','RETRY_PENDING')
		   AND EXISTS (
		     SELECT 1 FROM node_attempts
		     WHERE job_id=? AND node_id=? AND attempt=? AND status='RUNNING'
		   )`,
		jobID, nodeID, attempt,
		jobID, nodeID, attempt,
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

func (s *Store) TryFinalizeNodeSucceeded(ctx context.Context, q Querier, e NodeEvent) (bool, error) {
	res, err := q.ExecContext(ctx,
		`UPDATE job_nodes
		 SET status='SUCCEEDED', not_before_ms=NULL
		 WHERE job_id=? AND node_id=? AND current_attempt=? AND status='RUNNING'
		   AND EXISTS (
		     SELECT 1 FROM node_attempts
		     WHERE job_id=? AND node_id=? AND attempt=? AND status='RUNNING'
		   )`,
		e.JobID, e.NodeID, e.Attempt,
		e.JobID, e.NodeID, e.Attempt,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	if err := updateNodeAttemptStatus(ctx, q, e.JobID, e.NodeID, e.Attempt, "SUCCEEDED", ""); err != nil {
		return false, err
	}
	if err := upsertNodeResult(ctx, q, e.JobID, e.NodeID, e.OutputText, e.ArtifactURI, ""); err != nil {
		return false, err
	}
	if err := insertNodeEvent(ctx, q, e); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) TryScheduleNodeRetry(ctx context.Context, q Querier, e NodeEvent, nextAttempt int, notBeforeMs int64) (bool, error) {
	res, err := q.ExecContext(ctx,
		`UPDATE job_nodes
		 SET status='RETRY_PENDING', current_attempt=?, not_before_ms=?
		 WHERE job_id=? AND node_id=? AND current_attempt=? AND status='RUNNING'
		   AND EXISTS (
		     SELECT 1 FROM node_attempts
		     WHERE job_id=? AND node_id=? AND attempt=? AND status='RUNNING'
		   )`,
		nextAttempt, notBeforeMs,
		e.JobID, e.NodeID, e.Attempt,
		e.JobID, e.NodeID, e.Attempt,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	if err := updateNodeAttemptStatus(ctx, q, e.JobID, e.NodeID, e.Attempt, "FAILED", e.ErrorMsg); err != nil {
		return false, err
	}
	if err := upsertNodeResult(ctx, q, e.JobID, e.NodeID, "", "", e.ErrorMsg); err != nil {
		return false, err
	}
	if err := insertNodeEvent(ctx, q, e); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) TryFinalizeNodeFailed(ctx context.Context, q Querier, e NodeEvent) (bool, error) {
	res, err := q.ExecContext(ctx,
		`UPDATE job_nodes
		 SET status='FAILED', not_before_ms=NULL
		 WHERE job_id=? AND node_id=? AND current_attempt=? AND status='RUNNING'
		   AND EXISTS (
		     SELECT 1 FROM node_attempts
		     WHERE job_id=? AND node_id=? AND attempt=? AND status='RUNNING'
		   )`,
		e.JobID, e.NodeID, e.Attempt,
		e.JobID, e.NodeID, e.Attempt,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 0 {
		return false, nil
	}
	if err := updateNodeAttemptStatus(ctx, q, e.JobID, e.NodeID, e.Attempt, "FAILED", e.ErrorMsg); err != nil {
		return false, err
	}
	if err := upsertNodeResult(ctx, q, e.JobID, e.NodeID, "", "", e.ErrorMsg); err != nil {
		return false, err
	}
	if err := insertNodeEvent(ctx, q, e); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) ListRecoverableDispatches(ctx context.Context, staleBeforeMs, nowMs int64, limit int) ([]DispatchCandidate, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT job_id, node_id, current_attempt
		 FROM job_nodes
		 WHERE (status='DISPATCHING' AND updated_at <= FROM_UNIXTIME(? / 1000.0))
		    OR (status='RETRY_PENDING' AND not_before_ms IS NOT NULL AND not_before_ms<=?)
		 ORDER BY updated_at ASC
		 LIMIT ?`,
		staleBeforeMs, nowMs, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []DispatchCandidate
	for rows.Next() {
		var row DispatchCandidate
		if err := rows.Scan(&row.JobID, &row.NodeID, &row.CurrentAttempt); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
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
	return insertNodeEvent(ctx, s.DB, e)
}

func (s *Store) InsertNodeEventTx(ctx context.Context, q Querier, e NodeEvent) error {
	return insertNodeEvent(ctx, q, e)
}

func insertNodeEvent(ctx context.Context, q Querier, e NodeEvent) error {
	_, err := q.ExecContext(ctx,
		`INSERT INTO node_events(event_id,job_id,node_id,status,attempt,ts_ms,worker_id,output_text,artifact_uri,error_msg)
		 VALUES(?,?,?,?,?,?,?,?,?,?)`,
		e.EventID, e.JobID, nullIfEmpty(e.NodeID), e.Status, nullIntIfZero(e.Attempt), e.TSMs,
		nullIfEmpty(e.WorkerID), nullIfEmpty(e.OutputText), nullIfEmpty(e.ArtifactURI), nullIfEmpty(e.ErrorMsg),
	)
	if IsDuplicateKey(err) {
		return nil
	}
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
