package store

import (
	"context"
	"database/sql"
	"encoding/json"
)

type NodeRow struct {
	JobID             string
	NodeID            string
	NodeType          string
	ParamsJSON        string
	Status            string
	IndegreeRemaining int
	CurrentAttempt    int
}

type EdgeRow struct {
	From string
	To   string
}

type NodeResultRow struct {
	JobID       string
	NodeID      string
	OutputText  sql.NullString
	ArtifactURI sql.NullString
	ErrorMsg    sql.NullString
}

func (s *Store) InsertNode(ctx context.Context, jobID, nodeID, nodeType string, params any, status string, indegree int) error {
	return insertNode(ctx, s.DB, jobID, nodeID, nodeType, params, status, indegree)
}

func (s *Store) InsertNodeTx(ctx context.Context, q Querier, jobID, nodeID, nodeType string, params any, status string, indegree int) error {
	return insertNode(ctx, q, jobID, nodeID, nodeType, params, status, indegree)
}

func insertNode(ctx context.Context, q Querier, jobID, nodeID, nodeType string, params any, status string, indegree int) error {
	b, err := json.Marshal(params)
	if err != nil {
		return err
	}
	_, err = q.ExecContext(ctx,
		`INSERT IGNORE INTO job_nodes(job_id,node_id,node_type,params_json,status,indegree_remaining,current_attempt,not_before_ms)
		 VALUES(?,?,?,?,?,?,1,NULL)`,
		jobID, nodeID, nodeType, string(b), status, indegree)
	return err
}

func (s *Store) InsertEdge(ctx context.Context, jobID, from, to string) error {
	return insertEdge(ctx, s.DB, jobID, from, to)
}

func (s *Store) InsertEdgeTx(ctx context.Context, q Querier, jobID, from, to string) error {
	return insertEdge(ctx, q, jobID, from, to)
}

func insertEdge(ctx context.Context, q Querier, jobID, from, to string) error {
	_, err := q.ExecContext(ctx,
		`INSERT IGNORE INTO job_edges(job_id,from_node,to_node) VALUES(?,?,?)`,
		jobID, from, to)
	return err
}

func (s *Store) ListDownstream(ctx context.Context, jobID, from string) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT to_node FROM job_edges WHERE job_id=? AND from_node=?`,
		jobID, from)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var to string
		if err := rows.Scan(&to); err != nil {
			return nil, err
		}
		out = append(out, to)
	}
	return out, rows.Err()
}

func (s *Store) MarkNodeStatus(ctx context.Context, jobID, nodeID, status string) error {
	return markNodeStatus(ctx, s.DB, jobID, nodeID, status)
}

func (s *Store) MarkNodeStatusTx(ctx context.Context, q Querier, jobID, nodeID, status string) error {
	return markNodeStatus(ctx, q, jobID, nodeID, status)
}

func markNodeStatus(ctx context.Context, q Querier, jobID, nodeID, status string) error {
	_, err := q.ExecContext(ctx,
		`UPDATE job_nodes SET status=? WHERE job_id=? AND node_id=?`,
		status, jobID, nodeID)
	return err
}

func (s *Store) GetNodeTypeAndParams(ctx context.Context, jobID, nodeID string) (string, map[string]any, error) {
	var nodeType string
	var paramsJSON string
	err := s.DB.QueryRowContext(ctx,
		`SELECT node_type, params_json FROM job_nodes WHERE job_id=? AND node_id=?`,
		jobID, nodeID).Scan(&nodeType, &paramsJSON)
	if err != nil {
		return "", nil, err
	}
	var params map[string]any
	if err := json.Unmarshal([]byte(paramsJSON), &params); err != nil {
		return "", nil, err
	}
	return nodeType, params, nil
}

func (s *Store) ListReadyNodes(ctx context.Context, jobID string) ([]string, error) {
	rows, err := s.DB.QueryContext(ctx,
		`SELECT node_id FROM job_nodes WHERE job_id=? AND status='PENDING' AND indegree_remaining=0`,
		jobID)
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

func (s *Store) DecrementIndegree(ctx context.Context, jobID, nodeID string) error {
	_, err := s.DB.ExecContext(ctx,
		`UPDATE job_nodes SET indegree_remaining=indegree_remaining-1 WHERE job_id=? AND node_id=? AND indegree_remaining>0`,
		jobID, nodeID)
	return err
}

func (s *Store) CountNodesByStatus(ctx context.Context, jobID, status string) (int, error) {
	var c int
	err := s.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM job_nodes WHERE job_id=? AND status=?`,
		jobID, status).Scan(&c)
	return c, err
}

func (s *Store) CountAllNodes(ctx context.Context, jobID string) (int, error) {
	var c int
	err := s.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM job_nodes WHERE job_id=?`,
		jobID).Scan(&c)
	return c, err
}

func (s *Store) UpsertNodeResult(ctx context.Context, jobID, nodeID, outputText, artifactURI, errorMsg string) error {
	return upsertNodeResult(ctx, s.DB, jobID, nodeID, outputText, artifactURI, errorMsg)
}

func (s *Store) UpsertNodeResultTx(ctx context.Context, q Querier, jobID, nodeID, outputText, artifactURI, errorMsg string) error {
	return upsertNodeResult(ctx, q, jobID, nodeID, outputText, artifactURI, errorMsg)
}

func upsertNodeResult(ctx context.Context, q Querier, jobID, nodeID, outputText, artifactURI, errorMsg string) error {
	_, err := q.ExecContext(ctx,
		`INSERT INTO node_results(job_id,node_id,output_text,artifact_uri,error_msg)
		 VALUES(?,?,?,?,?)
		 ON DUPLICATE KEY UPDATE output_text=VALUES(output_text), artifact_uri=VALUES(artifact_uri), error_msg=VALUES(error_msg)`,
		jobID, nodeID, nullIfEmpty(outputText), nullIfEmpty(artifactURI), nullIfEmpty(errorMsg))
	return err
}

func (s *Store) GetNodeOutputText(ctx context.Context, jobID, nodeID string) (string, error) {
	var out sql.NullString
	err := s.DB.QueryRowContext(ctx,
		`SELECT output_text FROM node_results WHERE job_id=? AND node_id=?`,
		jobID, nodeID).Scan(&out)
	if err != nil {
		return "", err
	}
	if out.Valid {
		return out.String, nil
	}
	return "", nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
