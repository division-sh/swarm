package llmpersistence

import (
	"context"
	"database/sql"
)

type PostgresSessionTerminationStorage struct {
	Status string
	Reason string
	Detail string
}

func ReadPostgresSessionTerminationStorageTx(ctx context.Context, tx *sql.Tx, sessionID string) (PostgresSessionTerminationStorage, error) {
	var out PostgresSessionTerminationStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(status, ''), COALESCE(termination_reason, ''), COALESCE(termination_detail, '') FROM agent_sessions WHERE session_id = $1::uuid`, sessionID).Scan(&out.Status, &out.Reason, &out.Detail)
	if err != nil {
		return PostgresSessionTerminationStorage{}, err
	}
	return out, nil
}

type PostgresSessionFlowStorage struct {
	Status       string
	FlowInstance string
}

func ReadPostgresSessionFlowStorageTx(ctx context.Context, tx *sql.Tx, sessionID string) (PostgresSessionFlowStorage, error) {
	var out PostgresSessionFlowStorage
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(status, ''), COALESCE(flow_instance, '') FROM agent_sessions WHERE session_id = $1::uuid`, sessionID).Scan(&out.Status, &out.FlowInstance)
	if err != nil {
		return PostgresSessionFlowStorage{}, err
	}
	return out, nil
}

func CountPostgresAgentOneSupportLiveSessionsTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid AND agent_id = 'agent-1' AND flow_instance = 'support/inst-1' AND memory_enabled = TRUE AND status = 'active'`, runID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func CountPostgresAgentOneSessionsTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_sessions WHERE agent_id = 'agent-1'`).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
