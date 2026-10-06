package llmpersistence

import (
	"context"
	"database/sql"
)

type H1TurnAccountingEvidence struct{ Identities, Turns, Sessions, Failed int }

func (s *LLMPostgresOwner) ObserveH1TurnAccountingForTest(ctx context.Context, runID string) (H1TurnAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H1TurnAccountingEvidence{}, err
	}
	var out H1TurnAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH1TurnAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H1TurnAccountingEvidence{}, err
	}
	return out, nil
}

func (s *LLMSQLiteOwner) ObserveH1TurnAccountingForTest(ctx context.Context, runID string) (H1TurnAccountingEvidence, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return H1TurnAccountingEvidence{}, err
	}
	var out H1TurnAccountingEvidence
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return observeH1TurnAccountingForTest(ctx, tx, runID, &out)
	})
	if err != nil {
		return H1TurnAccountingEvidence{}, err
	}
	return out, nil
}

func observeH1TurnAccountingForTest(ctx context.Context, tx *sql.Tx, runID string, out *H1TurnAccountingEvidence) error {
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM (SELECT DISTINCT agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance FROM agent_turns WHERE run_id=$1 AND execution_mode='live' AND parse_ok=TRUE) identities`, runID).Scan(&out.Identities); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE run_id=$1 AND execution_mode='live' AND parse_ok=TRUE`, runID).Scan(&out.Turns); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT session_id) FROM agent_turns WHERE run_id=$1 AND execution_mode='live' AND parse_ok=TRUE`, runID).Scan(&out.Sessions); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_turns WHERE run_id=$1 AND (execution_mode<>'live' OR parse_ok=FALSE OR retry_count<>0 OR failure IS NOT NULL)`, runID).Scan(&out.Failed); err != nil {
		return err
	}
	return nil
}
