package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *PipelinePostgresOwner) AdmitRunExecutionPosture(ctx context.Context, runID string, posture executionposture.Posture) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return admitRunExecutionPostureTx(ctx, tx, true, runID, posture)
		})
	})
	return standalonePipelineMutationError(result)
}

func (s *PipelineSQLiteOwner) AdmitRunExecutionPosture(ctx context.Context, runID string, posture executionposture.Posture) error {
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite deployment run posture admission", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.runLifecycleCandidates, func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			return admitRunExecutionPostureTx(ctx, tx, false, runID, posture)
		})
	})
	return standalonePipelineMutationError(result)
}

func admitRunExecutionPostureTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string, posture executionposture.Posture) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return fmt.Errorf("deployment run posture admission requires run_id")
	}
	if err := posture.Admit(executionmode.Mock, "deployment run lifecycle admission"); err != nil {
		return err
	}
	if posture == executionposture.Live {
		return nil
	}
	query := `SELECT EXISTS (
		SELECT 1 FROM event_deliveries d JOIN events e ON e.event_id = d.event_id AND e.run_id = d.run_id
		WHERE d.run_id = ? AND d.status IN ('pending', 'in_progress', 'failed') AND e.execution_mode = 'live'
		UNION ALL SELECT 1 FROM timers WHERE run_id = ? AND status = 'active' AND execution_mode = 'live'
		UNION ALL SELECT 1 FROM decision_cards WHERE run_id = ? AND status = 'pending' AND execution_mode = 'live'
		UNION ALL SELECT 1 FROM activity_attempts WHERE run_id = ? AND status IN ('started', 'uncertain') AND execution_mode = 'live'
		UNION ALL SELECT 1 FROM flow_instance_runtime_readiness
			WHERE run_id = ? AND (phase != 'ready' OR creation_event_emitted_at IS NULL)
			AND json_extract(plan, '$.execution_mode') = 'live'
		UNION ALL SELECT 1 FROM events e
			LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id
				AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
			WHERE e.run_id = ? AND e.execution_mode = 'live' AND receipt.event_id IS NULL
			AND NOT EXISTS (
				SELECT 1 FROM decision_card_route_obligations route
				WHERE route.event_id = e.event_id AND route.status <> 'completed'
			)
			AND ` + sqliteDiagnosticDirectReplayExclusionSQL("e") + `
		UNION ALL SELECT 1 FROM decision_card_route_obligations route
			JOIN events e ON e.event_id = route.event_id
			WHERE route.run_id = ? AND route.status = 'pending' AND e.execution_mode = 'live'
	)`
	args := []any{runID, runID, runID, runID, runID, runID}
	args = append(args, diagnosticDirectReplayEventArgs()...)
	args = append(args, runID)
	if postgres {
		query = `SELECT EXISTS (
			SELECT 1 FROM event_deliveries d JOIN events e ON e.event_id = d.event_id AND e.run_id = d.run_id
			WHERE d.run_id = $1::uuid AND d.status IN ('pending', 'in_progress', 'failed') AND e.execution_mode = 'live'
			UNION ALL SELECT 1 FROM timers WHERE run_id = $1::uuid AND status = 'active' AND execution_mode = 'live'
			UNION ALL SELECT 1 FROM decision_cards WHERE run_id = $1::uuid AND status = 'pending' AND execution_mode = 'live'
			UNION ALL SELECT 1 FROM activity_attempts WHERE run_id = $1::uuid AND status IN ('started', 'uncertain') AND execution_mode = 'live'
			UNION ALL SELECT 1 FROM flow_instance_runtime_readiness
				WHERE run_id = $1::uuid AND (phase != 'ready' OR creation_event_emitted_at IS NULL)
				AND plan->>'execution_mode' = 'live'
			UNION ALL SELECT 1 FROM events e
				LEFT JOIN event_receipts receipt ON receipt.event_id = e.event_id
					AND receipt.subscriber_type = 'platform' AND receipt.subscriber_id = 'pipeline'
				WHERE e.run_id = $1::uuid AND e.execution_mode = 'live' AND receipt.event_id IS NULL
				AND NOT EXISTS (
					SELECT 1 FROM decision_card_route_obligations route
					WHERE route.event_id = e.event_id AND route.status <> 'completed'
				)
				AND ` + postgresDiagnosticDirectReplayExclusionSQL("e", 2) + `
			UNION ALL SELECT 1 FROM decision_card_route_obligations route
				JOIN events e ON e.event_id = route.event_id
				WHERE route.run_id = $1::uuid AND route.status = 'pending' AND e.execution_mode = 'live'
		)`
		args = []any{runID}
		args = append(args, diagnosticDirectReplayEventArgs()...)
	}
	var blocked bool
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&blocked); err != nil {
		return fmt.Errorf("inspect deployment run execution posture: %w", err)
	}
	if blocked {
		return posture.Admit(executionmode.Live, "deployment run activation or lifecycle mutation")
	}
	return nil
}
