package effectpersistence

import (
	"context"
	"database/sql"
	"errors"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

// The selected owner fences predecessor authority in this transaction before
// delegating effect settlement. This operation never grants or retries execution.
func (s *EffectPostgresOwner) RecoverSelectedForkEffectsTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *runforkrevision.Effects, executionID string, req runtimeeffects.RecoveryRequest) (runtimeeffects.RecoverySummary, error) {
	candidates, err := selectedRecoveryCandidates(ctx, tx, true, executionID, req)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	completion, err := reconcileCompletionAttemptsPostgres(ctx, tx, s.llm, s.delivery, s.directives, story, effects, externalEffectRecoveryAttemptSet(candidates), req.Now(), executionID)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	generic, err := reconcileGenericExternalEffectCandidates(ctx, tx, true, candidates, req.Now())
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	if err := recordRecoveredExternalEffectStories(ctx, story, tx, candidates, req.Now(), true); err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	return runtimeeffects.RecoverySummary{PrelaunchTerminal: completion.PrelaunchTerminal + generic.PrelaunchTerminal, OutcomeUncertain: completion.OutcomeUncertain + generic.OutcomeUncertain}, nil
}

func (s *EffectSQLiteOwner) RecoverSelectedForkEffectsTx(ctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation, effects *runforkrevision.Effects, executionID string, req runtimeeffects.RecoveryRequest) (runtimeeffects.RecoverySummary, error) {
	candidates, err := selectedRecoveryCandidates(ctx, tx, false, executionID, req)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	completion, err := reconcileCompletionAttemptsSQLite(ctx, tx, s.llm, s.delivery, s.directives, story, effects, externalEffectRecoveryAttemptSet(candidates), req.Now(), executionID)
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	generic, err := reconcileGenericExternalEffectCandidates(ctx, tx, false, candidates, req.Now())
	if err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	if err := recordRecoveredExternalEffectStories(ctx, story, tx, candidates, req.Now(), false); err != nil {
		return runtimeeffects.RecoverySummary{}, err
	}
	return runtimeeffects.RecoverySummary{PrelaunchTerminal: completion.PrelaunchTerminal + generic.PrelaunchTerminal, OutcomeUncertain: completion.OutcomeUncertain + generic.OutcomeUncertain}, nil
}

func selectedRecoveryCandidates(ctx context.Context, tx *sql.Tx, postgres bool, executionID string, req runtimeeffects.RecoveryRequest) ([]externalEffectRecoveryCandidate, error) {
	id, err := uuid.Parse(executionID)
	if err != nil || id == uuid.Nil || id.String() != executionID {
		return nil, errors.New("selected effect recovery requires exact execution identity")
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	var state string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, executionID).Scan(&state); err != nil {
		return nil, err
	}
	if state != "failed" {
		return nil, errors.New("selected effect recovery requires fenced failed authority")
	}
	candidates, err := loadExternalEffectRecoveryCandidates(ctx, tx, postgres, executionID)
	if err != nil {
		return nil, err
	}
	if err := admitExternalEffectRecoveryCandidates(req, candidates); err != nil {
		return nil, err
	}
	return candidates, nil
}
