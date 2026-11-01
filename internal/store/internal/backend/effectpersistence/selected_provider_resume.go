package effectpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *EffectPostgresOwner) CanResumeSelectedProviderEffectsTx(ctx context.Context, mutation *mutationprotocol.Attempt, executionID string, req runtimeeffects.RecoveryRequest) (bool, error) {
	var ready bool
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		ready, err = selectedProviderResumeBeforeFenceTx(ctx, tx, executionID, req, false)
		return err
	})
	return ready, err
}

func (s *EffectSQLiteOwner) CanResumeSelectedProviderEffectsTx(ctx context.Context, mutation *mutationprotocol.Attempt, executionID string, req runtimeeffects.RecoveryRequest) (bool, error) {
	var ready bool
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		ready, err = selectedProviderResumeBeforeFenceTx(ctx, tx, executionID, req, true)
		return err
	})
	return ready, err
}

func selectedProviderResumeBeforeFenceTx(ctx context.Context, tx *sql.Tx, executionID string, req runtimeeffects.RecoveryRequest, sqlite bool) (bool, error) {
	if err := req.Validate(); err != nil {
		return false, err
	}
	candidates, err := loadExternalEffectRecoveryCandidates(ctx, tx, !sqlite, executionID)
	if err != nil {
		return false, err
	}
	if err := admitExternalEffectRecoveryCandidates(req, candidates); err != nil {
		return false, err
	}
	var runID string
	if err := tx.QueryRowContext(ctx, `SELECT fork_run_id FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, executionID).Scan(&runID); err != nil {
		return false, err
	}
	return selectedProviderEffectsReadyTx(ctx, tx, runID, sqlite, true, req.Now())
}

// SelectedProviderEffectsSettledTx recognizes only settled work or the exact
// immutable retry chain. It does not grant execution or accept other uncertainty.
func SelectedProviderEffectsSettledTx(ctx context.Context, tx *sql.Tx, runID string, sqlite bool) (bool, error) {
	return selectedProviderEffectsReadyTx(ctx, tx, runID, sqlite, false, time.Now().UTC())
}

func selectedProviderEffectsReadyTx(ctx context.Context, tx *sql.Tx, runID string, sqlite, beforeFence bool, now time.Time) (bool, error) {
	var unsafe int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_operations o WHERE o.selected_execution_id IN
		(SELECT execution_id FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1)
		AND (o.state IS NULL OR o.state<>'settled'
			OR NOT EXISTS (SELECT 1 FROM runtime_external_effect_attempts a WHERE a.operation_id=o.operation_id)
			OR EXISTS (SELECT 1 FROM runtime_external_effect_attempts a WHERE a.operation_id=o.operation_id AND (a.state IS NULL OR a.state<>'settled')))`, runID).Scan(&unsafe); err != nil {
		return false, err
	}
	if unsafe == 0 {
		return !beforeFence, nil
	}
	query := postgresCompletionRecoverySelect
	if sqlite {
		query = sqliteCompletionRecoverySelect
	}
	rows, err := tx.QueryContext(ctx, query+`WHERE o.effect_kind='provider_turn' AND a.state<>'settled'
		AND o.selected_execution_id IN (SELECT execution_id FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1)`, runID)
	if err != nil {
		return false, err
	}
	candidates, err := scanCompletionRecoveryAttempts(rows)
	if err != nil {
		return false, err
	}
	proved := make(map[string]struct{})
	for _, recovered := range candidates {
		attempt, _, err := completionRecoverySettlement(recovered, runtimeeffects.StateOutcomeUncertain, nil, now)
		if err != nil {
			return false, err
		}
		journal, err := validatedProviderCallJournalTx(ctx, tx, !sqlite, recovered, attempt)
		if err != nil || journal == nil {
			return false, err
		}
		ready := false
		switch recovered.State {
		case string(runtimeeffects.StateLaunched), string(runtimeeffects.StateResponseObserved):
			if !beforeFence {
				return false, nil
			}
			ready, err = providerResumeTurnCurrentTx(ctx, tx, !sqlite, attempt, journal.Call, now)
			if err == nil && ready {
				ready, err = providerResumeRetryAvailableTx(ctx, tx, attempt)
			}
		case string(runtimeeffects.StateOutcomeUncertain):
			ready, err = providerResumeDecisionRecordedTx(ctx, tx, sqlite, recovered, *journal, now)
		}
		if err != nil || !ready {
			return false, err
		}
		proved[recovered.OperationID] = struct{}{}
	}
	return len(proved) == unsafe, nil
}

func providerResumeRetryAvailableTx(ctx context.Context, tx *sql.Tx, attempt runtimeeffects.Attempt) (bool, error) {
	claim := attempt.Origin.Delivery
	var available bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM event_deliveries d JOIN event_delivery_attempts a
		ON a.delivery_id=d.delivery_id AND a.claim_version=d.claim_version
		WHERE d.delivery_id=$1 AND a.claim_token=$2 AND d.claim_version=$3 AND d.status='in_progress'
		AND a.open_marker=TRUE AND d.retry_count<d.max_retries)`, claim.DeliveryID(), claim.PersistenceToken(), claim.Version()).Scan(&available)
	return available, err
}

func providerResumeDecisionRecordedTx(ctx context.Context, tx *sql.Tx, sqlite bool, recovered completionRecoveryAttempt, journal runtimeeffects.ProviderCallJournal, now time.Time) (bool, error) {
	var pending bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM event_delivery_attempts prior JOIN event_deliveries d ON d.delivery_id=prior.delivery_id
		WHERE prior.delivery_id=$1 AND prior.claim_version=$2 AND prior.claim_token=$3 AND prior.closure_kind='settled'
		AND prior.open_marker=FALSE AND prior.completed_at IS NOT NULL AND prior.outcome='retry_scheduled'
		AND prior.reason_code='effect_recovery_outcome_unconfirmed' AND d.status='failed' AND d.claim_version=prior.claim_version
		AND d.current_attempt_version IS NULL AND d.next_eligible_at IS NOT NULL AND d.retry_count>0 AND d.retry_count<=d.max_retries)`,
		recovered.OriginDeliveryID, recovered.OriginClaimVersion, recovered.OriginClaimToken).Scan(&pending)
	if err != nil || pending {
		return pending, err
	}
	query := postgresCompletionRecoverySelect
	if sqlite {
		query = sqliteCompletionRecoverySelect
	}
	rows, err := tx.QueryContext(ctx, query+`JOIN event_delivery_attempts prior ON prior.delivery_id=a.origin_delivery_id
		AND prior.claim_version=$2 AND prior.claim_token=$3
		WHERE a.origin_delivery_id=$1 AND a.origin_claim_version=$2+1 AND o.effect_kind='provider_turn'
		AND prior.closure_kind='settled' AND prior.open_marker=FALSE AND prior.completed_at IS NOT NULL
		AND prior.outcome='retry_scheduled' AND prior.reason_code='effect_recovery_outcome_unconfirmed'
		AND a.state='settled' AND o.state='settled' LIMIT 2`, recovered.OriginDeliveryID, recovered.OriginClaimVersion, recovered.OriginClaimToken)
	if err != nil {
		return false, err
	}
	children, err := scanCompletionRecoveryAttempts(rows)
	if err != nil || len(children) != 1 {
		return false, err
	}
	child := children[0]
	attempt, _, err := completionRecoverySettlement(child, runtimeeffects.StateSettled, nil, now)
	if err != nil {
		return false, err
	}
	successor, err := validatedProviderCallJournalTx(ctx, tx, !sqlite, child, attempt)
	if err != nil || successor == nil || successor.Call.PredecessorAttemptID != recovered.AttemptID || successor.SourceHash != journal.SourceHash {
		return false, err
	}
	oldInput, _ := json.Marshal(journal.Call.Request)
	newInput, _ := json.Marshal(successor.Call.Request)
	return string(oldInput) == string(newInput), nil
}
