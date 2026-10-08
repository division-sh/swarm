package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func selectedCancellationRecoveryScope(ctx context.Context, tx *sql.Tx, executionID string) (selectedRuntimeAuthorityRow, error) {
	var scope selectedRuntimeAuthorityRow
	id, err := uuid.Parse(executionID)
	if err != nil || id == uuid.Nil || id.String() != executionID {
		return scope, fmt.Errorf("selected cancellation recovery requires exact execution identity")
	}
	var noLease bool
	var bundleHash string
	err = tx.QueryRowContext(ctx, `SELECT CAST(e.execution_id AS TEXT),CAST(e.fork_run_id AS TEXT),e.generation,
		e.admission_fingerprint,e.container_plan_fingerprint,e.actor_census_fingerprint,e.effective_config_fingerprint,
		e.state,e.execution_owner,e.fence_generation,e.lease_expires_at IS NULL,r.bundle_hash
		FROM run_fork_selected_contract_runtime_executions e JOIN runs r ON r.run_id=e.fork_run_id WHERE e.execution_id=$1`, executionID).
		Scan(&scope.executionID, &scope.forkRunID, &scope.generation, &scope.admissionFingerprint, &scope.containerFingerprint,
			&scope.actorFingerprint, &scope.configFingerprint, &scope.state, &scope.owner, &scope.fence, &noLease, &bundleHash)
	if err != nil {
		return scope, err
	}
	fact, found := correlation.SourceArtifactFactFromContext(ctx)
	if !found || fact.Validate() != nil || fact.BundleHash() != bundleHash || !noLease || (scope.state != "failed" && scope.state != "closed") {
		return scope, fmt.Errorf("selected cancellation recovery lacks fenced exact-source possession")
	}
	return scope, nil
}

func hasSelectedCanceledOrigins(ctx context.Context, mutation *mutationprotocol.Attempt, executionID string) (pending bool, err error) {
	err = mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM runtime_agent_turn_lifetimes t
			JOIN run_fork_selected_contract_runtime_executions e ON e.fork_run_id=t.run_id
			WHERE e.execution_id=$1 AND t.cancel_reason IS NOT NULL AND t.settled_at IS NULL)`, executionID).Scan(&pending)
	})
	return pending, err
}

func (s *EffectPostgresOwner) HasSelectedCanceledOriginsTx(ctx context.Context, mutation *mutationprotocol.Attempt, executionID string) (bool, error) {
	return hasSelectedCanceledOrigins(ctx, mutation, executionID)
}

func (s *EffectSQLiteOwner) HasSelectedCanceledOriginsTx(ctx context.Context, mutation *mutationprotocol.Attempt, executionID string) (bool, error) {
	return hasSelectedCanceledOrigins(ctx, mutation, executionID)
}

func listSelectedCanceledTurnRecoveries(ctx context.Context, mutation *mutationprotocol.Attempt, postgres bool, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, executionID string, request effects.RecoveryRequest) (turns []effects.TurnExecutionResult, err error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	err = mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := selectedCancellationRecoveryScope(ctx, tx, executionID); err != nil {
			return err
		}
		turns, err = listCanceledTurnRecoveries(ctx, tx, postgres, delivery, directives, request, executionID)
		return err
	})
	return turns, err
}

func (s *EffectPostgresOwner) ListSelectedCanceledTurnRecoveriesTx(ctx context.Context, mutation *mutationprotocol.Attempt, executionID string, request effects.RecoveryRequest) ([]effects.TurnExecutionResult, error) {
	return listSelectedCanceledTurnRecoveries(ctx, mutation, true, s.delivery, s.directives, executionID, request)
}

func (s *EffectSQLiteOwner) ListSelectedCanceledTurnRecoveriesTx(ctx context.Context, mutation *mutationprotocol.Attempt, executionID string, request effects.RecoveryRequest) ([]effects.TurnExecutionResult, error) {
	return listSelectedCanceledTurnRecoveries(ctx, mutation, false, s.delivery, s.directives, executionID, request)
}

func commitSelectedCanceledTurnRecovery(ctx context.Context, mutation *mutationprotocol.Attempt, postgres bool, delivery providerDrainDeliveryOwner, directives providerDrainDirectiveOwner, publications canceledTurnPublicationOwner, executionID string, command effects.CanceledTurnCommand) (result effects.CanceledTurnCommit, err error) {
	if err := command.Validate(); err != nil {
		return result, err
	}
	err = mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		scope, err := selectedCancellationRecoveryScope(ctx, tx, executionID)
		if err != nil {
			return err
		}
		if command.Attempt != nil && (command.Attempt.Authority.Kind != effects.AuthoritySelectedContractFork || !selectedRuntimeAuthorityIdentityMatches(command.Attempt.Authority, scope) || command.Attempt.Authority.ExecutionOwner != scope.owner) {
			return fmt.Errorf("selected cancellation cannot consume another execution's physical evidence")
		}
		turnID, _, err := businessTurnIdentity(command.Origin)
		if err != nil {
			return err
		}
		var runID string
		if err := tx.QueryRowContext(ctx, `SELECT CAST(run_id AS TEXT) FROM runtime_agent_turn_lifetimes WHERE turn_id=$1`, turnID).Scan(&runID); err != nil {
			return err
		}
		if runID != scope.forkRunID {
			return fmt.Errorf("selected cancellation origin differs from its recorded fork")
		}
		if command.Origin.Kind == effects.CompletionOriginDelivery {
			if err := delivery.ValidateSelectedOriginExecutionTx(ctx, tx, command.Origin.Delivery, executionID); err != nil {
				return err
			}
		}
		result, err = commitCanceledTurn(ctx, mutation, postgres, delivery, directives, publications, command, executionID)
		return err
	})
	return result, err
}

func (s *EffectPostgresOwner) CommitSelectedCanceledTurnRecoveryTx(ctx context.Context, mutation *mutationprotocol.Attempt, executionID string, command effects.CanceledTurnCommand) (effects.CanceledTurnCommit, error) {
	return commitSelectedCanceledTurnRecovery(ctx, mutation, true, s.delivery, s.directives, s.publications, executionID, command)
}

func (s *EffectSQLiteOwner) CommitSelectedCanceledTurnRecoveryTx(ctx context.Context, mutation *mutationprotocol.Attempt, executionID string, command effects.CanceledTurnCommand) (effects.CanceledTurnCommit, error) {
	return commitSelectedCanceledTurnRecovery(ctx, mutation, false, s.delivery, s.directives, s.publications, executionID, command)
}
