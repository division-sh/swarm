package agentpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func requireDirectiveTurnUncanceled(ctx context.Context, tx *sql.Tx, postgres bool, operationID string) error {
	query := `SELECT cancel_reason FROM runtime_agent_turn_lifetimes WHERE origin_kind='directive' AND origin_id=$1::uuid FOR UPDATE`
	if !postgres {
		query = `SELECT cancel_reason FROM runtime_agent_turn_lifetimes WHERE origin_kind='directive' AND origin_id=?`
	}
	var reason sql.NullString
	if err := tx.QueryRowContext(ctx, query, operationID).Scan(&reason); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if reason.Valid {
		if _, err := deliverylifecycle.ParseCancellationReason(reason.String); err != nil {
			return err
		}
		return fmt.Errorf("%w: authored cancellation requires canceled directive settlement", agentcontrol.ErrDirectiveTransitionConflict)
	}
	return nil
}

func (s *AgentPostgresOwner) SettleProviderCanceledDirectiveTx(ctx context.Context, mutation *mutationprotocol.Attempt, origin agentcontrol.DirectiveExecutionOrigin, reason deliverylifecycle.CancellationReason, at time.Time) (agentcontrol.DirectiveOperation, error) {
	return settleProviderCanceledDirective(ctx, mutation, s.pipeline, true, origin, reason, at)
}

func (s *AgentSQLiteOwner) SettleProviderCanceledDirectiveTx(ctx context.Context, mutation *mutationprotocol.Attempt, origin agentcontrol.DirectiveExecutionOrigin, reason deliverylifecycle.CancellationReason, at time.Time) (agentcontrol.DirectiveOperation, error) {
	return settleProviderCanceledDirective(ctx, mutation, s.pipeline, false, origin, reason, at)
}

func settleProviderCanceledDirective(ctx context.Context, mutation *mutationprotocol.Attempt, pipeline DirectivePipelineOwner, postgres bool, origin agentcontrol.DirectiveExecutionOrigin, reason deliverylifecycle.CancellationReason, at time.Time) (agentcontrol.DirectiveOperation, error) {
	if origin.Validate() != nil {
		return agentcontrol.DirectiveOperation{}, fmt.Errorf("canceled directive requires exact admitted operation authority")
	}
	if _, err := deliverylifecycle.ParseCancellationReason(string(reason)); err != nil {
		return agentcontrol.DirectiveOperation{}, err
	}
	var result agentcontrol.DirectiveOperation
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var op agentcontrol.DirectiveOperation
		var found bool
		var err error
		if postgres {
			op, found, err = loadPostgresDirectiveOperationByID(ctx, tx, origin.OperationID, true)
		} else {
			op, found, err = loadSQLiteDirectiveOperationByID(ctx, tx, origin.OperationID)
		}
		if err != nil || !found {
			return fmt.Errorf("load canceled directive: found=%t error=%v", found, err)
		}
		if op.OperationID != origin.OperationID || op.ExecutionOwnerID != origin.ExecutionOwnerID {
			return fmt.Errorf("canceled directive contradicts exact operation owner")
		}
		if op.State == agentcontrol.DirectiveOperationCanceled && op.CancellationReason == reason {
			result = op
			return nil
		}
		if op.State != agentcontrol.DirectiveOperationExecuting {
			return agentcontrol.ErrorForDirectiveOperation(op)
		}
		if err := pipeline.TerminalizePipelineObligationTx(ctx, mutation, op.DirectiveEventID, pipelineobligation.Terminal(string(reason), nil), at); err != nil {
			return err
		}
		query := `UPDATE agent_directive_operations SET state='canceled',cancellation_reason=$1,execution_lease_expires_at=NULL,completed_at=$2,updated_at=$2,expires_at=$3 WHERE operation_id=$4::uuid AND execution_owner_id=$5 AND state='executing'`
		args := []any{string(reason), at, terminalDirectiveExpiry(agentcontrol.DirectiveOperationCanceled, at, directiveOperationDefaultTTL), origin.OperationID, origin.ExecutionOwnerID}
		if !postgres {
			query = `UPDATE agent_directive_operations SET state='canceled',cancellation_reason=?,execution_lease_expires_at=NULL,completed_at=?,updated_at=?,expires_at=? WHERE operation_id=? AND execution_owner_id=? AND state='executing'`
			args = []any{string(reason), at, at, terminalDirectiveExpiry(agentcontrol.DirectiveOperationCanceled, at, directiveOperationDefaultTTL), origin.OperationID, origin.ExecutionOwnerID}
		}
		write, err := tx.ExecContext(ctx, query, args...)
		if err := requireDirectiveTransition(write, err); err != nil {
			return err
		}
		if postgres {
			result, found, err = loadPostgresDirectiveOperationByID(ctx, tx, origin.OperationID, false)
		} else {
			result, found, err = loadSQLiteDirectiveOperationByID(ctx, tx, origin.OperationID)
		}
		if err != nil || !found {
			return fmt.Errorf("load acknowledged canceled directive: found=%t error=%v", found, err)
		}
		return recordDirectiveAuthorActivity(ctx, mutation, result, result.CompletedAt, nil)
	})
	return result, err
}
