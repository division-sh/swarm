package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func (s *AgentPostgresOwner) CancelPreparedDirectiveTx(ctx context.Context, mutation *mutationprotocol.Attempt, expected agentcontrol.DirectiveOperation, at time.Time) (agentcontrol.DirectiveOperation, error) {
	return cancelPreparedDirective(ctx, mutation, s.pipeline, true, expected, at)
}

func (s *AgentSQLiteOwner) CancelPreparedDirectiveTx(ctx context.Context, mutation *mutationprotocol.Attempt, expected agentcontrol.DirectiveOperation, at time.Time) (agentcontrol.DirectiveOperation, error) {
	return cancelPreparedDirective(ctx, mutation, s.pipeline, false, expected, at)
}

func cancelPreparedDirective(ctx context.Context, mutation *mutationprotocol.Attempt, pipeline DirectivePipelineOwner, postgres bool, expected agentcontrol.DirectiveOperation, at time.Time) (agentcontrol.DirectiveOperation, error) {
	var result agentcontrol.DirectiveOperation
	err := mutation.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var op agentcontrol.DirectiveOperation
		var found bool
		var err error
		if postgres {
			op, found, err = loadPostgresDirectiveOperationByID(ctx, tx, expected.OperationID, true)
		} else {
			op, found, err = loadSQLiteDirectiveOperationByID(ctx, tx, expected.OperationID)
		}
		if err != nil || !found {
			return fmt.Errorf("load queued directive cancellation: found=%t error=%v", found, err)
		}
		if op.State != agentcontrol.DirectiveOperationPrepared || expected.State != op.State || op.AgentIdentity != expected.AgentIdentity || op.DirectiveEventID != expected.DirectiveEventID ||
			op.ExecutionOwnerID != "" || !op.ExecutionAdmittedAt.IsZero() || !op.ExecutionLeaseExpiresAt.IsZero() {
			return fmt.Errorf("%w: queued cancellation lost exact prepared operation", agentcontrol.ErrDirectiveTransitionConflict)
		}
		if err := pipeline.TerminalizePipelineObligationTx(ctx, mutation, op.DirectiveEventID, pipelineobligation.Terminal("terminate", nil), at); err != nil {
			return err
		}
		query := `UPDATE agent_directive_operations SET state='canceled',cancellation_reason='terminate',completed_at=$1,updated_at=$1,expires_at=$2 WHERE operation_id=$3 AND state='prepared' AND execution_owner_id IS NULL AND execution_admitted_at IS NULL AND execution_lease_expires_at IS NULL`
		write, err := tx.ExecContext(ctx, query, at, terminalDirectiveExpiry(agentcontrol.DirectiveOperationCanceled, at, directiveOperationDefaultTTL), op.OperationID)
		if err := requireDirectiveTransition(write, err); err != nil {
			return err
		}
		if postgres {
			result, found, err = loadPostgresDirectiveOperationByID(ctx, tx, op.OperationID, false)
		} else {
			result, found, err = loadSQLiteDirectiveOperationByID(ctx, tx, op.OperationID)
		}
		if err != nil || !found {
			return fmt.Errorf("load canceled prepared directive: found=%t error=%v", found, err)
		}
		return recordDirectiveAuthorActivity(ctx, mutation, result, result.CompletedAt, nil)
	})
	return result, err
}
