package agentpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func (s *AgentPostgresOwner) ValidateProviderDirectiveOriginTx(ctx context.Context, tx *sql.Tx, origin runtimeagentcontrol.DirectiveExecutionOrigin, runID string, identity runtimeagentidentity.Identity) error {
	op, err := requireActivePostgresDirectiveOperation(ctx, tx, origin.OperationID)
	if err != nil {
		return err
	}
	return requireProviderDirectiveOrigin(op, origin, runID, identity)
}

func (s *AgentSQLiteOwner) ValidateProviderDirectiveOriginTx(ctx context.Context, tx *sql.Tx, origin runtimeagentcontrol.DirectiveExecutionOrigin, runID string, identity runtimeagentidentity.Identity) error {
	op, err := requireActiveSQLiteDirectiveOperation(ctx, tx, origin.OperationID)
	if err != nil {
		return err
	}
	return requireProviderDirectiveOrigin(op, origin, runID, identity)
}

func requireProviderDirectiveOrigin(op runtimeagentcontrol.DirectiveOperation, origin runtimeagentcontrol.DirectiveExecutionOrigin, runID string, identity runtimeagentidentity.Identity) error {
	if err := origin.Validate(); err != nil {
		return err
	}
	want, err := runtimeagentcontrol.NewDirectiveExecutionOrigin(op)
	if err != nil || !want.Same(origin) {
		return fmt.Errorf("directive provider origin conflicts with executing operation %s", origin.OperationID)
	}
	if runID != "" && (op.ResolvedRunID != runID || op.AgentIdentity.Normalize() != identity.Normalize()) {
		return fmt.Errorf("directive provider origin execution target conflicts with operation %s", origin.OperationID)
	}
	return nil
}

func (s *AgentPostgresOwner) RenewProviderDirectiveOriginTx(ctx context.Context, tx *sql.Tx, origin runtimeagentcontrol.DirectiveExecutionOrigin, now time.Time, lease time.Duration) error {
	if err := s.ValidateProviderDirectiveOriginTx(ctx, tx, origin, "", runtimeagentidentity.Identity{}); err != nil {
		return err
	}
	expires := now.UTC().Add(normalizeDirectiveLease(lease))
	res, err := tx.ExecContext(ctx, `UPDATE agent_directive_operations SET execution_lease_expires_at=GREATEST(execution_lease_expires_at,$3),updated_at=$4 WHERE operation_id=$1::uuid AND execution_owner_id=$2 AND state='executing'`, origin.OperationID, origin.ExecutionOwnerID, expires, now.UTC())
	return requireDirectiveTransition(res, err)
}

func (s *AgentSQLiteOwner) RenewProviderDirectiveOriginTx(ctx context.Context, tx *sql.Tx, origin runtimeagentcontrol.DirectiveExecutionOrigin, now time.Time, lease time.Duration) error {
	if err := s.ValidateProviderDirectiveOriginTx(ctx, tx, origin, "", runtimeagentidentity.Identity{}); err != nil {
		return err
	}
	expires := now.UTC().Add(normalizeDirectiveLease(lease))
	res, err := tx.ExecContext(ctx, `UPDATE agent_directive_operations SET execution_lease_expires_at=CASE WHEN execution_lease_expires_at>? THEN execution_lease_expires_at ELSE ? END,updated_at=? WHERE operation_id=? AND execution_owner_id=? AND state='executing'`, expires, expires, now.UTC(), origin.OperationID, origin.ExecutionOwnerID)
	return requireDirectiveTransition(res, err)
}

func (s *AgentPostgresOwner) SettleProviderDirectiveOriginTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, origin runtimeagentcontrol.DirectiveExecutionOrigin, state runtimeagentcontrol.DirectiveOperationState, failure runtimefailures.Envelope, now time.Time) error {
	op, ok, err := loadPostgresDirectiveOperationByID(ctx, tx, origin.OperationID, true)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("directive operation not found")
	}
	if err := requireProviderDirectiveOriginOrExactTerminal(op, origin, state, failure); err != nil {
		return err
	}
	if op.State == state {
		return nil
	}
	if err := s.pipeline.TerminalizePipelineObligationTx(ctx, tx, op.DirectiveEventID, runtimepipelineobligation.Terminal("", &failure), now); err != nil {
		return err
	}
	raw, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE agent_directive_operations SET state=$3,failure=$4::jsonb,execution_lease_expires_at=NULL,completed_at=$5,updated_at=$5,expires_at=$6 WHERE operation_id=$1::uuid AND execution_owner_id=$2 AND state='executing'`, origin.OperationID, origin.ExecutionOwnerID, string(state), string(raw), now.UTC(), terminalDirectiveExpiry(state, now, directiveOperationDefaultTTL))
	if err := requireDirectiveTransition(res, err); err != nil {
		return err
	}
	op.State, op.Failure = state, runtimefailures.CloneEnvelope(&failure)
	op.ExecutionLeaseExpiresAt = time.Time{}
	op.CompletedAt, op.UpdatedAt = now.UTC(), now.UTC()
	if state == runtimeagentcontrol.DirectiveOperationFailed {
		op.ExpiresAt = now.Add(directiveOperationDefaultTTL).UTC()
	}
	return recordDirectiveAuthorActivity(ctx, story, op, now, &failure)
}

func (s *AgentSQLiteOwner) SettleProviderDirectiveOriginTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, origin runtimeagentcontrol.DirectiveExecutionOrigin, state runtimeagentcontrol.DirectiveOperationState, failure runtimefailures.Envelope, now time.Time) error {
	op, ok, err := loadSQLiteDirectiveOperationByID(ctx, tx, origin.OperationID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("directive operation not found")
	}
	if err := requireProviderDirectiveOriginOrExactTerminal(op, origin, state, failure); err != nil {
		return err
	}
	if op.State == state {
		return nil
	}
	if err := s.pipeline.TerminalizePipelineObligationTx(ctx, tx, op.DirectiveEventID, runtimepipelineobligation.Terminal("", &failure), now); err != nil {
		return err
	}
	raw, err := runtimefailures.MarshalEnvelope(failure)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE agent_directive_operations SET state=?,failure=?,execution_lease_expires_at=NULL,completed_at=?,updated_at=?,expires_at=? WHERE operation_id=? AND execution_owner_id=? AND state='executing'`, string(state), string(raw), now.UTC(), now.UTC(), terminalDirectiveExpiry(state, now, directiveOperationDefaultTTL), origin.OperationID, origin.ExecutionOwnerID)
	if err := requireDirectiveTransition(res, err); err != nil {
		return err
	}
	op.State, op.Failure = state, runtimefailures.CloneEnvelope(&failure)
	op.ExecutionLeaseExpiresAt = time.Time{}
	op.CompletedAt, op.UpdatedAt = now.UTC(), now.UTC()
	if state == runtimeagentcontrol.DirectiveOperationFailed {
		op.ExpiresAt = now.Add(directiveOperationDefaultTTL).UTC()
	}
	return recordDirectiveAuthorActivity(ctx, story, op, now, &failure)
}

func requireProviderDirectiveOriginOrExactTerminal(op runtimeagentcontrol.DirectiveOperation, origin runtimeagentcontrol.DirectiveExecutionOrigin, state runtimeagentcontrol.DirectiveOperationState, failure runtimefailures.Envelope) error {
	if state != runtimeagentcontrol.DirectiveOperationFailed && state != runtimeagentcontrol.DirectiveOperationIndeterminate {
		return fmt.Errorf("directive provider origin terminal state %q is invalid", state)
	}
	if op.OperationID != origin.OperationID || op.ExecutionOwnerID != origin.ExecutionOwnerID {
		return fmt.Errorf("directive provider origin identity conflicts with operation %s", origin.OperationID)
	}
	if op.State == runtimeagentcontrol.DirectiveOperationExecuting {
		return nil
	}
	raw, err := runtimefailures.MarshalEnvelope(failure)
	if err == nil && op.State == state && sameDirectiveFailure(op.Failure, raw) {
		return nil
	}
	return runtimeagentcontrol.ErrorForDirectiveOperation(op)
}
