package agentpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func executingDirectiveFlowOrigins(ctx context.Context, tx *sql.Tx, postgres bool, owner flowidentity.RunScopedFlowInstance) ([]agentcontrol.DirectiveOperation, error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	query := `SELECT CAST(operation_id AS TEXT) FROM agent_directive_operations WHERE resolved_run_id=$1 AND state='executing'
		AND agent_route_presence='present' AND flow_scope_key=$2 AND flow_instance_id=$3 AND flow_instance=$4 ORDER BY operation_id`
	args := []any{owner.RunID, owner.Route.ScopeKey, owner.Route.InstanceID, owner.Route.InstancePath}
	if owner.Route.ScopeKey == "." {
		if owner.Route.InstanceID != owner.RunID || owner.Route.InstancePath != owner.RunID {
			return nil, fmt.Errorf("root directive termination requires its canonical run-root owner")
		}
		query = `SELECT CAST(operation_id AS TEXT) FROM agent_directive_operations WHERE resolved_run_id=$1 AND state='executing' AND agent_route_presence='root' ORDER BY operation_id`
		args = []any{owner.RunID}
	}
	if postgres {
		query += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	result := make([]agentcontrol.DirectiveOperation, 0, len(ids))
	for _, id := range ids {
		var op agentcontrol.DirectiveOperation
		var found bool
		if postgres {
			op, found, err = loadPostgresDirectiveOperationByID(ctx, tx, id, false)
		} else {
			op, found, err = loadSQLiteDirectiveOperationByID(ctx, tx, id)
		}
		if err != nil || !found {
			return nil, fmt.Errorf("load claimed directive: found=%t error=%v", found, err)
		}
		if op.State != agentcontrol.DirectiveOperationExecuting || !owner.MatchesAgentRoute(op.AgentIdentity) {
			return nil, fmt.Errorf("claimed directive contradicts its constructed owner")
		}
		result = append(result, op)
	}
	return result, nil
}

func (s *AgentPostgresOwner) ExecutingDirectiveFlowOriginsTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance) ([]agentcontrol.DirectiveOperation, error) {
	return executingDirectiveFlowOrigins(ctx, tx, true, owner)
}

func (s *AgentSQLiteOwner) ExecutingDirectiveFlowOriginsTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance) ([]agentcontrol.DirectiveOperation, error) {
	return executingDirectiveFlowOrigins(ctx, tx, false, owner)
}

func directiveTurnOrigin(ctx context.Context, tx *sql.Tx, postgres, lock bool, origin agentcontrol.DirectiveExecutionOrigin) (agentcontrol.DirectiveOperation, error) {
	var op agentcontrol.DirectiveOperation
	var found bool
	var err error
	if postgres {
		op, found, err = loadPostgresDirectiveOperationByID(ctx, tx, origin.OperationID, lock)
	} else {
		op, found, err = loadSQLiteDirectiveOperationByID(ctx, tx, origin.OperationID)
	}
	if err != nil || !found {
		return op, fmt.Errorf("load exact directive turn origin: found=%t error=%v", found, err)
	}
	_, err = providerDirectiveOriginPending(op, origin, op.ResolvedRunID, op.AgentIdentity)
	return op, err
}

func (s *AgentPostgresOwner) DirectiveTurnOriginTx(ctx context.Context, tx *sql.Tx, origin agentcontrol.DirectiveExecutionOrigin, lock bool) (agentcontrol.DirectiveOperation, error) {
	return directiveTurnOrigin(ctx, tx, true, lock, origin)
}

func (s *AgentSQLiteOwner) DirectiveTurnOriginTx(ctx context.Context, tx *sql.Tx, origin agentcontrol.DirectiveExecutionOrigin, lock bool) (agentcontrol.DirectiveOperation, error) {
	return directiveTurnOrigin(ctx, tx, false, lock, origin)
}
