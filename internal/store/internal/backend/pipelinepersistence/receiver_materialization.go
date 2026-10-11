package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func (s *PipelinePostgresOwner) ReceiverMaterializedTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute) (bool, error) {
	return receiverMaterializedTx(ctx, tx, route, false)
}

func (s *PipelineSQLiteOwner) ReceiverMaterializedTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute) (bool, error) {
	return receiverMaterializedTx(ctx, tx, route, true)
}

// Delivery settlement is not state ownership. The existing target persistence
// owner admits the exact persisted halves before dependent execution is ready.
func receiverMaterializedTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute, sqlite bool) (bool, error) {
	if tx == nil || !route.Recipient.IsAgent() || route.Initialization.Empty() {
		return false, fmt.Errorf("receiver materialization requires a transaction and exact construction dependency")
	}
	if err := events.ValidateDeliveryRoutes([]events.DeliveryRoute{route}); err != nil {
		return false, err
	}
	if err := route.Initialization.ValidateRoute(route); err != nil {
		return false, err
	}
	target := route.Initialization.ConstructionTarget().Route()
	receiver := route.Initialization.ReceiverRoute()
	scope, logical := receiver.ScopeKey, receiver.InstanceID
	if receiver.Presence == agentidentity.RouteRoot {
		scope, logical = ".", route.AgentIdentity.RunID
	}
	flow, err := flowidentity.NewRunScopedFlowInstance(route.AgentIdentity.RunID,
		flowidentity.StoredRoute(scope, logical, target.FlowInstance))
	if err != nil {
		return false, err
	}
	if !flow.MatchesAgentRoute(route.AgentIdentity) {
		return false, fmt.Errorf("receiver materialization target contradicts exact agent coordinates")
	}
	entity := identity.NormalizeEntityID(target.EntityID)
	record, err := loadWorkflowTargetPersistence(ctx, tx, flow, entity, sqlite)
	if err != nil {
		return false, err
	}
	if record.Presence == pipeline.WorkflowTargetPersistenceAbsent {
		return false, nil
	}
	if !record.Presence.Constructed() {
		return false, fmt.Errorf("receiver construction persistence is incomplete")
	}
	if _, err := record.DecodeComplete(flow.Route, entity); err != nil {
		return false, err
	}
	if record.Lifecycle.Status == "terminated" {
		return false, fmt.Errorf("materialized receiver lifecycle has terminated")
	}
	if !route.Initialization.Empty() {
		if err := route.Initialization.ValidateRoute(route); err != nil {
			return false, err
		}
		var receipt []byte
		if err := tx.QueryRowContext(ctx, `SELECT projection FROM workflow_instance_initial_materializations
			WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3`, flow.RunID, flow.Route.InstancePath, entity.String()).Scan(&receipt); err != nil {
			return false, fmt.Errorf("load exact receiver construction receipt: %w", err)
		}
		original, err := pipeline.ProjectFlowConstructionPublication(receipt, flow, entity.String())
		if err != nil {
			return false, err
		}
		if original.CreatingInput.EventID != route.Initialization.CreatingEventID() {
			return false, fmt.Errorf("receiver dependency contradicts original construction publication")
		}
	}
	return true, nil
}
