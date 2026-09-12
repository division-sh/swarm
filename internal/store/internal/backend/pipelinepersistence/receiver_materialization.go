package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
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
	if tx == nil || !route.Recipient.IsAgent() || !route.Target.MaterializingEntity() {
		return false, fmt.Errorf("receiver materialization requires a transaction and exact future agent target")
	}
	if err := events.ValidateDeliveryRoutes([]events.DeliveryRoute{route}); err != nil {
		return false, err
	}
	scope, instance, path, err := route.AgentIdentity.ExecutionCoordinates()
	if err != nil {
		return false, fmt.Errorf("receiver materialization agent coordinates: %w", err)
	}
	if path != route.Target.Route().FlowInstance {
		return false, fmt.Errorf("receiver materialization target contradicts exact agent coordinates")
	}
	flow, err := flowidentity.NewRunScopedFlowInstance(route.AgentIdentity.RunID, flowidentity.StoredRoute(scope, instance, path))
	if err != nil {
		return false, err
	}
	entity := identity.NormalizeEntityID(route.Target.Route().EntityID)
	record, err := loadWorkflowTargetPersistence(ctx, tx, flow, entity, sqlite)
	if err != nil {
		return false, err
	}
	if record.Presence == pipeline.WorkflowTargetPersistenceLifecycleOnly {
		return false, fmt.Errorf("receiver lifecycle survived without its materialized state")
	}
	if record.Presence == pipeline.WorkflowTargetPersistenceComplete {
		if _, err := record.DecodeComplete(flow.Route, entity); err != nil {
			return false, err
		}
		if record.Lifecycle.Status == "terminated" {
			return false, fmt.Errorf("materialized receiver lifecycle has terminated")
		}
	}
	return record.Presence.HasState(), nil
}
