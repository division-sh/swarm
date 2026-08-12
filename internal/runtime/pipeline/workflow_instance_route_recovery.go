package pipeline

import (
	"context"
	"fmt"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
)

// WorkflowInstanceRouteRecoveryProjection is the run-independent persisted
// identity/config needed to restore one active materialized route at startup.
type WorkflowInstanceRouteRecoveryProjection struct {
	Identity runtimeflowidentity.Instance
	Config   map[string]any
}

func (s *workflowInstanceStore) LoadRouteRecoveryProjection(
	ctx context.Context,
	route runtimeflowidentity.Route,
) (WorkflowInstanceRouteRecoveryProjection, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if !route.Valid() {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("flow-instance route recovery identity is required")
	}
	if s == nil || s.routeRecovery == nil {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("workflow instance store is required for route recovery %s", route.InstancePath)
	}
	record, err := s.routeRecovery.LoadActiveWorkflowRoute(ctx, route.InstancePath)
	if err != nil {
		return WorkflowInstanceRouteRecoveryProjection{}, err
	}

	config, control, err := decodeWorkflowInstanceConfigPayload(record.Config, workflowInstancePersistedControl{
		StorageRef: route.InstancePath,
		EntityID:   record.EntityID,
	})
	if err != nil {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("decode flow instance for route recovery %s: %w", route.InstancePath, err)
	}
	persistedProjection := workflowInstancePersistedProjection{Config: config, Control: control}
	instance := WorkflowInstance{
		StorageRef:   route.InstancePath,
		WorkflowName: record.WorkflowName,
		Config:       config,
		Metadata:     persistedProjection.Metadata(),
	}
	persistedIdentity, err := requireWorkflowInstanceIdentity(route, identity.NormalizeEntityID(record.EntityID), instance)
	if err != nil {
		return WorkflowInstanceRouteRecoveryProjection{}, fmt.Errorf("validate flow instance identity for route recovery %s: %w", route.InstancePath, err)
	}
	return WorkflowInstanceRouteRecoveryProjection{
		Identity: persistedIdentity,
		Config:   cloneStringAnyMap(config),
	}, nil
}
