package pipeline

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func workflowInstanceRouteForPath(instancePath string) (runtimeflowidentity.Route, error) {
	route := runtimeflowidentity.RouteForInstancePath(instancePath)
	if !route.Valid() {
		return runtimeflowidentity.Route{}, fmt.Errorf("workflow instance route requires an exact instance path")
	}
	return route, nil
}

func workflowInstanceRouteForExecution(source semanticview.Source, flowID, explicitPath string) (runtimeflowidentity.Route, error) {
	flowID = strings.TrimSpace(flowID)
	instancePath := strings.Trim(strings.TrimSpace(explicitPath), "/")
	expectedScope := runtimeflowidentity.ScopeKey(source, flowID)
	if instancePath == "" && source != nil {
		if schema, ok := source.FlowSchemaByID(flowID); ok && !strings.EqualFold(strings.TrimSpace(schema.Mode), "template") && flowID != strings.TrimSpace(source.WorkflowName()) {
			instancePath = expectedScope
		}
	}
	if instancePath == "" {
		return runtimeflowidentity.Route{}, fmt.Errorf("workflow instance route is unavailable for flow %q", flowID)
	}
	rootRunScope := source != nil && flowID == strings.TrimSpace(source.WorkflowName())
	if rootRunScope {
		if _, err := uuid.Parse(instancePath); err == nil {
			return runtimeflowidentity.StoredRoute(instancePath, runtimeflowidentity.LogicalInstanceID(instancePath), instancePath), nil
		}
	}
	if expectedScope == "" || (instancePath != expectedScope && !strings.HasPrefix(instancePath, expectedScope+"/")) {
		return runtimeflowidentity.Route{}, fmt.Errorf("workflow instance route %q is outside flow scope %q", instancePath, expectedScope)
	}
	return runtimeflowidentity.StoredRoute(expectedScope, runtimeflowidentity.LogicalInstanceID(instancePath), instancePath), nil
}

func workflowInstanceRouteForPersisted(source semanticview.Source, instance WorkflowInstance) (runtimeflowidentity.Route, error) {
	instancePath := strings.Trim(strings.TrimSpace(instance.StorageRef), "/")
	if instancePath == "" {
		return runtimeflowidentity.Route{}, fmt.Errorf("persisted workflow instance is missing its canonical route")
	}
	route, err := workflowInstanceRouteForExecution(source, instance.WorkflowName, instancePath)
	if err != nil {
		return runtimeflowidentity.Route{}, fmt.Errorf("persisted workflow instance is missing its canonical route: %w", err)
	}
	if err := validateWorkflowInstanceRouteFacts(route, instance); err != nil {
		return runtimeflowidentity.Route{}, err
	}
	return route, nil
}

func validateWorkflowInstanceRouteFacts(route runtimeflowidentity.Route, instance WorkflowInstance) error {
	instancePath := strings.Trim(strings.TrimSpace(instance.StorageRef), "/")
	if instancePath == "" {
		return fmt.Errorf("persisted workflow instance is missing its row route")
	}
	if instancePath != route.InstancePath {
		return fmt.Errorf("persisted workflow instance route %q disagrees with requested route %q", instancePath, route.InstancePath)
	}
	storedPath := strings.Trim(strings.TrimSpace(instance.StorageRef), "/")
	if storedPath == "" {
		return fmt.Errorf("persisted workflow instance %q is missing flow_path", instancePath)
	}
	if storedPath != route.InstancePath {
		return fmt.Errorf("persisted workflow instance flow_path %q disagrees with requested route %q", storedPath, route.InstancePath)
	}
	storedID := strings.TrimSpace(instance.InstanceID)
	if storedID == "" {
		return fmt.Errorf("persisted workflow instance %q is missing instance_id", instancePath)
	}
	if storedID != route.InstanceID {
		return fmt.Errorf("persisted workflow instance instance_id %q disagrees with requested route %q", storedID, route.InstanceID)
	}
	return nil
}

func workflowInstancePersistedEntityID(instance WorkflowInstance) (identity.EntityID, error) {
	entityID := identity.NormalizeEntityID(instance.EntityID)
	if entityID.IsZero() {
		return identity.EntityID(""), fmt.Errorf("persisted workflow instance %q is missing entity_id", strings.TrimSpace(instance.StorageRef))
	}
	return entityID, nil
}

func requireWorkflowInstanceIdentity(route runtimeflowidentity.Route, entityID identity.EntityID, instance WorkflowInstance) (runtimeflowidentity.Instance, error) {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	entityID = identity.NormalizeEntityID(entityID.String())
	if !route.Valid() || entityID.IsZero() {
		return runtimeflowidentity.Instance{}, fmt.Errorf("persisted workflow instance validation requires exact route and entity identity")
	}
	if err := validateWorkflowInstanceRouteFacts(route, instance); err != nil {
		return runtimeflowidentity.Instance{}, err
	}
	persistedEntityID, err := workflowInstancePersistedEntityID(instance)
	if err != nil {
		return runtimeflowidentity.Instance{}, err
	}
	if persistedEntityID != entityID {
		return runtimeflowidentity.Instance{}, fmt.Errorf("persisted workflow instance entity_id %q disagrees with requested entity %q", persistedEntityID.String(), entityID.String())
	}
	return runtimeflowidentity.Instance{
		TemplateID:     strings.TrimSpace(instance.WorkflowName),
		ScopeKey:       route.ScopeKey,
		InstanceID:     route.InstanceID,
		InstancePath:   route.InstancePath,
		EntityID:       entityID.String(),
		ParentEntityID: strings.TrimSpace(instance.ParentEntityID),
		ParentRoute: runtimeflowidentity.ParentRoute{
			FlowID: instance.ParentFlowID, FlowInstance: instance.ParentFlowInstance, EntityID: instance.ParentEntityID,
		}.Normalized(),
		HasStoredPath: true,
	}, nil
}

type FlowInstanceIdentity struct {
	runtimeflowidentity.Instance
}

func DeriveFlowInstanceIdentity(source semanticview.Source, flowID, instanceID string) FlowInstanceIdentity {
	return FlowInstanceIdentity{Instance: runtimeflowidentity.Derive(source, flowID, instanceID)}
}

func deriveFlowInstanceIdentity(source semanticview.Source, flowID, instanceID string) FlowInstanceIdentity {
	return DeriveFlowInstanceIdentity(source, flowID, instanceID)
}

func workflowInstancePersistedIdentity(source semanticview.Source, instance WorkflowInstance) (runtimeflowidentity.Persisted, error) {
	flowPath := strings.Trim(strings.TrimSpace(instance.StorageRef), "/")
	hasStoredPath := flowPath != ""
	instanceID := strings.TrimSpace(instance.InstanceID)
	entityID := strings.TrimSpace(instance.EntityID)
	persisted, err := runtimeflowidentity.StoredPersisted(
		source,
		strings.TrimSpace(instance.WorkflowName),
		strings.TrimSpace(instance.StorageRef),
		flowPath,
		instanceID,
		entityID,
		instance.ParentEntityID,
	)
	if err != nil {
		return runtimeflowidentity.Persisted{}, err
	}
	persisted.HasStoredPath = hasStoredPath
	if !hasStoredPath {
		persisted.InstanceID = runtimeflowidentity.LogicalInstanceID(persisted.InstancePath)
	}
	persisted.ParentRoute = runtimeflowidentity.ParentRoute{
		FlowID: instance.ParentFlowID, FlowInstance: instance.ParentFlowInstance, EntityID: instance.ParentEntityID,
	}.Normalized()
	return persisted, nil
}

func workflowStateIdentity(source semanticview.Source, flowID string, state WorkflowState) runtimeflowidentity.Instance {
	instance := runtimeflowidentity.Stored(
		source,
		strings.TrimSpace(flowID),
		state.Control.FlowPath,
		state.Control.InstanceID,
		strings.TrimSpace(state.EntityID),
		state.Control.ParentEntityID,
	)
	instance.ParentRoute = runtimeflowidentity.ParentRoute{
		FlowID: state.Control.ParentFlowID, FlowInstance: state.Control.ParentFlowInstance, EntityID: state.Control.ParentEntityID,
	}.Normalized()
	return instance
}

func isDescendantFlowInstance(scopeKey, instancePath string) bool {
	return runtimeflowidentity.IsDescendant(scopeKey, instancePath)
}

func resolveEmittedEntityID(
	source semanticview.Source,
	flowID, eventType string,
	state WorkflowState,
	trigger events.Event,
	currentEntityID string,
	inboundEntityID string,
) string {
	instance := workflowStateIdentity(source, flowID, state)
	entityID := strings.TrimSpace(firstNonEmptyString(
		currentEntityID,
		instance.EntityID,
		inboundEntityID,
		workflowEventEntityID(trigger),
		trigger.EntityID(),
	))
	return entityID
}
