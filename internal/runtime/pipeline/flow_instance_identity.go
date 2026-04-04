package pipeline

import (
	"strings"

	"swarm/internal/events"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	"swarm/internal/runtime/semanticview"
)

type FlowInstanceIdentity struct {
	TemplateID   string
	ScopeKey     string
	InstanceID   string
	InstancePath string
	EntityID     string
}

func DeriveFlowInstanceIdentity(source semanticview.Source, flowID, instanceID string) FlowInstanceIdentity {
	identity := runtimeflowidentity.Derive(source, flowID, instanceID)
	return FlowInstanceIdentity{
		TemplateID:   identity.TemplateID,
		ScopeKey:     identity.ScopeKey,
		InstanceID:   identity.InstanceID,
		InstancePath: identity.InstancePath,
		EntityID:     identity.EntityID,
	}
}

func deriveFlowInstanceIdentity(source semanticview.Source, flowID, instanceID string) FlowInstanceIdentity {
	return DeriveFlowInstanceIdentity(source, flowID, instanceID)
}

func workflowInstanceScopeKey(source semanticview.Source, instance WorkflowInstance, fallbackFlowID string) string {
	return runtimeflowidentity.InstanceScopeKey(
		source,
		strings.TrimSpace(instance.WorkflowName),
		workflowInstancePath(instance),
		fallbackFlowID,
	)
}

func workflowInstancePath(instance WorkflowInstance) string {
	if flowPath := strings.Trim(strings.TrimSpace(asString(instance.Metadata["flow_path"])), "/"); flowPath != "" {
		return flowPath
	}
	if storageRef := strings.Trim(strings.TrimSpace(instance.StorageRef), "/"); storageRef != "" {
		return storageRef
	}
	return strings.Trim(strings.TrimSpace(asString(instance.Metadata["storage_ref"])), "/")
}

func workflowInstanceMaterializedPath(instance WorkflowInstance) string {
	return strings.Trim(strings.TrimSpace(asString(instance.Metadata["flow_path"])), "/")
}

func workflowInstanceLogicalIDFromInstance(instance WorkflowInstance) string {
	if instanceID := strings.TrimSpace(asString(instance.Metadata["instance_id"])); instanceID != "" {
		return instanceID
	}
	return runtimeflowidentity.LogicalInstanceID(workflowInstancePath(instance))
}

func workflowInstanceMaterializedIdentity(instance WorkflowInstance) (flowPath string, instanceID string, ok bool) {
	flowPath = workflowInstanceMaterializedPath(instance)
	if flowPath == "" {
		return "", "", false
	}
	instanceID = strings.TrimSpace(asString(instance.Metadata["instance_id"]))
	if instanceID == "" {
		instanceID = runtimeflowidentity.LogicalInstanceID(flowPath)
	}
	if instanceID == "" {
		return "", "", false
	}
	return flowPath, instanceID, true
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
	entityID := strings.TrimSpace(firstNonEmptyString(
		currentEntityID,
		state.EntityID,
		inboundEntityID,
		workflowEventEntityID(trigger),
		trigger.EntityID(),
	))
	if !workflowEmitTargetsParentEntity(source, flowID, eventType) {
		return entityID
	}
	if strings.TrimSpace(asString(state.Metadata["flow_path"])) == "" {
		return entityID
	}
	return strings.TrimSpace(firstNonEmptyString(
		asString(state.Metadata["parent_entity_id"]),
		trigger.EntityID(),
		entityID,
	))
}
