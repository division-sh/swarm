package pipeline

import (
	"strings"

	"swarm/internal/events"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	"swarm/internal/runtime/semanticview"
)

type FlowInstanceIdentity struct {
	runtimeflowidentity.Instance
}

func DeriveFlowInstanceIdentity(source semanticview.Source, flowID, instanceID string) FlowInstanceIdentity {
	return FlowInstanceIdentity{Instance: runtimeflowidentity.Derive(source, flowID, instanceID)}
}

func deriveFlowInstanceIdentity(source semanticview.Source, flowID, instanceID string) FlowInstanceIdentity {
	return DeriveFlowInstanceIdentity(source, flowID, instanceID)
}

func StoredFlowInstance(source semanticview.Source, instance WorkflowInstance) runtimeflowidentity.Instance {
	materializedPath := strings.Trim(strings.TrimSpace(asString(instance.Metadata["flow_path"])), "/")
	entityID := strings.TrimSpace(asString(instance.Metadata["entity_id"]))
	if entityID == "" && materializedPath == "" {
		entityID = strings.TrimSpace(instance.InstanceID)
	}
	return runtimeflowidentity.Stored(
		source,
		strings.TrimSpace(instance.WorkflowName),
		materializedPath,
		strings.TrimSpace(firstNonEmptyString(asString(instance.Metadata["instance_id"]), instance.InstanceID)),
		entityID,
		strings.TrimSpace(asString(instance.Metadata["parent_entity_id"])),
	)
}

func workflowInstanceIdentity(source semanticview.Source, instance WorkflowInstance) runtimeflowidentity.Instance {
	return StoredFlowInstance(source, instance)
}

func workflowInstancePersistedIdentity(source semanticview.Source, instance WorkflowInstance) (runtimeflowidentity.Persisted, error) {
	flowPath := strings.Trim(strings.TrimSpace(asString(instance.Metadata["flow_path"])), "/")
	instanceID := strings.TrimSpace(asString(instance.Metadata["instance_id"]))
	if instanceID == "" && flowPath == "" {
		instanceID = strings.TrimSpace(instance.InstanceID)
	}
	entityID := strings.TrimSpace(asString(instance.Metadata["entity_id"]))
	if entityID == "" && flowPath == "" {
		entityID = strings.TrimSpace(firstNonEmptyString(instance.StorageRef, asString(instance.Metadata["storage_ref"]), instance.InstanceID))
	}
	return runtimeflowidentity.StoredPersisted(
		source,
		strings.TrimSpace(instance.WorkflowName),
		strings.TrimSpace(firstNonEmptyString(instance.StorageRef, asString(instance.Metadata["storage_ref"]))),
		flowPath,
		instanceID,
		entityID,
		asString(instance.Metadata["parent_entity_id"]),
	)
}

func workflowInstanceScopeKey(source semanticview.Source, instance WorkflowInstance) string {
	return workflowInstanceIdentity(source, instance).ScopeKey
}

func workflowInstancePath(source semanticview.Source, instance WorkflowInstance) string {
	return workflowInstanceIdentity(source, instance).InstancePath
}

func workflowStateIdentity(source semanticview.Source, flowID string, state WorkflowState) runtimeflowidentity.Instance {
	return runtimeflowidentity.Stored(
		source,
		strings.TrimSpace(flowID),
		asString(state.Metadata["flow_path"]),
		asString(state.Metadata["instance_id"]),
		strings.TrimSpace(state.EntityID),
		asString(state.Metadata["parent_entity_id"]),
	)
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
