package pipeline

import (
	"strings"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

func workflowInstanceStateControl(instance WorkflowInstance) runtimeengine.StateControl {
	return runtimeengine.StateControl{
		FlowPath:           strings.TrimSpace(instance.StorageRef),
		InstanceID:         strings.TrimSpace(instance.InstanceID),
		StorageRef:         strings.TrimSpace(instance.StorageRef),
		EntityType:         strings.TrimSpace(instance.EntityType),
		InstanceKind:       strings.TrimSpace(instance.InstanceKind),
		TemplateVersion:    strings.TrimSpace(instance.TemplateVersion),
		ParentFlowID:       strings.TrimSpace(instance.ParentFlowID),
		ParentFlowInstance: strings.Trim(strings.TrimSpace(instance.ParentFlowInstance), "/"),
		ParentEntityID:     strings.TrimSpace(instance.ParentEntityID),
	}
}

func workflowInstanceStateCarrier(instance WorkflowInstance) (runtimeengine.StateCarrier, error) {
	carrier, err := runtimeengine.StateCarrierFromPersisted(
		instance.Fields,
		instance.Bookkeeping,
		instance.Gates,
		instance.StateBuckets,
	)
	if err != nil {
		return runtimeengine.StateCarrier{}, err
	}
	carrier.Control = workflowInstanceStateControl(instance)
	return carrier, nil
}
