package bus

import (
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func ConstructedFlowInstanceIdentityFixture(source semanticview.Source, flowID, instanceID, runID string) runtimeflowidentity.Instance {
	if source == nil {
		return runtimeflowidentity.Instance{}
	}
	schema, found := source.FlowSchemaByID(flowID)
	if !found {
		return runtimeflowidentity.Instance{}
	}
	if flowID == semanticview.RootExecutionFlowID(source) {
		return runtimeflowidentity.Stored(source, flowID, runID, runID, runtimeflowidentity.EntityID(runID), "")
	}
	bundle, found := semanticview.Bundle(source)
	if !found {
		return runtimeflowidentity.Instance{}
	}
	view, found := bundle.FlowViewByID(flowID)
	if !found || view.Parent == nil {
		return runtimeflowidentity.Instance{}
	}
	parent := ConstructedFlowInstanceIdentityFixture(source, view.Parent.Paths.FlowPath, "", runID)
	var child runtimeflowidentity.Instance
	var err error
	if schema.Instance.Empty() {
		child, err = runtimeflowidentity.KeylessChild(source, parent, flowID)
	} else {
		child, err = runtimeflowidentity.KeyedChild(source, parent, flowID, instanceID)
	}
	if err != nil {
		return runtimeflowidentity.Instance{}
	}
	return child
}

func StoredFlowInstanceIdentityFixture(source semanticview.Source, flowID, instanceID, runID, entityID string) runtimeflowidentity.Instance {
	instance := ConstructedFlowInstanceIdentityFixture(source, flowID, instanceID, runID)
	instance.EntityID = entityID
	return instance
}
