package flowidentity

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// KeylessChild binds an authored immediate child to its constructed parent.
// A keyed ancestor's discriminator stays in the concrete path, not the scope.
func KeylessChild(source semanticview.Source, parent Instance, childFlowID string) (Instance, error) {
	if source == nil || !parent.Route().Valid() || parent.EntityID == "" {
		return Instance{}, fmt.Errorf("keyless child requires its exact constructed parent")
	}
	schema, found := source.FlowSchemaByID(childFlowID)
	if !found || !schema.Instance.Empty() {
		return Instance{}, fmt.Errorf("eager child %s must have a keyless constructor", childFlowID)
	}
	bundle, found := semanticview.Bundle(source)
	if !found {
		return Instance{}, fmt.Errorf("keyless child requires the admitted flow tree")
	}
	view, found := bundle.FlowViewByID(childFlowID)
	if !found || view.Parent == nil || view.Parent.Paths.FlowPath != parent.TemplateID {
		return Instance{}, fmt.Errorf("flow %s is not an immediate child of %s", childFlowID, parent.TemplateID)
	}
	scope := ScopeKey(source, childFlowID)
	local := strings.TrimPrefix(scope, ScopeKey(source, parent.TemplateID)+"/")
	if parent.TemplateID == semanticview.RootExecutionFlowID(source) {
		local = scope
	}
	if local == "" || strings.Contains(local, "/") {
		return Instance{}, fmt.Errorf("keyless child %s has no exact local coordinate", childFlowID)
	}
	instancePath := parent.InstancePath + "/" + local
	if parent.TemplateID == semanticview.RootExecutionFlowID(source) {
		instancePath = local
	}
	return Instance{
		TemplateID: childFlowID, ScopeKey: scope, InstanceID: local,
		InstancePath: instancePath, EntityID: EntityID(instancePath), HasStoredPath: true,
		ParentEntityID: parent.EntityID,
		ParentRoute:    ParentRoute{FlowID: parent.TemplateID, FlowInstance: parent.InstancePath, EntityID: parent.EntityID},
	}, nil
}
