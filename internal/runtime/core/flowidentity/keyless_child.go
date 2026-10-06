package flowidentity

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// KeylessChildFlowIDs is the immediate eager-construction family. Admission and
// construction consume the same selected tree, never filesystem descendants.
func KeylessChildFlowIDs(source semanticview.Source, parentFlowID string) ([]string, error) {
	if source == nil {
		return nil, fmt.Errorf("keyless children require a selected source")
	}
	bundle, found := semanticview.Bundle(source)
	if !found || bundle == nil {
		return nil, fmt.Errorf("keyless children require the admitted flow tree")
	}
	view, found := bundle.FlowViewByID(parentFlowID)
	if !found || view == nil {
		return nil, fmt.Errorf("constructor flow %s has no admitted tree owner", parentFlowID)
	}
	parentSchema, known := source.FlowSchemaByID(parentFlowID)
	if !known || parentSchema.Instance != view.Schema.Instance {
		return nil, fmt.Errorf("flow %s contradicts its admitted constructor", parentFlowID)
	}
	var children []string
	for _, child := range view.Children {
		flowID := child.Paths.FlowPath
		schema, known := source.FlowSchemaByID(flowID)
		if !known || schema.Instance != child.Schema.Instance || child.Parent == nil || child.Parent.Paths.FlowPath != parentFlowID {
			return nil, fmt.Errorf("child %s contradicts its admitted constructor", flowID)
		}
		if schema.Instance.Empty() {
			children = append(children, flowID)
		}
	}
	return children, nil
}

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
