package semanticview

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// ExecutableNodeSemanticScope is the canonical semantic projection for one
// exact declaration. Declaration package context and inherited flow context
// remain separate facts.
type ExecutableNodeSemanticScope struct {
	Node        runtimeidentity.ExecutableNode
	Declaration runtimecontracts.ScopedNodeRecord

	declarationProject *ProjectScope
	owningFlow         *FlowScope
}

func (s ExecutableNodeSemanticScope) DeclarationProject() (ProjectScope, bool) {
	if s.declarationProject == nil {
		return ProjectScope{}, false
	}
	return *s.declarationProject, true
}

func (s ExecutableNodeSemanticScope) OwningFlow() (FlowScope, bool) {
	if s.owningFlow == nil {
		return FlowScope{}, false
	}
	return *s.owningFlow, true
}

func ResolveExecutableNodeSemanticScope(source Source, node runtimeidentity.ExecutableNode) (ExecutableNodeSemanticScope, error) {
	if source == nil || !node.Valid() {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node semantic scope requires exact declaration identity")
	}
	if bundle, ok := Bundle(source); ok {
		contractScope, err := bundle.ExecutableNodeSemanticScope(node)
		if err != nil {
			return ExecutableNodeSemanticScope{}, err
		}
		result := ExecutableNodeSemanticScope{Node: node, Declaration: contractScope.Declaration}
		if _, found := contractScope.PackageView(); found {
			project, err := exactExecutableNodeProjectScope(source, node.PackageKey())
			if err != nil {
				return ExecutableNodeSemanticScope{}, err
			}
			result.declarationProject = &project
		}
		if view, found := contractScope.OwningFlow(); found {
			flowID := strings.TrimSpace(view.Paths.ID)
			flow := flowScopeFromView(*view, source.FlowInputEvents(flowID), source.FlowOutputEvents(flowID))
			flow.PackageKey = bundle.ExecutableFlowViewPackageKey(view)
			result.owningFlow = &flow
		}
		return result, nil
	}

	records := make([]runtimecontracts.ScopedNodeRecord, 0, 1)
	for _, record := range source.ExecutableNodeRecords() {
		candidate, err := record.Identity()
		if err == nil && candidate.Equal(node) {
			records = append(records, record)
		}
	}
	if len(records) != 1 {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q requires exactly one declaration record, found %d", node.Key(), len(records))
	}
	record := records[0]
	result := ExecutableNodeSemanticScope{Node: node, Declaration: record}

	project, projectErr := exactExecutableNodeProjectScope(source, node.PackageKey())
	if projectErr == nil {
		result.declarationProject = &project
	}
	layer := strings.TrimSpace(record.Source.Layer)
	if layer == "project" && projectErr != nil {
		return ExecutableNodeSemanticScope{}, projectErr
	}
	if node.FlowID() == "" {
		if layer != "project" {
			return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q empty owning flow requires project declaration", node.Key())
		}
		return result, nil
	}
	flows := make([]FlowScope, 0, 1)
	for _, scope := range source.FlowScopes() {
		if strings.TrimSpace(scope.ID) == node.FlowID() {
			flows = append(flows, scope)
		}
	}
	if len(flows) != 1 {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q requires exactly one owning flow %q, found %d", node.Key(), node.FlowID(), len(flows))
	}
	if layer == "flow" && normalizedExecutablePackageKey(flows[0].PackageKey) != node.PackageKey() {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q flow declaration package contradicts owning flow package %q", node.Key(), flows[0].PackageKey)
	}
	if layer != "flow" && layer != "project" {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q has unsupported declaration layer %q", node.Key(), layer)
	}
	result.owningFlow = &flows[0]
	return result, nil
}

func exactExecutableNodeProjectScope(source Source, packageKey string) (ProjectScope, error) {
	matches := make([]ProjectScope, 0, 1)
	for _, scope := range source.ProjectScopes() {
		if normalizedExecutablePackageKey(scope.Key) == packageKey {
			matches = append(matches, scope)
		}
	}
	if len(matches) != 1 {
		return ProjectScope{}, fmt.Errorf("executable node package %q requires exactly one declaration project, found %d", packageKey, len(matches))
	}
	return matches[0], nil
}

func normalizedExecutablePackageKey(raw string) string {
	ref, err := runtimeidentity.AdmitExecutableNodeDeclaration(raw, "", "scope")
	if err != nil {
		return ""
	}
	return ref.PackageKey()
}
