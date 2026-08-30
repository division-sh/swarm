package semanticview

import (
	"fmt"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// ExecutableNodeSemanticScope is the canonical projection of one node into
// its exact filesystem flow.
type ExecutableNodeSemanticScope struct {
	Node        runtimeidentity.ExecutableNode
	Declaration runtimecontracts.ScopedNodeRecord
	owningFlow  *FlowScope
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
		if view, found := contractScope.OwningFlow(); found {
			flowPath := strings.TrimSpace(view.Paths.FlowPath)
			flow := flowScopeFromView(*view, source.FlowInputEvents(flowPath), source.FlowOutputEvents(flowPath))
			result.owningFlow = &flow
		}
		return result, nil
	}

	var records []runtimecontracts.ScopedNodeRecord
	for _, record := range source.ExecutableNodeRecords() {
		candidate, err := record.Identity()
		if err == nil && candidate.Equal(node) {
			records = append(records, record)
		}
	}
	if len(records) != 1 {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q requires exactly one declaration record, found %d", node.Key(), len(records))
	}
	flows := make([]FlowScope, 0, 1)
	for _, scope := range source.FlowScopes() {
		if strings.TrimSpace(scope.ID) == node.FlowPath() {
			flows = append(flows, scope)
		}
	}
	if len(flows) != 1 {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q requires exactly one owning flow %q, found %d", node.Key(), node.FlowPath(), len(flows))
	}
	return ExecutableNodeSemanticScope{Node: node, Declaration: records[0], owningFlow: &flows[0]}, nil
}
