package contracts

import (
	"fmt"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// ExecutableNodeSemanticScope binds one node declaration to its exact
// filesystem flow. There is no package/project declaration layer.
type ExecutableNodeSemanticScope struct {
	Node        runtimeidentity.ExecutableNode
	Declaration ScopedNodeRecord

	declarationView *FlowContractView
}

func (s ExecutableNodeSemanticScope) DeclarationView() (*FlowContractView, bool) {
	return s.declarationView, s.declarationView != nil
}

func (s ExecutableNodeSemanticScope) OwningFlow() (*FlowContractView, bool) {
	return s.declarationView, s.declarationView != nil
}

func (b *WorkflowContractBundle) ExecutableNodeSemanticScope(ref runtimeidentity.ExecutableNode) (ExecutableNodeSemanticScope, error) {
	if b == nil || !ref.Valid() {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node semantic scope requires exact declaration identity")
	}
	var records []ScopedNodeRecord
	for _, record := range b.ScopedNodeRecords() {
		candidate, err := record.Identity()
		if err == nil && candidate.Equal(ref) {
			records = append(records, record)
		}
	}
	if len(records) != 1 {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q requires exactly one declaration record, found %d", ref.Key(), len(records))
	}
	record := records[0]
	if sourceFlow := strings.TrimSpace(record.Source.FlowPath); sourceFlow != ref.FlowPath() {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q declaration flow %q contradicts identity flow %q", ref.Key(), sourceFlow, ref.FlowPath())
	}
	view, ok := b.FlowViewByID(ref.FlowPath())
	if !ok || view == nil {
		if b.FlowTree.Root == nil && ref.FlowPath() == "." {
			view = &FlowContractView{
				Paths:  FlowContractPaths{FlowPath: "."},
				Nodes:  map[string]SystemNodeContract{ref.NodeID(): record.Entry},
				Events: b.Events,
				Policy: b.Policy,
			}
		} else {
			return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q references missing flow %q", ref.Key(), ref.FlowPath())
		}
	}
	if _, declared := view.Nodes[ref.NodeID()]; !declared {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q is absent from flow %q", ref.NodeID(), ref.FlowPath())
	}
	return ExecutableNodeSemanticScope{Node: ref, Declaration: record, declarationView: view}, nil
}
