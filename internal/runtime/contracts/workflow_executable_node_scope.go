package contracts

import (
	"fmt"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// ExecutableNodeSemanticScope keeps declaration package context distinct from
// the owning flow inherited by project-package declarations.
type ExecutableNodeSemanticScope struct {
	Node        runtimeidentity.ExecutableNode
	Declaration ScopedNodeRecord

	declarationView *FlowContractView
	packageView     *ProjectContractView
	owningFlow      *FlowContractView
}

func (s ExecutableNodeSemanticScope) DeclarationView() (*FlowContractView, bool) {
	return s.declarationView, s.declarationView != nil
}

func (s ExecutableNodeSemanticScope) PackageView() (*ProjectContractView, bool) {
	return s.packageView, s.packageView != nil
}

func (s ExecutableNodeSemanticScope) OwningFlow() (*FlowContractView, bool) {
	return s.owningFlow, s.owningFlow != nil
}

func (b *WorkflowContractBundle) ExecutableNodeSemanticScope(ref runtimeidentity.ExecutableNode) (ExecutableNodeSemanticScope, error) {
	if b == nil || !ref.Valid() {
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node semantic scope requires exact declaration identity")
	}
	records := make([]ScopedNodeRecord, 0, 1)
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
	scope := ExecutableNodeSemanticScope{Node: ref, Declaration: record}

	if project, found := b.executableNodePackageView(ref.PackageKey()); found {
		scope.packageView = project
	}
	declaration, inheritedOwner, err := b.executableNodeDeclarationView(record, ref)
	if err != nil {
		return ExecutableNodeSemanticScope{}, err
	}
	scope.declarationView = declaration

	layer := strings.TrimSpace(record.Source.Layer)
	switch layer {
	case "flow":
		if strings.TrimSpace(declaration.Paths.ID) != ref.FlowID() {
			return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q declaration flow contradicts owning flow %q", ref.Key(), ref.FlowID())
		}
		scope.owningFlow = declaration
	case "project":
		owner := inheritedOwner
		if ref.FlowID() == "" {
			if owner != nil {
				return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q declares empty owning flow below flow %q", ref.Key(), strings.TrimSpace(owner.Paths.ID))
			}
			return scope, nil
		}
		if owner == nil {
			return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q references missing owning flow %q", ref.Key(), ref.FlowID())
		}
		if strings.TrimSpace(owner.Paths.ID) != ref.FlowID() {
			return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q owning flow %q contradicts declaration ancestry %q", ref.Key(), ref.FlowID(), strings.TrimSpace(owner.Paths.ID))
		}
		scope.owningFlow = owner
	default:
		return ExecutableNodeSemanticScope{}, fmt.Errorf("executable node %q has unsupported declaration layer %q", ref.Key(), layer)
	}
	return scope, nil
}

type executableNodeDeclarationContext struct {
	declaration *FlowContractView
	owningFlow  *FlowContractView
}

func (b *WorkflowContractBundle) executableNodeDeclarationView(record ScopedNodeRecord, ref runtimeidentity.ExecutableNode) (*FlowContractView, *FlowContractView, error) {
	matches := make([]executableNodeDeclarationContext, 0, 1)
	var walk func(*FlowContractView, string, string, *FlowContractView)
	walk = func(view *FlowContractView, inheritedPackageKey, inheritedFlowID string, inheritedOwner *FlowContractView) {
		if view == nil {
			return
		}
		packageKey := strings.TrimSpace(view.Paths.PackageKey)
		if packageKey == "" {
			packageKey = inheritedPackageKey
		}
		declaredFlowID := strings.TrimSpace(view.Paths.ID)
		if declaredFlowID != "" {
			packageKey = b.executableFlowViewPackageKey(view)
		}
		packageRef, packageErr := runtimeidentity.AdmitExecutableNodeDeclaration(packageKey, "", "scope")
		if packageErr != nil {
			return
		}
		flowID := declaredFlowID
		owner := inheritedOwner
		if declaredFlowID != "" {
			owner = view
		} else if flowID == "" {
			flowID = inheritedFlowID
		}
		layer := "project"
		if declaredFlowID != "" {
			layer = "flow"
		}
		if packageRef.PackageKey() == ref.PackageKey() && flowID == ref.FlowID() && layer == strings.TrimSpace(record.Source.Layer) {
			if _, declared := view.Nodes[ref.NodeID()]; declared {
				matches = append(matches, executableNodeDeclarationContext{declaration: view, owningFlow: owner})
			}
		}
		for index := range view.Children {
			walk(&view.Children[index], packageRef.PackageKey(), flowID, owner)
		}
	}
	walk(b.FlowTree.Root, "", "", nil)
	if len(matches) == 0 && b.FlowTree.Root == nil && strings.TrimSpace(record.Source.Layer) == "project" && ref.FlowID() == "" {
		return &FlowContractView{
			Paths:  FlowContractPaths{PackageKey: ref.PackageKey()},
			Nodes:  map[string]SystemNodeContract{ref.NodeID(): record.Entry},
			Events: b.Events,
			Policy: b.Policy,
		}, nil, nil
	}
	if len(matches) != 1 {
		return nil, nil, fmt.Errorf("executable node %q requires exactly one declaration context, found %d", ref.Key(), len(matches))
	}
	return matches[0].declaration, matches[0].owningFlow, nil
}

func (b *WorkflowContractBundle) executableNodePackageView(packageKey string) (*ProjectContractView, bool) {
	for _, view := range b.ProjectViews() {
		candidate, err := runtimeidentity.AdmitExecutableNodeDeclaration(view.Paths.Key, "", "scope")
		if err != nil || candidate.PackageKey() != packageKey {
			continue
		}
		copy := view
		return &copy, true
	}
	return nil, false
}
