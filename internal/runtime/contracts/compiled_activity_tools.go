package contracts

import (
	"fmt"
	"strings"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// ActivityToolEntry consumes frozen effective bindings when present. Missing
// entries in that snapshot never fall back to an authored or global alias.
func (b *WorkflowContractBundle) ActivityToolEntry(node runtimeidentity.ExecutableNode, toolID string) (ToolSchemaEntry, bool) {
	if b == nil || !node.Valid() || toolID == "" || toolID != strings.TrimSpace(toolID) {
		return ToolSchemaEntry{}, false
	}
	if b.activityToolsByFlow != nil {
		tool, present := b.activityToolsByFlow[node.FlowPath()][toolID]
		return tool, present
	}
	return b.ToolEntryForFlow(node.FlowPath(), toolID)
}

// CompileActivityToolBindings completes generated schemas and dependent pins
// after tool imports have been admitted. It does not mutate the input bundle.
func CompileActivityToolBindings(bundle *WorkflowContractBundle, toolsByFlow map[string]map[string]ToolSchemaEntry) (*WorkflowContractBundle, error) {
	if bundle == nil {
		return nil, fmt.Errorf("activity tool compilation requires a bundle")
	}
	clone := *bundle
	clone.activityToolsByFlow = make(map[string]map[string]ToolSchemaEntry, len(toolsByFlow))
	for flow, tools := range toolsByFlow {
		if _, exists := bundle.exactFlowEventDeclarationView(flow); !exists {
			return nil, fmt.Errorf("activity tools have unknown flow owner %q", flow)
		}
		for name, tool := range tools {
			if err := tool.Validate(); err != nil {
				return nil, fmt.Errorf("activity tool %q in flow %q: %w", name, flow, err)
			}
		}
		clone.activityToolsByFlow[flow] = cloneToolSchemaEntryMap(tools)
	}
	for _, site := range bundle.ActivitySites() {
		if _, ok := clone.activityToolsByFlow[site.Node.FlowPath()][strings.TrimSpace(site.Spec.Tool)]; !ok {
			return nil, fmt.Errorf("activity %s at %s requires tool %q admitted in its declaring flow %q", site.Spec.ID, site.Node.Key(), site.Spec.Tool, site.Node.FlowPath())
		}
	}
	if err := CompileWorkflowSemantics(&clone); err != nil {
		return nil, err
	}
	clone.PrepareFanOutPlans()
	return &clone, nil
}
