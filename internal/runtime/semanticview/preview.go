package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
)

func CloneBundleForPreview(bundle *runtimecontracts.WorkflowContractBundle, policyOverrides map[string]any) *runtimecontracts.WorkflowContractBundle {
	if bundle == nil {
		return nil
	}
	clone := *bundle
	clone.Policy = flowmodel.ClonePolicyDocument(bundle.Policy)
	if len(policyOverrides) > 0 {
		flowmodel.ApplyPolicyOverrides(&clone.Policy, policyOverrides)
	}
	if bundle.FlowTree.Root != nil {
		root, byPath, byID := flowmodel.CloneViewTree(
			bundle.FlowTree.Root,
			func(view *runtimecontracts.FlowContractView) string { return strings.TrimSpace(view.Paths.ID) },
			func(view *runtimecontracts.FlowContractView) {
				flowmodel.ApplyPolicyOverrides(&view.Policy, policyOverrides)
			},
		)
		clone.FlowTree = runtimecontracts.FlowTree{
			Root:   root,
			ByPath: byPath,
			ByID:   byID,
		}
	}
	return &clone
}
