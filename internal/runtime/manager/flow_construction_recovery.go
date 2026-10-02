package manager

import (
	"context"
	"fmt"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Recovery verifies the whole committed constructor tree before attachment. It
// must not regenerate initial fields, creating receipts, or missing descendants.
func (am *AgentManager) verifyConstructedFlowTree(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest, runID string) ([]runtimeflowidentity.Instance, error) {
	bundle, found := semanticview.Bundle(req.ContractBundle)
	if !found {
		return nil, fmt.Errorf("constructed tree recovery requires the admitted flow tree")
	}
	var active []runtimeflowidentity.Instance
	var visit func(runtimeflowidentity.Instance) error
	visit = func(expected runtimeflowidentity.Instance) error {
		owner, err := runtimeflowidentity.NewRunScopedFlowInstance(runID, expected.Route())
		if err != nil {
			return err
		}
		stored, found, err := am.workflowInstances.LoadConstructedFlowInstance(ctx, owner, identity.NormalizeEntityID(expected.EntityID))
		if err != nil {
			return fmt.Errorf("verify constructed tree member %s: %w", expected.InstancePath, err)
		}
		if !found {
			return fmt.Errorf("constructed tree member %s is missing; recovery cannot construct it", expected.InstancePath)
		}
		contract, _ := entityruntime.ResolveForFlow(req.ContractBundle, expected.TemplateID)
		if stored.WorkflowName != expected.TemplateID || stored.EntityID != expected.EntityID || stored.EntityType != contract.EntityType ||
			stored.StorageRef != expected.InstancePath || stored.ParentFlowID != expected.ParentRoute.FlowID ||
			stored.ParentFlowInstance != expected.ParentRoute.FlowInstance || stored.ParentEntityID != expected.ParentEntityID {
			return fmt.Errorf("constructed tree member %s contradicts its admitted identity or field contract", expected.InstancePath)
		}
		view, found := bundle.FlowViewByID(expected.TemplateID)
		if !found {
			return fmt.Errorf("constructed tree member %s has no admitted template", expected.TemplateID)
		}
		for _, child := range view.Children {
			if !child.Schema.Instance.Empty() {
				continue
			}
			childIdentity, err := runtimeflowidentity.KeylessChild(req.ContractBundle, expected, child.Paths.FlowPath)
			if err != nil {
				return err
			}
			if err := visit(childIdentity); err != nil {
				return err
			}
		}
		if stored.Status == "active" {
			active = append(active, expected)
		}
		return nil
	}
	if err := visit(req.Instance); err != nil {
		return nil, err
	}
	return active, nil
}
