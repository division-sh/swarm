package runforkexecution

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type selectedContractProjectedWorkflowOwner struct {
	entityID    string
	flowID      string
	addressKind runfork.RunForkSelectedContractWorkflowStateAddressKind
	route       runtimeflowidentity.Route
}

// This ephemeral correspondence is minted by the source-event workflow-state
// projection. It does not replace the persisted, runless selected evidence.
type selectedContractWorkflowProjection struct {
	childRunID    string
	bySourceEvent map[string]selectedContractProjectedWorkflowOwner
}

func (p selectedContractWorkflowProjection) requireChildRun(runID string) error {
	if p.childRunID == "" || runID != p.childRunID {
		return fmt.Errorf("selected-contract recipient projection requires exact child run %q; got %q", p.childRunID, runID)
	}
	return nil
}

func (p selectedContractWorkflowProjection) BindRecipient(sourceEventID string, selected forkrecipient.Evidence) (forkrecipient.Evidence, error) {
	canonical, err := forkrecipient.CanonicalSet([]forkrecipient.Evidence{selected})
	if err != nil {
		return forkrecipient.Evidence{}, err
	}
	selected = canonical[0]
	if err := p.requireChildRun(p.childRunID); err != nil {
		return forkrecipient.Evidence{}, err
	}
	// Agent coordinates remain the canonical full Plan; only Plan.Live binds run.
	node := selected.HandlerNode()
	if !node.Valid() {
		return selected, nil
	}
	owner, ok := p.bySourceEvent[strings.TrimSpace(sourceEventID)]
	if !ok {
		// Absence supplies no path equivalence. Actual evidence must still agree
		// exactly with the unchanged blueprint.
		return selected, nil
	}
	if owner.entityID == "" || owner.flowID != node.FlowPath() {
		return forkrecipient.Evidence{}, fmt.Errorf("selected-contract recipient %s has no matching admitted workflow/entity correspondence", node.Key())
	}
	switch owner.addressKind {
	case runfork.RunForkSelectedContractWorkflowStateRunScope:
		if selected.Path != owner.flowID {
			return forkrecipient.Evidence{}, fmt.Errorf("selected-contract run-scope recipient %s path %q disagrees with admitted workflow %q", node.Key(), selected.Path, owner.flowID)
		}
		selected.Path = owner.route.InstancePath
	case runfork.RunForkSelectedContractWorkflowStateExact:
		if selected.Path != owner.route.InstancePath {
			return forkrecipient.Evidence{}, fmt.Errorf("selected-contract exact recipient %s path %q disagrees with admitted workflow route %q", node.Key(), selected.Path, owner.route.InstancePath)
		}
	default:
		return forkrecipient.Evidence{}, fmt.Errorf("selected-contract recipient projection has unsupported address kind %q", owner.addressKind)
	}
	return selected, selected.Validate()
}

// A root Plan has no concrete instance path. Its recipient declaration path
// comes from the selected source's root owner, not from an empty-path alias.
func selectedContractAgentRecipientPath(source semanticview.Source, plan agentidentity.Plan) (string, error) {
	plan = plan.Normalize()
	if err := plan.Validate(); err != nil {
		return "", err
	}
	if plan.Route.Presence == agentidentity.RouteRoot {
		path := semanticview.RootExecutionFlowID(source)
		if path == "" {
			return "", fmt.Errorf("selected-contract root agent recipient requires its selected declaration root")
		}
		return path, nil
	}
	return plan.FlowInstance(), nil
}
