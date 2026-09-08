package runforkexecution

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkrecipient"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Root recipient binding is independent of the producer's entity ownership.
// The source-event admission supplies event membership, not receiver state.
type selectedContractWorkflowProjection struct {
	root         semanticview.RootExecutionCoordinate
	sourceEvents map[string]struct{}
}

func (p selectedContractWorkflowProjection) requireChildRun(runID string) error {
	if !p.root.Valid() || runID != p.root.RunID() {
		return fmt.Errorf("selected-contract recipient projection requires exact child run %q; got %q", p.root.RunID(), runID)
	}
	return nil
}

func (p selectedContractWorkflowProjection) BindRecipient(sourceEventID string, selected forkrecipient.Evidence) (forkrecipient.Evidence, error) {
	canonical, err := forkrecipient.CanonicalSet([]forkrecipient.Evidence{selected})
	if err != nil {
		return forkrecipient.Evidence{}, err
	}
	selected = canonical[0]
	if err := p.requireChildRun(p.root.RunID()); err != nil {
		return forkrecipient.Evidence{}, err
	}
	// Agent coordinates remain the canonical full Plan; only Plan.Live binds run.
	node := selected.HandlerNode()
	if !node.Valid() {
		return selected, nil
	}
	_, ok := p.sourceEvents[strings.TrimSpace(sourceEventID)]
	if !ok {
		// Absence supplies no path equivalence. Actual evidence must still agree
		// exactly with the unchanged blueprint.
		return selected, nil
	}
	if node.FlowPath() != p.root.FlowID() {
		return selected, nil
	}
	if selected.Path != p.root.FlowID() {
		return forkrecipient.Evidence{}, fmt.Errorf("selected-contract run-scope recipient %s path %q disagrees with admitted root %q", node.Key(), selected.Path, p.root.FlowID())
	}
	selected.Path = p.root.RunID()
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
