package runforkexecution

import (
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Observe the real selected preparation registration, without replacing its
// store catalog, lease, or later execution ownership.
type observedSelectedJoinCatalog struct {
	SelectedContractForkLifecycle
	scopes []authoractivity.Scope
}

type selectedJoinCatalogSource struct{ semanticview.Source }

func (s selectedJoinCatalogSource) WorkflowJoins() []contracts.WorkflowJoinPlan {
	return []contracts.WorkflowJoinPlan{{Mode: contracts.WorkflowJoinModeArrival, Spec: contracts.JoinSpec{Deadline: &contracts.JoinDeadlineSpec{After: "1h", From: contracts.JoinDeadlineFromStageEntry}}}}
}

func (o *observedSelectedJoinCatalog) RegisterAuthorActivityEventCatalog(scope authoractivity.Scope, descriptors []authoractivity.EventDescriptor) (*authoractivity.EventCatalogLease, error) {
	lease, err := o.SelectedContractForkLifecycle.RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		return nil, err
	}
	o.scopes = append(o.scopes, scope)
	return lease, nil
}
