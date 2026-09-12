package pipeline

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
)

type missingAcknowledgementEngineOwner struct{ calls int }

func (o *missingAcknowledgementEngineOwner) CommitWorkflowEngineMutation(context.Context, WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	o.calls++
	return CommittedWorkflowEngineMutation{}, nil
}

type outcomeEnginePlanner struct {
	*recordingPipelineBus
	releases, finalizes int
}

func (p *outcomeEnginePlanner) ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error {
	p.releases++
	return nil
}
func (p *outcomeEnginePlanner) FinalizeEnginePublications(context.Context, []runtimeengine.CommittedDurablePublication) error {
	p.finalizes++
	return nil
}

func TestEntitylessEngineMissingAcknowledgementDoesNotFinalize(t *testing.T) {
	storeOwner := &missingAcknowledgementEngineOwner{}
	planner := &outcomeEnginePlanner{recordingPipelineBus: &recordingPipelineBus{}}
	owner := pipelineEngineMutationOwner{store: &workflowInstanceStore{engineMutations: storeOwner}, publication: planner}
	flow := runtimeflowidentity.RunScopedFlowInstance{RunID: "11111111-1111-4111-8111-111111111111", Route: runtimeflowidentity.RouteForInstancePath("11111111-1111-4111-8111-111111111111")}
	result, err := owner.commitEntitylessEngineMutation(context.Background(), runtimeengine.EngineMutation{Address: runtimeengine.StateAddress{FlowInstance: flow}}, events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: flow.Route.InstancePath}))
	if err == nil || !strings.Contains(err.Error(), "no acknowledged result") || result.Committed || storeOwner.calls != 1 || planner.releases != 1 || planner.finalizes != 0 {
		t.Fatalf("result=%+v error=%v commits=%d releases=%d finalizes=%d", result, err, storeOwner.calls, planner.releases, planner.finalizes)
	}
}
