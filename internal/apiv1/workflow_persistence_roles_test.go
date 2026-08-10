package apiv1

import (
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type apiTestDurableWorkflowRoles interface {
	runtimebus.TargetFailureDeadLetterRecorder
	runtimedelivery.Store
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	runtimepipeline.DecisionCardDraftExpiry
	runtimepipeline.HumanTaskExpiry
	runtimerunlifecycle.OperationOwner
	PipelineObligations() runtimepipelineobligation.Store
}

func completeAPITestDurableWorkflowOptions(t testing.TB, selected any, bus any, opts runtimepipeline.PipelineCoordinatorOptions) runtimepipeline.PipelineCoordinatorOptions {
	t.Helper()
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	roles, ok := selected.(apiTestDurableWorkflowRoles)
	if !ok {
		t.Fatalf("selected API test store %T does not provide complete durable workflow roles", selected)
	}
	if opts.DeliveryStore == nil {
		opts.DeliveryStore = roles
	}
	if opts.DeadLetters == nil {
		opts.DeadLetters = roles
	}
	if opts.PipelineObligations == nil {
		opts.PipelineObligations = roles.PipelineObligations()
	}
	if opts.DecisionCards == nil {
		opts.DecisionCards = roles
	}
	if opts.ProposedEffects == nil {
		opts.ProposedEffects = roles
	}
	if opts.HumanTasks == nil {
		opts.HumanTasks = roles
	}
	if opts.DecisionCardDraftExpiry == nil {
		opts.DecisionCardDraftExpiry = roles
	}
	if opts.HumanTaskExpiry == nil {
		opts.HumanTaskExpiry = roles
	}
	if opts.DeliveryRuntime == nil {
		deliveryRuntime, ok := bus.(runtimepipeline.WorkflowDeliveryRuntime)
		if !ok {
			t.Fatalf("API test bus %T does not provide delivery continuation state", bus)
		}
		opts.DeliveryRuntime = deliveryRuntime
	}
	if opts.RunLifecycle == nil {
		opts.RunLifecycle = roles
	}
	return opts
}
