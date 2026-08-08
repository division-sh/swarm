package bustest

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type BeginPublishFunc func(context.Context, events.AdmittedEvent) (runtimebus.EventAppendOutcome, error)
type FinalizePublishFunc func(context.Context, runtimebus.CommitPublishRequest) error

// CommitPublish models the closed selected-store publication operation for
// tests without exposing a transaction or executable callback to EventBus.
func CommitPublish(
	ctx context.Context,
	command runtimebus.PublicationCommand,
	begin BeginPublishFunc,
	finalize FinalizePublishFunc,
) (runtimebus.CommittedPublication, error) {
	if err := command.Validate(); err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	outcome := runtimebus.EventAppendInserted
	var err error
	if begin != nil {
		outcome, err = begin(ctx, command.Commit.Event)
	}
	if err != nil {
		return runtimebus.CommittedPublication{}, err
	}
	result := runtimebus.CommittedPublication{AppendOutcome: outcome, RouteTopology: command.RouteTopology}
	if outcome == runtimebus.EventAppendExactDuplicate {
		return result, result.Validate()
	}
	if finalize != nil {
		if err := finalize(ctx, command.Commit); err != nil {
			return runtimebus.CommittedPublication{}, err
		}
	}
	for _, plan := range command.Activations {
		result.Activations = append(result.Activations, runtimepipeline.CommittedFlowInstanceActivation{Plan: plan, Created: true})
	}
	return result, result.Validate()
}

func CommitPublishNoop(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return CommitPublish(ctx, command, nil, nil)
}
