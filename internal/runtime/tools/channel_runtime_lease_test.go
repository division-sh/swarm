package tools

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	runtimechannelactivation "github.com/division-sh/swarm/internal/runtime/channelactivation"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type unusedChannelActivityExecutor struct{}

func (unusedChannelActivityExecutor) ExecuteDurableActivity(context.Context, runtimeengine.ActivityIntent) (runtimepipeline.ActivityAttemptRecord, error) {
	return runtimepipeline.ActivityAttemptRecord{}, nil
}

func TestChannelRuntimeDispatchRequiresPreValidationGenerationLease(t *testing.T) {
	publication, err := channelonboarding.NewChannelActivationPublication(nil)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := runtimechannelactivation.NewOwner(publication)
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{
		channelActivations: owner,
		activityExecutor:   unusedChannelActivityExecutor{},
	}
	_, err = executor.execChannelOperation(context.Background(), models.AgentConfig{}, "channel.test.deliver", map[string]any{})
	failure, ok := runtimefailures.As(err)
	if err == nil || !ok || failure.Failure.Detail.Code != "channel_operation_lease_required" {
		t.Fatalf("dispatch without pre-validation generation lease = %v", err)
	}
}
