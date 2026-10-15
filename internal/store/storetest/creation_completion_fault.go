package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func InstallDynamicFlowCreationCompletionFailure(ctx context.Context, selected any, request pipeline.DynamicFlowRuntimeCreationOccurrenceRequest) (func(context.Context) error, error) {
	return private.InstallDynamicFlowCreationCompletionFailureForTest(ctx, selected, request)
}
