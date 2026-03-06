package runtime

import (
	"context"

	"empireai/internal/models"
	runtimetools "empireai/internal/runtime/tools"
)

func WithActor(ctx context.Context, actor models.AgentConfig) context.Context {
	return runtimetools.WithActor(ctx, actor)
}

func ActorFromContext(ctx context.Context) (models.AgentConfig, bool) {
	return runtimetools.ActorFromContext(ctx)
}
