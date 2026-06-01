package tools

import (
	"context"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func WithActor(ctx context.Context, actor models.AgentConfig) context.Context {
	return models.WithActor(ctx, actor)
}

func ActorFromContext(ctx context.Context) (models.AgentConfig, bool) {
	return models.ActorFromContext(ctx)
}
