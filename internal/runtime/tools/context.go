package tools

import (
	"context"

	"empireai/internal/models"
	runtimeactor "empireai/internal/runtime/actorctx"
)

func WithActor(ctx context.Context, actor models.AgentConfig) context.Context {
	return runtimeactor.WithActor(ctx, actor)
}

func ActorFromContext(ctx context.Context) (models.AgentConfig, bool) {
	return runtimeactor.ActorFromContext(ctx)
}
