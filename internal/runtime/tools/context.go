package tools

import (
	"context"

	runtimeactor "empireai/internal/runtime/actorctx"
	models "empireai/internal/runtime/actors"
)

func WithActor(ctx context.Context, actor models.AgentConfig) context.Context {
	return runtimeactor.WithActor(ctx, actor)
}

func ActorFromContext(ctx context.Context) (models.AgentConfig, bool) {
	return runtimeactor.ActorFromContext(ctx)
}
