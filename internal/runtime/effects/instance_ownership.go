package effects

import (
	"context"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/forkcancelcapture"
)

type turnInstanceSourceKey struct{}
type turnInstanceOwnershipKey struct{}

// The native admission revalidates this canonical source and copied fact.
// Neither context value grants provider execution or owns a mutable header.
func WithTurnInstanceSource(ctx context.Context, source agentcontrol.TurnInstanceSource) context.Context {
	if source == nil {
		return ctx
	}
	return forkcancelcapture.RecordValue(ctx, context.WithValue(ctx, turnInstanceSourceKey{}, source))
}

func WithTurnInstanceOwnership(ctx context.Context, ownership events.InstanceOwnership) context.Context {
	return forkcancelcapture.RecordValue(ctx, context.WithValue(ctx, turnInstanceOwnershipKey{}, ownership.Clone()))
}
