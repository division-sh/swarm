package effects

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

type claudeBackingStore interface {
	ClaudeBackingOrigin(context.Context, agentidentity.Identity, string, string) (string, error)
}

// ClaudeBackingOrigin derives storage provenance only. It never admits an old
// execution authority or changes the current workspace's source selection.
func ClaudeBackingOrigin(ctx context.Context, identity agentidentity.Identity, session, head string) (string, error) {
	controller, ok := ControllerFromContext(ctx)
	if ok {
		if owner, ok := controller.store.(claudeBackingStore); ok {
			return owner.ClaudeBackingOrigin(ctx, identity, session, head)
		}
	}
	return "", failures.New(failures.ClassLifecycleConflict, "claude_provider_backing_owner_missing", "external-effects", "resolve_claude_backing", nil)
}
