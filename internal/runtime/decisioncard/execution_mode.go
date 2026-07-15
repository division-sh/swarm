package decisioncard

import (
	"context"
	"fmt"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

// CausalExecutionMode returns the active executor's typed mode when execution
// authority exists. The source event is the fallback for event-only pipeline
// work; it is not required to share the downstream executor's mode.
func CausalExecutionMode(ctx context.Context) (executionmode.Mode, error) {
	authorityMode, hasAuthority := runtimeeffects.ExecutionModeFromContext(ctx)
	if authority, ok := runtimeeffects.AuthorityFromContext(ctx); ok {
		if hasAuthority && authorityMode != authority.ExecutionMode {
			return "", fmt.Errorf("decision card execution context conflicts with completion authority mode")
		}
		authorityMode, hasAuthority = authority.ExecutionMode, true
	}
	event, hasEvent := runtimecorrelation.InboundEventFromContext(ctx)
	if hasEvent && !event.ExecutionMode().Valid() {
		return "", fmt.Errorf("decision card source event has invalid execution mode")
	}
	if hasAuthority {
		return authorityMode, nil
	}
	if hasEvent {
		return event.ExecutionMode(), nil
	}
	return "", fmt.Errorf("decision card requires typed causal execution mode")
}
