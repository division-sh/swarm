package agentmemory

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

type Plan struct {
	Enabled bool `json:"enabled"`
}

func ValidateFlowOwnership(plan Plan, flowInstance string) error {
	if plan.Enabled && strings.Trim(strings.TrimSpace(flowInstance), "/") == "" {
		return fmt.Errorf("memory true requires a flow-instance owner")
	}
	return nil
}

// Identity is the canonical concrete live-agent owner. Memory and session
// consumers must not restate its run axis in a parallel wrapper.
type Identity = agentidentity.Identity

func ValidateIdentity(identity Identity, requireFlowInstance bool) error {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("agent memory identity: %w", err)
	}
	if requireFlowInstance && identity.Route.Presence != agentidentity.RoutePresent {
		return fmt.Errorf("agent memory flow_instance is required")
	}
	return nil
}

type executionContextKey struct{}

type Execution struct {
	Plan     Plan
	Identity Identity
}

func WithExecution(ctx context.Context, plan Plan, identity Identity) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, executionContextKey{}, Execution{Plan: plan, Identity: identity.Normalize()})
}

func FromContext(ctx context.Context) (Execution, bool) {
	if ctx == nil {
		return Execution{}, false
	}
	execution, ok := ctx.Value(executionContextKey{}).(Execution)
	if !ok {
		return Execution{}, false
	}
	execution.Identity = execution.Identity.Normalize()
	return execution, true
}
