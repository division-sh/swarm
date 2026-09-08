package agentmemory

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

func Authored(enabled bool) Plan {
	return Plan{Enabled: enabled, Source: SourceAuthored}
}

func PlatformDefault() Plan {
	return Plan{Enabled: false, Source: SourcePlatformDefault}
}

type Source string

const (
	SourceAuthored        Source = "authored"
	SourcePlatformDefault Source = "platform_default"
)

type Plan struct {
	Enabled bool   `json:"enabled"`
	Source  Source `json:"source"`
}

func (p Plan) Normalize() (Plan, error) {
	if strings.TrimSpace(string(p.Source)) == "" {
		p.Source = SourcePlatformDefault
	}
	return NewPlan(p.Enabled, p.Source)
}

func NewPlan(enabled bool, source Source) (Plan, error) {
	source = Source(strings.TrimSpace(string(source)))
	switch source {
	case SourceAuthored:
		return Plan{Enabled: enabled, Source: source}, nil
	case SourcePlatformDefault:
		if enabled {
			return Plan{}, fmt.Errorf("agent memory enabled requires source %q", SourceAuthored)
		}
		return Plan{Enabled: false, Source: source}, nil
	default:
		return Plan{}, fmt.Errorf("invalid agent memory source %q", source)
	}
}

func ValidateFlowOwnership(plan Plan, flowInstance string) error {
	plan, err := plan.Normalize()
	if err != nil {
		return err
	}
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
