package toolcapabilities

import (
	"context"

	"swarm/internal/runtime/core/toolidentity"
)

type ToolKind string

const (
	KindStandard ToolKind = "standard"
	KindEmit     ToolKind = "emit"
)

type ContextRequirement string

const (
	ContextRequirementActorContext ContextRequirement = "actor_context"
	ContextRequirementTurnContext  ContextRequirement = "turn_context"
)

type StartupProbeMode string

const (
	StartupProbeModeVisibilityOnly  StartupProbeMode = "visibility_only"
	StartupProbeModeCallEmptyObject StartupProbeMode = "call_empty_object"
)

type Capability struct {
	Name               string             `json:"name"`
	Kind               ToolKind           `json:"kind,omitempty"`
	Visible            bool               `json:"visible"`
	Callable           bool               `json:"callable"`
	ContextRequirement ContextRequirement `json:"context_requirement,omitempty"`
	StartupProbeMode   StartupProbeMode   `json:"startup_probe_mode,omitempty"`
	DenialReason       string             `json:"denial_reason,omitempty"`
	AuthorizationClass string             `json:"authorization_class,omitempty"`
}

type Set struct {
	ByName map[string]Capability `json:"by_name"`
}

func NewSet(caps []Capability) Set {
	byName := make(map[string]Capability, len(caps))
	for _, cap := range caps {
		name := toolidentity.CanonicalName(cap.Name)
		if name == "" {
			continue
		}
		cap.Name = name
		byName[name] = cap
	}
	return Set{ByName: byName}
}

func (s Set) Capability(name string) (Capability, bool) {
	if len(s.ByName) == 0 {
		return Capability{}, false
	}
	cap, ok := s.ByName[toolidentity.CanonicalName(name)]
	return cap, ok
}

type contextKey struct{}

func WithContext(ctx context.Context, set Set) context.Context {
	return context.WithValue(ctx, contextKey{}, set)
}

func FromContext(ctx context.Context) (Set, bool) {
	if ctx == nil {
		return Set{}, false
	}
	set, ok := ctx.Value(contextKey{}).(Set)
	return set, ok
}
