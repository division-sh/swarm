package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type forkChatToolDispatchKey struct{}

// ForkChatToolDispatch carries the existing conversation's snapshot/stub
// executor through its opaque MCP registration. No request payload can create
// this value or substitute the live workflow executor for sandbox operations.
type ForkChatToolDispatch struct {
	executor    CapabilityAwareToolExecutor
	definitions []byte
	authority   runtimeeffects.Authority
	actorID     string
}

func newForkChatToolDispatch(ctx context.Context, executor CapabilityAwareToolExecutor, tools []ToolDefinition) (*ForkChatToolDispatch, error) {
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	actor, actorOK := models.ActorFromContext(ctx)
	if !ok || !authority.Valid() || authority.Kind != runtimeeffects.AuthorityConversationForkChat || !actorOK || actor.ID == "" || executor == nil {
		return nil, fmt.Errorf("forkchat tool transport requires its exact conversation authority and executor")
	}
	if err := ValidateProviderToolDefinitions(tools); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(tools)
	if err != nil {
		return nil, err
	}
	if _, err := canonicaljson.Decode(raw); err != nil {
		return nil, err
	}
	return &ForkChatToolDispatch{executor: executor, definitions: raw, authority: authority, actorID: actor.ID}, nil
}

func (d *ForkChatToolDispatch) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, forkChatToolDispatchKey{}, d)
}

func ForkChatToolDispatchFromContext(ctx context.Context) (*ForkChatToolDispatch, bool) {
	d, ok := ctx.Value(forkChatToolDispatchKey{}).(*ForkChatToolDispatch)
	return d, ok && d != nil && d.Validate(ctx) == nil
}

func (d *ForkChatToolDispatch) Validate(ctx context.Context) error {
	if d == nil || d.executor == nil {
		return fmt.Errorf("forkchat tool transport missing")
	}
	authority, ok := runtimeeffects.AuthorityFromContext(ctx)
	actor, actorOK := models.ActorFromContext(ctx)
	if !ok || !authority.Valid() || authority.Kind != runtimeeffects.AuthorityConversationForkChat || authority.ForkChat != d.authority.ForkChat || authority.ID != d.authority.ID || authority.ExecutionOwner != d.authority.ExecutionOwner || authority.FenceGeneration != d.authority.FenceGeneration || authority.ExecutionMode != d.authority.ExecutionMode || !authority.LeaseExpiresAt.Equal(d.authority.LeaseExpiresAt) || !authority.LeaseExpiresAt.After(time.Now()) || !actorOK || actor.ID != d.actorID {
		return fmt.Errorf("forkchat tool transport is expired or belongs to another conversation")
	}
	return ctx.Err()
}

func (d *ForkChatToolDispatch) Execute(ctx context.Context, name string, input any) (any, error) {
	if err := d.Validate(ctx); err != nil {
		return nil, err
	}
	for _, def := range d.ToolDefinitionsForActor(models.AgentConfig{}) {
		if def.Name == name {
			return d.executor.Execute(ctx, name, input)
		}
	}
	return nil, fmt.Errorf("forkchat tool %q is absent from the frozen sandbox", name)
}

func (d *ForkChatToolDispatch) ExecuteOutputEvent(ctx context.Context, name string, input any, identity ToolOutputEventIdentity) (any, error) {
	if err := d.Validate(ctx); err != nil {
		return nil, err
	}
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	for _, def := range d.ToolDefinitionsForActor(models.AgentConfig{}) {
		if def.Name == name {
			if output, ok := d.executor.(ToolOutputEventExecutor); ok {
				return output.ExecuteOutputEvent(ctx, name, input, identity)
			}
			return nil, fmt.Errorf("frozen forkchat executor has no terminal-output boundary")
		}
	}
	return nil, fmt.Errorf("forkchat output tool %q is absent from the frozen sandbox", name)
}

func (d *ForkChatToolDispatch) ToolDefinitionsForActor(models.AgentConfig) []ToolDefinition {
	var tools []ToolDefinition
	_ = canonicaljson.DecodePreservingNumberLexemes(d.definitions, &tools)
	return tools
}

func (d *ForkChatToolDispatch) ToolCapabilitiesForActor(actor models.AgentConfig, names []string, allowed map[string]struct{}) toolcapabilities.Set {
	return d.executor.ToolCapabilitiesForActor(actor, names, allowed)
}
