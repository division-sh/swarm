package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type toolOutputCallKey struct{}

// ToolOutputCall transports the existing settled completion's exact call
// authority in the server-side MCP context. No HTTP payload can mint one.
type ToolOutputCall struct {
	identity      ToolOutputEventIdentity
	name          string
	argumentsHash string
	occurrence    string
}

func withToolOutputCall(ctx context.Context, authority ToolOutputAuthority, occurrence, name string, input any) (context.Context, error) {
	identity, err := authority.eventIdentity(occurrence)
	if err != nil {
		return ctx, err
	}
	hash, err := toolOutputArgumentsHash(input)
	if err != nil {
		return ctx, err
	}
	call := ToolOutputCall{identity: identity, name: strings.TrimSpace(name), argumentsHash: hash, occurrence: occurrence}
	return call.WithContext(ctx), nil
}

func ToolOutputCallFromContext(ctx context.Context) (ToolOutputCall, bool) {
	call, ok := ctx.Value(toolOutputCallKey{}).(ToolOutputCall)
	return call, ok
}

func (c ToolOutputCall) WithContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, toolOutputCallKey{}, c)
}

func (c ToolOutputCall) Authorize(name string, input any, occurrence string) (ToolOutputEventIdentity, error) {
	hash, err := toolOutputArgumentsHash(input)
	if err != nil || c.name != strings.TrimSpace(name) || c.occurrence != occurrence || c.argumentsHash != hash {
		return ToolOutputEventIdentity{}, fmt.Errorf("settled MCP output call does not match completion authority")
	}
	return c.identity, c.identity.Validate()
}

func toolOutputArgumentsHash(input any) (string, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	var normalized any
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &normalized); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(normalized)
	return runtimeeffects.Fingerprint(canonical), err
}
