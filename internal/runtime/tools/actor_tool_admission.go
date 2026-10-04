package tools

import (
	"context"
	"errors"
	"fmt"
	"sort"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/llm"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// PrepareActorToolAdmission consumes the execution catalog and grant owners
// without constructing an executor, presenting a turn or granting authority.
func PrepareActorToolAdmission(ctx context.Context, source semanticview.Source, actor models.AgentConfig, discovered map[string]runtimemcp.DiscoveredTool, native NativeToolAdmissionOptions) ([]llm.ToolDefinition, toolcapabilities.Set, error) {
	if err := ctx.Err(); err != nil {
		return nil, toolcapabilities.Set{}, err
	}
	if source == nil {
		return nil, toolcapabilities.Set{}, fmt.Errorf("actor tool admission requires an admitted source")
	}
	native.Source = source
	if err := validateNativeToolAgentCapabilityAdmission(ctx, actor, native); err != nil {
		return nil, toolcapabilities.Set{}, err
	}
	emits := NewEmitRegistry(source, nil)
	authorize := func(name string) toolAuthorizationDecision {
		return sourceToolAuthorizationDecision(source, actor, name, emits, false)
	}
	admitNative := func(name string) (bool, string) {
		return nativeToolAdmissionForTool(ctx, actor, name, native, workspaceResolutionCapabilityAdmission)
	}
	var warnings []error
	definitions, err := admittedActorToolDefinitions(source, actor, discovered, emits, authorize,
		func(name string) bool { admitted, _ := admitNative(name); return admitted },
		func(_ string, _ string, format string, args ...any) {
			warnings = append(warnings, fmt.Errorf(format, args...))
		})
	if err = errors.Join(err, errors.Join(warnings...), ctx.Err()); err != nil {
		return nil, toolcapabilities.Set{}, err
	}
	names := make([]string, len(definitions))
	for i, definition := range definitions {
		names[i] = definition.Name
	}
	return definitions, actorToolCapabilities(names, nil, authorize, admitNative), nil
}

func admittedActorToolDefinitions(source semanticview.Source, actor models.AgentConfig, discovered map[string]runtimemcp.DiscoveredTool, emits *EmitRegistry, authorize func(string) toolAuthorizationDecision, native func(string) bool, warn func(string, string, string, ...any)) ([]llm.ToolDefinition, error) {
	entries, err := executionToolsForActor(source, actor, discovered)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	sort.Strings(names)
	definitions := make([]llm.ToolDefinition, 0, len(names))
	for _, name := range names {
		if !authorize(name).allowed || !native(name) {
			continue
		}
		entry := entries[name]
		definitions = append(definitions, llm.ToolDefinition{
			Name: name, Description: entry.Description(), Usage: entry.Usage(), Schema: entry.InputSchema(),
		})
	}
	if emits != nil {
		definitions = append(definitions, emits.GenerateEmitToolsForActor(actor, warn)...)
	}
	return definitions, nil
}
