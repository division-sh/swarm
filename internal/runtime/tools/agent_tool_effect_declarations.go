package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type AgentToolEffectDeclaration struct {
	Identity   string
	ToolName   string
	Provenance contracts.EffectiveValueProvenance
}

// AgentCallableEffectDeclarations projects authored declarations through the
// ordinary actor capability owners. Required MCP declarations do not require a
// live discovery result to explain their authored effect_class scope.
func AgentCallableEffectDeclarations(source semanticview.Source) []AgentToolEffectDeclaration {
	declarations := map[string]AgentToolEffectDeclaration{}
	for _, declaration := range semanticview.AgentDeclarations(source) {
		for _, effect := range callableEffectDeclarationsForAgent(source, declaration) {
			declarations[effect.Identity] = effect
		}
	}
	keys := make([]string, 0, len(declarations))
	for key := range declarations {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]AgentToolEffectDeclaration, 0, len(keys))
	for _, key := range keys {
		out = append(out, declarations[key])
	}
	return out
}

func callableEffectDeclarationsForAgent(source semanticview.Source, declaration semanticview.AgentDeclaration) []AgentToolEffectDeclaration {
	actor, err := configuredToolActor(source, declaration)
	if err != nil {
		return nil
	}
	projection, ok := semanticview.ScopedAgentContractProjection(source, declaration)
	if !ok {
		return nil
	}
	candidates, err := executionToolsForActor(source, actor, nil)
	if err != nil {
		return nil
	}
	names := make(map[string]struct{}, len(candidates)+len(actor.Tools))
	for name := range candidates {
		names[name] = struct{}{}
	}
	for _, name := range actor.Tools {
		names[strings.TrimSpace(name)] = struct{}{}
	}
	emits := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))
	var out []AgentToolEffectDeclaration
	for name := range names {
		if runtimeToolHiddenFromAgents(name) || !classifyToolAuthorization(actor, name, emits).allowed {
			continue
		}
		if _, executable := candidates[name]; !executable && !IsAgentRequiredMCPToolReference(source, declaration, name) {
			continue
		}
		entry, present := projection.ToolEntry(name)
		if !present || !entry.AgentExposable() {
			continue
		}
		provenance, authored := entry.EffectClassProvenance()
		if !authored || provenance.Origin != contracts.EffectiveValueOriginAuthored {
			continue
		}
		identity := fmt.Sprintf("%s:%d:%d:%s", provenance.SourceFile, provenance.SourceLine, provenance.SourceColumn, name)
		out = append(out, AgentToolEffectDeclaration{Identity: identity, ToolName: name, Provenance: provenance})
	}
	return out
}
