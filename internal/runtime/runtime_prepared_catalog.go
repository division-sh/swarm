package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// PreparedSelectedForkProviderCatalog freezes the resolved prospective inputs.
// It contains no execution grant and exposes no provider dispatch operation.
type PreparedSelectedForkProviderCatalog struct {
	fingerprint string
	targets     map[agentidentity.Plan]preparedProviderCatalogTarget
}

type preparedProviderCatalogTarget struct {
	revision     string
	prompt       string
	resolved     llm.AgentRuntimeResolution
	tools        []llm.ToolDefinition
	capabilities toolcapabilities.Set
}

func (p *PreparedSelectedForkProviderCatalog) Fingerprint() string {
	if p == nil {
		return ""
	}
	return p.fingerprint
}

// Actors returns values, not the mutable resolved configuration or tool plans.
func (p *PreparedSelectedForkProviderCatalog) Actors() []runfork.SelectedForkPreparedActor {
	if p == nil {
		return nil
	}
	actors := make([]runfork.SelectedForkPreparedActor, 0, len(p.targets))
	for plan, target := range p.targets {
		actors = append(actors, runfork.SelectedForkPreparedActor{
			Plan: plan, ConfigurationRevision: target.revision,
			Backend: target.resolved.Selection.Profile.ID, Mode: target.resolved.Selection.Mode,
		})
	}
	sort.Slice(actors, func(i, j int) bool { return agentidentity.LessPlan(actors[i].Plan, actors[j].Plan) })
	return actors
}

func (p *PreparedSelectedForkProviderCatalog) ValidateActors(blueprints []manager.AgentMaterializationBlueprint) error {
	if p == nil || p.fingerprint == "" || len(p.targets) != len(blueprints) {
		return fmt.Errorf("prepared provider catalog actor census changed")
	}
	seen := make(map[agentidentity.Plan]bool, len(blueprints))
	for _, blueprint := range blueprints {
		plan := blueprint.Identity
		if plan != plan.Normalize() || seen[plan] {
			return fmt.Errorf("prepared provider catalog actor plan changed")
		}
		seen[plan] = true
		target, ok := p.targets[plan]
		revision, err := manager.AgentConfigPlanRevision(blueprint.Config, plan)
		if !ok || err != nil || revision != target.revision {
			return fmt.Errorf("prepared provider catalog actor configuration changed")
		}
		prompt, err := preparedProviderPrompt(blueprint.Config)
		if err != nil || prompt != target.prompt {
			return fmt.Errorf("prepared provider catalog actor prompt changed")
		}
	}
	return nil
}

func PrepareSelectedForkProviderCatalog(ctx context.Context, runtimes *llm.AgentRuntimeSet, tools claudeStartupToolSource, blueprints []manager.AgentMaterializationBlueprint) (*PreparedSelectedForkProviderCatalog, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(blueprints) > 0 && (runtimes == nil || tools == nil) {
		return nil, fmt.Errorf("selected preparation catalog requires runtime and tool resolution")
	}
	catalog := &PreparedSelectedForkProviderCatalog{targets: make(map[agentidentity.Plan]preparedProviderCatalogTarget, len(blueprints))}
	ordered := append([]manager.AgentMaterializationBlueprint(nil), blueprints...)
	sort.Slice(ordered, func(i, j int) bool { return agentidentity.LessPlan(ordered[i].Identity, ordered[j].Identity) })
	// Reject the complete census before resolving even its first provider.
	for _, blueprint := range ordered {
		plan := blueprint.Identity
		if plan != plan.Normalize() {
			return nil, fmt.Errorf("selected catalog requires a canonical actor plan")
		}
		if err := plan.Validate(); err != nil {
			return nil, err
		}
		if !blueprint.Config.Identity.IsZero() || blueprint.Config.ID != plan.AgentID() || blueprint.Config.CanonicalFlowPath() != plan.FlowInstance() {
			return nil, fmt.Errorf("selected catalog requires exact runless actor configuration")
		}
		if _, exists := catalog.targets[plan]; exists {
			return nil, fmt.Errorf("selected catalog contains duplicate actor plan")
		}
		revision, err := manager.AgentConfigPlanRevision(blueprint.Config, plan)
		if err != nil {
			return nil, err
		}
		catalog.targets[plan] = preparedProviderCatalogTarget{revision: revision}
	}
	type definition struct {
		Name            string
		Description     string
		Schema          any
		GeneratedSchema bool
	}
	type entry struct {
		Plan                  agentidentity.Plan
		ConfigurationRevision string
		ResolvedConfiguration json.RawMessage
		Selection             json.RawMessage
		Prompt                string
		Tools                 []definition
		Capabilities          toolcapabilities.Set
	}
	entries := make([]entry, 0, len(ordered))
	for _, blueprint := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Configuration and tool schemas may contain maps. Snapshot through their
		// structured representation so caller mutation cannot rewrite the plan.
		frozenConfig, err := clonePreparedProviderActor(blueprint.Config)
		if err != nil {
			return nil, err
		}
		resolved, err := runtimes.ResolveAgentRuntime(frozenConfig)
		if err != nil {
			return nil, err
		}
		prompt, err := preparedProviderPrompt(resolved.Actor)
		if err != nil {
			return nil, err
		}
		definitions, capabilities, err := startupToolPlan(actors.WithActor(ctx, resolved.Actor), resolved.Actor, resolved.Runtime, tools)
		if err != nil {
			return nil, err
		}
		frozenTools, frozenCapabilities, err := clonePreparedProviderTools(definitions, capabilities)
		if err != nil {
			return nil, err
		}
		plannedTools := make([]definition, len(definitions))
		for i, tool := range frozenTools {
			plannedTools[i] = definition{tool.Name, llm.DeliveredToolDescription(tool), tool.Schema, tool.GeneratedSchema}
		}
		resolvedConfig, err := canonicaljson.Bytes(resolved.Actor)
		if err != nil {
			return nil, err
		}
		selection, err := canonicaljson.Bytes(resolved.Selection)
		if err != nil {
			return nil, err
		}
		target := catalog.targets[blueprint.Identity]
		target.resolved, target.tools, target.capabilities = resolved, frozenTools, frozenCapabilities
		target.prompt = prompt
		catalog.targets[blueprint.Identity] = target
		entries = append(entries, entry{blueprint.Identity, target.revision, resolvedConfig, selection, prompt, plannedTools, frozenCapabilities})
	}
	raw, err := canonicaljson.Bytes(entries)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(raw)
	catalog.fingerprint = hex.EncodeToString(digest[:])
	return catalog, nil
}

func preparedProviderPrompt(actor actors.AgentConfig) (string, error) {
	prompt, err := actor.ProviderPrompt(agentintent.RuntimeEnvironmentContext())
	if err != nil {
		return "", err
	}
	return prompt.Text()
}

func clonePreparedProviderActor(actor actors.AgentConfig) (actors.AgentConfig, error) {
	raw, err := canonicaljson.Bytes(actor)
	if err != nil {
		return actors.AgentConfig{}, err
	}
	var frozen actors.AgentConfig
	if err := canonicaljson.DecodeInto(raw, &frozen); err != nil {
		return actors.AgentConfig{}, err
	}
	// DerivedPrompt is immutable and intentionally absent from the JSON DTO.
	frozen.Prompt = actor.Prompt
	return frozen, nil
}

func clonePreparedProviderTools(definitions []llm.ToolDefinition, capabilities toolcapabilities.Set) ([]llm.ToolDefinition, toolcapabilities.Set, error) {
	frozen := make([]llm.ToolDefinition, len(definitions))
	for i, tool := range definitions {
		raw, err := canonicaljson.Bytes(tool)
		if err != nil {
			return nil, toolcapabilities.Set{}, err
		}
		if err := canonicaljson.DecodeInto(raw, &frozen[i]); err != nil {
			return nil, toolcapabilities.Set{}, err
		}
		frozen[i].Usage, frozen[i].GeneratedSchema = tool.Usage, tool.GeneratedSchema
	}
	raw, err := canonicaljson.Bytes(capabilities)
	if err != nil {
		return nil, toolcapabilities.Set{}, err
	}
	var frozenCapabilities toolcapabilities.Set
	if err := canonicaljson.DecodeInto(raw, &frozenCapabilities); err != nil {
		return nil, toolcapabilities.Set{}, err
	}
	return frozen, frozenCapabilities, nil
}

func (t preparedProviderCatalogTarget) snapshot() (preparedProviderCatalogTarget, error) {
	actor, err := clonePreparedProviderActor(t.resolved.Actor)
	if err != nil {
		return preparedProviderCatalogTarget{}, err
	}
	definitions, capabilities, err := clonePreparedProviderTools(t.tools, t.capabilities)
	if err != nil {
		return preparedProviderCatalogTarget{}, err
	}
	t.resolved.Actor, t.tools, t.capabilities = actor, definitions, capabilities
	return t, nil
}
