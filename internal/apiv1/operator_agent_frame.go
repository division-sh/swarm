package apiv1

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

type AgentFrameEffectiveResolver interface {
	ResolveAgentFrameConfig(agentID, flowInstance string, root bool) (runtimemanager.AgentFrameConfig, error)
}

type AgentFrameHandlerOptions struct {
	Catalog   BundleCatalogReadStore
	Effective AgentFrameEffectiveResolver
}

func OperatorAgentFrameHandlers(opts AgentFrameHandlerOptions) map[string]MethodHandler {
	if opts.Catalog == nil && opts.Effective == nil {
		return nil
	}
	return map[string]MethodHandler{
		"agent.frame": func(ctx context.Context, req Request) (any, error) {
			scope, err := requiredStringParam(req.Params, "scope")
			if err != nil {
				return nil, err
			}
			switch agentframe.InspectionScope(scope) {
			case agentframe.InspectionStatic:
				return inspectStaticAgentFrame(ctx, opts.Catalog, req.Params)
			case agentframe.InspectionEffective:
				return inspectEffectiveAgentFrame(opts.Effective, req.Params)
			default:
				return nil, NewInvalidParamsError(map[string]any{"field": "scope", "reason": "must be static or effective"})
			}
		},
	}
}

func inspectStaticAgentFrame(ctx context.Context, catalog BundleCatalogReadStore, params map[string]any) (agentframe.Inspection, error) {
	if catalog == nil {
		return agentframe.Inspection{}, fmt.Errorf("bundle catalog read store is required for static agent frame inspection")
	}
	bundleHash, err := requiredBundleHashParam(params, "bundle_hash")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	flow, err := requiredStringParam(params, "flow")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	agentID, err := requiredStringParam(params, "agent_id")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	root, err := optionalBoolParam(params, "root", false)
	if err != nil {
		return agentframe.Inspection{}, err
	}
	flowInstance, _, err := optionalStringParam(params, "flow_instance")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	if root || flowInstance != "" {
		return agentframe.Inspection{}, NewInvalidParamsError(map[string]any{"field": "scope", "reason": "static inspection forbids root and flow_instance selectors"})
	}

	result, err := catalog.ListBundleCatalogAgents(ctx, bundleHash)
	if errors.Is(err, bundlecatalog.ErrNotFound) {
		return agentframe.Inspection{}, NewApplicationError(BundleNotFoundCode, false, map[string]any{"bundle_hash": bundleHash})
	}
	if err != nil {
		return agentframe.Inspection{}, err
	}
	wantFlow := strings.Trim(strings.TrimSpace(flow), "/")
	if wantFlow == "root" {
		wantFlow = ""
	}
	matches := make([]bundlecatalog.AgentDefinition, 0, 1)
	for _, definition := range result.Agents {
		if definition.AgentID == agentID && strings.Trim(strings.TrimSpace(definition.FlowInstance), "/") == wantFlow {
			matches = append(matches, definition)
		}
	}
	if len(matches) == 0 {
		return agentframe.Inspection{}, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID, "bundle_hash": bundleHash, "flow": flow})
	}
	if len(matches) != 1 {
		return agentframe.Inspection{}, fmt.Errorf("static agent frame selector is ambiguous")
	}
	definition := matches[0]
	intent := agentintent.Resolved{
		Kind:        agentintent.SourceKind(definition.IntentKind),
		Coordinate:  definition.IntentSource,
		Provenance:  definition.IntentProvenance,
		Content:     definition.IntentContent,
		ContentHash: definition.IntentContentHash,
		Identity:    definition.IntentIdentity,
	}
	return agentframe.InspectStatic(agentframe.InspectionSelector{
		BundleHash: bundleHash,
		Flow:       strings.Trim(strings.TrimSpace(flow), "/"),
		AgentID:    agentID,
	}, agentframe.PreviewSeed{
		BundleHash:     bundleHash,
		AgentID:        agentID,
		AuthoredFlow:   strings.Trim(strings.TrimSpace(flow), "/"),
		Role:           definition.Role,
		FlowID:         definition.FlowInstance,
		Intent:         intent,
		Criteria:       definition.Criteria,
		ProviderPrompt: definition.ProviderPrompt,
	})
}

func inspectEffectiveAgentFrame(resolver AgentFrameEffectiveResolver, params map[string]any) (agentframe.Inspection, error) {
	if resolver == nil {
		return agentframe.Inspection{}, fmt.Errorf("effective agent frame resolver is required")
	}
	agentID, err := requiredStringParam(params, "agent_id")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	root, err := optionalBoolParam(params, "root", false)
	if err != nil {
		return agentframe.Inspection{}, err
	}
	flowInstance, _, err := optionalStringParam(params, "flow_instance")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	bundleHash, _, err := optionalStringParam(params, "bundle_hash")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	flow, _, err := optionalStringParam(params, "flow")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	if bundleHash != "" || flow != "" || root == (flowInstance != "") {
		return agentframe.Inspection{}, NewInvalidParamsError(map[string]any{"field": "scope", "reason": "effective inspection requires exactly one of root or flow_instance and forbids bundle_hash and flow"})
	}
	resolved, err := resolver.ResolveAgentFrameConfig(agentID, flowInstance, root)
	if errors.Is(err, runtimemanager.ErrAgentNotFound) {
		return agentframe.Inspection{}, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID, "root": root, "flow_instance": flowInstance})
	}
	if err != nil {
		return agentframe.Inspection{}, err
	}
	return effectiveFrameInspection(resolved, agentID, flowInstance, root)
}

func effectiveFrameInspection(resolved runtimemanager.AgentFrameConfig, agentID, flowInstance string, root bool) (agentframe.Inspection, error) {
	cfg := resolved.Config
	identity, err := cfg.ConcreteIdentity()
	if err != nil {
		return agentframe.Inspection{}, err
	}
	prompt, err := cfg.ProviderPrompt(agentintent.RuntimeEnvironmentContext())
	if err != nil {
		return agentframe.Inspection{}, err
	}
	promptText, err := prompt.Text()
	if err != nil {
		return agentframe.Inspection{}, err
	}
	provider, err := effectiveAgentFrameProvider(cfg)
	if err != nil {
		return agentframe.Inspection{}, err
	}
	return agentframe.InspectEffective(agentframe.InspectionSelector{
		AgentID: agentID, Root: root, FlowInstance: flowInstance,
	}, agentframe.PreviewSeed{
		BundleHash: resolved.BundleHash, BundleSource: resolved.BundleSource,
		AgentID: agentID, AuthoredFlow: cfg.FlowID, AgentIdentity: &identity,
		Role: cfg.Role, FlowID: cfg.FlowID, Intent: cfg.Intent, Criteria: cfg.Criteria,
		Provider: &provider, ProviderPrompt: promptText,
	})
}

func effectiveAgentFrameProvider(cfg runtimeactors.AgentConfig) (agentframe.Provider, error) {
	provider := agentframe.Provider{
		RuntimeMode: strings.TrimSpace(cfg.ResolvedLLMBackend),
		Provider:    strings.TrimSpace(cfg.ResolvedLLMProvider),
		Transport:   strings.TrimSpace(cfg.ResolvedLLMTransport),
		Model:       strings.TrimSpace(cfg.ResolvedModel),
	}
	if provider.RuntimeMode == "" || provider.Provider == "" || provider.Transport == "" {
		return agentframe.Provider{}, fmt.Errorf("effective agent frame provider selection is incomplete")
	}
	return provider, nil
}
