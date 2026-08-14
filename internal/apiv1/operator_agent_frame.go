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
	"github.com/division-sh/swarm/internal/runtime/core/bundleidentity"
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
			scope, err := requiredExactAgentFrameScalarParam(req.Params, "scope")
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
	bundleHash, err := requiredExactAgentFrameScalarParam(params, "bundle_hash")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	if err := bundleidentity.ValidateCanonicalHash(bundleHash); err != nil {
		return agentframe.Inspection{}, NewInvalidParamsError(map[string]any{"field": "bundle_hash", "reason": "must be bundle-v1:sha256:<64 lowercase hex>"})
	}
	flow, err := requiredExactAgentFramePathParam(params, "flow")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	agentID, err := requiredExactAgentFrameScalarParam(params, "agent_id")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	root, err := optionalBoolParam(params, "root", false)
	if err != nil {
		return agentframe.Inspection{}, err
	}
	flowInstance, _, err := optionalExactAgentFramePathParam(params, "flow_instance")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	if root || flowInstance != "" {
		return agentframe.Inspection{}, NewInvalidParamsError(map[string]any{"field": "scope", "reason": "static inspection forbids root and flow_instance selectors"})
	}

	wantFlow := flow
	if wantFlow == "root" {
		wantFlow = ""
	}
	matches := make([]bundlecatalog.AgentDefinition, 0, 1)
	cursor := ""
	seenCursors := map[string]struct{}{}
	for {
		result, err := catalog.ListBundleCatalogAgents(ctx, bundleHash, bundlecatalog.AgentListOptions{
			Limit: bundlecatalog.MaxAgentListLimit, Cursor: cursor,
		})
		if errors.Is(err, bundlecatalog.ErrNotFound) {
			return agentframe.Inspection{}, NewApplicationError(BundleNotFoundCode, false, map[string]any{"bundle_hash": bundleHash})
		}
		if err != nil {
			return agentframe.Inspection{}, err
		}
		for _, definition := range result.Agents {
			if definition.AgentID == agentID && definition.FlowInstance == wantFlow {
				matches = append(matches, definition)
			}
		}
		nextCursor := strings.TrimSpace(result.NextCursor)
		if nextCursor == "" {
			break
		}
		if _, seen := seenCursors[nextCursor]; seen {
			return agentframe.Inspection{}, fmt.Errorf("static agent frame catalog pagination repeated a cursor")
		}
		seenCursors[nextCursor] = struct{}{}
		cursor = nextCursor
	}
	if len(matches) == 0 {
		return agentframe.Inspection{}, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
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
		Flow:       flow,
		AgentID:    agentID,
	}, agentframe.PreviewSeed{
		BundleHash:     bundleHash,
		AgentID:        agentID,
		AuthoredFlow:   flow,
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
	agentID, err := requiredExactAgentFrameScalarParam(params, "agent_id")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	root, err := optionalBoolParam(params, "root", false)
	if err != nil {
		return agentframe.Inspection{}, err
	}
	flowInstance, _, err := optionalExactAgentFramePathParam(params, "flow_instance")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	bundleHash, _, err := optionalExactAgentFrameScalarParam(params, "bundle_hash")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	flow, _, err := optionalExactAgentFramePathParam(params, "flow")
	if err != nil {
		return agentframe.Inspection{}, err
	}
	if bundleHash != "" || flow != "" || root == (flowInstance != "") {
		return agentframe.Inspection{}, NewInvalidParamsError(map[string]any{"field": "scope", "reason": "effective inspection requires exactly one of root or flow_instance and forbids bundle_hash and flow"})
	}
	resolved, err := resolver.ResolveAgentFrameConfig(agentID, flowInstance, root)
	if errors.Is(err, runtimemanager.ErrAgentNotFound) {
		return agentframe.Inspection{}, NewApplicationError(AgentNotFoundCode, false, map[string]any{"agent_id": agentID})
	}
	if err != nil {
		return agentframe.Inspection{}, err
	}
	return effectiveFrameInspection(resolved, agentID, flowInstance, root)
}

func requiredExactAgentFramePathParam(params map[string]any, name string) (string, error) {
	value, present, err := optionalExactAgentFramePathParam(params, name)
	if err != nil {
		return "", err
	}
	if !present || value == "" {
		return "", NewInvalidParamsError(map[string]any{"field": name, "reason": "is required"})
	}
	return value, nil
}

func requiredExactAgentFrameScalarParam(params map[string]any, name string) (string, error) {
	value, present, err := optionalExactAgentFrameScalarParam(params, name)
	if err != nil {
		return "", err
	}
	if !present || value == "" {
		return "", NewInvalidParamsError(map[string]any{"field": name, "reason": "is required"})
	}
	return value, nil
}

func optionalExactAgentFrameScalarParam(params map[string]any, name string) (string, bool, error) {
	if params == nil {
		return "", false, nil
	}
	raw, present := params[name]
	if !present || raw == nil {
		return "", present, nil
	}
	value, ok := raw.(string)
	if !ok {
		return "", true, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be a string"})
	}
	if value != strings.TrimSpace(value) {
		return "", true, NewInvalidParamsError(map[string]any{
			"field": name, "reason": "must be an exact value without surrounding whitespace",
		})
	}
	return value, true, nil
}

func optionalExactAgentFramePathParam(params map[string]any, name string) (string, bool, error) {
	if params == nil {
		return "", false, nil
	}
	raw, present := params[name]
	if !present || raw == nil {
		return "", present, nil
	}
	value, ok := raw.(string)
	if !ok {
		return "", true, NewInvalidParamsError(map[string]any{"field": name, "reason": "must be a string"})
	}
	if value == "" {
		return "", true, nil
	}
	if value != strings.TrimSpace(value) || value != strings.Trim(value, "/") {
		return "", true, NewInvalidParamsError(map[string]any{
			"field":  name,
			"reason": "must be an exact canonical path without surrounding whitespace or leading or trailing slash",
		})
	}
	return value, true, nil
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
