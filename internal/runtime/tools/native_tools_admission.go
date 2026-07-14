package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type NativeToolAdmissionOptions struct {
	Runtime     llm.Runtime
	Credentials runtimecredentials.Store
	Source      semanticview.Source
	Workspaces  workspace.Resolver
}

type NativeToolAdmissionDecision struct {
	Capability             string
	ToolNames              []string
	Admitted               bool
	ProviderNativeAdmitted bool
	FallbackAdmitted       bool
	Owner                  string
	DenialReason           string
}

const (
	nativeToolOwnerProviderNative      = "llm provider native capability"
	nativeToolOwnerWorkspaceExecution  = "workspace execution target"
	nativeToolOwnerWebSearchProvider   = "policy.web_search_provider"
	nativeToolProviderOnlyFallbackDeny = "native tool is provider-native only for selected runtime; platform fallback is not admitted"
)

func ValidateNativeToolAgentAdmission(ctx context.Context, actor models.AgentConfig, opts NativeToolAdmissionOptions) error {
	decisions := NativeToolAdmissionDecisions(ctx, actor, opts)
	var denied []string
	for _, decision := range decisions {
		if decision.Admitted {
			continue
		}
		reason := strings.TrimSpace(decision.DenialReason)
		if reason == "" {
			reason = "native tool admission was denied"
		}
		denied = append(denied, fmt.Sprintf("native_tools.%s for agent %s: %s", decision.Capability, actorLabel(actor), reason))
	}
	if len(denied) == 0 {
		return nil
	}
	sort.Strings(denied)
	return fmt.Errorf("%s", strings.Join(denied, "; "))
}

func NativeToolAdmissionDecisions(ctx context.Context, actor models.AgentConfig, opts NativeToolAdmissionOptions) []NativeToolAdmissionDecision {
	if !actor.NativeTools.Any() {
		return nil
	}
	capabilities := actor.NativeTools.Names()
	sort.Strings(capabilities)
	decisions := make([]NativeToolAdmissionDecision, 0, len(capabilities))
	for _, capability := range capabilities {
		decisions = append(decisions, nativeToolAdmissionDecision(ctx, actor, capability, opts))
	}
	return decisions
}

func nativeToolAdmissionDecision(ctx context.Context, actor models.AgentConfig, capability string, opts NativeToolAdmissionOptions) NativeToolAdmissionDecision {
	capability = strings.TrimSpace(capability)
	decision := NativeToolAdmissionDecision{
		Capability: capability,
		ToolNames:  nativeToolNameForCapability(capability),
	}
	if capability == "" {
		decision.DenialReason = "native tool capability is empty"
		return decision
	}
	providerCaps := llm.NativeToolCapabilitiesForRuntime(opts.Runtime)
	if nativeToolCapabilitySupported(providerCaps, capability) {
		decision.Admitted = true
		decision.ProviderNativeAdmitted = true
		decision.Owner = nativeToolOwnerProviderNative
		return decision
	}
	contract, hasContract := llm.ProviderContractForRuntime(opts.Runtime)
	if llm.RuntimeEnforcesProviderNativeTools(opts.Runtime) {
		decision.DenialReason = "selected runtime is strict provider-native and does not support provider-native capability"
		return decision
	}
	if !hasContract || !contract.NativeTools.FallbackToolsAllowed {
		decision.DenialReason = "selected runtime does not allow native tool fallback"
		return decision
	}
	switch capability {
	case "bash":
		return admitWorkspaceCapability(ctx, actor, decision, opts.Workspaces, workspace.ExecutionCapabilityNativeCommand)
	case "file_io":
		if first := workspaceCapabilityDenial(ctx, actor, opts.Workspaces, workspace.ExecutionCapabilityFileRead); first != "" {
			decision.DenialReason = first
			return decision
		}
		if second := workspaceCapabilityDenial(ctx, actor, opts.Workspaces, workspace.ExecutionCapabilityFileWrite); second != "" {
			decision.DenialReason = second
			return decision
		}
		decision.Admitted = true
		decision.FallbackAdmitted = true
		decision.Owner = nativeToolOwnerWorkspaceExecution
		return decision
	case "web_search":
		return admitWebSearchFallback(ctx, actor, decision, opts)
	default:
		decision.DenialReason = "unsupported native tool capability"
		return decision
	}
}

func nativeToolCapabilitySupported(caps llm.NativeToolCapabilities, capability string) bool {
	switch strings.TrimSpace(capability) {
	case "bash":
		return caps.Bash
	case "web_search":
		return caps.WebSearch
	case "file_io":
		return caps.FileIO
	default:
		return false
	}
}

func admitWorkspaceCapability(ctx context.Context, actor models.AgentConfig, decision NativeToolAdmissionDecision, resolver workspace.Resolver, capability workspace.ExecutionCapability) NativeToolAdmissionDecision {
	if reason := workspaceCapabilityDenial(ctx, actor, resolver, capability); reason != "" {
		decision.DenialReason = reason
		return decision
	}
	decision.Admitted = true
	decision.FallbackAdmitted = true
	decision.Owner = nativeToolOwnerWorkspaceExecution
	return decision
}

func workspaceCapabilityDenial(ctx context.Context, actor models.AgentConfig, resolver workspace.Resolver, capability workspace.ExecutionCapability) string {
	if resolver == nil {
		return "workspace resolver is not configured"
	}
	target, err := resolver.ResolveWorkspace(ctx, actor)
	if err != nil {
		return err.Error()
	}
	execTarget := target.ExecutionTarget()
	if !execTarget.Supports(capability) {
		return execTarget.UnsupportedMessage(capability)
	}
	return ""
}

func admitWebSearchFallback(ctx context.Context, actor models.AgentConfig, decision NativeToolAdmissionDecision, opts NativeToolAdmissionOptions) NativeToolAdmissionDecision {
	flowID := emitActorFlowID(opts.Source, actor, "")
	cfg, err := resolveWebSearchProviderConfigFromSourceForFlow(opts.Source, flowID)
	if err != nil {
		decision.DenialReason = err.Error()
		return decision
	}
	if requiresWebSearchCredential(cfg) {
		if err := validateWebSearchCredential(ctx, opts.Source, actor, cfg, opts.Credentials); err != nil {
			decision.DenialReason = err.Error()
			return decision
		}
	}
	decision.Admitted = true
	decision.FallbackAdmitted = true
	decision.Owner = nativeToolOwnerWebSearchProvider
	return decision
}

func requiresWebSearchCredential(cfg webSearchProviderConfig) bool {
	switch strings.ToLower(strings.TrimSpace(cfg.Provider)) {
	case "brave", "serper", "tavily":
		return true
	default:
		return strings.TrimSpace(cfg.CredentialsKey) != ""
	}
}

func validateWebSearchCredential(ctx context.Context, source semanticview.Source, actor models.AgentConfig, cfg webSearchProviderConfig, store runtimecredentials.Store) error {
	key := strings.TrimSpace(cfg.CredentialsKey)
	if key == "" {
		return fmt.Errorf("web_search provider %q requires credentials_key", strings.TrimSpace(cfg.Provider))
	}
	flowID := emitActorFlowID(source, actor, "")
	storeKey, mapped := semanticview.CredentialStoreKeyForActorFlow(source, actor.ID, flowID, key)
	if mapped && strings.TrimSpace(storeKey) == "" {
		return fmt.Errorf("credential %q is not declared and bound for imported package actor %s", key, strings.TrimSpace(actor.ID))
	}
	storeKey = strings.TrimSpace(storeKey)
	if storeKey == "" {
		return fmt.Errorf("credential %q does not resolve to a deployment credential key", key)
	}
	if store == nil {
		return fmt.Errorf("credential store is not configured")
	}
	value, ok, err := store.Get(ctx, storeKey)
	if err != nil {
		return err
	}
	if !ok || strings.TrimSpace(value) == "" {
		return fmt.Errorf("missing credential %q", storeKey)
	}
	return nil
}

func nativeToolAgentConfig(agentID string, entry runtimecontracts.AgentRegistryEntry) models.AgentConfig {
	cfg := models.AgentConfig{
		ID:              coalesce(strings.TrimSpace(entry.ID), strings.TrimSpace(agentID)),
		Type:            strings.TrimSpace(entry.Type),
		Role:            strings.TrimSpace(entry.Role),
		Model:           strings.TrimSpace(entry.Model),
		Memory:          entry.MemoryPlan,
		MaxTurnsPerTask: entry.MaxTurnsPerTask,
		Subscriptions:   append([]string{}, entry.Subscriptions...),
		EmitEvents:      append([]string{}, entry.EmitEvents...),
		Tools:           entry.ConfiguredTools(),
		Permissions:     append([]string{}, entry.Permissions...),
		FlowDataAccess:  append([]string{}, entry.FlowDataAccess...),
		Criteria:        append([]string{}, entry.Criteria...),
		WorkspaceClass:  strings.TrimSpace(entry.WorkspaceClass),
		ManagerFallback: strings.TrimSpace(entry.ManagerFallback),
	}
	cfg.NativeTools = nativeToolConfigFromContract(entry.NativeTools)
	cfg.NormalizeRuntimeDescriptor()
	return cfg
}

func nativeToolConfigFromContract(raw map[string]any) models.NativeToolConfig {
	return models.NativeToolConfig{
		Bash:      nativeToolContractFlag(raw, "bash"),
		WebSearch: nativeToolContractFlag(raw, "web_search"),
		FileIO:    nativeToolContractFlag(raw, "file_io"),
	}
}

func nativeToolContractFlag(raw map[string]any, name string) bool {
	value, ok := raw[strings.TrimSpace(name)]
	flag, isBool := value.(bool)
	return ok && isBool && flag
}

func actorLabel(actor models.AgentConfig) string {
	if strings.TrimSpace(actor.ID) != "" {
		return strings.TrimSpace(actor.ID)
	}
	if strings.TrimSpace(actor.Role) != "" {
		return strings.TrimSpace(actor.Role)
	}
	return "<unknown>"
}
