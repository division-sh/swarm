package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/google/uuid"
)

func validateClaudeStartupConfig(ctx context.Context, cfg *config.Config, opts RuntimeOptions, source semanticview.Source) error {
	enabled, err := isClaudeCLIBackend(cfg)
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	hasAgents, err := workflowSourceDeclaresAgents(source)
	if err != nil {
		return err
	}
	if !hasAgents {
		return nil
	}
	if bundleFullyMocked(source) {
		return nil
	}
	return validateClaudeStartupRequirements(ctx, cfg, opts)
}

func validateClaudeStartupConfigForActiveAgents(ctx context.Context, cfg *config.Config, opts RuntimeOptions, source semanticview.Source, manager *runtimemanager.AgentManager) error {
	enabled, err := isClaudeCLIBackend(cfg)
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	hasAgents, err := workflowSourceOrManagerDeclaresAgents(source, manager)
	if err != nil {
		return err
	}
	if !hasAgents {
		return nil
	}
	if bundleFullyMocked(source) {
		return activeAgentMockConsistencyError(manager)
	}
	return validateClaudeStartupRequirements(ctx, cfg, opts)
}

func validateSelectedBackendCredentialForDeclaredAgents(ctx context.Context, cfg *config.Config, opts RuntimeOptions, source semanticview.Source) error {
	hasAgents, err := workflowSourceDeclaresAgents(source)
	if err != nil {
		return err
	}
	if !hasAgents {
		return nil
	}
	if bundleFullyMocked(source) {
		return nil
	}
	return enrichCredentialFailureWithMockCensus(validateSelectedBackendCredential(ctx, cfg, providerCredentialResolverForRuntimeOptions(opts)), cfg, source)
}

func validateSelectedBackendModelAliasesForDeclaredAgents(cfg *config.Config, source semanticview.Source) error {
	if cfg == nil {
		return nil
	}
	if source == nil {
		return fmt.Errorf("semantic source is required for llm model alias validation")
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return err
	}
	declarations := semanticview.AgentDeclarations(source)
	localIDCounts := map[string]int{}
	for _, declaration := range declarations {
		localIDCounts[declaration.LocalID]++
	}
	for _, declaration := range declarations {
		agent := declaration.Entry
		agentID := declaration.Label(localIDCounts[declaration.LocalID] > 1)
		selection, err := llmselection.ResolveAgentExecutionSelection(llmselection.AgentExecutionSelectionInput{
			ConfiguredDefault: profile,
			MockConfigured:    agent.Mock.Configured(),
		})
		if err != nil {
			return fmt.Errorf("agent %s execution selection failed: %w", strings.TrimSpace(agentID), err)
		}
		if _, err := llmselection.ResolveModel(selection.ModelProfile, llmselection.ModelResolution{
			Model:  agent.Model,
			Models: cfg.LLM.Models,
		}); err != nil {
			return fmt.Errorf("agent %s model resolution failed: %w", strings.TrimSpace(agentID), err)
		}
	}
	return nil
}

func validateSelectedBackendCredentialForActiveAgents(ctx context.Context, cfg *config.Config, opts RuntimeOptions, source semanticview.Source, manager *runtimemanager.AgentManager) error {
	hasAgents, err := workflowSourceOrManagerDeclaresAgents(source, manager)
	if err != nil {
		return err
	}
	if !hasAgents {
		return nil
	}
	if bundleFullyMocked(source) {
		return activeAgentMockConsistencyError(manager)
	}
	return enrichCredentialFailureWithMockCensus(validateSelectedBackendCredential(ctx, cfg, providerCredentialResolverForRuntimeOptions(opts)), cfg, source)
}

func validateSelectedBackendCredential(ctx context.Context, cfg *config.Config, credentials llm.ProviderCredentialResolver) error {
	if cfg == nil {
		return nil
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return err
	}
	_, err = credentials.Resolve(ctx, profile)
	return err
}

// declaredAgentMockCensus is the single owner of the fully-mocked waiver fact:
// every entry in the effective source agent registry is mock-configured. It
// reports the mocked/total counts and the sorted unmocked agent ids.
func declaredAgentMockCensus(source semanticview.Source) (mocked, total int, unmocked []string) {
	if source == nil {
		return 0, 0, nil
	}
	entries := semanticview.AgentDeclarations(source)
	total = len(entries)
	localIDCounts := map[string]int{}
	for _, declaration := range entries {
		localIDCounts[declaration.LocalID]++
	}
	for _, declaration := range entries {
		agent := declaration.Entry
		if agent.Mock.Configured() {
			mocked++
		} else {
			unmocked = append(unmocked, declaration.Label(localIDCounts[declaration.LocalID] > 1))
		}
	}
	sort.Strings(unmocked)
	return mocked, total, unmocked
}

// bundleFullyMocked reports whether every declared model agent in the bundle
// carries a configured mock performance. A fully-mocked bundle can never
// launch a provider turn, so boot provider-backend startup obligations are
// waived for it (zero-credential testing affordance; production validity is
// unchanged and mock mode still fails closed on real external surfaces).
func bundleFullyMocked(source semanticview.Source) bool {
	mocked, total, _ := declaredAgentMockCensus(source)
	return total > 0 && mocked == total
}

// activeAgentMockConsistencyError enforces the waiver invariant at the
// active-agents site: recovered and cloned agents are bundle-derived, so a
// fully-mocked source must never observe a non-mock-configured active agent.
// A divergence is corruption, not a case to handle; fail closed loudly rather
// than silently falling back to credential gating.
func activeAgentMockConsistencyError(manager *runtimemanager.AgentManager) error {
	if manager == nil {
		return nil
	}
	unmocked := make([]string, 0)
	for _, agentCfg := range manager.ListAgentConfigs() {
		if !agentCfg.Mock.Configured() {
			unmocked = append(unmocked, strings.TrimSpace(agentCfg.ID))
		}
	}
	sort.Strings(unmocked)
	if len(unmocked) == 0 {
		return nil
	}
	return fmt.Errorf("mock waiver invariant violation: bundle agents are fully mocked but active agents %s are not mock-configured", strings.Join(unmocked, ", "))
}

// enrichCredentialFailureWithMockCensus keeps the existing typed
// provider_credential_missing failure and extends its message with the mock
// census and unmocked agent names when the resolve fails with unmocked agents
// present. Fully-mocked bundles never reach here (waived earlier).
func enrichCredentialFailureWithMockCensus(err error, cfg *config.Config, source semanticview.Source) error {
	if err == nil {
		return nil
	}
	mocked, total, unmocked := declaredAgentMockCensus(source)
	if len(unmocked) == 0 {
		return err
	}
	envVar := ""
	if cfg != nil {
		if profile, profileErr := cfg.LLMBackendProfile(); profileErr == nil {
			envVar = strings.TrimSpace(profile.Credential.EnvVar)
		}
	}
	return fmt.Errorf("%w (%d of %d declared agents are mocked; unmocked: %s — mock them or provide %s)", err, mocked, total, strings.Join(unmocked, ", "), envVar)
}

func validateClaudeStartupRequirements(ctx context.Context, cfg *config.Config, opts RuntimeOptions) error {
	if err := llm.ValidateClaudeCLIRuntimeConfig(ctx, cfg, opts.ToolGatewayBinding, providerCredentialResolverForRuntimeOptions(opts)); err != nil {
		return err
	}
	if opts.WorkspaceLifecycle == nil {
		return fmt.Errorf("workspace lifecycle is required for claude cli runtime")
	}
	if !opts.EnableToolGateway {
		return fmt.Errorf("tool gateway must be enabled for claude cli runtime")
	}
	return nil
}

func workflowSourceDeclaresAgents(source semanticview.Source) (bool, error) {
	if source == nil {
		return false, fmt.Errorf("semantic source is required for claude cli runtime")
	}
	return semanticview.SourceDeclaresAgents(source), nil
}

func workflowSourceOrManagerDeclaresAgents(source semanticview.Source, manager *runtimemanager.AgentManager) (bool, error) {
	hasAgents, err := workflowSourceDeclaresAgents(source)
	if err != nil {
		return false, err
	}
	if hasAgents {
		return true, nil
	}
	if manager == nil {
		return false, nil
	}
	return len(manager.ListAgentConfigs()) > 0, nil
}

func validateClaudeManagedAgentWorkspaces(ctx context.Context, cfg *config.Config, source semanticview.Source, workspaces workspace.Lifecycle, manager *runtimemanager.AgentManager) error {
	enabled, err := isClaudeCLIBackend(cfg)
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	hasAgents, err := workflowSourceOrManagerDeclaresAgents(source, manager)
	if err != nil {
		return err
	}
	if !hasAgents {
		return nil
	}
	if bundleFullyMocked(source) {
		return activeAgentMockConsistencyError(manager)
	}
	if workspaces == nil {
		return fmt.Errorf("workspace lifecycle is required for claude cli runtime")
	}
	if manager == nil {
		return fmt.Errorf("agent manager is required for claude cli runtime")
	}
	for _, agentCfg := range manager.ListAgentConfigs() {
		profile, err := llmselection.ResolveActiveBackend(agentCfg.ResolvedLLMBackend)
		if err != nil {
			return fmt.Errorf("agent %s selected invalid llm backend: %w", strings.TrimSpace(agentCfg.ID), err)
		}
		if profile.ID != llmselection.BackendClaudeCLI {
			continue
		}
		target, err := workspaces.ResolveWorkspace(ctx, agentCfg)
		if err != nil {
			return fmt.Errorf("resolve workspace for agent %s: %w", strings.TrimSpace(agentCfg.ID), err)
		}
		execTarget := target.ExecutionTarget()
		if err := execTarget.Require(workspace.ExecutionCapabilityClaudeCLI); err != nil {
			return fmt.Errorf("agent %s resolved workspace target that does not support Claude CLI execution: %w", strings.TrimSpace(agentCfg.ID), err)
		}
	}
	return nil
}

type claudeStartupToolSource interface {
	ToolDefinitionsForActor(runtimeactors.AgentConfig) []llm.ToolDefinition
	ToolCapabilitiesForActor(runtimeactors.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set
}

type claudeStartupContextAwareToolSource interface {
	ToolDefinitionsForActorInContext(context.Context, runtimeactors.AgentConfig) []llm.ToolDefinition
	ToolCapabilitiesForActorInContext(context.Context, runtimeactors.AgentConfig, []string, map[string]struct{}) toolcapabilities.Set
}

type ManagedProviderPreflightAuthority struct {
	ExecutionKind        managedcapabilities.ExecutionKind
	ExecutionAuthorityID string
	RunID                string
	StartupOwnerID       string
	StartupGeneration    uint64
	EffectController     *runtimeeffects.Controller
	CapabilityStore      managedcapabilities.Persistence
	EffectAuthority      func(probeID, actorID string) (runtimeeffects.Authority, error)
}

func (a ManagedProviderPreflightAuthority) validate() error {
	if strings.TrimSpace(a.ExecutionAuthorityID) == "" || strings.TrimSpace(a.StartupOwnerID) == "" || a.StartupGeneration == 0 {
		return fmt.Errorf("managed provider preflight authority is incomplete")
	}
	if a.ExecutionKind != managedcapabilities.ExecutionNormalAgent && a.ExecutionKind != managedcapabilities.ExecutionSelectedContractFork {
		return fmt.Errorf("managed provider preflight execution kind %q is invalid", a.ExecutionKind)
	}
	if a.ExecutionKind == managedcapabilities.ExecutionNormalAgent && strings.TrimSpace(a.RunID) != "" {
		return fmt.Errorf("normal managed provider preflight cannot carry a fork run identity")
	}
	if a.ExecutionKind == managedcapabilities.ExecutionSelectedContractFork {
		if _, err := uuid.Parse(strings.TrimSpace(a.RunID)); err != nil {
			return fmt.Errorf("selected-fork managed provider preflight run id is invalid: %w", err)
		}
	}
	if a.EffectController == nil || a.CapabilityStore == nil || a.EffectAuthority == nil {
		return fmt.Errorf("managed provider preflight requires effect controller, capability persistence, and exact effect authority")
	}
	return nil
}

func ValidateManagedProviderPreflight(ctx context.Context, cfg *config.Config, source semanticview.Source, gatewayBinding toolgateway.Binding, runtimes *llm.AgentRuntimeSet, turnStore llm.MCPTurnContextStore, tools claudeStartupToolSource, manager *runtimemanager.AgentManager, authority ManagedProviderPreflightAuthority) ([]string, error) {
	hasAgents, err := workflowSourceOrManagerDeclaresAgents(source, manager)
	if err != nil {
		return nil, err
	}
	if !hasAgents {
		return nil, nil
	}
	if bundleFullyMocked(source) {
		return nil, activeAgentMockConsistencyError(manager)
	}
	if manager == nil {
		return nil, fmt.Errorf("agent manager is required for managed provider preflight")
	}
	if runtimes == nil {
		return nil, fmt.Errorf("managed provider preflight requires the agent runtime resolver")
	}
	type preflightTarget struct {
		config  runtimeactors.AgentConfig
		runtime llm.Runtime
		probe   llm.StartupVisibleToolSurfaceProber
	}
	targets := make([]preflightTarget, 0)
	for _, agentCfg := range manager.ListAgentConfigs() {
		resolved, resolveErr := runtimes.ResolveAgentRuntime(agentCfg)
		if resolveErr != nil {
			return nil, fmt.Errorf("resolve managed provider runtime for agent %s: %w", strings.TrimSpace(agentCfg.ID), resolveErr)
		}
		if resolved.Selection.Mode == runtimeeffects.ExecutionModeMock {
			continue
		}
		if resolved.Selection.Profile.ID != llmselection.BackendClaudeCLI {
			continue
		}
		startupProbe, ok := llm.StartupVisibleToolSurfaceProberForRuntime(resolved.Runtime)
		if !ok {
			return nil, fmt.Errorf("managed provider startup probe is required for agent %s", strings.TrimSpace(resolved.Actor.ID))
		}
		targets = append(targets, preflightTarget{config: resolved.Actor, runtime: resolved.Runtime, probe: startupProbe})
	}
	if len(targets) == 0 {
		return nil, nil
	}
	if err := authority.validate(); err != nil {
		return nil, err
	}
	if turnStore == nil {
		return nil, fmt.Errorf("mcp turn context store is required for claude cli runtime")
	}
	if tools == nil {
		return nil, fmt.Errorf("tool executor is required for claude cli runtime")
	}
	if err := gatewayBinding.Validate(); err != nil {
		return nil, fmt.Errorf("tool gateway binding is invalid for claude cli runtime: %w", err)
	}
	if !gatewayBinding.IsRuntimeOwned() {
		return nil, fmt.Errorf("tool gateway binding is not runtime-owned for claude cli runtime")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	surfaceIDs := make([]string, 0, len(targets))
	for _, target := range targets {
		agentCfg := target.config
		modelRuntime := target.runtime
		startupProbe := target.probe
		agentID := strings.TrimSpace(agentCfg.ID)
		if agentID == "" {
			continue
		}
		agentCtx := runtimeactors.WithActor(ctx, agentCfg)
		sessionTools, capabilities, err := startupToolPlan(agentCtx, agentCfg, modelRuntime, tools)
		if err != nil {
			return nil, fmt.Errorf("build managed capability startup inputs for agent %s: %w", agentID, err)
		}
		probeID := uuid.NewString()
		capabilityAuthority := managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityStartupProbe, ID: probeID,
			ExecutionKind: authority.ExecutionKind, ExecutionAuthorityID: strings.TrimSpace(authority.ExecutionAuthorityID),
			RunID: strings.TrimSpace(authority.RunID), StartupOwnerID: strings.TrimSpace(authority.StartupOwnerID), StartupGeneration: authority.StartupGeneration,
		}
		surface, err := llm.ManagedCapabilitySurfaceForStartup(agentCtx, modelRuntime, sessionTools, capabilities, capabilityAuthority)
		if err != nil {
			return nil, fmt.Errorf("build managed capability startup surface for agent %s: %w", agentID, err)
		}
		if err := authority.CapabilityStore.SaveManagedCapabilitySurface(agentCtx, surface); err != nil {
			return nil, fmt.Errorf("persist managed capability startup plan for agent %s: %w", agentID, err)
		}
		effectAuthority, err := authority.EffectAuthority(probeID, agentID)
		if err != nil {
			return nil, fmt.Errorf("build startup effect authority for agent %s: %w", agentID, err)
		}
		agentCtx = managedcapabilities.WithContext(agentCtx, surface)
		agentCtx = runtimeeffects.WithAuthority(agentCtx, effectAuthority)
		agentCtx = runtimeeffects.WithController(agentCtx, authority.EffectController)
		probeResp, err := startupProbe.ProbeStartupVisibleToolSurface(agentCtx, agentCfg, agentCfg.SystemPrompt, sessionTools)
		if err != nil {
			return nil, fmt.Errorf("claude cli startup probe failed for agent %s: %w", agentID, err)
		}
		if probeResp == nil || probeResp.CapabilitySurface == nil {
			return nil, fmt.Errorf("claude cli startup probe omitted managed capability evidence for agent %s", agentID)
		}
		surface = probeResp.CapabilitySurface.Clone()
		if err := authority.CapabilityStore.SaveManagedCapabilitySurface(agentCtx, surface); err != nil {
			return nil, fmt.Errorf("persist managed capability provider evidence for agent %s: %w", agentID, err)
		}
		expectedNames := startupSurfaceCanonicalNames(surface, managedcapabilities.BindingMCPTool)
		if len(expectedNames) == 0 {
			if err := validateEffectiveManagedCapabilitySurface(surface); err != nil {
				return nil, fmt.Errorf("managed capability startup surface is incomplete for agent %s: %w", agentID, err)
			}
			surfaceIDs = append(surfaceIDs, surface.ID)
			continue
		}
		agentCtx = managedcapabilities.WithContext(agentCtx, surface)
		binding, enabled, err := llm.BuildMCPHTTPBinding(agentCtx, cfg, turnStore, &llm.Session{
			AgentID: agentID,
			Tools:   sessionTools,
		}, gatewayBinding, llm.MCPGatewayHostEndpoint)
		if err != nil {
			return nil, fmt.Errorf("build mcp startup probe transport for agent %s: %w", agentID, err)
		}
		if !enabled {
			return nil, fmt.Errorf("mcp startup probe transport is disabled for agent %s", agentID)
		}
		if strings.TrimSpace(binding.ContextToken) == "" {
			return nil, fmt.Errorf("mcp startup probe missing turn context token for agent %s", agentID)
		}
		actualTools, err := startupProbeMCPToolsList(agentCtx, client, binding)
		if err != nil {
			turnStore.UnregisterTurnContext(binding.ContextToken)
			return nil, fmt.Errorf("mcp tools/list startup probe failed for agent %s: %w", agentID, err)
		}
		actualNames := startupToolNames(actualTools)
		if !equalSortedStrings(actualNames, expectedNames) {
			turnStore.UnregisterTurnContext(binding.ContextToken)
			return nil, fmt.Errorf("mcp tools/list returned unexpected tool surface for agent %s: expected [%s], got [%s]", agentID, strings.Join(expectedNames, ", "), strings.Join(actualNames, ", "))
		}
		listedSurface, ok := turnStore.ResolveManagedCapabilitySurface(binding.ContextToken)
		if !ok {
			turnStore.UnregisterTurnContext(binding.ContextToken)
			return nil, fmt.Errorf("mcp tools/list did not settle capability evidence for agent %s", agentID)
		}
		if err := authority.CapabilityStore.SaveManagedCapabilitySurface(agentCtx, listedSurface); err != nil {
			turnStore.UnregisterTurnContext(binding.ContextToken)
			return nil, fmt.Errorf("persist managed capability MCP evidence for agent %s: %w", agentID, err)
		}
		if err := validateEffectiveManagedCapabilitySurface(listedSurface); err != nil {
			turnStore.UnregisterTurnContext(binding.ContextToken)
			return nil, fmt.Errorf("managed capability startup surface is incomplete for agent %s: %w", agentID, err)
		}
		probeName, ok, err := selectStartupCallableTool(actualTools, listedSurface.CapabilitySet())
		if err != nil {
			turnStore.UnregisterTurnContext(binding.ContextToken)
			return nil, fmt.Errorf("mcp startup probe select callable tool for agent %s: %w", agentID, err)
		}
		if ok {
			err = startupProbeMCPToolsCall(agentCtx, client, binding, probeName)
		}
		turnStore.UnregisterTurnContext(binding.ContextToken)
		if err != nil {
			return nil, fmt.Errorf("mcp tools/call startup probe failed for agent %s tool %s: %w", agentID, probeName, err)
		}
		surfaceIDs = append(surfaceIDs, listedSurface.ID)
	}
	sort.Strings(surfaceIDs)
	return surfaceIDs, nil
}

func validateEffectiveManagedCapabilitySurface(surface managedcapabilities.Surface) error {
	if err := surface.Validate(); err != nil {
		return err
	}
	if surface.HasMismatch() {
		return fmt.Errorf("surface contains typed delivery mismatch")
	}
	for _, tool := range surface.Tools {
		if tool.Capability.Visible && tool.Capability.Callable && (!tool.EffectiveVisible || !tool.EffectiveCallable) {
			return fmt.Errorf("capability %s is not effectively callable: %s", tool.Name, tool.EffectiveDenial)
		}
	}
	return nil
}

func isClaudeCLIBackend(cfg *config.Config) (bool, error) {
	if cfg == nil {
		return false, nil
	}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		return false, err
	}
	return profile.ID == llmselection.BackendClaudeCLI, nil
}

func startupToolPlan(ctx context.Context, agentCfg runtimeactors.AgentConfig, modelRuntime llm.Runtime, tools claudeStartupToolSource) ([]llm.ToolDefinition, toolcapabilities.Set, error) {
	concreteTools := tools.ToolDefinitionsForActor(agentCfg)
	if contextAware, ok := tools.(claudeStartupContextAwareToolSource); ok {
		concreteTools = contextAware.ToolDefinitionsForActorInContext(ctx, agentCfg)
	}
	var err error
	concreteTools, err = llm.ConcreteManagedToolDefinitions(modelRuntime, agentCfg, concreteTools)
	if err != nil {
		return nil, toolcapabilities.Set{}, err
	}
	concreteNames := startupSessionToolNames(concreteTools)
	var concreteCaps toolcapabilities.Set
	if contextAware, ok := tools.(claudeStartupContextAwareToolSource); ok {
		concreteCaps = contextAware.ToolCapabilitiesForActorInContext(ctx, agentCfg, concreteNames, nil)
	} else {
		concreteCaps = tools.ToolCapabilitiesForActor(agentCfg, concreteNames, nil)
	}
	return concreteTools, concreteCaps, nil
}

func startupSurfaceCanonicalNames(surface managedcapabilities.Surface, kind managedcapabilities.BindingKind) []string {
	var names []string
	for _, tool := range surface.Tools {
		for _, binding := range tool.Bindings {
			if binding.Kind == kind && tool.Capability.Visible && tool.Capability.Callable {
				names = append(names, tool.Name)
				break
			}
		}
	}
	sort.Strings(names)
	return names
}

func startupSessionToolNames(tools []llm.ToolDefinition) []string {
	names := make([]string, 0, len(tools))
	seen := make(map[string]struct{}, len(tools))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func startupToolNames(tools []runtimemcp.ToolDef) []string {
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		if name := strings.TrimSpace(tool.Name); name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func equalSortedStrings(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if strings.TrimSpace(left[i]) != strings.TrimSpace(right[i]) {
			return false
		}
	}
	return true
}

func selectStartupCallableTool(tools []runtimemcp.ToolDef, capabilities toolcapabilities.Set) (string, bool, error) {
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		capability, ok := capabilities.Capability(name)
		if !ok || !capability.Callable || capability.Kind == toolcapabilities.KindEmit {
			continue
		}
		if capability.StartupProbeMode != toolcapabilities.StartupProbeModeCallEmptyObject {
			continue
		}
		if !schemaAllowsEmptyObjectArguments(tool.InputSchema) {
			return "", false, fmt.Errorf("tool %s is marked startup-probe call_empty_object but its schema requires arguments", name)
		}
		return name, true, nil
	}
	return "", false, nil
}

func schemaAllowsEmptyObjectArguments(schema any) bool {
	typed, ok := schema.(map[string]any)
	if !ok {
		return false
	}
	if schemaType := strings.TrimSpace(asString(typed["type"])); schemaType != "" && schemaType != "object" {
		return false
	}
	return len(schemaRequiredFieldNames(typed["required"])) == 0
}

func schemaRequiredFieldNames(raw any) []string {
	switch typed := raw.(type) {
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if name := strings.TrimSpace(asString(item)); name != "" {
				out = append(out, name)
			}
		}
		return out
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if name := strings.TrimSpace(item); name != "" {
				out = append(out, name)
			}
		}
		return out
	default:
		return nil
	}
}

func startupProbeMCPToolsList(ctx context.Context, client *http.Client, binding llm.MCPHTTPBinding) ([]runtimemcp.ToolDef, error) {
	if _, err := startupCallMCP(ctx, client, binding, runtimemcp.RPCRequest{
		JSONRPC: "2.0",
		Method:  "initialize",
		Params: map[string]any{
			"protocolVersion": "2025-03-26",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]any{
				"name":    "swarm-startup",
				"version": "1.0.0",
			},
		},
		ID: "startup-initialize",
	}); err != nil {
		return nil, err
	}
	if _, err := startupCallMCP(ctx, client, binding, runtimemcp.RPCRequest{
		JSONRPC: "2.0",
		Method:  "notifications/initialized",
		Params:  map[string]any{},
	}); err != nil {
		return nil, err
	}
	resp, err := startupCallMCP(ctx, client, binding, runtimemcp.RPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/list",
		Params:  map[string]any{},
		ID:      "startup-tools-list",
	})
	if err != nil {
		return nil, err
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("tools/list returned invalid result payload")
	}
	rawTools, ok := result["tools"]
	if !ok {
		return nil, fmt.Errorf("tools/list returned no tools array")
	}
	encoded, err := json.Marshal(rawTools)
	if err != nil {
		return nil, err
	}
	var tools []runtimemcp.ToolDef
	if err := json.Unmarshal(encoded, &tools); err != nil {
		return nil, fmt.Errorf("decode tools/list payload: %w", err)
	}
	return tools, nil
}

func startupProbeMCPToolsCall(ctx context.Context, client *http.Client, binding llm.MCPHTTPBinding, name string) error {
	resp, err := startupCallMCP(ctx, client, binding, runtimemcp.RPCRequest{
		JSONRPC: "2.0",
		Method:  "tools/call",
		Params: map[string]any{
			"name":      strings.TrimSpace(name),
			"arguments": map[string]any{},
			"_meta": map[string]any{
				runtimemcp.ClaudeCodeToolUseIDMetaKey: "startup-" + uuid.NewString(),
			},
			"swarmProbe": map[string]any{
				"contract": runtimemcp.StartupProbeContractManagedAgentCallable,
			},
		},
		ID: "startup-tools-call",
	})
	if err != nil {
		return err
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		return fmt.Errorf("tools/call returned invalid result payload")
	}
	probeResult, err := runtimemcp.DecodeStartupProbeResult(result["swarmStartupProbe"])
	if err != nil {
		return fmt.Errorf("tools/call returned invalid startup probe result (result=%v): %w", result, err)
	}
	if probeResult.ToolName != strings.TrimSpace(name) {
		return fmt.Errorf("tools/call returned startup probe result for unexpected tool %q", probeResult.ToolName)
	}
	isError := false
	if raw, present := result["isError"]; present {
		var ok bool
		isError, ok = raw.(bool)
		if !ok {
			return fmt.Errorf("tools/call returned non-boolean isError")
		}
	}
	rawRuntimeError, hasRuntimeError := result["runtimeError"]
	switch probeResult.Outcome {
	case runtimemcp.StartupProbeOutcomeSuccess:
		if isError {
			return fmt.Errorf("tools/call returned inconsistent startup probe success result")
		}
		if hasRuntimeError {
			return fmt.Errorf("tools/call returned runtimeError with startup probe success")
		}
		return nil
	case runtimemcp.StartupProbeOutcomeValidationOnly, runtimemcp.StartupProbeOutcomeExecutionFailure:
		if !isError {
			return fmt.Errorf("tools/call returned inconsistent startup probe %s result", probeResult.Outcome)
		}
		if !hasRuntimeError {
			return fmt.Errorf("tools/call omitted canonical failure envelope for startup probe %s", probeResult.Outcome)
		}
		runtimeErr, err := runtimemcp.DecodeRuntimeErrorPayload(rawRuntimeError)
		if err != nil || runtimeErr == nil || runtimeErr.Failure == nil {
			return fmt.Errorf("tools/call returned invalid canonical startup failure: %w", err)
		}
		isValidationFailure := runtimeErr.Failure.Class == failures.ClassSchemaInvalid && runtimeErr.Failure.Detail.Code == "invalid_tool_input"
		if probeResult.Outcome == runtimemcp.StartupProbeOutcomeValidationOnly {
			if !isValidationFailure {
				return fmt.Errorf("tools/call validation-only probe requires platform.schema_invalid/invalid_tool_input")
			}
			return nil
		}
		if isValidationFailure {
			return fmt.Errorf("tools/call execution-failure probe cannot carry validation-only failure")
		}
		return failures.FromEnvelope(*runtimeErr.Failure)
	default:
		return fmt.Errorf("tools/call returned unsupported startup probe outcome %q", probeResult.Outcome)
	}
}

func startupCallMCP(ctx context.Context, client *http.Client, binding llm.MCPHTTPBinding, req runtimemcp.RPCRequest) (runtimemcp.RPCResponse, error) {
	if !binding.IsRuntimeOwned() {
		return runtimemcp.RPCResponse{}, fmt.Errorf("mcp startup transport requires runtime-owned construction provenance")
	}
	body, err := json.Marshal(req)
	if err != nil {
		return runtimemcp.RPCResponse{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSpace(binding.URL), bytes.NewReader(body))
	if err != nil {
		return runtimemcp.RPCResponse{}, err
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("mcp-protocol-version", "2025-03-26")
	for key, value := range binding.Headers {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		httpReq.Header.Set(key, value)
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return runtimemcp.RPCResponse{}, err
	}
	defer resp.Body.Close()
	var decoded runtimemcp.RPCResponse
	if resp.StatusCode >= 400 {
		return runtimemcp.RPCResponse{}, fmt.Errorf("mcp transport returned status %d", resp.StatusCode)
	}
	if req.Method == "notifications/initialized" {
		return decoded, nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, runtimemcp.MaxWireResponseBytes+1))
	if err != nil {
		return runtimemcp.RPCResponse{}, err
	}
	if len(raw) > runtimemcp.MaxWireResponseBytes {
		return runtimemcp.RPCResponse{}, fmt.Errorf("mcp startup response exceeds %d bytes", runtimemcp.MaxWireResponseBytes)
	}
	decoded, err = runtimemcp.DecodeRPCResponse(raw, req.ID)
	if err != nil {
		return runtimemcp.RPCResponse{}, err
	}
	if decoded.Error != nil {
		if data, ok := decoded.Error.Data.(map[string]any); ok {
			runtimeErr, decodeErr := runtimemcp.DecodeRuntimeErrorPayload(data["runtimeError"])
			if decodeErr == nil && runtimeErr != nil {
				if runtimeErr.Failure != nil {
					return runtimemcp.RPCResponse{}, failures.FromEnvelope(*runtimeErr.Failure)
				}
				if runtimeErr.Protocol != nil {
					return runtimemcp.RPCResponse{}, runtimemcp.NewProtocolError(runtimeErr.Protocol.Code, runtimeErr.Protocol.Operation, runtimeErr.Protocol.Message, runtimeErr.Protocol.Detail, nil)
				}
			}
		}
		return runtimemcp.RPCResponse{}, runtimemcp.NewProtocolError("mcp_rpc_error", strings.TrimSpace(req.Method), strings.TrimSpace(decoded.Error.Message), map[string]any{"jsonrpc_code": decoded.Error.Code}, nil)
	}
	return decoded, nil
}
