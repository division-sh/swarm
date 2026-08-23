package llm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/google/uuid"
)

const (
	evidenceAPIRequestDelivered = "api_request_delivered"
	evidenceProviderVisible     = "provider_visible"
	evidenceMCPListed           = "mcp_listed"
	evidenceMCPVisible          = "mcp_visible"
	evidenceMockInputDelivered  = "mock_python_input_delivered"
)

type providerTurnAuthorityKey struct{}

func withProviderTurnAuthority(ctx context.Context, session *Session) (context.Context, managedcapabilities.Authority, error) {
	if session == nil {
		return ctx, managedcapabilities.Authority{}, fmt.Errorf("managed capability provider turn requires session")
	}
	actor, ok := models.ActorFromContext(ctx)
	if !ok {
		return ctx, managedcapabilities.Authority{}, fmt.Errorf("managed capability provider turn requires actor context")
	}
	actorIdentity, err := actor.ConcreteIdentity()
	if err != nil {
		return ctx, managedcapabilities.Authority{}, fmt.Errorf("managed capability provider turn actor identity: %w", err)
	}
	sessionIdentity := session.MemoryIdentity.Agent.Normalize()
	sameSessionActor, sessionIdentityErr := runtimeagentidentity.Equal(actorIdentity, sessionIdentity)
	if sessionIdentityErr != nil || !sameSessionActor || strings.TrimSpace(session.AgentID) != actorIdentity.AgentID() {
		return ctx, managedcapabilities.Authority{}, fmt.Errorf("managed capability provider turn session actor mismatch")
	}
	if existing, ok := providerTurnAuthorityFromContext(ctx); ok {
		if existing.Kind != managedcapabilities.AuthorityProviderTurn ||
			existing.SessionID != strings.TrimSpace(session.ID) ||
			existing.TurnOrdinal != session.TurnCount+1 {
			return ctx, managedcapabilities.Authority{}, fmt.Errorf("managed capability provider turn authority does not match session")
		}
		return ctx, existing, nil
	}
	executionKind := managedcapabilities.ExecutionNormalAgent
	executionID := ""
	admission, admitted := managedexecution.FromContext(ctx)
	if authority, ok := runtimeeffects.AuthorityFromContext(ctx); ok {
		if !admitted {
			return ctx, managedcapabilities.Authority{}, fmt.Errorf("managed provider turn requires execution admission")
		}
		if authority.Kind == runtimeeffects.AuthoritySelectedContractFork {
			executionKind = managedcapabilities.ExecutionSelectedContractFork
			if !admission.AuthorizesSelected(authority.SelectedFork.ExecutionID, authority.SelectedFork.ForkRunID, authority.SelectedFork.Generation) {
				return ctx, managedcapabilities.Authority{}, fmt.Errorf("selected-fork managed execution admission mismatch")
			}
			executionID = strings.TrimSpace(authority.SelectedFork.ExecutionID)
		} else if authority.Kind == runtimeeffects.AuthorityNormalAgent {
			if !admission.AuthorizesNormal() {
				return ctx, managedcapabilities.Authority{}, fmt.Errorf("normal managed execution admission mismatch")
			}
			if equal, equalErr := runtimeagentidentity.Equal(authority.Normal.Identity, actorIdentity); equalErr != nil || !equal {
				return ctx, managedcapabilities.Authority{}, fmt.Errorf("normal managed provider turn actor identity mismatch")
			}
			executionID = strings.TrimSpace(admission.ExecutionAuthorityID)
		} else {
			return ctx, managedcapabilities.Authority{}, fmt.Errorf("managed provider turn rejects execution authority %q", authority.Kind)
		}
	} else if token, ok := runtimeeffects.LifecycleTokenFromContext(ctx); ok {
		if !admitted || !admission.AuthorizesNormal() {
			return ctx, managedcapabilities.Authority{}, fmt.Errorf("normal managed provider turn requires execution admission")
		}
		if equal, equalErr := runtimeagentidentity.Equal(token.Identity, actorIdentity); equalErr != nil || !equal {
			return ctx, managedcapabilities.Authority{}, fmt.Errorf("normal managed provider turn lifecycle actor mismatch")
		}
		executionID = strings.TrimSpace(admission.ExecutionAuthorityID)
	} else {
		return ctx, managedcapabilities.Authority{}, fmt.Errorf("managed provider turn requires lifecycle or selected-fork authority")
	}
	if executionID == "" {
		return ctx, managedcapabilities.Authority{}, fmt.Errorf("managed capability provider turn requires execution authority")
	}
	runID := strings.TrimSpace(runtimecorrelation.RunIDFromContext(ctx))
	if session.Memory.Enabled {
		runID = strings.TrimSpace(session.MemoryIdentity.RunID)
	}
	turnOrdinal := session.TurnCount + 1
	authorityID, err := providerTurnAuthorityID(ctx, actorIdentity, executionKind, executionID, runID, session.ID, turnOrdinal)
	if err != nil {
		return ctx, managedcapabilities.Authority{}, err
	}
	authority := managedcapabilities.Authority{
		Kind:                 managedcapabilities.AuthorityProviderTurn,
		ID:                   authorityID,
		ExecutionKind:        executionKind,
		ExecutionAuthorityID: executionID,
		RunID:                runID,
		SessionID:            strings.TrimSpace(session.ID),
		TurnOrdinal:          turnOrdinal,
	}
	if err := authority.Validate(); err != nil {
		return ctx, managedcapabilities.Authority{}, err
	}
	return context.WithValue(ctx, providerTurnAuthorityKey{}, authority), authority, nil
}

func providerTurnAuthorityID(ctx context.Context, actorIdentity runtimeagentidentity.Identity, executionKind managedcapabilities.ExecutionKind, executionID, runID, sessionID string, turnOrdinal int) (string, error) {
	logicalIdentity, ok := runtimeeffects.LogicalOperationIdentityFromContext(ctx)
	if !ok {
		return "", fmt.Errorf("managed capability provider turn requires logical operation identity")
	}
	actorFingerprint, err := actorIdentity.Fingerprint()
	if err != nil {
		return "", fmt.Errorf("managed capability provider turn actor fingerprint: %w", err)
	}
	seed := strings.Join([]string{
		"provider-turn-authority-v1",
		string(executionKind),
		strings.TrimSpace(executionID),
		actorFingerprint,
		strings.TrimSpace(runID),
		strings.TrimSpace(sessionID),
		fmt.Sprintf("%d", turnOrdinal),
		logicalIdentity,
	}, "\x00")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed)).String(), nil
}

func providerTurnAuthorityFromContext(ctx context.Context) (managedcapabilities.Authority, bool) {
	if ctx == nil {
		return managedcapabilities.Authority{}, false
	}
	authority, ok := ctx.Value(providerTurnAuthorityKey{}).(managedcapabilities.Authority)
	return authority, ok
}

func managedCapabilityPlanForTurn(ctx context.Context, runtime Runtime, session *Session, tools []ToolDefinition, capabilities toolcapabilities.Set) (managedcapabilities.Surface, error) {
	actor, ok := models.ActorFromContext(ctx)
	if !ok || strings.TrimSpace(actor.ID) == "" {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface requires actor context")
	}
	actorIdentity, err := actor.ConcreteIdentity()
	if err != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface actor identity: %w", err)
	}
	if session == nil || strings.TrimSpace(session.AgentID) != actorIdentity.AgentID() {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface requires exact session actor")
	}
	if equal, equalErr := runtimeagentidentity.Equal(session.MemoryIdentity.Agent, actorIdentity); equalErr != nil || !equal {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface session identity mismatch")
	}
	authority, ok := providerTurnAuthorityFromContext(ctx)
	if !ok {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface requires provider-turn authority")
	}
	return managedCapabilityPlan(ctx, runtime, "", tools, capabilities, authority)
}

func ManagedCapabilitySurfaceForStartup(ctx context.Context, runtime Runtime, tools []ToolDefinition, capabilities toolcapabilities.Set, authority managedcapabilities.Authority) (managedcapabilities.Surface, error) {
	if authority.Kind != managedcapabilities.AuthorityStartupProbe {
		return managedcapabilities.Surface{}, fmt.Errorf("startup capability surface requires startup-probe authority")
	}
	return managedCapabilityPlan(ctx, runtime, "startup_probe", tools, capabilities, authority)
}

func managedCapabilityPlan(ctx context.Context, runtime Runtime, runtimeMode string, tools []ToolDefinition, capabilities toolcapabilities.Set, authority managedcapabilities.Authority) (managedcapabilities.Surface, error) {
	actor, ok := models.ActorFromContext(ctx)
	if !ok || strings.TrimSpace(actor.ID) == "" {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface requires actor context")
	}
	actorIdentity, err := actor.ConcreteIdentity()
	if err != nil {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface actor identity: %w", err)
	}
	contract, ok := ProviderContractForRuntime(runtime)
	if !ok {
		return managedcapabilities.Surface{}, fmt.Errorf("managed capability surface requires provider contract")
	}
	if strings.TrimSpace(runtimeMode) == "" {
		runtimeMode = contract.RuntimeMode
	}
	nativeNames := nativeCapabilityNames(actor)
	providerNative, err := managedProviderNativeNames(contract, nativeNames)
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	planned := make([]managedcapabilities.PlannedTool, 0, len(tools)+4)
	seen := map[string]struct{}{}
	for _, def := range tools {
		name := toolidentity.CanonicalName(def.Name)
		if name == "" {
			continue
		}
		if _, providerOwned := providerNative[name]; providerOwned {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			return managedcapabilities.Surface{}, fmt.Errorf("managed capability tool %s is duplicated", name)
		}
		seen[name] = struct{}{}
		capability, found := capabilities.Capability(name)
		if !found {
			capability = toolcapabilities.Capability{Name: name, Visible: false, Callable: false, DenialReason: "planned_capability_missing"}
		}
		planned = append(planned, managedcapabilities.PlannedTool{
			Name:           name,
			DefinitionHash: ToolDefinitionIdentity(def),
			Capability:     capability,
			Bindings:       managedConcreteCapabilityBindings(contract, name),
		})
	}
	for _, nativeName := range nativeNames {
		if _, duplicate := seen[nativeName]; duplicate {
			continue
		}
		if _, providerOwned := providerNative[nativeName]; !providerOwned {
			return managedcapabilities.Surface{}, fmt.Errorf("authored native capability %s has no selected provider-native support or concrete fallback definition", nativeName)
		}
		bindings := managedProviderNativeBindings(actor, nativeName)
		if len(bindings) == 0 {
			return managedcapabilities.Surface{}, fmt.Errorf("provider-native capability %s has no exact provider binding", nativeName)
		}
		seen[nativeName] = struct{}{}
		planned = append(planned, managedcapabilities.PlannedTool{
			Name:           nativeName,
			DefinitionHash: hashJSON(map[string]any{"canonical_name": nativeName, "provider_contract": contract}),
			Capability: toolcapabilities.Capability{
				Name:               nativeName,
				Visible:            true,
				Callable:           true,
				AuthorizationClass: "provider_native",
			},
			Bindings: bindings,
		})
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity:    actorIdentity,
		RuntimeMode:      strings.TrimSpace(runtimeMode),
		Provider:         contract.Provider,
		Transport:        string(contract.Transport),
		ProviderContract: hashJSON(contract),
		Authority:        authority,
		Tools:            planned,
		CreatedAt:        time.Now().UTC(),
	})
	if err != nil {
		return managedcapabilities.Surface{}, err
	}
	return surface, nil
}

// ConcreteManagedToolDefinitions removes provider-owned native capabilities
// before the platform executor is asked to classify concrete definitions.
func ConcreteManagedToolDefinitions(runtime Runtime, actor models.AgentConfig, definitions []ToolDefinition) ([]ToolDefinition, error) {
	contract, ok := ProviderContractForRuntime(runtime)
	if !ok {
		return nil, fmt.Errorf("managed capability surface requires provider contract")
	}
	providerNative, err := managedProviderNativeNames(contract, nativeCapabilityNames(actor))
	if err != nil {
		return nil, err
	}
	concrete := make([]ToolDefinition, 0, len(definitions))
	for _, definition := range definitions {
		if _, providerOwned := providerNative[toolidentity.CanonicalName(definition.Name)]; providerOwned {
			continue
		}
		concrete = append(concrete, definition)
	}
	return concrete, nil
}

func managedProviderNativeNames(contract ProviderContract, nativeNames []string) (map[string]struct{}, error) {
	providerNative := make(map[string]struct{}, len(nativeNames))
	for _, name := range nativeNames {
		if providerNativeCapabilitySupported(contract.NativeTools.Capabilities, name) {
			providerNative[name] = struct{}{}
			continue
		}
		if !contract.NativeTools.FallbackToolsAllowed {
			return nil, fmt.Errorf("provider contract %s does not support authored native capability %s", contract.RuntimeMode, name)
		}
	}
	return providerNative, nil
}

func managedConcreteCapabilityBindings(contract ProviderContract, name string) []managedcapabilities.DeliveryBinding {
	name = toolidentity.CanonicalName(name)
	if contract.Transport == ProviderTransportAPI {
		return []managedcapabilities.DeliveryBinding{{Kind: managedcapabilities.BindingAPIDefinition, ExactName: name, RequiredEvidenceKind: evidenceAPIRequestDelivered}}
	}
	if contract.Transport == ProviderTransportInProcess {
		return []managedcapabilities.DeliveryBinding{{Kind: managedcapabilities.BindingLocalRuntime, ExactName: name, RequiredEvidenceKind: evidenceMockInputDelivered}}
	}
	exactName := toolidentity.RuntimeToolsMCPPrefix + name
	return []managedcapabilities.DeliveryBinding{
		{Kind: managedcapabilities.BindingMCPTool, ExactName: exactName, RequiredEvidenceKind: evidenceMCPListed},
		{Kind: managedcapabilities.BindingMCPProvider, ExactName: exactName, RequiredEvidenceKind: evidenceMCPVisible},
	}
}

func managedProviderNativeBindings(actor models.AgentConfig, name string) []managedcapabilities.DeliveryBinding {
	builtins := providerBuiltinNamesForCanonical(actor, name)
	out := make([]managedcapabilities.DeliveryBinding, 0, len(builtins))
	for _, builtin := range builtins {
		out = append(out, managedcapabilities.DeliveryBinding{
			Kind:                 managedcapabilities.BindingProviderBuiltin,
			ExactName:            builtin,
			RequiredEvidenceKind: evidenceProviderVisible,
		})
	}
	return out
}

func providerNativeCapabilitySupported(capabilities NativeToolCapabilities, name string) bool {
	switch toolidentity.CanonicalName(name) {
	case "bash":
		return capabilities.Bash
	case "web_search":
		return capabilities.WebSearch
	case "read_file", "write_file":
		return capabilities.FileIO
	default:
		return false
	}
}

func providerBuiltinNamesForCanonical(actor models.AgentConfig, name string) []string {
	switch toolidentity.CanonicalName(name) {
	case "bash":
		if actor.NativeTools.Bash {
			return []string{"Bash"}
		}
	case "web_search":
		if actor.NativeTools.WebSearch {
			return []string{"WebFetch", "WebSearch"}
		}
	case "read_file":
		if actor.NativeTools.FileIO {
			return []string{"Read"}
		}
	case "write_file":
		if actor.NativeTools.FileIO {
			return []string{"Edit", "Write"}
		}
	}
	return nil
}

func nativeCapabilityNames(actor models.AgentConfig) []string {
	var names []string
	if actor.NativeTools.Bash {
		names = append(names, "bash")
	}
	if actor.NativeTools.WebSearch {
		names = append(names, "web_search")
	}
	if actor.NativeTools.FileIO {
		names = append(names, "read_file", "write_file")
	}
	slices.Sort(names)
	return names
}

func observeCLIResponse(surface managedcapabilities.Surface, response *Response) (managedcapabilities.Surface, error) {
	if response == nil {
		return surface, nil
	}
	providerVisible := exactCLIProviderVisibleTools(response)
	mcpVisible := exactMCPVisibleTools(response)
	plannedProvider := surface.PlannedBindingNames(managedcapabilities.BindingProviderBuiltin)
	plannedMCP := surface.PlannedBindingNames(managedcapabilities.BindingMCPProvider)
	var mismatches []managedcapabilities.DeliveryMismatch
	for _, name := range providerVisible {
		if !slices.Contains(plannedProvider, name) {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingProviderBuiltin, ExactName: name, Kind: "unplanned_provider_visible", Detail: "provider reported a builtin outside the planned callable surface"})
		}
	}
	for _, name := range mcpVisible {
		if !slices.Contains(plannedMCP, name) {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingMCPProvider, ExactName: name, Kind: "unplanned_mcp_visible", Detail: "provider reported an MCP tool outside the planned callable surface"})
		}
	}
	if len(mismatches) > 0 {
		var err error
		surface, err = surface.ObserveMismatch(mismatches...)
		if err != nil {
			return managedcapabilities.Surface{}, err
		}
	}
	var evidence []managedcapabilities.DeliveryEvidence
	for _, tool := range surface.Tools {
		if !tool.Capability.Visible || !tool.Capability.Callable {
			continue
		}
		for _, binding := range tool.Bindings {
			switch binding.Kind {
			case managedcapabilities.BindingProviderBuiltin:
				confirmed := slices.Contains(providerVisible, binding.ExactName)
				status := managedcapabilities.EvidenceUnavailable
				if confirmed {
					status = managedcapabilities.EvidenceConfirmed
				}
				evidence = append(evidence, managedcapabilities.DeliveryEvidence{BindingKind: binding.Kind, ExactName: binding.ExactName, Kind: evidenceProviderVisible, Status: status})
			case managedcapabilities.BindingMCPProvider:
				confirmed := slices.Contains(mcpVisible, binding.ExactName)
				status := managedcapabilities.EvidenceUnavailable
				if confirmed && mcpRuntimeServerConnected(response.MCPServers) {
					status = managedcapabilities.EvidenceConfirmed
				}
				evidence = append(evidence, managedcapabilities.DeliveryEvidence{BindingKind: binding.Kind, ExactName: binding.ExactName, Kind: evidenceMCPVisible, Status: status})
			}
		}
	}
	if len(evidence) == 0 {
		return surface, nil
	}
	return surface.Observe(evidence...)
}

func ObserveCLIResponseCapabilitySurface(surface managedcapabilities.Surface, response *Response) (managedcapabilities.Surface, error) {
	return observeCLIResponse(surface, response)
}

func ValidateCLIProviderCapabilitySurface(surface managedcapabilities.Surface, response *Response) error {
	if surface.HasMismatch() {
		return fmt.Errorf("provider-visible capability surface contains typed delivery mismatch")
	}
	expected := surface.PlannedBindingNames(managedcapabilities.BindingProviderBuiltin)
	actual := exactCLIProviderVisibleTools(response)
	if !slices.Equal(expected, actual) {
		return fmt.Errorf("provider-visible capability mismatch: expected [%s], got [%s]", strings.Join(expected, ", "), strings.Join(actual, ", "))
	}
	return nil
}

func ObserveAPIRequestCapabilitySurface(surface managedcapabilities.Surface, deliveredTools []ToolDefinition) (managedcapabilities.Surface, error) {
	planned := map[string]string{}
	for _, tool := range surface.Tools {
		if !tool.Capability.Visible || !tool.Capability.Callable {
			continue
		}
		for _, binding := range tool.Bindings {
			if binding.Kind == managedcapabilities.BindingAPIDefinition {
				planned[binding.ExactName] = tool.DefinitionHash
			}
		}
	}
	actual := map[string]string{}
	var mismatches []managedcapabilities.DeliveryMismatch
	for _, def := range deliveredTools {
		name := toolidentity.CanonicalName(def.Name)
		if name == "" {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: "<unnamed>", Kind: "unnamed_api_definition"})
			continue
		}
		if _, duplicate := actual[name]; duplicate {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: name, Kind: "duplicate_api_definition"})
			continue
		}
		actual[name] = ToolDefinitionIdentity(def)
	}
	for name := range actual {
		if _, ok := planned[name]; !ok {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: name, Kind: "unplanned_api_definition"})
		}
	}
	for name, definitionHash := range planned {
		actualHash, ok := actual[name]
		if !ok {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: name, Kind: "missing_api_definition"})
		} else if actualHash != definitionHash {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingAPIDefinition, ExactName: name, Kind: "api_definition_identity_mismatch"})
		}
	}
	if len(mismatches) > 0 {
		observed, err := surface.ObserveMismatch(mismatches...)
		if err != nil {
			return managedcapabilities.Surface{}, err
		}
		return observed, fmt.Errorf("API request capability definition mismatch: planned %d, delivered %d", len(planned), len(actual))
	}
	return observeAllBindings(surface, managedcapabilities.BindingAPIDefinition, evidenceAPIRequestDelivered, managedcapabilities.EvidenceConfirmed, "exact provider request definition")
}

func ObserveMockRuntimeCapabilitySurface(surface managedcapabilities.Surface, deliveredTools []ToolDefinition, moduleDigest string) (managedcapabilities.Surface, error) {
	moduleDigest = strings.TrimSpace(moduleDigest)
	if moduleDigest == "" {
		return surface, fmt.Errorf("mock runtime capability observation requires module digest")
	}
	planned := map[string]string{}
	for _, tool := range surface.Tools {
		if !tool.Capability.Visible || !tool.Capability.Callable {
			continue
		}
		for _, binding := range tool.Bindings {
			if binding.Kind == managedcapabilities.BindingLocalRuntime {
				planned[binding.ExactName] = tool.DefinitionHash
			}
		}
	}
	actual := map[string]string{}
	var mismatches []managedcapabilities.DeliveryMismatch
	for _, def := range deliveredTools {
		name := toolidentity.CanonicalName(def.Name)
		if name == "" {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingLocalRuntime, ExactName: "<unnamed>", Kind: "unnamed_local_runtime_definition"})
			continue
		}
		if _, duplicate := actual[name]; duplicate {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingLocalRuntime, ExactName: name, Kind: "duplicate_local_runtime_definition"})
			continue
		}
		actual[name] = ToolDefinitionIdentity(def)
	}
	for name := range actual {
		if _, ok := planned[name]; !ok {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingLocalRuntime, ExactName: name, Kind: "unplanned_local_runtime_definition"})
		}
	}
	for name, definitionHash := range planned {
		actualHash, ok := actual[name]
		if !ok {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingLocalRuntime, ExactName: name, Kind: "missing_local_runtime_definition"})
		} else if actualHash != definitionHash {
			mismatches = append(mismatches, managedcapabilities.DeliveryMismatch{BindingKind: managedcapabilities.BindingLocalRuntime, ExactName: name, Kind: "local_runtime_definition_identity_mismatch"})
		}
	}
	if len(mismatches) > 0 {
		observed, err := surface.ObserveMismatch(mismatches...)
		if err != nil {
			return managedcapabilities.Surface{}, err
		}
		return observed, fmt.Errorf("mock runtime capability definition mismatch: planned %d, delivered %d", len(planned), len(actual))
	}
	return observeAllBindings(surface, managedcapabilities.BindingLocalRuntime, evidenceMockInputDelivered, managedcapabilities.EvidenceConfirmed, "mock_python module_digest="+moduleDigest)
}

func withObservedAPIRequestCapabilitySurface(ctx context.Context, deliveredTools []ToolDefinition) (context.Context, managedcapabilities.Surface, error) {
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		if managedAgentExecutionContext(ctx) {
			return ctx, managedcapabilities.Surface{}, fmt.Errorf("API request requires exact managed capability surface")
		}
		return ctx, managedcapabilities.Surface{}, nil
	}
	observed, err := ObserveAPIRequestCapabilitySurface(surface, deliveredTools)
	if err != nil {
		if strings.TrimSpace(observed.ID) != "" {
			ctx = managedcapabilities.WithContext(ctx, observed)
		}
		return ctx, observed, err
	}
	return managedcapabilities.WithContext(ctx, observed), observed, nil
}

func withObservedMockRuntimeCapabilitySurface(ctx context.Context, deliveredTools []ToolDefinition, moduleDigest string) (context.Context, managedcapabilities.Surface, error) {
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		if managedAgentExecutionContext(ctx) {
			return ctx, managedcapabilities.Surface{}, fmt.Errorf("mock runtime request requires exact managed capability surface")
		}
		return ctx, managedcapabilities.Surface{}, nil
	}
	observed, err := ObserveMockRuntimeCapabilitySurface(surface, deliveredTools, moduleDigest)
	if err != nil {
		if strings.TrimSpace(observed.ID) != "" {
			ctx = managedcapabilities.WithContext(ctx, observed)
		}
		return ctx, observed, err
	}
	return managedcapabilities.WithContext(ctx, observed), observed, nil
}

func exactCLIProviderVisibleTools(response *Response) []string {
	if response == nil {
		return nil
	}
	raw := response.ProviderVisibleTools
	if len(raw) == 0 {
		raw = response.VisibleTools
	}
	var out []string
	for _, name := range raw {
		name = strings.TrimSpace(name)
		if name == "" || isCLIControlToolName(name) || strings.HasPrefix(name, toolidentity.RuntimeToolsMCPPrefix) {
			continue
		}
		out = append(out, name)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func exactMCPVisibleTools(response *Response) []string {
	if response == nil {
		return nil
	}
	var out []string
	for _, name := range response.MCPVisibleTools {
		name = strings.TrimSpace(name)
		if strings.HasPrefix(name, toolidentity.RuntimeToolsMCPPrefix) {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func mcpRuntimeServerConnected(servers map[string]string) bool {
	for name, status := range servers {
		if strings.TrimSpace(name) == "runtime-tools" && strings.EqualFold(strings.TrimSpace(status), "connected") {
			return true
		}
	}
	return false
}

func observeAllBindings(surface managedcapabilities.Surface, kind managedcapabilities.BindingKind, evidenceKind string, status managedcapabilities.EvidenceStatus, detail string) (managedcapabilities.Surface, error) {
	var evidence []managedcapabilities.DeliveryEvidence
	for _, tool := range surface.Tools {
		for _, binding := range tool.Bindings {
			if binding.Kind == kind {
				evidence = append(evidence, managedcapabilities.DeliveryEvidence{BindingKind: binding.Kind, ExactName: binding.ExactName, Kind: evidenceKind, Status: status, Detail: detail})
			}
		}
	}
	if len(evidence) == 0 {
		return surface, nil
	}
	return surface.Observe(evidence...)
}

// ToolDefinitionIdentity is the canonical identity of a definition delivered to a provider transport.
func ToolDefinitionIdentity(def ToolDefinition) string {
	schema := def.Schema
	if schema == nil {
		schema = map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return hashJSON(struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		Schema      any    `json:"schema,omitempty"`
	}{toolidentity.CanonicalName(def.Name), DeliveredToolDescription(def), schema})
}

func hashJSON(value any) string {
	raw, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func capabilitySurfaceForResponse(response *Response) (managedcapabilities.Surface, bool) {
	if response == nil || response.CapabilitySurface == nil {
		return managedcapabilities.Surface{}, false
	}
	return response.CapabilitySurface.Clone(), true
}

func effectiveCapabilitySetForResponse(response *Response) (toolcapabilities.Set, bool) {
	surface, ok := capabilitySurfaceForResponse(response)
	if !ok {
		return toolcapabilities.Set{}, false
	}
	return surface.CapabilitySet(), true
}
