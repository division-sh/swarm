package runtime

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestValidateSelectedBackendCredentialForDeclaredAgents_WaivesFullyMockedBundle(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "anthropic"
	t.Setenv("ANTHROPIC_API_KEY", "")

	err := validateSelectedBackendCredentialForDeclaredAgents(testAuthorActivityContext(context.Background()), cfg, RuntimeOptions{
		ProviderCredentials: testProviderCredentialStore(t, "ANTHROPIC_API_KEY", ""),
	}, fullyMockedRuntimeAgentMemorySource(t))
	if err != nil {
		t.Fatalf("fully-mocked bundle must waive the boot credential requirement, got %v", err)
	}
}

func TestValidateSelectedBackendCredentialForDeclaredAgents_NamesUnmockedAgents(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "anthropic"
	t.Setenv("ANTHROPIC_API_KEY", "")

	err := validateSelectedBackendCredentialForDeclaredAgents(testAuthorActivityContext(context.Background()), cfg, RuntimeOptions{
		ProviderCredentials: testProviderCredentialStore(t, "ANTHROPIC_API_KEY", ""),
	}, partiallyMockedRuntimeAgentMemorySource(t, 1))
	if err == nil {
		t.Fatal("partially-mocked bundle must still require the credential")
	}
	failure, ok := runtimefailures.As(err)
	if !ok || failure.Failure.Class != runtimefailures.ClassAuthenticationNeeded || failure.Failure.Detail.Code != "provider_credential_missing" {
		t.Fatalf("error = %v, want typed provider_credential_missing preserved", err)
	}
	for _, want := range []string{"declared agents are mocked", "ANTHROPIC_API_KEY"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error missing %q: %v", want, err)
		}
	}
}

func TestValidateSelectedBackendCredentialForDeclaredAgents_UnmockedAgentWithCredentialPasses(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "anthropic"
	t.Setenv("ANTHROPIC_API_KEY", "")

	err := validateSelectedBackendCredentialForDeclaredAgents(testAuthorActivityContext(context.Background()), cfg, RuntimeOptions{
		ProviderCredentials: testProviderCredentialStore(t, "ANTHROPIC_API_KEY", "sk-test"),
	}, partiallyMockedRuntimeAgentMemorySource(t, 1))
	if err != nil {
		t.Fatalf("unmocked agent with credential present must pass, got %v", err)
	}
}

func TestValidateSelectedBackendCredentialForActiveAgents_MockWaiverInvariant(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "anthropic"
	t.Setenv("ANTHROPIC_API_KEY", "")
	manager := runtimemanager.NewAgentManagerWithOptions(nil, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:  executionposture.Live,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err := registerRuntimeTestAgent(manager, runtimeTestAgentConfig(t, runtimeactors.AgentConfig{
		ExecutionMode: "live", ID: "recovered-agent", Role: "recovered",
		Model: "regular",
	})); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	err := validateSelectedBackendCredentialForActiveAgents(testAuthorActivityContext(context.Background()), cfg, RuntimeOptions{}, fullyMockedRuntimeAgentMemorySource(t), manager)
	if err == nil || !strings.Contains(err.Error(), "mock waiver invariant violation") || !strings.Contains(err.Error(), "recovered-agent") {
		t.Fatalf("error = %v, want mock waiver invariant violation naming the active agent", err)
	}
}

func TestValidateSelectedBackendCredentialForActiveAgents_MockWaiverAcceptsNoDivergentActiveAgents(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "anthropic"
	t.Setenv("ANTHROPIC_API_KEY", "")
	manager := runtimemanager.NewAgentManagerWithOptions(nil, nil, runtimemanager.AgentManagerOptions{
		ExecutionPosture:  executionposture.Live,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})

	err := validateSelectedBackendCredentialForActiveAgents(testAuthorActivityContext(context.Background()), cfg, RuntimeOptions{}, fullyMockedRuntimeAgentMemorySource(t), manager)
	if err != nil {
		t.Fatalf("fully-mocked source with bundle-derived active agents must waive, got %v", err)
	}
}

func TestValidateClaudeStartupConfig_WaivesFullyMockedBundle(t *testing.T) {
	cfg := &config.Config{}
	cfg.LLM.Backend = "claude_cli"
	t.Setenv("SWARM_CLAUDE_USE_MCP", "")
	t.Setenv("SWARM_TOOL_GATEWAY_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", "")
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")
	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")

	err := validateClaudeStartupConfig(testAuthorActivityContext(context.Background()), cfg, RuntimeOptions{}, fullyMockedRuntimeAgentMemorySource(t))
	if err != nil {
		t.Fatalf("fully-mocked bundle must waive the claude startup chain, got %v", err)
	}
}

func fullyMockedRuntimeAgentMemorySource(t *testing.T) semanticview.Source {
	t.Helper()
	return mockAgentMemorySource(t, 0)
}

func partiallyMockedRuntimeAgentMemorySource(t *testing.T, unmockedCount int) semanticview.Source {
	t.Helper()
	return mockAgentMemorySource(t, unmockedCount)
}

// mockAgentMemorySource loads the package-backed agent memory fixture and
// configures a mock performance on every declared agent except the first
// (sorted) unmockedCount entries, which stay live so the partial-mock paths
// are exercised against real fixture agent ids.
func mockAgentMemorySource(t *testing.T, unmockedCount int) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := canonicalrouting.CopyRuntimeAgentMemory(t, canonicalrouting.RuntimeAgentMemoryDirectFlow)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	type declaration struct {
		label   string
		localID string
		entries map[string]runtimecontracts.AgentRegistryEntry
	}
	declarations := []declaration{}
	for _, view := range bundle.FlowViews() {
		for id := range view.Agents {
			declarations = append(declarations, declaration{label: "flow " + view.Paths.FlowPath + " agent " + id, localID: id, entries: view.Agents})
		}
	}
	if len(declarations) == 0 {
		for id := range bundle.Agents {
			declarations = append(declarations, declaration{label: "agent " + id, localID: id, entries: bundle.Agents})
		}
	}
	sort.Slice(declarations, func(i, j int) bool { return declarations[i].label < declarations[j].label })
	if len(declarations) == 0 {
		t.Fatal("agent memory fixture unexpectedly declares zero agents")
	}
	if unmockedCount > len(declarations) {
		unmockedCount = len(declarations)
	}
	unmockedLocalIDs := map[string]struct{}{}
	for _, declaration := range declarations[:unmockedCount] {
		unmockedLocalIDs[declaration.localID] = struct{}{}
	}
	for _, declaration := range declarations {
		// Loader project and flow views can project the same logical agent.
		// Mutate every projection consistently so a later view cannot remock it.
		if _, skip := unmockedLocalIDs[declaration.localID]; skip {
			continue
		}
		entry := declaration.entries[declaration.localID]
		entry.Mock = mockperformance.Performance{
			Kind:   "python",
			Module: "mocks/" + declaration.localID + ".py",
			Source: []byte("def handle(input):\n    return {}\n"),
			Digest: "sha256:" + strings.Repeat("a", 64),
		}
		declaration.entries[declaration.localID] = entry
	}
	for id, entry := range bundle.Agents {
		if _, skip := unmockedLocalIDs[id]; skip {
			continue
		}
		entry.Mock = mockperformance.Performance{
			Kind:   "python",
			Module: "mocks/" + id + ".py",
			Source: []byte("def handle(input):\n    return {}\n"),
			Digest: "sha256:" + strings.Repeat("a", 64),
		}
		bundle.Agents[id] = entry
	}
	return semanticview.Wrap(bundle)
}
