package tools

import (
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	workspace "github.com/division-sh/swarm/internal/runtime/workspace"
)

type nativeCapabilityRuntimeStub struct {
	llm.NoopRuntime
	caps   llm.NativeToolCapabilities
	strict bool
}

func (s nativeCapabilityRuntimeStub) ProviderContract() llm.ProviderContract {
	contract := llm.AnthropicAPIProviderContract()
	contract.NativeTools.Capabilities = s.caps
	contract.NativeTools.StrictProviderNativeSupport = s.strict
	contract.NativeTools.FallbackToolsAllowed = !s.strict
	return contract
}

type staticAgentRuntimeResolver struct {
	runtime llm.Runtime
}

func (r staticAgentRuntimeResolver) RuntimeForAgent(models.AgentConfig) (llm.Runtime, error) {
	return r.runtime, nil
}

type mappedAgentRuntimeResolver map[string]llm.Runtime

func (r mappedAgentRuntimeResolver) RuntimeForAgent(actor models.AgentConfig) (llm.Runtime, error) {
	runtime, ok := r[strings.TrimSpace(actor.ID)]
	if !ok {
		return nil, nil
	}
	return runtime, nil
}

type mockCapabilityRuntimeStub struct {
	llm.NoopRuntime
}

func (mockCapabilityRuntimeStub) ProviderContract() llm.ProviderContract {
	return llm.MockProviderContract()
}

func nativeCapabilityRuntimeSet(t testing.TB, runtime llm.Runtime) *llm.AgentRuntimeSet {
	t.Helper()
	profile, err := llmselection.ResolveActiveBackend(llmselection.BackendAnthropic)
	if err != nil {
		t.Fatalf("resolve anthropic profile: %v", err)
	}
	runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{}, runtime)
	if err != nil {
		t.Fatalf("build native capability runtime set: %v", err)
	}
	return runtimes
}

func TestExecutorNativeToolAdmissionUsesActorSelectedProviderContract(t *testing.T) {
	mockActor := models.AgentConfig{ID: "mock-agent", NativeTools: models.NativeToolConfig{FileIO: true}}
	liveActor := models.AgentConfig{ID: "live-agent", NativeTools: models.NativeToolConfig{FileIO: true}}
	exec := NewExecutorWithOptions(nil, nil, ExecutorOptions{
		ModelRuntimes: mappedAgentRuntimeResolver{
			mockActor.ID: mockCapabilityRuntimeStub{},
			liveActor.ID: nativeCapabilityRuntimeStub{},
		},
		WorkspaceResolver: relayWorkspaceResolverStub{
			target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
		},
	})

	mockOpts, err := exec.nativeToolAdmissionOptions(mockActor)
	if err != nil {
		t.Fatalf("mock nativeToolAdmissionOptions: %v", err)
	}
	liveOpts, err := exec.nativeToolAdmissionOptions(liveActor)
	if err != nil {
		t.Fatalf("live nativeToolAdmissionOptions: %v", err)
	}
	mockContract, _ := llm.ProviderContractForRuntime(mockOpts.Runtime)
	liveContract, _ := llm.ProviderContractForRuntime(liveOpts.Runtime)
	if mockContract.Provider != llmselection.ProviderMock || liveContract.Provider != llmselection.ProviderAnthropic {
		t.Fatalf("actor provider contracts = mock:%q live:%q", mockContract.Provider, liveContract.Provider)
	}
	if err := ValidateNativeToolAgentAdmission(unmanagedToolTestContext(), mockActor, mockOpts); err == nil || !strings.Contains(err.Error(), "does not allow native tool fallback") {
		t.Fatalf("mock native admission error = %v, want mock fallback denial", err)
	}
	if err := ValidateNativeToolAgentAdmission(unmanagedToolTestContext(), liveActor, liveOpts); err != nil {
		t.Fatalf("live native admission: %v", err)
	}
}

func TestValidateNativeToolBootConfig_FailsClosedWhenRuntimeLacksNativeCapability(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID: "agent-1",
				NativeTools: map[string]any{
					"web_search": true,
				},
			},
		},
	})

	_, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{strict: true}), nil)
	if err == nil || !strings.Contains(err.Error(), "selected runtime is strict provider-native and does not support provider-native capability") {
		t.Fatalf("expected unsupported native capability error, got %v", err)
	}
}

func TestValidateNativeToolBootConfig_CLINativeWebSearchDoesNotRequireFallbackProviderPolicy(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID: "agent-1",
				NativeTools: map[string]any{
					"web_search": true,
				},
			},
		},
	})

	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{
		caps:   llm.NativeToolCapabilities{WebSearch: true},
		strict: true,
	}), nil)
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}

func TestValidateNativeToolBootConfig_NonCLIRuntimeRequiresWebSearchFallbackCredential(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID: "agent-1",
				NativeTools: map[string]any{
					"web_search": true,
				},
			},
		},
		Policy: runtimecontracts.PolicyDocument{
			Values: map[string]runtimecontracts.PolicyValue{
				"web_search_provider": {
					Value: map[string]any{
						"provider":        "brave",
						"credentials_key": "brave_search_api_key",
					},
				},
			},
		},
	})

	emptyStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "empty-credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore empty: %v", err)
	}
	_, err = ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, emptyStore, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), nil)
	if err == nil || !strings.Contains(err.Error(), `missing credential "brave_search_api_key"`) {
		t.Fatalf("ValidateNativeToolBootConfig error = %v, want missing web_search credential", err)
	}

	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(unmanagedToolTestContext(), "brave_search_api_key", "secret"); err != nil {
		t.Fatalf("Set credential: %v", err)
	}
	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, store, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), nil)
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig with credential: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}

func TestValidateNativeToolBootConfig_FallbackFileIORequiresWorkspaceExecutionTarget(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				ID: "agent-1",
				NativeTools: map[string]any{
					"file_io": true,
				},
			},
		},
	})

	_, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), nil)
	if err == nil || !strings.Contains(err.Error(), "workspace resolver is not configured") {
		t.Fatalf("ValidateNativeToolBootConfig error = %v, want missing workspace resolver", err)
	}

	warnings, err := ValidateNativeToolBootConfig(unmanagedToolTestContext(), source, nil, nativeCapabilityRuntimeSet(t, nativeCapabilityRuntimeStub{}), relayWorkspaceResolverStub{
		target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
	})
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig with workspace: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}
