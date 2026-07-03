package tools

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
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
	contract.RuntimeMode = "stub"
	contract.Provider = "stub"
	contract.NativeTools.Capabilities = s.caps
	contract.NativeTools.StrictProviderNativeSupport = s.strict
	contract.NativeTools.FallbackToolsAllowed = !s.strict
	return contract
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

	_, err := ValidateNativeToolBootConfig(context.Background(), source, nil, nativeCapabilityRuntimeStub{strict: true}, nil)
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

	warnings, err := ValidateNativeToolBootConfig(context.Background(), source, nil, nativeCapabilityRuntimeStub{
		caps:   llm.NativeToolCapabilities{WebSearch: true},
		strict: true,
	}, nil)
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
	_, err = ValidateNativeToolBootConfig(context.Background(), source, emptyStore, nativeCapabilityRuntimeStub{}, nil)
	if err == nil || !strings.Contains(err.Error(), `missing credential "brave_search_api_key"`) {
		t.Fatalf("ValidateNativeToolBootConfig error = %v, want missing web_search credential", err)
	}

	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(context.Background(), "brave_search_api_key", "secret"); err != nil {
		t.Fatalf("Set credential: %v", err)
	}
	warnings, err := ValidateNativeToolBootConfig(context.Background(), source, store, nativeCapabilityRuntimeStub{}, nil)
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

	_, err := ValidateNativeToolBootConfig(context.Background(), source, nil, nativeCapabilityRuntimeStub{}, nil)
	if err == nil || !strings.Contains(err.Error(), "workspace resolver is not configured") {
		t.Fatalf("ValidateNativeToolBootConfig error = %v, want missing workspace resolver", err)
	}

	warnings, err := ValidateNativeToolBootConfig(context.Background(), source, nil, nativeCapabilityRuntimeStub{}, relayWorkspaceResolverStub{
		target: &workspace.Target{Backend: workspace.BackendHost, Workdir: t.TempDir()},
	})
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig with workspace: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}
