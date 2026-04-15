package tools

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
)

type nativeCapabilityRuntimeStub struct {
	llm.NoopRuntime
	caps   llm.NativeToolCapabilities
	strict bool
}

func (s nativeCapabilityRuntimeStub) NativeToolCapabilities() llm.NativeToolCapabilities {
	return s.caps
}

func (s nativeCapabilityRuntimeStub) EnforceProviderNativeToolSupport() bool {
	return s.strict
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

	_, err := ValidateNativeToolBootConfig(context.Background(), source, nil, nativeCapabilityRuntimeStub{strict: true})
	if err == nil || !strings.Contains(err.Error(), "native_tools.web_search enabled but runtime does not support provider-native capability") {
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
	})
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}

func TestValidateNativeToolBootConfig_NonCLIRuntimePreservesWebSearchFallbackValidation(t *testing.T) {
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

	warnings, err := ValidateNativeToolBootConfig(context.Background(), source, nil, nativeCapabilityRuntimeStub{})
	if err != nil {
		t.Fatalf("ValidateNativeToolBootConfig: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", warnings)
	}
}
