package llm

import (
	"strings"
	"testing"

	"swarm/internal/config"
	"swarm/internal/runtime/sessions"
)

func TestProviderContractsValidateShippedRuntimes(t *testing.T) {
	tests := []struct {
		name              string
		mode              string
		runtime           Runtime
		provider          string
		transport         ProviderTransport
		usageAccounting   BudgetUsageAccounting
		strictNativeTools bool
		startupProbe      bool
		caps              NativeToolCapabilities
	}{
		{
			name:            "anthropic api",
			mode:            "api",
			runtime:         NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil),
			provider:        "anthropic",
			transport:       ProviderTransportAPI,
			usageAccounting: BudgetUsageExact,
		},
		{
			name:              "claude cli",
			mode:              "cli_test",
			runtime:           NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, nil),
			provider:          "claude",
			transport:         ProviderTransportCLI,
			usageAccounting:   BudgetUsageEstimated,
			strictNativeTools: true,
			startupProbe:      true,
			caps: NativeToolCapabilities{
				Bash:      true,
				WebSearch: true,
				FileIO:    true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			contract, err := RequireProviderContract(tt.mode, tt.runtime)
			if err != nil {
				t.Fatalf("RequireProviderContract: %v", err)
			}
			if contract.Provider != tt.provider {
				t.Fatalf("provider = %q, want %q", contract.Provider, tt.provider)
			}
			if contract.Transport != tt.transport {
				t.Fatalf("transport = %q, want %q", contract.Transport, tt.transport)
			}
			if contract.Budget.UsageAccounting != tt.usageAccounting {
				t.Fatalf("usage accounting = %q, want %q", contract.Budget.UsageAccounting, tt.usageAccounting)
			}
			if contract.NativeTools.StrictProviderNativeSupport != tt.strictNativeTools {
				t.Fatalf("strict native tools = %v, want %v", contract.NativeTools.StrictProviderNativeSupport, tt.strictNativeTools)
			}
			if contract.NativeTools.StartupVisibleSurfaceProbe != tt.startupProbe {
				t.Fatalf("startup probe = %v, want %v", contract.NativeTools.StartupVisibleSurfaceProbe, tt.startupProbe)
			}
			if contract.NativeTools.Capabilities != tt.caps {
				t.Fatalf("capabilities = %#v, want %#v", contract.NativeTools.Capabilities, tt.caps)
			}
		})
	}
}

func TestRuntimeFactoryValidatesProviderContract(t *testing.T) {
	tests := []struct {
		mode     string
		provider string
	}{
		{mode: "api", provider: "anthropic"},
		{mode: "cli_test", provider: "claude"},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			runtime, err := RuntimeFactory{
				Cfg: &config.Config{
					LLM: config.LLMConfig{
						RuntimeMode: tt.mode,
					},
				},
			}.Build()
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			contract, err := RequireProviderContract(tt.mode, runtime)
			if err != nil {
				t.Fatalf("RequireProviderContract: %v", err)
			}
			if contract.Provider != tt.provider {
				t.Fatalf("provider = %q, want %q", contract.Provider, tt.provider)
			}
		})
	}
}

func TestProviderContractRejectsRuntimeWithoutContract(t *testing.T) {
	_, err := RequireProviderContract("api", NoopRuntime{})
	if err == nil || !strings.Contains(err.Error(), "does not expose provider contract") {
		t.Fatalf("RequireProviderContract error = %v, want missing contract", err)
	}
}

func TestProviderContractRejectsIncompleteContract(t *testing.T) {
	contract := ProviderContract{
		RuntimeMode: "api",
		Provider:    "anthropic",
		Transport:   ProviderTransportAPI,
	}
	if err := contract.Validate(); err == nil || !strings.Contains(err.Error(), "must validate provider input schemas") {
		t.Fatalf("Validate error = %v, want provider schema error", err)
	}
}

func TestProviderContractHelpersUseCanonicalContract(t *testing.T) {
	runtime := nativeContractRuntimeStub{
		contract: ProviderContract{
			RuntimeMode: "stub",
			Provider:    "stub",
			Transport:   ProviderTransportAPI,
			ToolSchema: ProviderToolSchemaContract{
				ValidatesInputSchemas: true,
				TranslatesTools:       true,
				ReturnsToolResults:    true,
			},
			SessionLifecycle: ProviderSessionLifecycleContract{
				StartsSessions:            true,
				ContinuesSessions:         true,
				SupportsConversationModes: true,
				ProviderSessionIDStrategy: "stub",
				RotatesSessions:           true,
			},
			Response: ProviderResponseContract{
				NormalizesMessages:   true,
				NormalizesToolCalls:  true,
				PreservesRawResponse: true,
			},
			NativeTools: ProviderNativeToolContract{
				Capabilities: NativeToolCapabilities{
					WebSearch: true,
				},
				StrictProviderNativeSupport: true,
			},
			Persistence: ProviderPersistenceContract{
				PersistsTurns:         true,
				PersistsTaskModeAudit: true,
			},
			Budget: ProviderBudgetContract{
				UsageAccounting: BudgetUsageEstimated,
			},
		},
	}
	if !RuntimeEnforcesProviderNativeTools(runtime) {
		t.Fatal("RuntimeEnforcesProviderNativeTools = false, want true")
	}
	if !NativeToolCapabilitiesForRuntime(runtime).WebSearch {
		t.Fatal("NativeToolCapabilitiesForRuntime().WebSearch = false, want true")
	}
}

type nativeContractRuntimeStub struct {
	NoopRuntime
	contract ProviderContract
}

func (s nativeContractRuntimeStub) ProviderContract() ProviderContract {
	return s.contract
}
