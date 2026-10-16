package effectpersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/google/uuid"
)

func TestCompletionResumeReadOnlyRequiresFrozenHTTPToolSurface(t *testing.T) {
	for _, backend := range []string{llmselection.BackendAnthropic, llmselection.BackendOpenAICompatible, llmselection.BackendOpenAIResponses} {
		for _, shape := range []string{"no_tools", "platform_tool", "provider_builtin", "hidden_provider_builtin", "native_authorization", "provider_mcp"} {
			t.Run(backend+"/"+shape, func(t *testing.T) {
				plan, request, adapter := completionResumeTestPlan(t, backend)
				if shape != "no_tools" {
					tool := managedcapabilities.PlannedTool{
						Name: "read_file", DefinitionHash: "frozen-tool-definition",
						Capability: toolcapabilities.Capability{Name: "read_file", Visible: true, Callable: true},
						Bindings: []managedcapabilities.DeliveryBinding{{Kind: managedcapabilities.BindingAPIDefinition, ExactName: "read_file", RequiredEvidenceKind: "api_request_delivered"}},
					}
					switch shape {
					case "provider_builtin", "hidden_provider_builtin":
						tool.Bindings[0] = managedcapabilities.DeliveryBinding{Kind: managedcapabilities.BindingProviderBuiltin, ExactName: "Read", RequiredEvidenceKind: "provider_visible"}
						if shape == "hidden_provider_builtin" {
							tool.Capability.Visible, tool.Capability.Callable = false, false
						}
					case "native_authorization":
						tool.Capability.AuthorizationClass = "provider_native"
					case "provider_mcp":
						tool.Bindings[0] = managedcapabilities.DeliveryBinding{Kind: managedcapabilities.BindingMCPProvider, ExactName: "provider_tool", RequiredEvidenceKind: "mcp_visible"}
					}
					plan.Tools = []managedcapabilities.PlannedTool{tool}
				}
				surface, err := managedcapabilities.New(plan)
				if err != nil {
					t.Fatal(err)
				}
				before := surface.Clone()
				readOnly, err := completionResumeSurfaceAllowsHTTP(adapter, request, surface)
				want := shape == "no_tools" || shape == "platform_tool"
				if err != nil || readOnly != want {
					t.Fatalf("frozen surface read-only=%t error=%v, want %t", readOnly, err, want)
				}
				if !reflect.DeepEqual(before, surface) {
					t.Fatal("read-only classification changed the admitted surface")
				}
			})
		}
	}
}

func TestCompletionResumeSurfaceRejectsUnsupportedOrContradictoryEvidence(t *testing.T) {
	for _, scenario := range []string{"claude_cli_without_tools", "mock", "authored_http_get", "unknown_adapter", "missing_surface", "tampered_surface", "foreign_profile", "foreign_provider", "foreign_runtime_mode", "selected", "missing_request"} {
		t.Run(scenario, func(t *testing.T) {
			backend := llmselection.BackendAnthropic
			if scenario == "claude_cli_without_tools" {
				backend = llmselection.BackendClaudeCLI
			} else if scenario == "mock" {
				backend = llmselection.BackendMock
			}
			plan, request, adapter := completionResumeTestPlan(t, backend)
			wantErr := false
			switch scenario {
			case "authored_http_get":
				adapter = "authored_http_tool"
			case "unknown_adapter":
				adapter = "foreign_http_provider"
			case "foreign_profile":
				request.BackendProfile, request.Provider = llmselection.BackendOpenAIResponses, llmselection.ProviderOpenAI
				wantErr = true
			case "foreign_provider":
				plan.Provider = llmselection.ProviderOpenAI
				wantErr = true
			case "foreign_runtime_mode":
				plan.RuntimeMode = llmselection.ProviderContractRuntimeModeOpenAIResponses
				wantErr = true
			case "selected":
				plan.Authority.ExecutionKind = managedcapabilities.ExecutionSelectedContractFork
				plan.Authority.ExecutionAuthorityID = uuid.NewString()
			case "missing_request":
				request = runtimeeffects.CompletionRequest{}
				wantErr = true
			}
			surface, err := managedcapabilities.New(plan)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "missing_surface" {
				surface = managedcapabilities.Surface{}
				wantErr = true
			} else if scenario == "tampered_surface" {
				surface.Tools = []managedcapabilities.Tool{}
				surface.Provider = "foreign"
				wantErr = true
			}
			readOnly, err := completionResumeSurfaceAllowsHTTP(adapter, request, surface)
			if readOnly || (err != nil) != wantErr {
				t.Fatalf("unsupported evidence read-only=%t error=%v, want error=%t", readOnly, err, wantErr)
			}
		})
	}
}

func completionResumeTestPlan(t *testing.T, backend string) (managedcapabilities.Plan, runtimeeffects.CompletionRequest, string) {
	t.Helper()
	profile, err := llmselection.ResolveActiveBackend(backend)
	if err != nil {
		t.Fatal(err)
	}
	identity := agentidentitytest.RootDeclared(t, "resume-agent", "resume-test")
	plan := managedcapabilities.Plan{
		ActorIdentity: identity, RuntimeMode: profile.RuntimeMode, Provider: profile.Provider, Transport: profile.Transport,
		ProviderContract: "frozen-provider-contract", CreatedAt: time.Unix(1, 0).UTC(),
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: uuid.NewString(),
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: uuid.NewString(),
			RunID: identity.RunID, SessionID: uuid.NewString(), TurnOrdinal: 1,
		},
	}
	request := runtimeeffects.CompletionRequest{
		ModelAlias: "regular", RequestedModel: "frozen-model", BackendProfile: profile.ID, Provider: profile.Provider, Transport: profile.Transport,
	}
	adapter := profile.ID
	if backend == llmselection.BackendAnthropic {
		adapter = "anthropic_api"
	}
	return plan, request, adapter
}
