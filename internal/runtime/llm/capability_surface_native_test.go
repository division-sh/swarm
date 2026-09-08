package llm

import (
	"context"
	"slices"
	"strings"
	"testing"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/google/uuid"
)

func TestManagedCapabilityPlanSeparatesAllProviderNativeFamiliesFromConcreteFallbackDenial(t *testing.T) {
	actor := runtimeactors.AgentConfig{
		ID: "native-agent",
		NativeTools: runtimeactors.NativeToolConfig{
			Bash:      true,
			WebSearch: true,
			FileIO:    true,
		},
	}
	actor.Identity = testAgentIdentity(actor.ID, "")
	definitions := nativeFallbackDefinitions()
	denied := make([]toolcapabilities.Capability, 0, len(definitions))
	for _, definition := range definitions {
		denied = append(denied, toolcapabilities.Capability{
			Name:         definition.Name,
			DenialReason: "platform fallback is not admitted",
		})
	}

	surface, err := managedCapabilityPlanForTest(
		runtimeactors.WithActor(context.Background(), actor),
		&ClaudeCLIRuntime{},
		"startup_probe",
		definitions,
		toolcapabilities.NewSet(denied),
		nativeCapabilityTestAuthority(),
	)
	if err != nil {
		t.Fatalf("managedCapabilityPlan: %v", err)
	}
	if got := surface.PlannedBindingNames(managedcapabilities.BindingProviderBuiltin); !slices.Equal(got, []string{"Bash", "Edit", "Read", "WebFetch", "WebSearch", "Write"}) {
		t.Fatalf("provider-native bindings = %v", got)
	}
	for _, kind := range []managedcapabilities.BindingKind{
		managedcapabilities.BindingAPIDefinition,
		managedcapabilities.BindingLocalRuntime,
		managedcapabilities.BindingMCPProvider,
		managedcapabilities.BindingMCPTool,
	} {
		if got := surface.BindingNames(kind); len(got) != 0 {
			t.Fatalf("provider-native surface acquired %s bindings %v", kind, got)
		}
	}
	for _, name := range []string{"bash", "read_file", "web_search", "write_file"} {
		capability, ok := surface.Capability(name)
		if !ok || capability.AuthorizationClass != "provider_native" {
			t.Fatalf("provider-native capability %s = %#v, present=%t", name, capability, ok)
		}
	}

	response := &Response{ProviderVisibleTools: []string{"Bash", "Edit", "Read", "WebFetch", "WebSearch", "Write"}}
	observed, err := ObserveCLIResponseCapabilitySurface(surface, response)
	if err != nil {
		t.Fatalf("ObserveCLIResponseCapabilitySurface: %v", err)
	}
	if err := ValidateCLIProviderCapabilitySurface(observed, response); err != nil {
		t.Fatalf("ValidateCLIProviderCapabilitySurface: %v", err)
	}
	if got := observed.EffectiveNames(); !slices.Equal(got, []string{"bash", "read_file", "web_search", "write_file"}) {
		t.Fatalf("effective provider-native capabilities = %v", got)
	}
}

func TestManagedCapabilityPlanRejectsUnsupportedProviderNativeFamilies(t *testing.T) {
	for _, test := range []struct {
		name   string
		native runtimeactors.NativeToolConfig
		clear  func(*NativeToolCapabilities)
		want   string
	}{
		{name: "bash", native: runtimeactors.NativeToolConfig{Bash: true}, clear: func(c *NativeToolCapabilities) { c.Bash = false }, want: "bash"},
		{name: "web search", native: runtimeactors.NativeToolConfig{WebSearch: true}, clear: func(c *NativeToolCapabilities) { c.WebSearch = false }, want: "web_search"},
		{name: "file IO", native: runtimeactors.NativeToolConfig{FileIO: true}, clear: func(c *NativeToolCapabilities) { c.FileIO = false }, want: "read_file"},
	} {
		t.Run(test.name, func(t *testing.T) {
			contract := ClaudeCLIProviderContract()
			test.clear(&contract.NativeTools.Capabilities)
			runtime := nativeCapabilityContractRuntime{contract: contract}
			actor := runtimeactors.AgentConfig{ID: "unsupported-agent", Identity: testAgentIdentity("unsupported-agent", ""), NativeTools: test.native}
			ctx := runtimeactors.WithActor(context.Background(), actor)

			_, err := managedCapabilityPlanForTest(ctx, runtime, "", nil, toolcapabilities.Set{}, nativeCapabilityTestAuthority())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("managedCapabilityPlan error = %v, want unsupported %s", err, test.want)
			}
		})
	}
}

func TestManagedCapabilityPlanPreservesConcreteAPIFallbackDefinition(t *testing.T) {
	actor := runtimeactors.AgentConfig{ID: "fallback-agent", NativeTools: runtimeactors.NativeToolConfig{WebSearch: true}}
	actor.Identity = testAgentIdentity(actor.ID, "")
	definition := ToolDefinition{Name: "web_search", Schema: map[string]any{"type": "object"}}
	ctx := runtimeactors.WithActor(context.Background(), actor)

	surface, err := managedCapabilityPlanForTest(
		ctx,
		&AnthropicAPIRuntime{},
		"",
		[]ToolDefinition{definition},
		toolcapabilities.NewSet([]toolcapabilities.Capability{{
			Name: "web_search", Visible: true, Callable: true, AuthorizationClass: "web_search_provider",
		}}),
		nativeCapabilityTestAuthority(),
	)
	if err != nil {
		t.Fatalf("managedCapabilityPlan: %v", err)
	}
	if got := surface.PlannedBindingNames(managedcapabilities.BindingAPIDefinition); !slices.Equal(got, []string{"web_search"}) {
		t.Fatalf("API fallback bindings = %v", got)
	}
	if got := surface.BindingNames(managedcapabilities.BindingProviderBuiltin); len(got) != 0 {
		t.Fatalf("concrete API fallback acquired provider-native bindings %v", got)
	}
	observed, err := ObserveAPIRequestCapabilitySurface(surface, []ToolDefinition{definition})
	if err != nil {
		t.Fatalf("ObserveAPIRequestCapabilitySurface: %v", err)
	}
	if got := observed.EffectiveNames(); !slices.Equal(got, []string{"web_search"}) {
		t.Fatalf("effective API fallback capabilities = %v", got)
	}
}

func TestManagedCapabilityPlanRejectsFallbackWithoutConcreteDefinition(t *testing.T) {
	actor := runtimeactors.AgentConfig{ID: "fallback-agent", NativeTools: runtimeactors.NativeToolConfig{WebSearch: true}}
	actor.Identity = testAgentIdentity(actor.ID, "")
	_, err := managedCapabilityPlanForTest(
		runtimeactors.WithActor(context.Background(), actor),
		&AnthropicAPIRuntime{},
		"",
		nil,
		toolcapabilities.Set{},
		nativeCapabilityTestAuthority(),
	)
	if err == nil || !strings.Contains(err.Error(), "no selected provider-native support or concrete fallback definition") {
		t.Fatalf("managedCapabilityPlan error = %v", err)
	}
}

func TestManagedCapabilityPlanRejectsMissingAndUnexpectedProviderBuiltins(t *testing.T) {
	actor := runtimeactors.AgentConfig{ID: "native-agent", NativeTools: runtimeactors.NativeToolConfig{WebSearch: true}}
	actor.Identity = testAgentIdentity(actor.ID, "")
	surface, err := managedCapabilityPlanForTest(
		runtimeactors.WithActor(context.Background(), actor),
		&ClaudeCLIRuntime{},
		"",
		nil,
		toolcapabilities.Set{},
		nativeCapabilityTestAuthority(),
	)
	if err != nil {
		t.Fatalf("managedCapabilityPlan: %v", err)
	}
	for _, test := range []struct {
		name     string
		response *Response
	}{
		{name: "missing", response: &Response{ProviderVisibleTools: []string{"WebSearch"}}},
		{name: "unexpected", response: &Response{ProviderVisibleTools: []string{"WebFetch", "WebSearch", "UnexpectedBuiltin"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed, observeErr := ObserveCLIResponseCapabilitySurface(surface, test.response)
			if test.name == "unexpected" && observeErr != nil {
				t.Fatalf("ObserveCLIResponseCapabilitySurface: %v", observeErr)
			}
			if err := ValidateCLIProviderCapabilitySurface(observed, test.response); err == nil {
				t.Fatal("provider builtin mismatch was accepted")
			}
		})
	}
}

func nativeFallbackDefinitions() []ToolDefinition {
	return []ToolDefinition{
		{Name: "bash", Schema: map[string]any{"type": "object"}},
		{Name: "web_search", Schema: map[string]any{"type": "object"}},
		{Name: "read_file", Schema: map[string]any{"type": "object"}},
		{Name: "write_file", Schema: map[string]any{"type": "object"}},
	}
}

func nativeCapabilityTestAuthority() managedcapabilities.Authority {
	return managedcapabilities.Authority{
		Kind:                 managedcapabilities.AuthorityStartupProbe,
		ID:                   uuid.NewString(),
		ExecutionKind:        managedcapabilities.ExecutionNormalAgent,
		ExecutionAuthorityID: uuid.NewString(),
		StartupOwnerID:       "native-test-owner",
		StartupGeneration:    1,
	}
}

type nativeCapabilityContractRuntime struct {
	NoopRuntime
	contract ProviderContract
}

func (r nativeCapabilityContractRuntime) ProviderContract() ProviderContract {
	return r.contract
}
