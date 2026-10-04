package llm

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func TestManagedCapabilityAdmissionSharesAllShippedProviderPlans(t *testing.T) {
	for _, backend := range []string{selection.BackendAnthropic, selection.BackendClaudeCLI, selection.BackendOpenAICompatible, selection.BackendOpenAIResponses, selection.BackendMock} {
		t.Run(backend, func(t *testing.T) {
			profile, err := selection.ResolveActiveBackend(backend)
			if err != nil {
				t.Fatal(err)
			}
			contract, err := ProviderContractForProfile(profile)
			if err != nil {
				t.Fatal(err)
			}
			actor := actors.AgentConfig{ID: "worker", Identity: testAgentIdentity("worker", "")}
			definition := ToolDefinition{Name: "health_check", Description: "Read-only health", Schema: map[string]any{"type": "object"}}
			capabilities := toolcapabilities.NewSet([]toolcapabilities.Capability{{Name: definition.Name, Visible: true, Callable: true}})
			planned, err := CompileManagedCapabilityAdmission(actor, contract, []ToolDefinition{definition}, capabilities)
			if err != nil || len(planned) != 1 || planned[0].DefinitionHash != ToolDefinitionIdentity(definition) {
				t.Fatalf("admission lost exact definition: planned=%#v err=%v", planned, err)
			}
			surface, err := managedCapabilityPlanForTest(actors.WithActor(context.Background(), actor), nativeCapabilityContractRuntime{contract: contract}, "", []ToolDefinition{definition}, capabilities, nativeCapabilityTestAuthority())
			if err != nil || len(surface.Tools) != 1 {
				t.Fatalf("surface: %#v err=%v", surface, err)
			}
			tool := surface.Tools[0]
			bindingsMatch := len(tool.Bindings) == len(planned[0].Bindings)
			for _, binding := range planned[0].Bindings {
				bindingsMatch = bindingsMatch && slices.Contains(tool.Bindings, binding)
			}
			if tool.Name != planned[0].Name || tool.DefinitionHash != planned[0].DefinitionHash || !reflect.DeepEqual(tool.Capability, planned[0].Capability) || !bindingsMatch || len(tool.Evidence) != 0 {
				t.Fatalf("admission and startup plan diverged: planned=%#v tool=%#v", planned[0], tool)
			}
		})
	}
}

func TestManagedCapabilityAdmissionRejectsSameStaticNativeAndDuplicateInputs(t *testing.T) {
	actor := actors.AgentConfig{ID: "worker", NativeTools: actors.NativeToolConfig{WebSearch: true}}
	if _, err := CompileManagedCapabilityAdmission(actor, AnthropicAPIProviderContract(), nil, toolcapabilities.Set{}); err == nil || !strings.Contains(err.Error(), "concrete fallback definition") {
		t.Fatalf("missing fallback admitted without an execution boundary: %v", err)
	}
	if _, err := CompileManagedCapabilityAdmission(actor, ClaudeCLIProviderContract(), nil, toolcapabilities.Set{}); err != nil {
		t.Fatalf("provider-native capability required a platform fallback: %v", err)
	}
	actor.NativeTools = actors.NativeToolConfig{}
	definition := ToolDefinition{Name: "health_check", Schema: map[string]any{"type": "object"}}
	if _, err := CompileManagedCapabilityAdmission(actor, ClaudeCLIProviderContract(), []ToolDefinition{definition, definition}, toolcapabilities.Set{}); err == nil || !strings.Contains(err.Error(), "duplicated") {
		t.Fatalf("duplicate input admitted: %v", err)
	}
}
