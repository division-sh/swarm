package llm

import (
	"context"
	"fmt"
	"slices"
	"testing"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/google/uuid"
)

func notifyHumanTestToolDefinition() ToolDefinition {
	return ToolDefinition{
		Name:        "notify_human",
		Description: "Sends an informational notice to the human operator. Does NOT request approval and does not pause the flow - to ask for a decision that gates the flow, use ask_human.",
		Schema: map[string]any{
			"type": "object", "additionalProperties": false,
			"required":   []any{"summary"},
			"properties": map[string]any{"summary": map[string]any{"type": "string"}, "context": map[string]any{}},
		},
	}
}

func TestNotifyHumanManagedPlanningCoversEveryShippedBackendAndKillsOmission(t *testing.T) {
	const toolName = "notify_human"
	tool := notifyHumanTestToolDefinition()
	capabilities := toolcapabilities.NewSet([]toolcapabilities.Capability{{
		Name: toolName, Visible: true, Callable: true, AuthorizationClass: "universal",
	}})
	tests := []struct {
		name       string
		contract   ProviderContract
		binding    managedcapabilities.BindingKind
		exactNames []string
	}{
		{name: "anthropic_api", contract: AnthropicAPIProviderContract(), binding: managedcapabilities.BindingAPIDefinition, exactNames: []string{toolName}},
		{name: "openai_compatible_api", contract: OpenAICompatibleProviderContract(), binding: managedcapabilities.BindingAPIDefinition, exactNames: []string{toolName}},
		{name: "openai_responses_api", contract: OpenAIResponsesProviderContract(), binding: managedcapabilities.BindingAPIDefinition, exactNames: []string{toolName}},
		{name: "claude_cli", contract: ClaudeCLIProviderContract(), binding: managedcapabilities.BindingMCPTool, exactNames: []string{"mcp__runtime-tools__notify_human"}},
		{name: "mock", contract: MockProviderContract(), binding: managedcapabilities.BindingLocalRuntime, exactNames: []string{toolName}},
	}

	actor := runtimeactors.AgentConfig{ID: "hitl-agent", Identity: testAgentIdentity("hitl-agent", "")}
	ctx := runtimeactors.WithActor(context.Background(), actor)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := func(definitions []ToolDefinition) (managedcapabilities.Surface, error) {
				return managedCapabilityPlan(ctx, NewNoopRuntime(test.contract), test.contract.RuntimeMode, definitions, capabilities, managedcapabilities.Authority{
					Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(), ExecutionKind: managedcapabilities.ExecutionNormalAgent,
					ExecutionAuthorityID: uuid.NewString(), StartupOwnerID: "hitl-planning-test", StartupGeneration: 1,
				})
			}
			surface, err := plan([]ToolDefinition{tool})
			if err != nil {
				t.Fatalf("plan notify_human: %v", err)
			}
			if err := requireNotifyHumanPlanningRow(surface, test.binding, test.exactNames); err != nil {
				t.Fatal(err)
			}

			mutated, err := plan(nil)
			if err != nil {
				t.Fatalf("plan omission mutation: %v", err)
			}
			if err := requireNotifyHumanPlanningRow(mutated, test.binding, test.exactNames); err == nil {
				t.Fatal("dropping notify_human from this backend row did not kill the invariant")
			}
		})
	}
}

func requireNotifyHumanPlanningRow(surface managedcapabilities.Surface, binding managedcapabilities.BindingKind, exactNames []string) error {
	if got := surface.PlannedBindingNames(binding); !slices.Equal(got, exactNames) {
		return fmt.Errorf("notify_human planned %s bindings = %v, want %v", binding, got, exactNames)
	}
	for _, tool := range surface.Tools {
		if tool.Name == "notify_human" && tool.Capability.Visible && tool.Capability.Callable {
			return nil
		}
	}
	return fmt.Errorf("notify_human is absent from the planned callable capability set")
}
