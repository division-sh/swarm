package packs_test

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestSchemaRelation2630ChannelGates(t *testing.T) {
	mode := runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaAny, runtimecontracts.ToolSchemaEnum("approved"))
	for _, tc := range []struct {
		name string
		opts []runtimecontracts.ToolInputSchemaOption
		want string
	}{
		{"open_object", nil, "object must be non-empty and additionalProperties false"},
		{"optional_member", []runtimecontracts.ToolInputSchemaOption{runtimecontracts.ToolSchemaAdditionalPropertiesAllowed(false)}, "object must require every property"},
		{"any_leaf", []runtimecontracts.ToolInputSchemaOption{runtimecontracts.ToolSchemaAdditionalPropertiesAllowed(false), runtimecontracts.ToolSchemaRequired("mode")}, `object leaf "mode" has unsupported type "any"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			channel, trigger, connector := mockChannelSatisfier()
			options := []runtimecontracts.ToolInputSchemaOption{runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{"mode": mode})}
			channel.Manifest.OpaqueTypes["destination"] = runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject, append(options, tc.opts...)...)
			_, err := packs.CompileChannel(loadChannelInterfaceRegistry(t), channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("opaque gate error=%v, want %q", err, tc.want)
			}
		})
	}
	t.Run("learned_object", func(t *testing.T) {
		channel, trigger, connector := mockChannelSatisfier()
		channel.Manifest.Onboarding = &packs.ChannelOnboardingProfile{
			Ceremony: "authenticated_text_challenge", ProviderCredentialRole: "mock_api_key", SigningCredentialRole: "mock_callback_key", Confirmation: "deliver",
			LearnedDestination: map[string]packs.ChannelMapping{"destination": {From: "conversation_reference"}},
		}
		_, err := packs.CompileChannel(loadChannelInterfaceRegistry(t), channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
		if err == nil || !strings.Contains(err.Error(), "permits only scalar-leaf projection") {
			t.Fatalf("learned gate error=%v, want scalar-leaf refusal", err)
		}
	})
}

func TestSchemaRelation2630ChannelDetachedEquality(t *testing.T) {
	channel, trigger, connector := mockChannelSatisfier()
	receipt, err := runtimecontracts.AdmitToolInputSchemaMap(map[string]any{
		"type": "object", "additionalProperties": false, "required": []any{"revision", "peer"},
		"properties": map[string]any{
			"revision": map[string]any{"type": "integer", "x-swarm-equalTo": "peer"},
			"peer":     map[string]any{"type": "integer"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := receipt.Validate(map[string]any{"revision": 1, "peer": 1}); err != nil {
		t.Fatal(err)
	}
	channel.Manifest.OpaqueTypes["delivery_receipt"] = receipt
	operation := channel.Manifest.Operations["edit"]
	operation.Output["delivery_receipt.peer"] = packs.ChannelMapping{From: "result.peer"}
	channel.Manifest.Operations["edit"] = operation
	tool := connector.Tools["mock.edit"]
	tool, err = tool.WithSchemas(tool.InputSchema(), receipt)
	if err != nil {
		t.Fatal(err)
	}
	connector.Tools["mock.edit"] = tool
	plan, err := packs.CompileChannel(loadChannelInterfaceRegistry(t), channel, []packs.TriggerPackDescriptor{trigger}, []packs.ConnectorPackDescriptor{connector})
	if err == nil || !strings.Contains(err.Error(), "edit output result.revision -> delivery_receipt.revision") || !strings.Contains(err.Error(), "requires its enclosing object relation") {
		t.Fatalf("public compiler must refuse detached output equality: %v", err)
	}
	if _, err := plan.Generation(); err == nil {
		t.Fatal("refused channel acquired a generation")
	}
}
