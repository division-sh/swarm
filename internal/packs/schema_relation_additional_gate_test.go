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
