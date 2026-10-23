package packs_test

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packs"
	"gopkg.in/yaml.v3"
)

func TestProductionCompilerFailsClosedAcrossChannelContractPhases(t *testing.T) {
	registry, source, _, _ := loadTelegramChannelCompilerInputs(t)
	tests := []struct {
		name   string
		mutate func(channel, trigger, connector map[string]any)
		want   string
	}{
		{
			name: "incomplete operation surface",
			mutate: func(channel, _, _ map[string]any) {
				delete(channel["operations"].(map[string]any), "edit")
			},
			want: "channel operations",
		},
		{
			name: "unknown operation",
			mutate: func(channel, _, _ map[string]any) {
				channel["operations"].(map[string]any)["delete"] = map[string]any{"tool": "edit_message"}
			},
			want: "channel operations",
		},
		{
			name: "missing opaque slot",
			mutate: func(channel, _, _ map[string]any) {
				delete(channel["opaque_types"].(map[string]any), "conversation_reference")
			},
			want: "channel opaque_types",
		},
		{
			name: "effect mismatch",
			mutate: func(_, _, connector map[string]any) {
				tool := connector["tools"].(map[string]any)["send_interactive"].(map[string]any)
				tool["category"] = "provider_registration"
			},
			want: "effect class does not match",
		},
		{
			name: "unconsumed interface input",
			mutate: func(channel, _, _ map[string]any) {
				delete(channel["operations"].(map[string]any)["deliver"].(map[string]any)["input"].(map[string]any), "text")
			},
			want: `selected channel constraint "presentation.text" is not mapped`,
		},
		{
			name: "incompatible selected patterns",
			mutate: func(_, _, connector map[string]any) {
				for name, pattern := range map[string]string{"send_interactive": "^[a-z0-9-]+$", "edit_message": "^[A-Z]+$"} {
					properties := connector["tools"].(map[string]any)[name].(map[string]any)["input_schema"].(map[string]any)["properties"].(map[string]any)
					keyboard := properties["reply_markup"].(map[string]any)["properties"].(map[string]any)["inline_keyboard"].(map[string]any)
					button := keyboard["items"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
					button["callback_data"].(map[string]any)["pattern"] = pattern
				}
			},
			want: "incompatible patterns",
		},
		{
			name: "missing finite text maximum",
			mutate: func(_, _, connector map[string]any) {
				for _, name := range []string{"send_interactive", "edit_message"} {
					properties := connector["tools"].(map[string]any)[name].(map[string]any)["input_schema"].(map[string]any)["properties"].(map[string]any)
					delete(properties["text"].(map[string]any), "maxLength")
				}
			},
			want: "finite maxLength",
		},
		{
			name: "event field type mismatch",
			mutate: func(_, trigger, _ map[string]any) {
				for _, item := range trigger["normalized_events"].([]any) {
					event := item.(map[string]any)
					if event["event"] == "text_message" {
						event["fields"].(map[string]any)["text"].(map[string]any)["schema"] = map[string]any{"type": "integer"}
						return
					}
				}
				panic("shipped text_message fixture is missing")
			},
			want: "incompatible types",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			members := source.Members()
			names := []string{"channel.yaml", "trigger.yaml", "connector.yaml"}
			var bodies [3]map[string]any
			for i, name := range names {
				if err := yaml.Unmarshal(members[name], &bodies[i]); err != nil {
					t.Fatal(err)
				}
			}
			tc.mutate(bodies[0], bodies[1], bodies[2])
			for i, name := range names {
				body, err := yaml.Marshal(bodies[i])
				if err != nil {
					t.Fatal(err)
				}
				members[name] = body
			}
			provider, trigger, connector := s1ChannelProviderDescriptors(t, "telegram", members)
			_, err := packs.CompileProviderChannel(registry, provider, trigger, connector)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("CompileProviderChannel error = %v, want containing %q", err, tc.want)
			}
		})
	}
}
