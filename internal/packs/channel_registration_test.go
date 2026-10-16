package packs

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

func TestParseChannelRegistrationTargetPreservesCanonicalFlowIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		raw      string
		wantFlow string
		wantErr  bool
	}{
		{name: "root", raw: "ingress:.:telegram", wantFlow: "."},
		{name: "nested", raw: "ingress:packages/channels:telegram", wantFlow: "packages/channels"},
		{name: "noncanonical nested", raw: "ingress:packages//channels:telegram", wantErr: true},
		{name: "escaping", raw: "ingress:../channels:telegram", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParseChannelRegistrationTarget(test.raw)
			if test.wantErr {
				if err == nil {
					t.Fatalf("ParseChannelRegistrationTarget(%q) succeeded: %#v", test.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseChannelRegistrationTarget(%q): %v", test.raw, err)
			}
			if got.FlowPath != test.wantFlow || got.Provider != "telegram" {
				t.Fatalf("ParseChannelRegistrationTarget(%q) = %#v", test.raw, got)
			}
		})
	}
}

func TestRegistrationSlotPolicyCannotBeReclassified(t *testing.T) {
	id, err := admitChannelPlanIdentity("test connector tool", "test.registration")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		policy runtimecontracts.ActivityEffectClass
	}{
		{RegistrationOperationIdentify, runtimecontracts.ActivityEffectClassReadOnly},
		{RegistrationOperationReadback, runtimecontracts.ActivityEffectClassReadOnly},
		{RegistrationOperationApply, runtimecontracts.ActivityEffectClassNonIdempotentWrite},
	} {
		t.Run(tc.name, func(t *testing.T) {
			operation := RegistrationOperationPlan{operation: compiledRegistrationOperation{name: tc.name, toolID: id, effect: tc.policy}}
			if policy, err := operation.EffectClass(); err != nil || policy != tc.policy {
				t.Fatalf("exact slot policy = %s, %v", policy, err)
			}
			for _, wrong := range []runtimecontracts.ActivityEffectClass{"", runtimecontracts.ActivityEffectClassIdempotentWrite, runtimecontracts.ActivityEffectClassNonIdempotentWrite, runtimecontracts.ActivityEffectClassReadOnly} {
				if wrong == tc.policy {
					continue
				}
				altered := operation
				altered.operation.effect = wrong
				if _, err := altered.EffectClass(); err == nil {
					t.Fatalf("%s accepted contradictory compiled policy %s", tc.name, wrong)
				}
			}
			operation.operation.name = "unknown_slot"
			if _, err := operation.EffectClass(); err == nil {
				t.Fatal("unknown slot acquired a platform policy")
			}
		})
	}
	if _, err := (RegistrationOperationPlan{}).EffectClass(); err == nil {
		t.Fatal("zero operation acquired a platform policy")
	}
}

func TestRegistrationCompilerPreservesSlotCategoryCredentialsAndMappings(t *testing.T) {
	tool := runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory("provider_registration"),
		runtimecontracts.WithToolCredentials("provider_token"),
		runtimecontracts.WithToolSchemas(
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject),
			runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject,
				runtimecontracts.ToolSchemaProperties(map[string]runtimecontracts.ToolInputSchema{
					"id": runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaString),
				})),
		),
	)
	declared := ChannelRegistrationOperation{Tool: "test.identify", Output: map[string]ChannelMapping{"resource_id": {From: "result.id"}}}
	connector := ConnectorPackDescriptor{Tools: map[string]runtimecontracts.ToolSchemaEntry{declared.Tool: tool}}
	if _, err := compileRegistrationOperation(RegistrationOperationIdentify, declared, connector, runtimecontracts.ActivityEffectClassReadOnly, []string{"provider_token"}, ""); err != nil {
		t.Fatal(err)
	}
	wrongCategory := runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolCategory("provider_connector"),
		runtimecontracts.WithToolCredentials("provider_token"),
		runtimecontracts.WithToolSchemas(tool.InputSchema(), tool.OutputSchema()),
	)
	for _, tc := range []struct {
		name, want  string
		declaration ChannelRegistrationOperation
		tool        runtimecontracts.ToolSchemaEntry
		policy      runtimecontracts.ActivityEffectClass
		credentials []string
	}{
		{"wrong_policy", "conflicting platform slot policy", declared, tool, runtimecontracts.ActivityEffectClassNonIdempotentWrite, []string{"provider_token"}},
		{"wrong_category", "provider_registration category", declared, wrongCategory, runtimecontracts.ActivityEffectClassReadOnly, []string{"provider_token"}},
		{"wrong_credentials", "credentials", declared, tool, runtimecontracts.ActivityEffectClassReadOnly, []string{"different_token"}},
		{"missing_tool", "unknown connector tool", ChannelRegistrationOperation{Tool: "test.absent"}, tool, runtimecontracts.ActivityEffectClassReadOnly, []string{"provider_token"}},
		{"wrong_mapping", "schema path", ChannelRegistrationOperation{Tool: declared.Tool, Output: map[string]ChannelMapping{"resource_id": {From: "result.absent"}}}, tool, runtimecontracts.ActivityEffectClassReadOnly, []string{"provider_token"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := ConnectorPackDescriptor{Tools: map[string]runtimecontracts.ToolSchemaEntry{declared.Tool: tc.tool}}
			if _, err := compileRegistrationOperation(RegistrationOperationIdentify, tc.declaration, candidate, tc.policy, tc.credentials, ""); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("slot compiler lost %s refusal: %v", tc.name, err)
			}
		})
	}
}
