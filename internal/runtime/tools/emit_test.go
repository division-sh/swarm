package tools

import (
	"testing"

	runtimeauthority "swarm/internal/runtime/authority"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/semanticview"
)

func TestEmitToolName_LocalizesScopedEventTypes(t *testing.T) {
	if got := EmitToolName("validation/spec.validation_failed"); got != "emit_spec_validation_failed" {
		t.Fatalf("EmitToolName(scoped) = %q, want %q", got, "emit_spec_validation_failed")
	}
	if got := EmitToolName("discovery/category.assessed"); got != "emit_category_assessed" {
		t.Fatalf("EmitToolName(scoped) = %q, want %q", got, "emit_category_assessed")
	}
}

func TestGenerateEmitToolsForActor_FallsBackToRoleWhenConfigIsSilent(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"campaign-coordinator": {
				ID:         "campaign-coordinator",
				Role:       "campaign_coordinator",
				EmitEvents: []string{"scan.requested"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
					},
				},
			},
		},
	})
	runtimeauthority.SetProvider(runtimeauthority.NewSourceProvider(source))
	defer runtimeauthority.SetProvider(nil)
	InitEventSchemaRegistry(source)

	tools := GenerateEmitToolsForActor(models.AgentConfig{
		ID:   "campaign-coordinator",
		Role: "campaign_coordinator",
	}, nil)
	for _, def := range tools {
		if def.Name == "emit_scan_requested" {
			return
		}
	}
	t.Fatalf("expected role-scoped emit tool in %#v", tools)
}
