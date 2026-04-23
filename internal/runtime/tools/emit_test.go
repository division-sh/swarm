package tools

import (
	"testing"

	runtimeauthority "swarm/internal/runtime/authority"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/flowmodel"
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
	registry := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))

	tools := registry.GenerateEmitToolsForActor(models.AgentConfig{
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

func TestEmitRegistry_KeepsRuntimeSourcesIsolated(t *testing.T) {
	sourceA := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"coordinator": {
				ID:         "coordinator",
				Role:       "coordinator",
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
	sourceB := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"coordinator": {
				ID:         "coordinator",
				Role:       "coordinator",
				EmitEvents: []string{"review.requested"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"review.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
					},
				},
			},
		},
	})

	registryA := NewEmitRegistry(sourceA, runtimeauthority.NewSourceProvider(sourceA))
	registryB := NewEmitRegistry(sourceB, runtimeauthority.NewSourceProvider(sourceB))
	actor := models.AgentConfig{ID: "coordinator", Role: "coordinator"}

	toolsA := registryA.GenerateEmitToolsForActor(actor, nil)
	toolsB := registryB.GenerateEmitToolsForActor(actor, nil)
	if len(toolsA) != 1 || toolsA[0].Name != "emit_scan_requested" {
		t.Fatalf("toolsA = %#v, want emit_scan_requested only", toolsA)
	}
	if len(toolsB) != 1 || toolsB[0].Name != "emit_review_requested" {
		t.Fatalf("toolsB = %#v, want emit_review_requested only", toolsB)
	}
}

func TestEmitSchemaForEventType_UsesOnlyUniqueScopedLocalMatch(t *testing.T) {
	scanSchema := EmitSchema{Description: "scan"}
	reviewSchema := EmitSchema{Description: "review"}

	schema, ok := emitSchemaForEventType(map[string]EmitSchema{
		"review/scan.requested": scanSchema,
	}, "review/inst-1/scan.requested")
	if !ok || schema.Description != "scan" {
		t.Fatalf("unique scoped schema = %#v, %v; want scan,true", schema, ok)
	}

	_, ok = emitSchemaForEventType(map[string]EmitSchema{
		"review/scan.requested":     scanSchema,
		"validation/scan.requested": reviewSchema,
	}, "scan.requested")
	if ok {
		t.Fatal("ambiguous scoped local match should not resolve")
	}
}

func TestGenerateEmitToolsForActor_ResolvesInstanceScopedFlowEmitEventsThroughOwningFlowProof(t *testing.T) {
	reviewFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "review",
			Flow: "review",
		},
		Path: "review",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
					},
					Required: []string{"entity_id"},
				},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{reviewFlow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": &reviewFlow,
			},
		},
	}
	source := semanticview.Wrap(bundle)
	registry := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))

	tools := registry.GenerateEmitToolsForActor(models.AgentConfig{
		ID:         "review-coordinator-inst-1",
		Role:       "review_coordinator",
		Mode:       "review",
		FlowPath:   "review/inst-1",
		EmitEvents: []string{"review/inst-1/scan.requested"},
	}, nil)

	if len(tools) != 1 {
		t.Fatalf("tool count = %d, want 1 (%#v)", len(tools), tools)
	}
	if tools[0].Name != "emit_scan_requested" {
		t.Fatalf("tool name = %q, want emit_scan_requested", tools[0].Name)
	}
}
