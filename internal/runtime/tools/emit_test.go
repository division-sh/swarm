package tools

import (
	"path/filepath"
	"slices"
	"strings"
	"testing"

	runtimeauthority "swarm/internal/runtime/authority"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/flowmodel"
	llm "swarm/internal/runtime/llm"
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

func TestGenerateEmitToolsForActor_FailsClosedOnDuplicateLocalToolNames(t *testing.T) {
	reviewFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review", Flow: "review"},
		Path:  "review",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"priority": {Type: "string"},
					},
				},
			},
		},
	}
	validationFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "validation", Flow: "validation"},
		Path:  "validation",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"priority": {Type: "string"},
					},
				},
			},
		},
	}
	root := &runtimecontracts.FlowContractView{
		Children: []runtimecontracts.FlowContractView{reviewFlow, validationFlow},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: root,
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review":     &root.Children[0],
				"validation": &root.Children[1],
			},
			ByPath: map[string]*runtimecontracts.FlowContractView{
				"review":     &root.Children[0],
				"validation": &root.Children[1],
			},
		},
	}
	registry := NewEmitRegistry(semanticview.Wrap(bundle), nil)
	actor := models.AgentConfig{
		ID:         "dual-scope-agent",
		Role:       "reviewer",
		Mode:       "review",
		FlowPath:   "review",
		EmitEvents: []string{"review/task.requested", "validation/task.requested"},
	}

	var warnings []string
	tools := registry.GenerateEmitToolsForActor(actor, func(code, component, format string, args ...any) {
		warnings = append(warnings, code)
	})
	if len(tools) != 0 {
		t.Fatalf("tools = %#v, want none on duplicate local tool name collision", tools)
	}
	if !slices.Contains(warnings, "emit-tool-ambiguous-name-emit_task_requested") {
		t.Fatalf("warnings = %#v, want duplicate-name warning", warnings)
	}
	if registry.IsEmitToolAllowedForActor(actor, "emit_task_requested") {
		t.Fatal("expected duplicate local tool name collision to deny actor emit tool authorization")
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

func TestGenerateEmitToolsForActor_ProviderSchemaNormalizesPrecisionRefs(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"RequiredCapabilities": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"automation_with_unlock": {Type: "numeric(5,2)"},
					},
				},
			},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"market-research-agent": {
				ID:         "market-research-agent",
				Role:       "market_research",
				EmitEvents: []string{"category.assessed"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"required_capabilities": {Type: "RequiredCapabilities"},
						"scores":                {Type: "[numeric(5,2)]"},
					},
					Required: []string{"required_capabilities"},
				},
			},
		},
	})
	registry := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))

	defs := registry.GenerateEmitToolsForActor(models.AgentConfig{
		ID:         "market-research-agent",
		Role:       "market_research",
		EmitEvents: []string{"category.assessed"},
	}, nil)
	if len(defs) != 1 {
		t.Fatalf("tool count = %d, want 1 (%#v)", len(defs), defs)
	}
	if err := llm.ValidateProviderToolDefinitions(defs); err != nil {
		t.Fatalf("ValidateProviderToolDefinitions: %v", err)
	}
	props := defs[0].Schema.(map[string]any)["properties"].(map[string]any)
	requiredCapabilities := props["required_capabilities"].(map[string]any)
	capabilityProps := requiredCapabilities["properties"].(map[string]any)
	automation := capabilityProps["automation_with_unlock"].(map[string]any)
	if got := automation["type"]; got != "number" {
		t.Fatalf("automation type = %#v, want number", got)
	}
	scores := props["scores"].(map[string]any)
	scoreItems := scores["items"].(map[string]any)
	if got := scoreItems["type"]; got != "number" {
		t.Fatalf("scores item type = %#v, want number", got)
	}
}

func TestValidateGeneratedEmitToolSchemasForSourceRejectsUnloweredContractRefs(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"market-research-agent": {
				ID:         "market-research-agent",
				Role:       "market_research",
				EmitEvents: []string{"category.assessed"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"unsupported": {Type: "NotDeclared"},
					},
				},
			},
		},
	})

	errs := ValidateGeneratedEmitToolSchemasForSource(source)
	if len(errs) != 1 {
		t.Fatalf("errors = %#v, want one provider schema error", errs)
	}
	if got := errs[0].Error(); !strings.Contains(got, "unsupported JSON Schema type \"NotDeclared\"") {
		t.Fatalf("error = %q, want unsupported type", got)
	}
}

func TestEmitRegistry_DoesNotGenerateMissingSchemaForChildLocalAgentEmitFixture(t *testing.T) {
	repoRoot, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-required-agents-child")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	source := semanticview.Wrap(bundle)
	registry := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))

	if missing := registry.GeneratedEmitSchemasForAgentRoles(); len(missing) != 0 {
		t.Fatalf("generated missing schemas = %v, active schemas = %#v", missing, registry.activeSchemas)
	}
}
