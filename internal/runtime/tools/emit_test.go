package tools

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	canonicalrouting "github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/requiredagentsparentconnect"
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
	actor := models.AgentConfig{
		ID:   "campaign-coordinator",
		Role: "campaign_coordinator",
	}

	tools := registry.GenerateEmitToolsForActor(actor, nil)
	for _, def := range tools {
		if def.Name == "emit_scan_requested" {
			if strings.TrimSpace(def.Usage) == "" {
				t.Fatal("emit_scan_requested usage hint is empty")
			}
			if strings.Contains(strings.ToLower(def.Usage), "cel") {
				t.Fatalf("emit usage should not mention CEL: %q", def.Usage)
			}
			schema := def.Schema.(map[string]any)
			if err := ValidatePayloadAgainstSchema(schema, map[string]any{
				"entity_id": "scan-1",
				"extra":     "must fail provider-visible schema",
			}); err == nil {
				t.Fatal("role-fallback delivered emit schema accepted undeclared field")
			}
			_, runtimeSchema, ok := registry.EventSchemaForActorTool(actor, "emit_scan_requested")
			if !ok {
				t.Fatal("role-fallback runtime schema not found")
			}
			if err := ValidatePayloadAgainstSchema(runtimeSchema.Schema, map[string]any{
				"entity_id": "scan-1",
				"extra":     "must fail runtime schema",
			}); err == nil {
				t.Fatal("role-fallback runtime emit schema accepted undeclared field")
			}
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

func TestGenerateEmitToolsForActor_GeneratedSchemaIsClosedRequiredAndRejectsUndeclaredPayload(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Enums: map[string]runtimecontracts.EnumTypeDecl{
				"SignalStrength": {Values: []string{"weak", "strong"}},
			},
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"DiscoveryContext": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"source": {Type: "text"},
						"score":  {Type: "numeric(5,2)"},
					},
				},
			},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analysis-agent": {
				ID:         "analysis-agent",
				Role:       "analysis",
				EmitEvents: []string{"vertical.derived"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"vertical.derived": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"opportunity_name":     {Type: "text"},
						"signal_strength":      {Type: "SignalStrength"},
						"discovery_context":    {Type: "DiscoveryContext"},
						"derivation_rationale": {Type: "text"},
					},
				},
			},
		},
	})
	if errs := ValidateGeneratedToolSchemaClosureForSource(source); len(errs) > 0 {
		t.Fatalf("ValidateGeneratedToolSchemaClosureForSource errors = %#v", errs)
	}
	registry := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))
	defs := registry.GenerateEmitToolsForActor(models.AgentConfig{
		ID:         "analysis-agent",
		Role:       "analysis",
		EmitEvents: []string{"vertical.derived"},
	}, nil)
	if len(defs) != 1 {
		t.Fatalf("tool count = %d, want 1 (%#v)", len(defs), defs)
	}
	schema := defs[0].Schema.(map[string]any)
	required := requiredSchemaSet(schema["required"])
	for _, field := range []string{"opportunity_name", "signal_strength", "discovery_context", "derivation_rationale"} {
		if _, ok := required[field]; !ok {
			t.Fatalf("generated emit schema missing required field %s in %#v", field, schema["required"])
		}
	}
	props := schema["properties"].(map[string]any)
	if got := props["signal_strength"].(map[string]any)["enum"]; len(schemaEnumValues(got)) != 2 {
		t.Fatalf("signal_strength enum = %#v, want two values", got)
	}
	contextSchema := props["discovery_context"].(map[string]any)
	if got := contextSchema["additionalProperties"]; got != false {
		t.Fatalf("discovery_context additionalProperties = %#v, want false", got)
	}
	if err := ValidatePayloadAgainstSchema(schema, map[string]any{
		"opportunity_name":     "AP automation",
		"signal_strength":      "strong",
		"discovery_context":    map[string]any{"source": "call", "score": 98.5},
		"derivation_rationale": "evidence",
		"vertical_id":          "parent-vertical",
	}); err == nil {
		t.Fatal("generated emit schema accepted undeclared vertical_id")
	}
	if err := ValidatePayloadAgainstSchema(schema, map[string]any{
		"opportunity_name":     "AP automation",
		"discovery_context":    map[string]any{"source": "call", "score": 98.5},
		"derivation_rationale": "evidence",
	}); err == nil {
		t.Fatal("generated emit schema accepted missing declared signal_strength")
	}
}

func TestGeneratedToolSchemaClosureRejectsOpenOrPartialObjectSchemas(t *testing.T) {
	errs := validateGeneratedJSONSchema("tool.input", map[string]any{
		"type": "object",
		"properties": map[string]any{
			"value": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"mode": map[string]any{"type": "string", "enum": []any{}},
				},
				"additionalProperties": false,
			},
		},
		"additionalProperties": false,
	})
	got := strings.Join(generatedSchemaClosureErrorStrings(errs), "\n")
	for _, want := range []string{
		"tool.input object schema must require declared property value",
		"tool.input.properties.value object schema must require declared property mode",
		"tool.input.properties.value.properties.mode enum schema must declare at least one allowed value",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("errors = %q, want %q", got, want)
		}
	}
}

func generatedSchemaClosureErrorStrings(errs []error) []string {
	out := make([]string, 0, len(errs))
	for _, err := range errs {
		out = append(out, err.Error())
	}
	return out
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

func TestValidateGeneratedEmitToolSchemasForSourceUsesPackageOwningFlowMode(t *testing.T) {
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/tools/emit_test.go:file-scope"))
	canonicalrouting.ProveSource(t, canonicalrouting.SourceID("internal/runtime/tools/emit_test.go:TestValidateGeneratedEmitToolSchemasForSourceUsesPackageOwningFlowMode"))
	root := t.TempDir()
	writeEmitFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: provider-schema-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
  - id: other
    flow: other
    mode: static
`)
	writeEmitFixtureFile(t, filepath.Join(root, "entities.yaml"), "item:\n  item_id: uuid\n")
	writeEmitFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: provider-schema-validation\n")
	writeEmitFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeEmitFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeEmitFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeEmitFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
flows: []
`)
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
`)
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "support", "tools.yaml"), "{}\n")
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
local.done:
  unsupported: NotDeclared
`)
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), `
flow-agent:
  id: flow-agent
  role: flow_agent
  mode: task
  emit_events:
    - local.done
`)
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "other", "package.yaml"), `
name: other
version: "1.0.0"
flows: []
`)
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "other", "schema.yaml"), `
name: other
initial_state: waiting
states:
  - waiting
`)
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "other", "policy.yaml"), "{}\n")
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "other", "tools.yaml"), "{}\n")
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "other", "events.yaml"), `
local.done:
  ok: string
`)
	writeEmitFixtureFile(t, filepath.Join(root, "flows", "other", "agents.yaml"), "{}\n")

	repoRoot, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	errs := ValidateGeneratedEmitToolSchemasForSource(semanticview.Wrap(bundle))
	if len(errs) != 1 {
		t.Fatalf("errors = %#v, want one provider schema error", errs)
	}
	got := errs[0].Error()
	for _, want := range []string{"flow-agent", "emit_local_done", "unsupported JSON Schema type \"NotDeclared\""} {
		if !strings.Contains(got, want) {
			t.Fatalf("error = %q, want substring %q", got, want)
		}
	}
}

func writeEmitFixtureFile(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(content, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func TestEmitRegistry_DoesNotGenerateMissingSchemaForChildLocalAgentEmitFixture(t *testing.T) {
	bundle := requiredagentsparentconnect.LoadBundle(t)
	source := semanticview.Wrap(bundle)
	registry := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))

	if missing := registry.GeneratedEmitSchemasForAgentRoles(); len(missing) != 0 {
		t.Fatalf("generated missing schemas = %v, active schemas = %#v", missing, registry.activeSchemas)
	}
}
