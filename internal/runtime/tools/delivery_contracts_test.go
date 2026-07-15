package tools_test

import (
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
)

func TestToolDefinitionsForActor_DeriveRoleScopedEntitySchemasFromActorContract(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "analyzer",
		// Type is config-authored; it must not be trusted to restore legacy tools.
		Type:  "internal",
		Role:  "analyzer",
		Tools: []string{"create_entity", "get_entity", "save_entity_field", "search_entities", "query_entities", "query_metrics"},
	}
	bundle := loadWave1EntityToolBundle(t, actor, "review", "review_subject", `
types:
  metadata:
    region: text
    score_band: score_band
enums:
  score_band: [low, medium, high]
`, `
review_subject:
  status: text
  priority: integer
  active: boolean
  metadata: metadata
`)

	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		WorkflowSource: semanticview.Wrap(bundle),
	})
	defs := exec.ToolDefinitionsForActor(actor)
	defByName := make(map[string]map[string]any, len(defs))
	for _, def := range defs {
		schema, _ := def.Schema.(map[string]any)
		defByName[def.Name] = schema
	}

	for _, legacy := range []string{"create_entity", "get_entity", "save_entity_field", "search_entities", "query_entities", "query_metrics"} {
		if _, ok := defByName[legacy]; ok {
			t.Fatalf("legacy entity tool %q remained visible in %#v", legacy, toolDefinitionNames(defs))
		}
	}
	caps := exec.ToolCapabilitiesForActor(actor, []string{"create_entity", "get_entity", "save_entity_field", "search_entities", "query_entities", "query_metrics"}, nil)
	for _, legacy := range []string{"create_entity", "get_entity", "save_entity_field", "search_entities", "query_entities", "query_metrics"} {
		cap, ok := caps.Capability(legacy)
		if !ok || cap.Visible || cap.Callable {
			t.Fatalf("legacy entity tool %q capability = %#v ok=%v, want denied", legacy, cap, ok)
		}
		_, err := exec.Execute(runtimetools.WithActor(unmanagedToolTestContext(), actor), legacy, map[string]any{})
		failure, ok := runtimefailures.As(err)
		if !ok || failure.Failure.Class != runtimefailures.ClassAuthorizationDenied || failure.Failure.Detail.Code != "tool_not_allowed" {
			t.Fatalf("direct legacy %s execution error = %v, want typed authorization denial", legacy, err)
		}
	}

	readSchema := defByName["read_review_subject"]
	if readSchema == nil {
		t.Fatalf("expected read_review_subject definition, got %v", toolDefinitionNames(defs))
	}
	props, _ := readSchema["properties"].(map[string]any)
	if len(props) != 0 {
		t.Fatalf("read_review_subject should not accept selector/entity_id input, got schema %#v", readSchema)
	}

	metadataSchema := defByName["read_review_subject_metadata"]
	if metadataSchema == nil {
		t.Fatalf("expected read_review_subject_metadata definition, got %v", toolDefinitionNames(defs))
	}
	metadataProps, _ := metadataSchema["properties"].(map[string]any)
	if len(metadataProps) != 0 {
		t.Fatalf("read_review_subject_metadata should not accept selector/entity_id input, got schema %#v", metadataSchema)
	}
}

func TestToolDefinitionsForActor_ExcludeForeignReadTargets(t *testing.T) {
	actor := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "researcher",
		Role:          "researcher",
		Tools:         []string{"search_entities", "query_entities", "query_metrics"},
	}
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"discovery": {
			EntitiesYAML: `
campaign:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(actor),
		},
		"signal-search": {
			EntitiesYAML: `
signal:
  signal_strength: integer
  processed: boolean
`,
		},
	})

	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		WorkflowSource: semanticview.Wrap(bundle),
	})
	defs := exec.ToolDefinitionsForActor(actor)
	defByName := make(map[string]map[string]any, len(defs))
	for _, def := range defs {
		schema, _ := def.Schema.(map[string]any)
		defByName[def.Name] = schema
	}

	for _, legacy := range []string{"search_entities", "query_entities", "query_metrics"} {
		if _, ok := defByName[legacy]; ok {
			t.Fatalf("legacy query tool %q remained visible in %#v", legacy, toolDefinitionNames(defs))
		}
	}
	if _, ok := defByName["read_campaign"]; !ok {
		t.Fatalf("expected read_campaign role-scoped definition, got %#v", toolDefinitionNames(defs))
	}
}

func TestToolDefinitionsForActor_HideEntityScopedUniversalToolsWithoutActorContract(t *testing.T) {
	lifecycle := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "lifecycle-coordinator",
		Role:          "lifecycle-coordinator",
		Tools:         []string{"schedule"},
	}
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"validation": {
			EntitiesYAML: `
validation_case:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "validator", Role: "validator"}),
		},
		"scoring": {
			EntitiesYAML: `
vertical:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "scorer", Role: "scorer"}),
		},
	})

	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		WorkflowSource: semanticview.Wrap(bundle),
	})
	defs := exec.ToolDefinitionsForActor(lifecycle)
	names := toolDefinitionNames(defs)
	for _, toolName := range []string{"create_entity", "get_entity", "save_entity_field", "search_entities", "query_entities", "query_metrics"} {
		if containsString(names, toolName) {
			t.Fatalf("expected %s to be hidden for actor with no entity contract/read scope, got %v", toolName, names)
		}
	}
}

func TestToolDefinitionsForActor_RetireSameNameEntityToolOverrideWithoutActorContract(t *testing.T) {
	lifecycle := models.AgentConfig{
		ExecutionMode: "live",
		ID:            "lifecycle-coordinator",
		Role:          "lifecycle-coordinator",
		Tools:         []string{"get_entity"},
	}
	bundle := loadWave1EntityToolMultiFlowBundle(t, map[string]entityToolFlowFixture{
		"lifecycle": {
			AgentsYAML: entityToolAgentYAML(lifecycle),
			ToolsYAML: `
get_entity:
  description: Lifecycle-owned external lookup override.
  handler_type: http
  input_schema:
    type: object
    properties:
      query:
        type: string
    required: [query]
  http:
    method: POST
    url: https://example.invalid/get_entity
`,
		},
		"validation": {
			EntitiesYAML: `
validation_case:
  status: text
`,
			AgentsYAML: entityToolAgentYAML(models.AgentConfig{ExecutionMode: "live", ID: "validator", Role: "validator"}),
		},
	})

	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		WorkflowSource: semanticview.Wrap(bundle),
	})
	defs := exec.ToolDefinitionsForActor(lifecycle)
	defByName := make(map[string]llm.ToolDefinition, len(defs))
	for _, def := range defs {
		defByName[def.Name] = def
	}
	if _, ok := defByName["get_entity"]; ok {
		t.Fatalf("same-name get_entity override remained visible, got %v", toolDefinitionNames(defs))
	}
}

func toolDefinitionNames(defs []llm.ToolDefinition) []string {
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.Name)
	}
	return names
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
