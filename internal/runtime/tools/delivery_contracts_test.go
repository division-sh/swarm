package tools_test

import (
	"strings"
	"testing"

	models "swarm/internal/runtime/core/actors"
	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
)

func TestToolDefinitionsForActor_DeriveWave1EntitySchemasFromActorContract(t *testing.T) {
	actor := models.AgentConfig{
		ID:    "analyzer",
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

	createSchema := defByName["create_entity"]
	if createSchema == nil {
		t.Fatal("expected create_entity definition")
	}
	props, _ := createSchema["properties"].(map[string]any)
	if _, ok := props["entity_id"]; ok {
		t.Fatalf("create_entity schema should not expose entity_id: %#v", props)
	}
	if _, ok := props["entity_type"]; ok {
		t.Fatalf("create_entity schema should not expose entity_type: %#v", props)
	}
	if _, ok := props["subject_id"]; ok {
		t.Fatalf("create_entity schema should not expose subject_id: %#v", props)
	}
	fields, _ := props["fields"].(map[string]any)
	fieldProps, _ := fields["properties"].(map[string]any)
	if _, ok := fieldProps["status"]; !ok {
		t.Fatalf("create_entity fields schema missing status: %#v", fieldProps)
	}
	if _, ok := fieldProps["metadata"]; !ok {
		t.Fatalf("create_entity fields schema missing metadata: %#v", fieldProps)
	}
	if additional, ok := fields["additionalProperties"].(bool); !ok || additional {
		t.Fatalf("create_entity fields additionalProperties = %#v, want false", fields["additionalProperties"])
	}

	saveSchema := defByName["save_entity_field"]
	saveProps, _ := saveSchema["properties"].(map[string]any)
	fieldEnum, _ := saveProps["field"].(map[string]any)
	values, _ := fieldEnum["enum"].([]any)
	if !containsAnyString(values, "status") {
		t.Fatalf("save_entity_field field enum = %#v, want status", values)
	}
	if !containsAnyString(values, "metadata") {
		t.Fatalf("save_entity_field field enum = %#v, want metadata", values)
	}
	if !containsAnyString(values, "metadata.region") {
		t.Fatalf("save_entity_field field enum = %#v, want metadata.region", values)
	}
	if containsAnyString(values, "entity_id") {
		t.Fatalf("save_entity_field field enum = %#v, should not include envelope field entity_id", values)
	}
	summaryLines := llm.AgentVisibleToolSummaryLinesForActor(actor, defs)
	wantWritablePathLine := "Writable entity paths for save_entity_field in this turn: " + strings.Join(anyStrings(values), ", ")
	if !containsString(summaryLines, wantWritablePathLine) {
		t.Fatalf("agent-visible writable path summary = %#v, want %q", summaryLines, wantWritablePathLine)
	}

	searchSchema := defByName["search_entities"]
	searchProps, _ := searchSchema["properties"].(map[string]any)
	searchTarget, _ := searchProps["entity_type"].(map[string]any)
	searchTargets, _ := searchTarget["enum"].([]any)
	if !containsAnyString(searchTargets, "review.review_subject") {
		t.Fatalf("search_entities entity_type enum = %#v, want review.review_subject", searchTargets)
	}
	filterSchema, _ := searchProps["filter"].(map[string]any)
	filterProps, _ := filterSchema["properties"].(map[string]any)
	if _, ok := filterProps["metadata.region"]; !ok {
		t.Fatalf("search_entities filter schema missing metadata.region: %#v", filterProps)
	}
	if _, ok := filterProps["status"]; !ok {
		t.Fatalf("search_entities filter schema missing status: %#v", filterProps)
	}
	if additional, ok := filterSchema["additionalProperties"].(bool); !ok || additional {
		t.Fatalf("search_entities filter additionalProperties = %#v, want false", filterSchema["additionalProperties"])
	}

	querySchema := defByName["query_entities"]
	queryProps, _ := querySchema["properties"].(map[string]any)
	queryTarget, _ := queryProps["entity_type"].(map[string]any)
	queryTargets, _ := queryTarget["enum"].([]any)
	if !containsAnyString(queryTargets, "review.review_subject") {
		t.Fatalf("query_entities entity_type enum = %#v, want review.review_subject", queryTargets)
	}
	selectSchema, _ := queryProps["select"].(map[string]any)
	selectItems, _ := selectSchema["items"].(map[string]any)
	selectEnum, _ := selectItems["enum"].([]any)
	if !containsAnyString(selectEnum, "metadata.region") {
		t.Fatalf("query_entities select enum = %#v, want metadata.region", selectEnum)
	}
	if !containsAnyString(selectEnum, "entity_id") {
		t.Fatalf("query_entities select enum = %#v, want entity_id", selectEnum)
	}

	metricSchema := defByName["query_metrics"]
	metricProps, _ := metricSchema["properties"].(map[string]any)
	metricTarget, _ := metricProps["entity_type"].(map[string]any)
	metricTargets, _ := metricTarget["enum"].([]any)
	if !containsAnyString(metricTargets, "review.review_subject") {
		t.Fatalf("query_metrics entity_type enum = %#v, want review.review_subject", metricTargets)
	}
	metricField, _ := metricProps["field"].(map[string]any)
	metricFieldEnum, _ := metricField["enum"].([]any)
	if !containsAnyString(metricFieldEnum, "status") {
		t.Fatalf("query_metrics field enum = %#v, want status", metricFieldEnum)
	}
	if containsAnyString(metricFieldEnum, "metadata") {
		t.Fatalf("query_metrics field enum = %#v, should not include composite metadata", metricFieldEnum)
	}
}

func containsAnyString(values []any, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func anyStrings(values []any) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if got, ok := value.(string); ok {
			out = append(out, got)
		}
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
