package tools

import (
	"reflect"
	"strings"
	"testing"

	models "swarm/internal/runtime/core/actors"
)

func TestOrderedEntityFieldNamesFromInput_NormalizesSortsAndDedupes(t *testing.T) {
	got := orderedEntityFieldNamesFromInput([]string{" status ", "", "score", "status", "score", "priority"})
	want := []string{"priority", "score", "status"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("orderedEntityFieldNamesFromInput() = %#v, want %#v", got, want)
	}
}

func TestEntityStateBaseQuery_OptionallyIncludesFlowInstance(t *testing.T) {
	payload := map[string]any{
		"flow_instance": " review/inst-1 ",
	}

	clausesWithoutFlow, argsWithoutFlow := entityStateBaseQuery(nil, models.AgentConfig{}, payload, false)
	whereWithoutFlow := joinEntityStateWhere(clausesWithoutFlow)
	if whereWithoutFlow != "" {
		t.Fatalf("where without flow = %q", whereWithoutFlow)
	}
	if !reflect.DeepEqual(argsWithoutFlow, []any{}) {
		t.Fatalf("args without flow = %#v", argsWithoutFlow)
	}

	clausesWithFlow, argsWithFlow := entityStateBaseQuery(nil, models.AgentConfig{}, payload, true)
	whereWithFlow := joinEntityStateWhere(clausesWithFlow)
	if whereWithFlow != " WHERE flow_instance = $1" {
		t.Fatalf("where with flow = %q", whereWithFlow)
	}
	if !reflect.DeepEqual(argsWithFlow, []any{"review/inst-1"}) {
		t.Fatalf("args with flow = %#v", argsWithFlow)
	}
}

func TestExistingEntityFlowInstanceSchemaDocumentsRootSemantics(t *testing.T) {
	entries := genericEntityRuntimeContractSchemas(entityReadTargetInputSchema(nil))
	for _, toolName := range []string{"get_entity", "save_entity_field", "query_entities", "query_metrics", "search_entities"} {
		properties := entries[toolName].InputSchema["properties"].(map[string]any)
		flowSchema := properties["flow_instance"].(map[string]any)
		description := strings.TrimSpace(flowSchema["description"].(string))
		if !strings.Contains(description, "concrete flow instance path") || !strings.Contains(description, "declared semantic flow root") || !strings.Contains(description, "descendant") {
			t.Fatalf("%s flow_instance description = %q, want concrete path and semantic-root descendant guidance", toolName, description)
		}
	}
	createProperties := entries["create_entity"].InputSchema["properties"].(map[string]any)
	if _, ok := createProperties["subject_id"]; ok {
		t.Fatalf("create_entity schema should not expose subject_id: %#v", createProperties)
	}
	searchProperties := entries["search_entities"].InputSchema["properties"].(map[string]any)
	if _, ok := searchProperties["subject_id"]; ok {
		t.Fatalf("search_entities schema should not expose subject_id: %#v", searchProperties)
	}
	createFlowSchema := createProperties["flow_instance"].(map[string]any)
	if _, ok := createFlowSchema["description"]; ok {
		t.Fatalf("create_entity flow_instance gained existing-entity guidance: %#v", createFlowSchema)
	}
}
