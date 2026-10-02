package tools

import (
	"reflect"
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func TestProjectAgentEntityStateRowUsesClosedTopLevelAllowlist(t *testing.T) {
	row := map[string]any{
		"entity_id":   "entity-1",
		"fields":      map[string]any{"status": "open"},
		"bookkeeping": map[string]any{"private_fact": "must-not-leak"},
		"loops":       map[string]any{"private_activation": true},
		"unknown":     "must-not-leak",
	}
	projected := projectAgentEntityStateRow(row)
	for _, forbidden := range []string{"bookkeeping", "loops", "unknown"} {
		if _, ok := projected[forbidden]; ok {
			t.Fatalf("agent projection exposed %q: %#v", forbidden, projected)
		}
	}
	projected["fields"].(map[string]any)["status"] = "changed"
	if row["fields"].(map[string]any)["status"] != "open" {
		t.Fatalf("agent projection aliases stored fields: %#v", row["fields"])
	}
}

func TestOrderedEntityFieldNamesFromInput_NormalizesSortsAndDedupes(t *testing.T) {
	got := orderedEntityFieldNamesFromInput([]string{" status ", "", "score", "status", "score", "priority"})
	want := []string{"priority", "score", "status"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("orderedEntityFieldNamesFromInput() = %#v, want %#v", got, want)
	}
}

func TestEntityStateQuery_OptionallyIncludesFlowInstance(t *testing.T) {
	t.Parallel()

	payload := map[string]any{
		"flow_instance": " review/inst-1 ",
	}
	ctx := runtimecorrelation.WithRunID(unmanagedToolTestContext(), "00000000-0000-0000-0000-000000000001")

	queryWithoutFlow, err := entityStateQueryForContractRun(ctx, nil, entityruntime.Contract{}, payload, false)
	if err != nil {
		t.Fatalf("query without flow: %v", err)
	}
	if queryWithoutFlow.RunID != "00000000-0000-0000-0000-000000000001" {
		t.Fatalf("run_id without flow = %q", queryWithoutFlow.RunID)
	}
	if queryWithoutFlow.RequestedFlowExact != "" || queryWithoutFlow.RequestedFlowScope.Root != "" {
		t.Fatalf("query without flow should not include requested flow: %#v", queryWithoutFlow)
	}

	queryWithFlow, err := entityStateQueryForContractRun(ctx, nil, entityruntime.Contract{}, payload, true)
	if err != nil {
		t.Fatalf("query with flow: %v", err)
	}
	if queryWithFlow.RequestedFlowExact != "review/inst-1" {
		t.Fatalf("requested flow exact = %q", queryWithFlow.RequestedFlowExact)
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
	if _, ok := entries["create_entity"]; ok {
		t.Fatal("retired create_entity tool remains in the runtime catalog")
	}
	searchProperties := entries["search_entities"].InputSchema["properties"].(map[string]any)
	if _, ok := searchProperties["subject_id"]; ok {
		t.Fatalf("search_entities schema should not expose subject_id: %#v", searchProperties)
	}
}
