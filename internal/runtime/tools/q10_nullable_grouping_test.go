package tools

import "testing"

func TestQ10AgentNullLifecycleGrouping(t *testing.T) {
	rows := []map[string]any{
		{"entity_id": "import", "current_state": nil}, {"entity_id": "unknown", "current_state": "unknown"},
		{"entity_id": "literal_nil", "current_state": "<nil>"}, {"entity_id": "no_stage", "current_state": nil},
	}
	groups := groupEntityStateRows(rows, "current_state", []string{"current_state"})
	metrics, err := aggregateEntityMetricGroups("count", "", "current_state", rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 3 || len(metrics) != 3 {
		t.Fatalf("absence collided: groups=%v metrics=%v", groups, metrics)
	}
	for i, want := range []any{"<nil>", "unknown", nil} {
		if groups[i]["group_key"] != want || metrics[i]["group_key"] != want {
			t.Fatalf("nullable sort/key %d: %v %v", i, groups, metrics)
		}
		count := 1
		if want == nil {
			count = 2
		}
		if len(groups[i]["items"].([]map[string]any)) != count || metrics[i]["value"] != count {
			t.Fatalf("membership/count changed: %v %v", groups, metrics)
		}
	}
	projected := projectAgentEntityStateRow(map[string]any{"current_state": nil, "stage_defined": nil, "gates": nil, "accumulator": nil, "lifecycle_created_at": nil, "lifecycle_updated_at": nil, "bookkeeping": map[string]any{"private": true}, "loops": []any{}})
	for _, key := range []string{"current_state", "stage_defined", "gates", "accumulator", "lifecycle_created_at", "lifecycle_updated_at"} {
		if value, ok := projected[key]; !ok || value != nil {
			t.Fatalf("required null %s lost: %v", key, projected)
		}
	}
	for _, key := range []string{"bookkeeping", "loops"} {
		if _, ok := projected[key]; ok {
			t.Fatalf("private %s leaked", key)
		}
	}
}
