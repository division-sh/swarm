package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const nativeCatalogWorkflowStateShape = `func workflowStateDebugRows(selected catalogOperatorEventLister) (string, error) {
if selected == nil { return "", nil }
rows, err := storetest.ReadWorkflowStateObservationRows(testAuthorActivityContext(context.Background()), selected)
if err != nil { return "", err }
out := []string{}
for _, row := range rows {
out = append(out, fmt.Sprintf("{entity_id:%s flow_instance:%s state:%s}", row.EntityID, row.FlowInstance, row.CurrentState))
}
if len(out) == 0 { return "[]", nil }
return "[" + strings.Join(out, ", ") + "]", nil
}`

func TestNativeCatalogWorkflowStateRecipesPreserveSemanticAssertionsAndPhysicalFormatting(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-workflow-state" {
			continue
		}
		count++
		if row.Function == "workflowStateDebugRows" {
			want, err := canonicalFunction(nativeCatalogWorkflowStateShape)
			got, parseErr := canonicalFunction(row.After)
			if err != nil || parseErr != nil || got != want {
				t.Fatalf("diagnostic formatting, emptiness or refusal changed: %v / %v", err, parseErr)
			}
			for _, pair := range [][2]string{
				{"context.Background()", "context.TODO()"}, {"selected)", "foreignStore)"},
				{"if err != nil", "if false"}, {"row.EntityID", "row.FlowInstance"},
				{"return \"[]\", nil", "return \"\", nil"}, {"strings.Join(out, \", \")", "strings.Join(out, \"\")"},
			} {
				changed := strings.Replace(row.After, pair[0], pair[1], 1)
				got, err := canonicalFunction(changed)
				if changed == row.After || (err == nil && got == want) {
					t.Fatalf("lost native context/owner or diagnostic detail accepted: %v", pair)
				}
			}
			continue
		}
		if row.Function != "assertEntityState" && row.Function != "TestTier11Probe" {
			t.Fatal("unreviewed workflow diagnostic consumer")
		}
		actual := undoNativeCatalogReaderProjection(t, row)
		actual = strings.Replace(actual, "workflowStateDebugRows(selected)", "workflowStateDebugRows(db)", 1)
		actual = strings.Replace(actual, "workflowStateDebugRows(reader)", "workflowStateDebugRows(h.db)", 1)
		want, err := canonicalFunction(row.Before)
		got, parseErr := canonicalFunction(actual)
		if err != nil || parseErr != nil || got != want {
			t.Fatalf("%s changed semantic assertion or diagnostic probe beyond native read propagation: %v / %v", row.Function, err, parseErr)
		}
	}
	if count != 3 {
		t.Fatalf("workflow diagnostic recipes=%d, want three", count)
	}
}

func TestNativeCatalogWorkflowStateOwnerPreservesPhysicalRowsAndErrors(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "pipelinepersistence", "workflow_projection_observation.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"SELECT entity_id::text, COALESCE(flow_instance, ''), current_state FROM entity_state ORDER BY created_at ASC",
		"SELECT entity_id, COALESCE(flow_instance, ''), current_state FROM entity_state ORDER BY created_at ASC",
		"defer rows.Close()", "if err := rows.Err(); err != nil", "rows.Scan(&row.EntityID, &row.FlowInstance, &row.CurrentState)",
	} {
		if !strings.Contains(string(data), fragment) {
			t.Fatalf("physical state diagnostic contract missing %q", fragment)
		}
	}
}
