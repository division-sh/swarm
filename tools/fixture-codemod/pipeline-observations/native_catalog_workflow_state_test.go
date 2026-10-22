package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
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
	if err := nativeCatalogWorkflowStateOwnerError(string(data)); err != nil {
		t.Fatal(err)
	}
}

func nativeCatalogWorkflowStateOwnerError(source string) error {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "owner.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	fn, err := uniqueFunction(file, "ReadWorkflowStateObservationRows")
	if err != nil {
		return err
	}
	body := source[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset]
	for _, fragment := range []string{
		"SELECT es.run_id::text, es.entity_id::text, COALESCE(es.flow_instance, ''), fi.current_state, es.fields",
		"SELECT es.run_id, es.entity_id, COALESCE(es.flow_instance, ''), fi.current_state, es.fields",
		"FROM entity_state es LEFT JOIN flow_instances fi",
		"ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance AND fi.entity_id=es.entity_id",
		"ORDER BY es.created_at ASC",
		"defer rows.Close()", "if err := rows.Err(); err != nil",
		"rows.Scan(&row.RunID, &row.EntityID, &row.FlowInstance, &row.CurrentState, &fields)",
		"row.Fields = append(json.RawMessage(nil), fields...)",
	} {
		if !strings.Contains(body, fragment) {
			return fmt.Errorf("physical state diagnostic contract missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		"FROM entity_state es LEFT JOIN flow_instances fi",
		"ON fi.run_id=es.run_id AND fi.instance_path=es.flow_instance AND fi.entity_id=es.entity_id",
		"ORDER BY es.created_at ASC",
	} {
		if strings.Count(body, fragment) != 2 {
			return fmt.Errorf("both database projections require the exact physical-row contract %q", fragment)
		}
	}
	shape := ""
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range group.Specs {
			row, ok := spec.(*ast.TypeSpec)
			if ok && row.Name.Name == "WorkflowStateObservationRow" {
				if shape != "" {
					return fmt.Errorf("ambiguous physical state observation type")
				}
				shape = strings.Join(strings.Fields(source[set.Position(row.Pos()).Offset:set.Position(row.End()).Offset]), " ")
			}
		}
	}
	if !strings.Contains(shape, "CurrentState *string") || !strings.Contains(shape, "Fields json.RawMessage") {
		return fmt.Errorf("physical state observation must retain explicit header absence and authored fields")
	}
	return nil
}

func TestNativeCatalogWorkflowStateOwnerRejectsLostHeaderAndFieldEvidence(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "pipelinepersistence", "workflow_projection_observation.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	source := string(data)
	if err := nativeCatalogWorkflowStateOwnerError(source); err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{
		{"SELECT es.run_id::text, es.entity_id::text", "SELECT es.entity_id::text"},
		{"SELECT es.run_id, es.entity_id", "SELECT es.entity_id"},
		{"fi.current_state, es.fields", "fi.current_state"},
		{"FROM entity_state es LEFT JOIN flow_instances fi", "FROM entity_state es JOIN flow_instances fi"},
		{"fi.run_id=es.run_id AND ", ""},
		{" AND fi.entity_id=es.entity_id", ""},
		{"fi.instance_path=es.flow_instance", "fi.instance_path='foreign'"},
		{"ORDER BY es.created_at ASC", "ORDER BY es.created_at DESC"},
		{"defer rows.Close()", ""},
		{"if err := rows.Err(); err != nil", "if false"},
		{"rows.Scan(&row.RunID, &row.EntityID", "rows.Scan(&row.EntityID, &row.RunID"},
		{"row.Fields = append(json.RawMessage(nil), fields...)", "row.Fields = fields"},
		{"CurrentState *string", "CurrentState string"},
		{"Fields                        json.RawMessage", "Fields                        string"},
	} {
		t.Run(pair[0], func(t *testing.T) {
			mutant := strings.Replace(source, pair[0], pair[1], 1)
			if mutant == source {
				t.Fatal("negative control did not change its exact source binding")
			}
			if err := nativeCatalogWorkflowStateOwnerError(mutant); err == nil {
				t.Fatal("lost physical header/field evidence was accepted")
			}
		})
	}
}
