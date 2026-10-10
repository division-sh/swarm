package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func catalogCreationRecipePreservesAssertions(row recipe) bool {
	source := row.After
	for _, step := range []struct{ native, oldStart, marker string }{
		{"\t\tcount, err := storetest.CountWorkflowHeadersCreatedSince(testAuthorActivityContext(context.Background()), selected, since)\n\t\tif err != nil {\n", "\t\tvar count int\n", "\t\t\tt.Fatalf(\"query flow_instances: %v\", err)"},
		{"\tinstanceCount, err := storetest.CountWorkflowHeadersForPathCreatedSince(testAuthorActivityContext(context.Background()), selected, instancePath, since)\n\tif err != nil {\n", "\tvar instanceCount int\n", "\t\tt.Fatalf(\"query flow instance row: %v\", err)"},
		{"\t\traw, found, err := storetest.ReadLatestWorkflowFieldsForPath(testAuthorActivityContext(context.Background()), selected, instancePath)\n\t\tif err == nil && !found {\n", "\t\tvar raw []byte\n", "\t\t\tt.Fatalf(\"expected flow instance fields for %s\", instancePath)"},
		{"\t\tcount, err := storetest.CountEventNameStorage(testAuthorActivityContext(context.Background()), selected, autoEmitted)\n\t\tif err != nil {\n", "\t\tvar count int\n", "\t\t\tt.Fatalf(\"query flow auto-emitted event: %v\", err)"},
	} {
		if strings.Count(source, step.native) != 1 {
			return false
		}
		end := strings.Index(row.Before, step.marker)
		if end < 0 {
			return false
		}
		start := strings.LastIndex(row.Before[:end], step.oldStart)
		if start < 0 {
			return false
		}
		source = strings.Replace(source, step.native, row.Before[start:end], 1)
	}
	source = strings.Replace(source, "selected catalogOperatorEventLister", "db *sql.DB", 1)
	source = strings.Replace(source, "if selected == nil", "if db == nil", 1)
	source = strings.Replace(source, "selected reader is required for flow_instance_created assertions", "database is required for flow_instance_created assertions", 1)
	want, err := canonicalFunction(row.Before)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogCreationRecipesPreserveEveryExpectation(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-creation-observation" {
			continue
		}
		count++
		if row.Function == "assertFlowInstanceCount" {
			shape := `func assertFlowInstanceCount(t testing.TB, selected catalogOperatorEventLister, since time.Time, want int) {
t.Helper()
count, err := storetest.CountWorkflowHeadersCreatedSince(testAuthorActivityContext(context.Background()), selected, since)
if err != nil { t.Fatalf("query flow_instances count: %v", err) }
if count != want { t.Fatalf("flow_instances count = %d, want %d", count, want) }
}`
			want, err := canonicalFunction(shape)
			got, parseErr := canonicalFunction(row.After)
			if err != nil || parseErr != nil || want != got {
				t.Fatal("physical header count changed scope, cutoff, owner or assertion")
			}
			continue
		}
		if row.Function != "assertFlowInstanceCreated" || !catalogCreationRecipePreservesAssertions(row) {
			t.Fatal("creation observation changed validation, scope, fields, cardinality or errors")
		}
		for _, pair := range [][2]string{
			{"selected, instancePath, since", "selected, otherPath, since"}, {"selected, since", "foreignOwner, since"},
			{"if err == nil && !found", "if !found"}, {"instanceCount != 1", "instanceCount > 1"},
			{"gotCanonical != wantCanonical", "gotCanonical == wantCanonical"},
			{"if !ok", "if false"}, {"count != 1", "count > 1"}, {"if err != nil", "if false"},
		} {
			mutant := row
			mutant.After = strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant.After == row.After || catalogCreationRecipePreservesAssertions(mutant) {
				t.Fatalf("lost owner/path, missing-field/refusal or weakened count/value accepted: %v", pair)
			}
		}
	}
	if count != 2 {
		t.Fatalf("creation recipes=%d, want two shared observations", count)
	}
}

func TestNativeCatalogCreationOwnerPreservesPhysicalScopeAndJoin(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "pipelinepersistence", "workflow_projection_observation.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"SELECT COUNT(*) FROM flow_instances WHERE created_at >= $1",
		"SELECT COUNT(*) FROM flow_instances WHERE instance_path=$1 AND created_at >= $2",
		"SELECT e.fields FROM flow_instances f", "JOIN entity_state e ON e.run_id=f.run_id AND e.flow_instance=f.instance_path",
		"WHERE f.instance_path=$1 ORDER BY f.created_at DESC LIMIT 1", "errors.Is(err, sql.ErrNoRows)",
	} {
		if !strings.Contains(string(data), fragment) {
			t.Fatalf("physical creation observation lost %q", fragment)
		}
	}
}
