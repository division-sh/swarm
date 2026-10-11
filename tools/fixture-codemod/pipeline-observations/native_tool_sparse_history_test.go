package main

import (
	"strings"
	"testing"
)

func nativeToolSparseHistorySource(source string) string {
	for _, change := range [][2]string{
		{"\t\t\tvar db *sql.DB\n", ""},
		{"persistence, db = s, storetest.DatabaseForTest(s)", "persistence = s"},
		{"\t\t\thistoryRows, err := db.QueryContext(ctx, `SELECT entity_id, domain, path, COALESCE(new_value, 'null')\n\t\t\t\tFROM entity_mutations WHERE run_id = $1 ORDER BY created_at DESC, mutation_id DESC`, runID)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tvar mutations []operatorread.RunDebugMutation\n\t\t\tfor historyRows.Next() {\n\t\t\t\tvar mutation operatorread.RunDebugMutation\n\t\t\t\tvar value []byte\n\t\t\t\tif err := historyRows.Scan(&mutation.EntityID, &mutation.Domain, &mutation.Path, &value); err != nil {\n\t\t\t\t\thistoryRows.Close()\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tmutation.NewValue = append(json.RawMessage(nil), value...)\n\t\t\t\tmutations = append(mutations, mutation)\n\t\t\t}\n\t\t\tif err := historyRows.Err(); err != nil {\n\t\t\t\thistoryRows.Close()\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\thistoryRows.Close()\n", "\t\t\tmutations, err := storetest.ReadRunEntityMutationHistoryStorage(ctx, persistence, runID)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n"},
	} {
		source = strings.ReplaceAll(source, change[0], change[1])
	}
	return source
}

func TestNativeToolSparseHistoryPreservesEntireRunWorkload(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-tool-sparse-run-bootstrap")
	if nativeToolRunSeedWorkload(t, row.Before) != nativeToolRunSeedWorkload(t, row.After) {
		t.Fatal("whole-run history, source, workload or assertions changed")
	}
	if strings.Contains(row.After, "DatabaseForTest") || strings.Contains(row.After, "db.QueryContext") {
		t.Fatal("sparse history retained raw authority")
	}
}

func TestNativeToolSparseHistoryOracleRejectsScopeHistoryAndWorkloadChanges(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-tool-sparse-run-bootstrap")
	for _, change := range [][2]string{
		{"ctx, persistence, runID)", "ctx, persistence, otherRun)"},
		{"len(report.Mutations) != len(mutations)", "len(report.Mutations) < len(mutations)"},
		{"mutation.EntityID != entityID", "mutation.EntityID != otherEntity"},
		{"labels != 1", "labels != 0"},
		{"mutation.WriterID != actor.ID", "mutation.WriterID != otherActor"},
		{"json.Unmarshal(mutation.OldValue, &previous)", "json.Unmarshal(mutation.NewValue, &previous)"},
	} {
		source := row.After
		if strings.Contains(row.Before, change[0]) && !strings.Contains(source, change[0]) {
			source = row.Before
		}
		changed := strings.Replace(source, change[0], change[1], 1)
		if changed == source || nativeToolRunSeedWorkload(t, row.Before) == nativeToolRunSeedWorkload(t, changed) {
			t.Fatalf("changed sparse history accepted: %s", change[0])
		}
	}
}
