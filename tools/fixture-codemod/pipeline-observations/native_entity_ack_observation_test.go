package main

import (
	"strings"
	"testing"
)

func nativeEntityAcknowledgmentWorkload(t *testing.T, source string) string {
	t.Helper()
	if strings.Contains(source, "for _, operation := range") {
		source = strings.ReplaceAll(source, "\t\t\t\t", "\t\t\t")
	}
	for _, change := range [][2]string{
		{"\t\t\tquery := `SELECT revision FROM entity_state WHERE run_id = ? AND entity_id = ?`\n", ""},
		{"\t\t\tmutationQuery := `SELECT COUNT(*) FROM entity_mutations WHERE run_id = ? AND entity_id = ?`\n", ""},
		{"\t\t\t\tquery = `SELECT revision FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`\n", ""},
		{"\t\t\t\tmutationQuery = `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid AND entity_id = $2::uuid`\n", ""},
		{"db := storetest.DatabaseForTest(base)\n\t\t\tvar before, baselineRevision int\n\t\t\tif err := db.QueryRowContext(ctx, mutationQuery, entityToolTestRunID, entityID).Scan(&before); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tif err := db.QueryRowContext(ctx, query, entityToolTestRunID, entityID).Scan(&baselineRevision); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}", "baseline, err := storetest.ReadTrackedEntityMutationProjectionStorage(ctx, base, entityToolTestRunID, entityID)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tbefore, baselineRevision := len(baseline.Mutations), baseline.Revision"},
		{"var revision, count int\n\t\t\tif err := db.QueryRowContext(ctx, query, entityToolTestRunID, entityID).Scan(&revision); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\tif err := db.QueryRowContext(ctx, mutationQuery, entityToolTestRunID, entityID).Scan(&count); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}", "committed, err := storetest.ReadTrackedEntityMutationProjectionStorage(ctx, base, entityToolTestRunID, entityID)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}\n\t\t\trevision, count := committed.Revision, len(committed.Mutations)"},
		{"if err := db.QueryRowContext(ctx, mutationQuery, entityToolTestRunID, entityID).Scan(&count); err != nil || count != before+1 {", "refused, err := storetest.ReadTrackedEntityMutationProjectionStorage(ctx, base, entityToolTestRunID, entityID)\n\t\t\tcount = len(refused.Mutations)\n\t\t\tif err != nil || count != before+1 {"},
	} {
		source = strings.Replace(source, change[0], change[1], 1)
	}
	return formattedNativeReadNode(projectionShapeFunction(t, source))
}

func TestNativeEntityAcknowledgmentObservationPreservesCompleteWorkload(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-entity-tool-acknowledgment-storage")
	if nativeEntityAcknowledgmentWorkload(t, row.Before) != nativeEntityAcknowledgmentWorkload(t, row.After) {
		t.Fatal("exact revision/history scope, receipts, provider failure or diagnostic assertions changed")
	}
	for _, change := range [][2]string{
		{"ctx, base, entityToolTestRunID, entityID", "ctx, store, entityToolTestRunID, entityID"},
		{"baselineRevision+1", "baselineRevision+2"},
		{"count != before+1", "count < before+1"},
		{"writer.writes != 1", "writer.writes != 0"},
		{"!errors.Is(err, fault)", "errors.Is(err, fault)"},
		{"response[\"retry_write\"] != false", "response[\"retry_write\"] != true"},
	} {
		changed := strings.Replace(row.After, change[0], change[1], 1)
		if changed == row.After || nativeEntityAcknowledgmentWorkload(t, row.Before) == nativeEntityAcknowledgmentWorkload(t, changed) {
			t.Fatalf("changed acknowledgment proof admitted: %s", change[0])
		}
	}
}
