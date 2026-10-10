package main

import (
	"strings"
	"testing"
)

func nativeToolContractReadWorkload(t *testing.T, source string) string {
	t.Helper()
	// Only the exact dialect selector and physical read are replaced. Everything
	// else, including the entire authored document and tool assertions, is kept.
	for _, change := range [][2]string{
		{"\t\t\tvar query string\n", ""},
		{"\t\t\t\tquery = `SELECT entity_type FROM entity_state WHERE run_id = ? AND entity_id = ?`\n", ""},
		{"\t\t\t\tquery = `SELECT entity_type FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`\n", ""},
		{"var persistedType string\n\t\t\tif err := storetest.DatabaseForTest(entityStore).QueryRowContext(ctx, query, entityToolTestRunID, entityID).Scan(&persistedType); err != nil {\n\t\t\t\tt.Fatalf(\"load persisted entity contract: %v\", err)\n\t\t\t}", "persisted, err := storetest.ReadTrackedEntityMutationProjectionStorage(ctx, entityStore, entityToolTestRunID, entityID)\n\t\t\tif err != nil {\n\t\t\t\tt.Fatalf(\"load persisted entity contract: %v\", err)\n\t\t\t}\n\t\t\tpersistedType := persisted.EntityType"},
	} {
		source = strings.Replace(source, change[0], change[1], 1)
	}
	return formattedNativeReadNode(projectionShapeFunction(t, source))
}

func TestNativeToolContractStorageReadPreservesCompleteWorkload(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-tool-contract-storage-read")
	if nativeToolContractReadWorkload(t, row.Before) != nativeToolContractReadWorkload(t, row.After) {
		t.Fatal("physical contract coordinates, workload or assertions changed")
	}
}

func TestNativeToolContractStorageOracleRejectsChangedPredicatesAndAssertions(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-tool-contract-storage-read")
	for _, change := range [][2]string{
		{"entityStore, entityToolTestRunID, entityID", "entityStore, otherRun, entityID"},
		{"entityStore, entityToolTestRunID, entityID", "entityStore, entityToolTestRunID, otherEntity"},
		{"persistedType != \"account_record\"", "persistedType == \"account_record\""},
		{"got != \"account_record\"", "got == \"account_record\""},
		{"account_record:\\n  status: text", "account_record:\\n  status: integer"},
	} {
		changed := strings.Replace(row.After, change[0], change[1], 1)
		if changed == row.After || nativeToolContractReadWorkload(t, row.Before) == nativeToolContractReadWorkload(t, changed) {
			t.Fatalf("changed contract read admitted: %s", change[0])
		}
	}
}

func nativeToolBookkeepingWorkload(t *testing.T, source string) string {
	t.Helper()
	for _, change := range [][2]string{
		{"if _, err := storetest.DatabaseForTest(sqliteStore).ExecContext(ctx, `\n\t\tUPDATE entity_state\n\t\tSET bookkeeping = '{\"private_fact\":\"must-not-leak\"}'\n\t\tWHERE run_id = ? AND entity_id = ?\n\t`, entityToolTestRunID, entityID); err != nil {\n\t\tt.Fatalf(\"inject sqlite hostile bookkeeping: %v\", err)\n\t}", "if changed, err := storetest.SetEntityProjectionPrivateBookkeeping(ctx, sqliteStore, entityToolTestRunID, entityID); err != nil || changed != 1 {\n\t\tt.Fatalf(\"inject sqlite hostile bookkeeping: rows=%d err=%v\", changed, err)\n\t}"},
		{"var mutationCount int\n\tif err := storetest.DatabaseForTest(sqliteStore).QueryRowContext(ctx, `\n\t\tSELECT COUNT(*)\n\t\tFROM entity_mutations\n\t\tWHERE run_id = ? AND entity_id = ?\n\t`, entityToolTestRunID, entityID).Scan(&mutationCount); err != nil {\n\t\tt.Fatalf(\"count sqlite entity mutations: %v\", err)\n\t}", "projection, err := storetest.ReadTrackedEntityMutationProjectionStorage(ctx, sqliteStore, entityToolTestRunID, entityID)\n\tif err != nil {\n\t\tt.Fatalf(\"count sqlite entity mutations: %v\", err)\n\t}\n\tmutationCount := len(projection.Mutations)"},
	} {
		source = strings.Replace(source, change[0], change[1], 1)
	}
	return formattedNativeReadNode(projectionShapeFunction(t, source))
}

func TestNativeToolBookkeepingPreservesCompleteWorkloadAndStorageCut(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-tool-private-bookkeeping")
	if nativeToolBookkeepingWorkload(t, row.Before) != nativeToolBookkeepingWorkload(t, row.After) {
		t.Fatal("hostile payload/scope, tool workload, no-leak assertions or history count changed")
	}
}

func TestNativeToolBookkeepingOracleRejectsChangedFaultAndWorkload(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-tool-private-bookkeeping")
	for _, change := range [][2]string{
		{"sqliteStore, entityToolTestRunID, entityID", "sqliteStore, otherRun, entityID"},
		{"changed != 1", "changed != 0"},
		{"mutationCount < 2", "mutationCount < 1"},
		{"if _, exists := wholeRows[0][\"bookkeeping\"]; exists", "if _, exists := wholeRows[0][\"bookkeeping\"]; !exists"},
		{"\"value\":     \"closed\"", "\"value\":     \"open\""},
	} {
		changed := strings.Replace(row.After, change[0], change[1], 1)
		if changed == row.After || nativeToolBookkeepingWorkload(t, row.Before) == nativeToolBookkeepingWorkload(t, changed) {
			t.Fatalf("changed bookkeeping proof admitted: %s", change[0])
		}
	}
}
