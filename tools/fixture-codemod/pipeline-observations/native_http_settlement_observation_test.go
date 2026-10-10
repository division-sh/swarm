package main

import (
	"strings"
	"testing"
)

func nativeHTTPSettlementWorkload(t *testing.T, source string) string {
	t.Helper()
	for _, change := range [][2]string{
		{"db := storetest.DatabaseForTest(selected)\n\tvar actual int\n\tif err := db.QueryRow(`SELECT COUNT(*) FROM runtime_external_effect_attempts`).Scan(&actual); err != nil || actual != count {", "attempts, err := storetest.ReadExternalAttemptStorage(context.Background(), selected)\n\tactual := len(attempts)\n\tif err != nil || actual != count {"},
		{"_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\tselected = storetest.AdmitPostgresRuntimeStore(t, db)", "native, _ := storetest.StartPostgresRuntimeStoreWithReopen(t)\n\t\t\t\tselected = native"},
		{"var operationID string\n\t\t\tif err := storetest.DatabaseForTest(selected).QueryRow(`SELECT CAST(operation_id AS TEXT) FROM runtime_external_effect_attempts`).Scan(&operationID); err != nil {\n\t\t\t\tt.Fatal(err)\n\t\t\t}", "attempts, err := storetest.ReadExternalAttemptStorage(context.Background(), selected)\n\t\t\tif err != nil || len(attempts) != 1 {\n\t\t\t\tt.Fatalf(\"expected one original physical attempt: %+v/%v\", attempts, err)\n\t\t\t}\n\t\t\toperationID := attempts[0].OperationID"},
		{"if err := storetest.DatabaseForTest(selected).QueryRow(`SELECT COUNT(*) FROM runtime_external_effect_attempts WHERE state='settled'`).Scan(&settledCount); err != nil || settledCount != 1 {", "attempts, err = storetest.ReadExternalAttemptStorage(context.Background(), selected)\n\t\t\tfor _, attempt := range attempts {\n\t\t\t\tif attempt.State == \"settled\" {\n\t\t\t\t\tsettledCount++\n\t\t\t\t}\n\t\t\t}\n\t\t\tif err != nil || settledCount != 1 {"},
	} {
		source = strings.Replace(source, change[0], change[1], 1)
	}
	return formattedNativeReadNode(projectionShapeFunction(t, source))
}

func TestNativeHTTPSettlementObservationPreservesCompleteWorkloadAndConsumers(t *testing.T) {
	for _, family := range []string{"native-http-settlement-physical-count", "native-http-settlement-attempt-inventory"} {
		row := nativeMissingHeaderRecipe(t, family)
		if nativeHTTPSettlementWorkload(t, row.Before) != nativeHTTPSettlementWorkload(t, row.After) {
			t.Fatalf("physical predicates, provider workload, receipts or assertions changed: %s", family)
		}
	}
	row := nativeMissingHeaderRecipe(t, "native-http-settlement-attempt-inventory")
	if strings.Count(row.After, "requireHTTPSettlementOutcome(t, selected, operationID,") != 4 {
		t.Fatal("HTTP outcome consumer inventory changed")
	}
}

func TestNativeHTTPSettlementObservationOracleRejectsLostWorkloadAndPhysicalPredicates(t *testing.T) {
	for _, probe := range []struct{ family, from, to string }{
		{"native-http-settlement-physical-count", "actual != count", "actual < count"},
		{"native-http-settlement-physical-count", "outcome.OperationID != operationID", "outcome.OperationID == operationID"},
		{"native-http-settlement-attempt-inventory", "attempt.State == \"settled\"", "attempt.State != \"settled\""},
		{"native-http-settlement-attempt-inventory", "calls.Load() != 2", "calls.Load() != 1"},
		{"native-http-settlement-attempt-inventory", "sink.submits != 1", "sink.submits != 0"},
		{"native-http-settlement-attempt-inventory", "!errors.Is(unackErr, unack)", "errors.Is(unackErr, unack)"},
	} {
		row := nativeMissingHeaderRecipe(t, probe.family)
		changed := strings.Replace(row.After, probe.from, probe.to, 1)
		if changed == row.After || nativeHTTPSettlementWorkload(t, row.Before) == nativeHTTPSettlementWorkload(t, changed) {
			t.Fatalf("lost HTTP receipt/failure/count proof admitted: %s", probe.from)
		}
	}
}
