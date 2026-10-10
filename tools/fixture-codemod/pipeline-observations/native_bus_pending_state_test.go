package main

import (
	"strings"
	"testing"
)

var busPendingObservationCuts = [][2]string{
	{"\t\t\t\tvar db *sql.DB\n\t\t\t\tplaceholder := \"?\"\n\t\t\t\tif backend == \"sqlite\" {\n\t\t\t\t\tselected = storetest.StartSQLiteRuntimeStore(t)\n\t\t\t\t\tdb = storetest.DatabaseForTest(selected)\n\t\t\t\t} else {\n\t\t\t\t\tvar cleanup func()\n\t\t\t\t\t_, db, cleanup = testutil.StartPostgres(t)\n\t\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\t\tselected = storetest.AdmitPostgresRuntimeStore(t, db)\n\t\t\t\t\tplaceholder = \"$1::uuid\"\n\t\t\t\t}\n", "\t\t\t\tif backend == \"sqlite\" {\n\t\t\t\t\tselected = storetest.StartSQLiteRuntimeStore(t)\n\t\t\t\t} else {\n\t\t\t\t\tselected = storetest.StartPostgresRuntimeStore(t)\n\t\t\t\t}\n"},
	{"\t\t\t\tvar eventCount int\n\t\t\t\tif err := db.QueryRowContext(ctx, \"SELECT COUNT(*) FROM events WHERE event_id = \"+placeholder, eventID).Scan(&eventCount); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tvar receiptCount int\n\t\t\t\tif err := db.QueryRowContext(ctx, \"SELECT COUNT(*) FROM event_receipts WHERE event_id = \"+placeholder+\" AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'\", eventID).Scan(&receiptCount); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n", "\t\t\t\tevidence, err := storetest.ObserveSemanticEventFixtureEvidence(ctx, selected, runID, eventID)\n\t\t\t\tif err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\teventCount := 0\n\t\t\t\tif evidence.RecordFound {\n\t\t\t\t\teventCount = 1\n\t\t\t\t}\n\t\t\t\treceiptCount := evidence.PipelineReceiptCount\n"},
	{"\t\t\t\tif err := db.QueryRowContext(ctx, \"SELECT COUNT(*) FROM event_receipts WHERE event_id = \"+placeholder+\" AND subscriber_type = 'platform' AND subscriber_id = 'pipeline' AND outcome = 'success'\", eventID).Scan(&receiptCount); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n", "\t\t\t\tevidence, err = storetest.ObserveSemanticEventFixtureEvidence(ctx, selected, runID, eventID)\n\t\t\t\tif err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\treceiptCount = evidence.PipelineReceiptCount\n\t\t\t\tif evidence.PipelineReceiptOutcome != \"success\" {\n\t\t\t\t\treceiptCount = 0\n\t\t\t\t}\n"},
}

func normalizedBusPendingState(t *testing.T, source string) string {
	t.Helper()
	actual, err := canonicalFunction(source)
	if err != nil {
		return "parse-refused"
	}
	for _, pair := range busPendingObservationCuts {
		actual = strings.Replace(actual, pendingObservationFragment(t, pair[0]), pendingObservationFragment(t, pair[1]), 1)
	}
	return actual
}

func pendingObservationFragment(t *testing.T, source string) string {
	t.Helper()
	value, err := canonicalFunction("func cut(){" + source + "}")
	if err != nil {
		t.Fatal(err)
	}
	value = strings.TrimSuffix(strings.TrimPrefix(value, "func cut() {\n"), "\n}")
	return "\t\t\t" + strings.ReplaceAll(value, "\n", "\n\t\t\t")
}

func TestNativeBusPendingStatePreservesFullFaultClaimAndRecoveryJourney(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-bus-pending-postcommit-state")
	if normalizedBusPendingState(t, row.Before) != normalizedBusPendingState(t, row.After) {
		t.Fatal("pending fault set, source/run setup, publication, claim/release, sweep or exact evidence assertions changed")
	}
	for _, pair := range [][2]string{
		{"ctx, selected, runID, eventID", "ctx, selected, runID, otherEvent"},
		{"eventCount = 1", "eventCount = 0"}, {"receiptCount != 0", "receiptCount < 0"},
		{"evidence.PipelineReceiptOutcome != \"success\"", "false"}, {"receiptCount != 1", "receiptCount < 1"},
		{"Release(ctx, claim)", "Release(ctx, otherClaim)"},
		{"SweepPipelineObligations(ctx, 10)", "SweepPipelineObligations(ctx, 0)"},
	} {
		mutant := strings.Replace(row.After, pair[0], pair[1], 1)
		if mutant == row.After || normalizedBusPendingState(t, row.Before) == normalizedBusPendingState(t, mutant) {
			t.Fatalf("weakened pending/recovered observation admitted: %v", pair)
		}
	}
}
