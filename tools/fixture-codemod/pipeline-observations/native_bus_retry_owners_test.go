package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func normalizedRetryOwnerJoin(source string) string {
	for _, exact := range []string{
		"\tvar firstWorker sync.WaitGroup\n\tvar releaseOnce sync.Once\n\trelease := func() { releaseOnce.Do(func() { close(releaseLater) }) }\n\tdefer func() {\n\t\trelease()\n\t\tfirstWorker.Wait()\n\t}()\n\tfirstWorker.Add(1)\n",
		"\t\tdefer firstWorker.Done()\n",
	} {
		source = strings.Replace(source, exact, "", 1)
	}
	return strings.Replace(source, "\trelease()\n", "\tclose(releaseLater)\n", 1)
}

func TestNativeBusRetryOwnersPreserveBoundedClaimAndExactReceiptWitness(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-retry-owners" {
			continue
		}
		matched++
		before, after := row.Before, row.After
		switch row.Function {
		case "TestPostgresRetryReleaseClaimSpansBoundedSweepWindow":
			before = strings.Replace(before, "\tcompetingStore := storetest.AdmitPostgresRuntimeStore(t, fixture.db)\n", "", 1)
			before = strings.Replace(before, "newScopedTestEventBus(competingStore)", "newScopedTestEventBus(fixture.store)", 1)
			after = normalizedRetryOwnerJoin(after)
		case "retryReleasePipelineReceiptCount":
			before = strings.Replace(before, "\tquery := `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`\n\tif fixture.dialect == \"postgres\" {\n\t\tquery = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`\n\t}\n\tvar count int\n\tif err := fixture.db.QueryRowContext(fixture.ctx, query, eventID).Scan(&count); err != nil {", "\tcount, err := storetest.CountPipelineEventReceiptStorage(fixture.ctx, fixture.store, eventID)\n\tif err != nil {", 1)
		default:
			t.Fatal("unknown retry-window owner consumer")
		}
		if before != after {
			t.Fatal("retry window, exact receipt, claim or assertion changed")
		}
		actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		want, wantErr := canonicalFunction(row.After)
		if err != nil || wantErr != nil || actual != want {
			t.Fatal("retry owner consumer diverged from finite migration")
		}
	}
	if matched != 2 {
		t.Fatalf("retry owner recipes=%d,want2", matched)
	}
}

func TestNativeBusRetryReceiptOwnerHasNoAdditionalOutcomeOrRunFilter(t *testing.T) {
	body := selectedCausalObservationBody(t, "internal/store/internal/backend/pipelinepersistence/owner_operations.go", "FixturePipelineReceiptCardinalityTx")
	if !strings.Contains(body, "`SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`") ||
		!strings.Contains(body, "tx.QueryRowContext(ctx,") ||
		!strings.Contains(body, "eventID).Scan(&count)") ||
		strings.Contains(body, "LIMIT") {
		t.Fatal("receipt witness no longer preserves exact physical cardinality")
	}
}
