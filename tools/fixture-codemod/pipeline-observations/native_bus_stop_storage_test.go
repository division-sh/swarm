package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestNativeBusStopStoragePreservesPhysicalPredicatesAndEveryField(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-stop-storage" {
			continue
		}
		matched++
		control := selectedCausalObservationBody(t, "internal/store/internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadStopRunControlStorageTx")
		deliveries := selectedCausalObservationBody(t, "internal/store/internal/backend/delivery/read_projections.go", "ReadStopPendingDeliveryCountTx")
		revisions := selectedCausalObservationBody(t, "internal/store/internal/backend/runforkrevision/postgres.go", "CountActivityJournalRevisionsForTest")
		if !reflect.DeepEqual(eventDeliveryDiagnosticSQL(t, row.Before), eventDeliveryDiagnosticSQL(t, control+"\n"+deliveries+"\n"+revisions)) {
			t.Fatal("stop witness changed physical status/default/pending/revision predicates")
		}
		if !strings.Contains(row.After, "ReadRunStopStorage(context.Background(), f.store, f.event.RunID())") ||
			!strings.Contains(row.After, "status: out.Status, control: out.Control, pending: out.Pending, revisions: int(out.Revisions)") ||
			selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("stop witness changed context, original owner, identity or field mapping")
		}
		adapter := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_run_stop_storage.go", "ReadRunStopStorageForTest")
		for _, call := range []string{"readServedDeliveryObservation(ctx, selected,", "runlifecycle.ReadStopRunControlStorageTx(ctx, tx, runID)", "delivery.ReadStopPendingDeliveryCountTx(ctx, tx, runID)", "runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)", "return RunStopStorage{}, err"} {
			if !strings.Contains(adapter, call) {
				t.Fatalf("stop witness lost %s", call)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("stop storage recipes=%d,want1", matched)
	}
}
