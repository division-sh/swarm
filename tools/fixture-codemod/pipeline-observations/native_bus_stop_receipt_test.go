package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func normalizeNativeBusStopReceipt(source string) string {
	query := "`SELECT outcome FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`"
	for _, cut := range []struct{ indent, event, diagnostic string }{
		{"\t\t\t", "event.ID()", "original foreground claimant could not settle after stop rollback: outcome=%s err=%v\", outcome, err)"},
		{"\t\t\t\t\t", "eventID", "stop overwrote completed recovery/publication: event=%s outcome=%s err=%v\", eventID, outcome, err)"},
	} {
		old := cut.indent + "var outcome string\n" + cut.indent + "if err := f.db.QueryRow(" + query + ", " + cut.event + ").Scan(&outcome); err != nil || outcome != \"success\" {"
		fresh := cut.indent + "outcome, err := storetest.ReadExactPipelineReceiptOutcomeReason(context.Background(), f.store, " + cut.event + ")\n" + cut.indent + "if err != nil || outcome.Outcome != \"success\" {"
		source = strings.Replace(source, old, fresh, 1)
		source = strings.Replace(source, cut.diagnostic, strings.Replace(cut.diagnostic, "outcome, err)", "outcome.Outcome, err)", 1), 1)
	}
	return source
}

func TestNativeBusStopReceiptPreservesEveryClaimAndDrainAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-stop-receipt" {
			continue
		}
		matched++
		if row.Function != "TestRunStopPreservesForegroundClaimFenceBothStores" || normalizeNativeBusStopReceipt(row.Before) != row.After {
			t.Fatal("foreground stop receipt changed its claim, error, context or settlement proof")
		}
		if selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("foreground stop proof diverged from finite migration")
		}
	}
	if matched != 1 {
		t.Fatalf("stop receipt recipes=%d,want1", matched)
	}
	for _, name := range []string{"TestRunStopPreservesForegroundClaimFenceBothStores", "TestRunStopDrainsRecoveryBeforeMutationAndRequiredPublicationBothStores"} {
		body := selectedCausalObservationBody(t, "internal/runtime/bus/run_stop_claim_test.go", name)
		if strings.Contains(body, "f.db.QueryRow") || strings.Count(body, "ReadExactPipelineReceiptOutcomeReason(context.Background(), f.store,") != 1 {
			t.Fatalf("%s: receipt escaped the exact original owner", name)
		}
	}
}
