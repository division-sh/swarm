package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func nativeAPIDeliveryReceiptEvidenceRecipes(t *testing.T) []recipe {
	t.Helper()
	wanted := map[string]bool{
		"countAllEventDeliveries": true, "loadPipelineReceiptOutcomeAndFailure": true,
		"TestOperatorEventPublishPreCommitFailureFailsClosedWithDeclaredError":       true,
		"TestOperatorEventPublishIsUnavailableWithoutDurableAckPublisher":            true,
		"TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate": true,
	}
	var rows, found []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if wanted[row.Function] {
			found = append(found, row)
			delete(wanted, row.Function)
		}
	}
	if len(wanted) != 0 || len(found) != 5 {
		t.Fatalf("delivery/receipt recipes missing/duplicate: %v/%d", wanted, len(found))
	}
	return found
}

func TestNativeAPIDeliveryReceiptEvidenceRetainsWorkContextAndExactScope(t *testing.T) {
	for _, row := range nativeAPIDeliveryReceiptEvidenceRecipes(t) {
		if strings.HasPrefix(row.Function, "Test") {
			if nativeAPIPhysicalCardinalityWorkload(t, row.Before) != nativeAPIPhysicalCardinalityWorkload(t, row.After) {
				t.Fatalf("precommit/ack/replay work or assertions changed: %s", row.Function)
			}
			continue
		}
		want := `func countAllEventDeliveries(t *testing.T, selected any) int {
			t.Helper()
			count, err := storetest.CountPhysicalEventDeliveries(context.Background(), selected)
			if err != nil { t.Fatalf("count event_deliveries rows: %v", err) }
			return count
		}`
		if row.Function == "loadPipelineReceiptOutcomeAndFailure" {
			want = `func loadPipelineReceiptOutcomeAndFailure(t *testing.T,ctx context.Context,selected any,eventID string)(string,*runtimefailures.Envelope) {
				t.Helper()
				outcome,failure,err:=storetest.ReadPipelineReceiptOutcomeStorage(ctx,selected,eventID)
				if err!=nil{t.Fatalf("load pipeline receipt for %s: %v",eventID,err)}
				return outcome,failure
			}`
		}
		if formattedNativeReadNode(projectionShapeFunction(t, row.After)) != formattedNativeReadNode(projectionShapeFunction(t, want)) {
			t.Fatalf("delivery/receipt original owner/context/key/refusal changed: %s", row.Function)
		}
	}
}

func TestNativeAPIDeliveryReceiptEvidenceRejectsChangedOwnersKeysAndAssertions(t *testing.T) {
	probes := 0
	for _, row := range nativeAPIDeliveryReceiptEvidenceRecipes(t) {
		if !strings.HasPrefix(row.Function, "Test") {
			continue
		}
		for _, change := range [][2]string{
			{"countAllEventDeliveries(t, pg)", "countAllEventDeliveries(t, wrongOwner)"},
			{"loadPipelineReceiptOutcomeAndFailure(t, ctx, pg, eventID)", "loadPipelineReceiptOutcomeAndFailure(t, wrongContext, pg, eventID)"},
			{"loadPipelineReceiptOutcomeAndFailure(t, ctx, pg, eventID)", "loadPipelineReceiptOutcomeAndFailure(t, ctx, pg, wrongEventID)"},
			{"got != 0", "got < 0"}, {"outcome != \"success\"", "outcome != \"waiting\""},
		} {
			broken := strings.Replace(row.After, change[0], change[1], 1)
			if broken == row.After {
				continue
			}
			probes++
			if nativeAPIPhysicalCardinalityWorkload(t, row.After) == nativeAPIPhysicalCardinalityWorkload(t, broken) {
				t.Fatalf("weakened delivery/receipt evidence admitted: %s/%s", row.Function, change[0])
			}
		}
	}
	if probes != 7 {
		t.Fatalf("delivery/receipt adversaries missing: %d", probes)
	}
}
