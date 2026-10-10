package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const busPipelineReceiptCountShape = `func countPipelineReceiptsForEvent(t *testing.T,ctx context.Context,selected any,eventID string) int {
t.Helper()
count,err := storetest.CountPipelineEventReceiptStorage(ctx,selected,eventID)
if err != nil { t.Fatalf("count pipeline receipts for %s: %v",eventID,err) }
return count
}`

func busPipelineReceiptCountPreserved(source string) bool {
	want, err := canonicalFunction(busPipelineReceiptCountShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeBusPipelineReceiptCountPreservesCompletePauseAndRecoveryCuts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-pipeline-receipt-count" {
			continue
		}
		matched++
		if row.Function == "countPipelineReceiptsForEvent" {
			if !busPipelineReceiptCountPreserved(row.After) {
				t.Fatal("receipt helper lost exact native read/context/event, count or failure handling")
			}
			continue
		}
		want := strings.ReplaceAll(row.Before, "countPipelineReceiptsForEvent(t, ctx, db,", "countPipelineReceiptsForEvent(t, ctx, pg,")
		if want == row.Before || normalizedBusRunControlConstruction(t, want) != normalizedBusRunControlConstruction(t, row.After) {
			t.Fatalf("pause, publication, delivery, replay, receipt or teardown changed: %s", row.Function)
		}
	}
	if matched != 5 {
		t.Fatalf("receipt count recipes=%d, want5 plus two updated construction roots", matched)
	}
	actual := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", "countPipelineReceiptsForEvent")
	for _, pair := range [][2]string{
		{"ctx, selected, eventID", "ctx, foreign, eventID"},
		{"ctx, selected, eventID", "ctx, selected, otherEvent"},
		{"if err != nil", "if false"}, {"return count", "return 1"},
	} {
		mutant := strings.Replace(actual, pair[0], pair[1], 1)
		if mutant == actual || busPipelineReceiptCountPreserved(mutant) {
			t.Fatalf("weakened native receipt count admitted: %v", pair)
		}
	}
	owner := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_api_event_storage.go", "CountPipelineEventReceiptStorageForTest")
	if !strings.Contains(owner, "pipelinepersistence.FixturePipelineReceiptCardinalityTx(ctx, tx, eventID)") || strings.Contains(owner, "SELECT ") {
		t.Fatal("receipt cardinality must consume the existing pipeline owner, not another SQL selector")
	}
}
