package main

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

func nativeAPIAcknowledgmentCountRecipes(t *testing.T) []recipe {
	t.Helper()
	wanted := map[string]bool{"TestOperatorEventPublishReturnsDurableAckBeforePostCommitDispatchCompletes": true,
		"TestOperatorEventPublishPostCommitReceiptFailureReplaysWithoutDuplicate":       true,
		"TestOperatorEventPublishPostCommitCompletionFailureReplaysWithoutDuplicate":    true,
		"TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence": true,
		"TestOperatorEventPublishQueuesWhileRuntimePaused":                              true,
		"TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation":                   true,
		"TestOperatorEventReplayQueuesWhenDispatchGated":                                true,
		"countEventDeliveriesForEvent":                                                  true,
		"countPipelineReceiptsForEvent":                                                 true}
	var all []recipe
	if err := json.Unmarshal(recipeBytes, &all); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range all {
		if wanted[row.Function] {
			found = append(found, row)
			delete(wanted, row.Function)
		}
	}
	if len(wanted) != 0 || len(found) != 9 {
		t.Fatalf("ack count recipes missing/duplicate: %v/%d", wanted, len(found))
	}
	return found
}
func TestNativeAPIAcknowledgmentCountsRetainAllWorkAndContext(t *testing.T) {
	for _, row := range nativeAPIAcknowledgmentCountRecipes(t) {
		if strings.HasPrefix(row.Function, "Test") {
			if nativeAPIPhysicalCardinalityWorkload(t, row.Before) != nativeAPIPhysicalCardinalityWorkload(t, row.After) {
				t.Fatalf("ack/replay/paused/source/workload/assertions changed: %s", row.Function)
			}
			continue
		}
		cfg := map[string][2]string{
			"countEventDeliveriesForEvent":  {"CountAgentEventDeliveryStorage", "count event deliveries for %s: %v"},
			"countPipelineReceiptsForEvent": {"CountPipelineEventReceiptStorage", "count pipeline receipts for %s: %v"},
		}[row.Function]
		want := "func " + row.Function + "(t *testing.T,ctx context.Context,selected any,eventID string)int{t.Helper();count,err:=storetest." + cfg[0] + "(ctx,selected,eventID);if err!=nil{t.Fatalf(" + strconv.Quote(cfg[1]) + ",eventID,err)};return count}"
		if formattedNativeReadNode(projectionShapeFunction(t, row.After)) != formattedNativeReadNode(projectionShapeFunction(t, want)) {
			t.Fatalf("ack key/context/owner/refusal changed: %s", row.Function)
		}
	}
}
func TestNativeAPIAcknowledgmentCountsRejectWrongContextOwnerOrKey(t *testing.T) {
	probes := 0
	for _, row := range nativeAPIAcknowledgmentCountRecipes(t) {
		for _, change := range [][2]string{{", ctx, pg,", ", wrongContext, pg,"}, {", ctx, pg,", ", ctx, wrongOwner,"}, {"eventID);", "otherEventID);"}, {"got != 1", "got < 1"}} {
			broken := strings.Replace(row.After, change[0], change[1], 1)
			if broken == row.After {
				continue
			}
			probes++
			if nativeAPIPhysicalCardinalityWorkload(t, row.After) == nativeAPIPhysicalCardinalityWorkload(t, broken) {
				t.Fatalf("changed ack proof admitted: %s/%s", row.Function, change[0])
			}
		}
	}
	if probes < 5 {
		t.Fatalf("ack adversaries missing: %d", probes)
	}
}
