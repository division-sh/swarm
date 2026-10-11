package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func normalizeInboundReceiptWitnessCalls(source string) string {
	return strings.NewReplacer(
		"loadPostgresAgentDeliveryStatus(t, ctx, pg,", "loadPostgresAgentDeliveryStatus(t, ctx, db,",
		"countPostgresAgentDeliveriesForEvent(t, ctx, pg,", "countPostgresAgentDeliveriesForEvent(t, ctx, db,",
		"countPostgresPipelineReceiptsForEvent(t, ctx, pg,", "countPostgresPipelineReceiptsForEvent(t, ctx, db,",
		"countPostgresAgentSettledAttemptsForEvent(t, ctx, pg,", "countPostgresAgentReceiptsForEvent(t, ctx, db,",
		"agent settled attempts while paused", "agent receipts while paused",
	).Replace(source)
}

func TestNativeInboundReceiptWitnessPreservesPipelineRowsAndRejectsRetiredAgentMeaning(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-inbound-receipt-witness" {
			continue
		}
		matched++
		name := row.Function
		switch row.Function {
		case "countPostgresAgentReceiptsForEvent":
			name = "countPostgresAgentSettledAttemptsForEvent"
			if strings.Contains(row.After, "event_receipts") || strings.Contains(row.After, "db") || !strings.Contains(row.After, "ReadAgentSettledAttemptCount(ctx, selected, eventID, agentID)") || !strings.Contains(row.After, "count agent settled attempts for %s") || !strings.Contains(row.After, "return count") {
				t.Fatal("retired receipt meaning survived or exact executable settlement witness changed")
			}
		default:
			t.Fatal("unknown inbound receipt witness")
		}
		if selectedCausalObservationBody(t, row.File, name) != row.After {
			t.Fatal("inbound receipt witness diverged from finite migration")
		}
	}
	if matched != 1 {
		t.Fatalf("inbound settlement witness recipes=%d,want1; pipeline read belongs to upstream owner", matched)
	}
	owner := selectedCausalObservationBody(t, "internal/runtime/inbound_readback_test.go", "countInboundPipelineReceipts")
	if !strings.Contains(owner, "storetest.ReadSemanticEventFixtureEvidence(t, ctx, selected, runID, eventID).PipelineReceiptCount") {
		t.Fatal("pipeline acknowledgment lost exact selected run/event/context readback")
	}
}
