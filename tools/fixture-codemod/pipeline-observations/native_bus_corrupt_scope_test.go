package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestNativeBusCorruptScopePreservesExactFaultReceiptAndAllConsumers(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-corrupt-scope" {
			continue
		}
		matched++
		var owner string
		switch row.Function {
		case "deleteCommittedReplayScope":
			owner = selectedCausalObservationBody(t, "internal/store/internal/backend/pipelinepersistence/owner_operations.go", "DeleteCommittedReplayScopeFixtureTx")
			before := strings.ReplaceAll(row.Before, "DELETE FROM", "SELECT * FROM")
			after := strings.ReplaceAll(owner, "DELETE FROM", "SELECT * FROM")
			if !reflect.DeepEqual(eventDeliveryDiagnosticSQL(t, before), eventDeliveryDiagnosticSQL(t, after)) ||
				!strings.Contains(row.After, "storetest.DeleteCommittedReplayScope(fixture.ctx, fixture.store, eventID)") ||
				!strings.Contains(owner, "tx.ExecContext(ctx, query, eventID)") ||
				!strings.Contains(owner, "if changed != 1") {
				t.Fatal("scope fault lost exact event/original writer/cardinality")
			}
		case "pipelineReceiptOutcome":
			owner = selectedCausalObservationBody(t, "internal/store/internal/backend/pipelinepersistence/owner_operations.go", "ReadExactPipelineReceiptOutcomeReasonTx")
			if !reflect.DeepEqual(eventDeliveryDiagnosticSQL(t, row.Before), eventDeliveryDiagnosticSQL(t, owner)) ||
				!strings.Contains(row.After, "ReadExactPipelineReceiptOutcomeReason(context.Background(), fixture.store, eventID)") ||
				!strings.Contains(row.After, "return out.Outcome, out.Reason") {
				t.Fatal("receipt witness changed exact event/role/columns")
			}
		default:
			t.Fatal("unknown corrupt-scope consumer")
		}
		if selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("corrupt-scope helper diverged from finite migration")
		}
	}
	if matched != 2 {
		t.Fatalf("corrupt-scope recipes=%d,want2", matched)
	}
}
