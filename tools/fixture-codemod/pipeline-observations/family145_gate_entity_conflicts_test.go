package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

var family145GateEntityConflictKeys = map[string]string{
	"TestReceiverCompositionActivationReuseAndConflictBothStores":                          "entityID",
	"TestTargetedDeclaredKeyAgreementAndConflictExecuteThroughDurableEventBusOnBothStores": "exactEntityID",
}

const family145ReceiverOriginalSettlementRead = "var status string\n\t\t\t\tvar outcomes int\n\t\t\t\tif err := selected.db.QueryRow(`SELECT d.status, (SELECT count(*) FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o WHERE o.delivery_id=d.delivery_id) FROM event_deliveries d WHERE d.event_id=$1`, evt.ID()).Scan(&status, &outcomes); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}"

const family145ReceiverNativeSettlementRead = "storage, err := storetest.ReadReceiverAppearanceSettlementStorage(context.Background(), selected.events, evt.ID())\n\t\t\t\tif err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n\t\t\t\tstatus, outcomes := storage.Status, storage.Outcomes"

func family145GateEntityConflictProofError(function, before, after string) error {
	if function == "TestReceiverCompositionActivationReuseAndConflictBothStores" {
		if strings.Count(before, family145ReceiverOriginalSettlementRead) != 1 || strings.Count(after, family145ReceiverNativeSettlementRead) != 1 {
			return fmt.Errorf("receiver conflict lost its one exact original/native settlement block")
		}
		after = strings.Replace(after, family145ReceiverNativeSettlementRead, family145ReceiverOriginalSettlementRead, 1)
	}
	key := family145GateEntityConflictKeys[function]
	original := "selected.db.ExecContext(ctx, `UPDATE entity_state SET entity_type=$1 WHERE run_id=$2 AND entity_id=$3`, \"wrong_entity_type\", runID, " + key + ")"
	replacement := "storetest.SetWorkflowProjectionConflictingEntityType(ctx, selected.events, runID, " + key + ")"
	if strings.Count(before, original) != 1 || strings.Replace(before, original, replacement, 1) != after {
		return fmt.Errorf("entity conflict changed original construction, authority, phase, predicates or assertions: %s", function)
	}
	return nil
}

func TestFamily145GateEntityConflictsReuseExactLiveFaultWithoutChangingConsumers(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range rows {
		if family145GateEntityConflictKeys[row.Function] == "" {
			continue
		}
		found++
		if err := family145GateEntityConflictProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{"ctx, selected.events, runID,", "if _, err := storetest.SetWorkflowProjectionConflictingEntityType", "if err != nil || !found"} {
			if family145GateEntityConflictProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignCut", 1)) == nil {
				t.Fatalf("lost owner/scope/phase/identity assertion passed: %s", cut)
			}
		}
		if row.Function == "TestReceiverCompositionActivationReuseAndConflictBothStores" {
			for _, replacement := range []string{
				"",
				family145ReceiverNativeSettlementRead + "\n" + family145ReceiverNativeSettlementRead,
				strings.Replace(family145ReceiverNativeSettlementRead, "evt.ID()", "foreignEvent.ID()", 1),
				strings.Replace(family145ReceiverNativeSettlementRead, "selected.events", "foreignOwner.events", 1),
				strings.Replace(family145ReceiverNativeSettlementRead, "context.Background()", "ctx", 1),
				strings.Replace(family145ReceiverNativeSettlementRead, "storage.Outcomes", "storage.ForeignOutcomes", 1),
				family145ReceiverOriginalSettlementRead,
			} {
				if family145GateEntityConflictProofError(row.Function, row.Before, strings.Replace(row.After, family145ReceiverNativeSettlementRead, replacement, 1)) == nil {
					t.Fatalf("missing/duplicate/foreign/stale receiver settlement block passed: %s", replacement)
				}
			}
			for _, cut := range []string{"selected.events, runID, entityID", "unrelated.Fields[\"account_id\"] != \"unrelated-key\"", "reloaded.DeliveryRoutes[0].Target != target"} {
				if !strings.Contains(row.After, cut) || family145GateEntityConflictProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignReceiverCut()", 1)) == nil {
					t.Fatalf("composed proof lost exact fault identity or sibling/receipt assertion: %s", cut)
				}
			}
		}
	}
	if found != len(family145GateEntityConflictKeys) {
		t.Fatalf("entity conflict inventory=%d/%d", found, len(family145GateEntityConflictKeys))
	}
}
