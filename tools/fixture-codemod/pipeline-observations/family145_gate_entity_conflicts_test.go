package main

import (
	"fmt"
	"strings"
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
