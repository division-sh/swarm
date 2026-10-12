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

func family145GateEntityConflictProofError(function, before, after string) error {
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
	}
	if found != len(family145GateEntityConflictKeys) {
		t.Fatalf("entity conflict inventory=%d/%d", found, len(family145GateEntityConflictKeys))
	}
}
