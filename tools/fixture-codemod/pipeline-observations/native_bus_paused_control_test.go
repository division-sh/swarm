package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestNativeBusPausedControlFaultRetainsBothPosturesAndEveryClaimRefusalAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-paused-control-fault" {
			continue
		}
		matched++
		if strings.Replace(row.Before, "\t\t\t\tquery := `DELETE FROM run_control_state WHERE run_id=$1`\n\t\t\t\tif posture == \"contradictory_control\" {\n\t\t\t\t\tquery = `UPDATE run_control_state SET control_status='running' WHERE run_id=$1`\n\t\t\t\t}\n\t\t\t\tif _, err := f.db.ExecContext(f.ctx, query, f.event.RunID()); err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}", "\t\t\t\tvar faultErr error\n\t\t\t\tif posture == \"contradictory_control\" {\n\t\t\t\t\tfaultErr = storetest.ContradictPausedRunControl(f.ctx, f.store, f.event.RunID())\n\t\t\t\t} else {\n\t\t\t\t\tfaultErr = storetest.RemovePausedRunControl(f.ctx, f.store, f.event.RunID())\n\t\t\t\t}\n\t\t\t\tif faultErr != nil {\n\t\t\t\t\tt.Fatal(faultErr)\n\t\t\t\t}", 1) != row.After || selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("control fault changed its predecessor pause, scan refusal, non-acquisition or snapshot conservation")
		}
		remove := selectedCausalObservationBody(t, "internal/store/internal/backend/runlifecycle/standalone_fixture_storage.go", "RemovePausedRunControlFixtureTx")
		contradict := selectedCausalObservationBody(t, "internal/store/internal/backend/runlifecycle/standalone_fixture_storage.go", "ContradictPausedRunControlFixtureTx")
		before := strings.ReplaceAll(strings.ReplaceAll(row.Before, "DELETE FROM", "SELECT * FROM"), "UPDATE run_control_state", "SELECT * FROM run_control_state")
		owners := strings.ReplaceAll(strings.ReplaceAll(remove+"\n"+contradict, "DELETE FROM", "SELECT * FROM"), "UPDATE run_control_state", "SELECT * FROM run_control_state")
		if !reflect.DeepEqual(eventDeliveryDiagnosticSQL(t, before), eventDeliveryDiagnosticSQL(t, owners)) {
			t.Fatal("named control faults changed exact physical run predicates")
		}
		for _, owner := range []string{remove, contradict} {
			if !strings.Contains(owner, "if count != 1") || !strings.Contains(owner, "tx.ExecContext(ctx,") || !strings.Contains(owner, ", runID)") {
				t.Fatal("control fault lost exact selected transaction/cardinality")
			}
		}
	}
	if matched != 1 {
		t.Fatalf("paused control recipes=%d,want1", matched)
	}
}
