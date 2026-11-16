package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestNativeBusTargetConflictKeepsEveryPhysicalDialectStatement(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	owners := map[string][2]string{"corruptPreparedDeliveryRow": {"test_prepared_target_conflict.go", "SetPreparedTargetConflictForTest"}, "loadPreparedDeliveryCorruption": {"test_selected_read_projections_queries.go", "readPreparedTargetConflictTx"}}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-target-conflict" || owners[row.Function][0] == "" {
			continue
		}
		matched++
		before := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(row.Before, "UPDATE event_deliveries", "SELECT event_deliveries"))
		var owner string
		if row.Function == "corruptPreparedDeliveryRow" {
			for _, receiver := range []string{"DeliverySQLiteOwner", "DeliveryPostgresOwner"} {
				owner += selectedCausalObservationBody(t, "internal/store/internal/backend/delivery/test_prepared_target_conflict.go", "SetPreparedTargetConflictForTest", receiver) + "\n"
			}
		} else {
			owner = selectedCausalObservationBody(t, "internal/store/internal/backend/delivery/"+owners[row.Function][0], owners[row.Function][1])
		}
		after := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(owner, "UPDATE event_deliveries", "SELECT event_deliveries"))
		if len(before) != 2 || !reflect.DeepEqual(before, after) {
			t.Fatalf("event/identity predicates, update columns, native JSON cast or COUNT/MIN projection changed: %v -> %v", before, after)
		}
	}
	if matched != 2 {
		t.Fatalf("physical conflict owners=%d,want2", matched)
	}
}
