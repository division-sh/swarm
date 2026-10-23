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
	owners := map[string][2]string{"corruptPreparedDeliveryRow": {"lifecycle.go", "SetPreparedTargetConflictTx"}, "loadPreparedDeliveryCorruption": {"read_projections.go", "ReadPreparedTargetConflictTx"}}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-target-conflict" || owners[row.Function][0] == "" {
			continue
		}
		matched++
		before := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(row.Before, "UPDATE event_deliveries", "SELECT event_deliveries"))
		owner := selectedCausalObservationBody(t, "internal/store/internal/backend/delivery/"+owners[row.Function][0], owners[row.Function][1])
		after := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(owner, "UPDATE event_deliveries", "SELECT event_deliveries"))
		if len(before) != 2 || !reflect.DeepEqual(before, after) {
			t.Fatalf("event/identity predicates, update columns, native JSON cast or COUNT/MIN projection changed: %v -> %v", before, after)
		}
	}
	if matched != 2 {
		t.Fatalf("physical conflict owners=%d,want2", matched)
	}
}
