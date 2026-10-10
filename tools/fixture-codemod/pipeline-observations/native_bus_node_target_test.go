package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestNativeBusNodeTargetPreservesExactStorageDecodeAndAllFiveConsumers(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-node-target" {
			continue
		}
		matched++
		before := strings.Replace(row.Before, "db *sql.DB", "selected runtimebus.EventStore", 1)
		before = strings.Replace(before, "\tvar raw string\n\tif err := db.QueryRowContext(context.Background(), `\n\t\tSELECT delivery_target_route::text\n\t\tFROM event_deliveries\n\t\tWHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2\n\t`, eventID, nodeID).Scan(&raw); err != nil {", "\traw, err := storetest.ReadNodeDeliveryTargetEncoding(context.Background(), selected, eventID, nodeID)\n\tif err != nil {", 1)
		if before != row.After || selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("target read changed identity, decode, target kind or exact equality")
		}
		owner := selectedCausalObservationBody(t, "internal/store/internal/backend/delivery/read_projections.go", "ReadNodeDeliveryTargetEncodingTx")
		queries := eventDeliveryDiagnosticSQL(t, owner)
		if len(queries) != 2 || !reflect.DeepEqual(eventDeliveryDiagnosticSQL(t, row.Before), queries[1:]) ||
			!strings.Contains(owner, "eventID, nodeID).Scan(&raw)") {
			t.Fatal("node target physical read changed projection or predicate")
		}
	}
	if matched != 1 {
		t.Fatalf("node target recipes=%d,want1", matched)
	}
	source := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", "TestEventBusPublish_NestedThreeLevelConnectChainExecutesEndToEnd")
	if count, valid := nativeNodeObservationCallsUseOriginalOwner(source, "assertNodeDeliveryTarget"); !valid || count != 5 {
		t.Fatalf("target owner consumers=%d,valid=%t,want5", count, valid)
	}
	mutant := strings.Replace(source, "assertNodeDeliveryTarget(t, pg,", "assertNodeDeliveryTarget(t, db,", 1)
	if _, valid := nativeNodeObservationCallsUseOriginalOwner(mutant, "assertNodeDeliveryTarget"); mutant == source || valid {
		t.Fatal("raw target-read negative control passed")
	}
}
