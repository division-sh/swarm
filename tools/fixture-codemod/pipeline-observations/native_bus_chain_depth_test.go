package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestNativeBusChainDepthPreservesPhysicalConstraintAndExactEvent(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-chain-depth" {
			continue
		}
		matched++
		want := "func (f completeEventDispatchFixture) updateChainDepth(depth int) error {\n\treturn storetest.SetEventChainDepth(f.ctx, f.store, f.event.ID(), depth)\n}"
		if row.After != want || selectedCausalObservationBody(t, row.File, row.Function) != want {
			t.Fatal("chain-depth fault changed exact selected owner, context or event")
		}
		owner := selectedCausalObservationBody(t, "internal/store/internal/backend/eventrecord/fixture_chain_depth.go", "SetFixtureEventChainDepthTx")
		before := strings.ReplaceAll(row.Before, "UPDATE events", "SELECT events")
		after := strings.ReplaceAll(owner, "UPDATE events", "SELECT events")
		if !reflect.DeepEqual(eventDeliveryDiagnosticSQL(t, before), eventDeliveryDiagnosticSQL(t, after)) ||
			!strings.Contains(owner, "tx.ExecContext(ctx, query, depth, eventID)") ||
			strings.Contains(owner, "depth <") {
			t.Fatal("chain-depth fixture bypassed the physical constraint or changed its update")
		}
	}
	if matched != 1 {
		t.Fatalf("chain-depth recipes=%d,want1", matched)
	}
}
