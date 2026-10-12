package pipeline_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestA2GateRootDiagnosticReadsActualCompiledRootBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			p := newA2MapFanOutExecution(t, backend.open(t), false)
			before := p.instance(t)
			want, err := json.Marshal(before.Fields)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := storetest.ReadGateRunRootFieldsStorage(p.ctx, p.selected.events, p.runID)
			if err != nil || len(wire) == 0 || a2AccumulatorJSONHash(t, wire) != a2AccumulatorJSONHash(t, want) {
				t.Fatalf("physical diagnostic lost actual compiled root's fields: %s/%s %v", wire, want, err)
			}
			original := string(wire)
			wire[0] = '!'
			again, err := storetest.ReadGateRunRootFieldsStorage(p.ctx, p.selected.events, p.runID)
			if err != nil || string(again) != original {
				t.Fatalf("diagnostic retained a mutable byte buffer or changed native storage text: %s/%s %v", again, original, err)
			}
			if !reflect.DeepEqual(before, p.instance(t)) {
				t.Fatal("diagnostics changed compiled root projection, identity or lifecycle clock")
			}
		})
	}
}
