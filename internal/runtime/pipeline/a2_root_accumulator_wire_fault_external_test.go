package pipeline_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestA2RootAccumulatorWireFaultKeepsOriginalCutsAndRestoresBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			proof := newA2CanonicalAccumulatorProof(t, backend.open(t))
			proof.execute(t, proof.publish(t, "numeric.requested", `{"id":"original","value":3,"amount":7,"nested":{"numbers":[1,2],"fraction":1.0}}`), "")
			waitForGateRecoveryQuiescence(t, proof.bus, proof.ctx)
			original, err := storetest.ReadRootFlowAccumulatorStorage(proof.ctx, proof.selected.events, proof.runID)
			if err != nil || len(original) == 0 || bytes.Equal(original, []byte("[]")) {
				t.Fatalf("actual compiled writer lacks positive retained accumulator: %s %v", original, err)
			}
			restored := false
			defer func() {
				if !restored {
					if changed, err := storetest.SetRootFlowAccumulatorWire(proof.ctx, proof.selected.events, proof.runID, original); err != nil || changed != 1 {
						t.Errorf("restore exact live root accumulator after assertion failure: %d %v", changed, err)
					}
				}
			}()
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(proof.ctx, proof.selected.events)
			if err != nil {
				t.Fatal(err)
			}
			probe := storetest.CollectTransactions(t, proof.selected.events, storetest.TransactionProbeOptions{})
			if changed, err := storetest.SetRootFlowAccumulatorWire(proof.ctx, proof.selected.events, proof.runID, []byte("[]")); err != nil || changed != 1 {
				t.Fatalf("exact root wire fault did not commit one existing row: %d %v", changed, err)
			}
			if got, err := storetest.ReadRootFlowAccumulatorStorage(proof.ctx, proof.selected.events, proof.runID); err != nil || !bytes.Equal(got, []byte("[]")) {
				t.Fatalf("wire fault was decoded/defaulted or written to the wrong header: %s %v", got, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 2 || counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 1 {
				t.Fatalf("wire/read bypassed exact selected admission or merged cuts: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(proof.ctx)
			cancel()
			if changed, err := storetest.SetRootFlowAccumulatorWire(cancelled, proof.selected.events, proof.runID, original); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled restoration supplied a write receipt: %d %v", changed, err)
			}
			if got, err := storetest.ReadRootFlowAccumulatorStorage(cancelled, proof.selected.events, proof.runID); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled read manufactured retained wire: %s %v", got, err)
			}
			if changed, err := storetest.SetRootFlowAccumulatorWire(proof.ctx, proof.selected.events, uuid.NewString(), original); err == nil || changed != 0 {
				t.Fatalf("absent run manufactured exact-one restoration: %d %v", changed, err)
			}
			if changed, err := storetest.SetRootFlowAccumulatorWire(proof.ctx, proof.selected.events, proof.runID, original); err != nil || changed != 1 {
				t.Fatalf("exact root restoration failed: %d %v", changed, err)
			}
			restored = true
			if got, err := storetest.ReadRootFlowAccumulatorStorage(proof.ctx, proof.selected.events, proof.runID); err != nil || !bytes.Equal(got, original) {
				t.Fatalf("restoration changed original physical wire: %s %v", got, err)
			}
			after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(proof.ctx, proof.selected.events)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("reversible root-wire fault changed other data/history/lease/clocks: %v", err)
			}
		})
	}
}
