package pipeline_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestA2RetainedFanOutMetadataFaultsPreserveClaimClocksAndRestoreBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			p := newA2MapFanOutExecution(t, backend.open(t), false)
			p.publish(t, "batch.ready", map[string]any{"A": []int64{1}, "a": []int64{2}, "z": []int64{3}})
			p.restart(t)
			handoff := p.handoff(t)
			_, claim, found, err := handoff.owner.ClaimFanOutIntent(p.ctx, pipeline.FanOutClaimRequest{Owner: "native-metadata-fault", BundleHash: p.fact.BundleHash(), Candidate: &handoff.key, Now: time.Now().UTC(), Lease: time.Minute})
			if err != nil || !found {
				t.Fatalf("real compiled intent has no genuine claim: %t %v", found, err)
			}
			defer func() {
				if result, err := handoff.owner.ReleaseFanOutClaim(p.ctx, claim); err != nil || !result.Acknowledged {
					t.Errorf("release exact metadata proof claim: %+v %v", result, err)
				}
			}()
			wire, err := storetest.ReadFanOutRunCapsuleWire(p.ctx, p.selected.events, p.runID)
			if err != nil || len(wire) == 0 || bytes.Equal(wire, []byte("{}")) {
				t.Fatalf("real compiled intent has no positive capsule wire: %s %v", wire, err)
			}
			metadata, err := storetest.ReadFanOutRunClaimMetadataStorage(p.ctx, p.selected.events, p.runID)
			if err != nil || metadata.Cardinality != 3 || metadata.Cursor != 0 || metadata.Owner != "native-metadata-fault" || metadata.Generation == "" || metadata.Lease == "" {
				t.Fatalf("metadata observation lost exact claim/clock/captured source: %+v %v", metadata, err)
			}
			restored := false
			defer func() {
				if !restored {
					if _, err := storetest.SetRetainedFanOutCapsuleWire(p.ctx, p.selected.events, p.runID, wire); err != nil {
						t.Error(err)
					}
					if _, err := storetest.SetRetainedFanOutCardinality(p.ctx, p.selected.events, p.runID, metadata.Cardinality); err != nil {
						t.Error(err)
					}
				}
			}()
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(p.ctx, p.selected.events)
			if err != nil {
				t.Fatal(err)
			}
			probe := storetest.CollectTransactions(t, p.selected.events, storetest.TransactionProbeOptions{})
			if changed, err := storetest.SetRetainedFanOutCapsuleWire(p.ctx, p.selected.events, p.runID, []byte("{}")); err != nil || changed != 1 {
				t.Fatalf("capsule fault did not commit the exact existing row: %d %v", changed, err)
			}
			want := metadata
			want.Capsule = "{}"
			if got, err := storetest.ReadFanOutRunClaimMetadataStorage(p.ctx, p.selected.events, p.runID); err != nil || got != want {
				t.Fatalf("capsule fault changed other retained claim/clock coordinates: %+v %v", got, err)
			}
			if got, err := storetest.ReadFanOutRunCapsuleWire(p.ctx, p.selected.events, p.runID); err != nil || !bytes.Equal(got, []byte("{}")) {
				t.Fatalf("capsule read decoded/reconstructed corrupt wire: %s %v", got, err)
			}
			if changed, err := storetest.SetRetainedFanOutCapsuleWire(p.ctx, p.selected.events, p.runID, wire); err != nil || changed != 1 {
				t.Fatalf("capsule restoration failed: %d %v", changed, err)
			}
			if changed, err := storetest.SetRetainedFanOutCardinality(p.ctx, p.selected.events, p.runID, 4); err != nil || changed != 1 {
				t.Fatalf("cardinality fault failed: %d %v", changed, err)
			}
			want = metadata
			want.Cardinality = 4
			if got, err := storetest.ReadFanOutRunClaimMetadataStorage(p.ctx, p.selected.events, p.runID); err != nil || got != want {
				t.Fatalf("cardinality fault changed other retained coordinates: %+v %v", got, err)
			}
			if changed, err := storetest.SetRetainedFanOutCardinality(p.ctx, p.selected.events, p.runID, metadata.Cardinality); err != nil || changed != 1 {
				t.Fatalf("cardinality restoration failed: %d %v", changed, err)
			}
			restored = true
			if got, err := storetest.ReadFanOutRunClaimMetadataStorage(p.ctx, p.selected.events, p.runID); err != nil || got != metadata {
				t.Fatalf("restoration changed exact original physical claim/clock wire: %+v %v", got, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 8 || counts.Total.WriteCommits != 4 || counts.Total.ReadCommits != 4 {
				t.Fatalf("metadata cuts escaped original selected coordinator: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(p.ctx)
			cancel()
			if changed, err := storetest.SetRetainedFanOutCapsuleWire(cancelled, p.selected.events, p.runID, []byte("{}")); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled capsule fault returned a commit receipt: %d %v", changed, err)
			}
			if changed, err := storetest.SetRetainedFanOutCardinality(cancelled, p.selected.events, p.runID, 4); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled cardinality fault returned a commit receipt: %d %v", changed, err)
			}
			if got, err := storetest.ReadFanOutRunCapsuleWire(cancelled, p.selected.events, p.runID); !errors.Is(err, context.Canceled) || got != nil {
				t.Fatalf("cancelled metadata read supplied capsule bytes: %s %v", got, err)
			}
			if got, err := storetest.ReadFanOutRunClaimMetadataStorage(cancelled, p.selected.events, p.runID); !errors.Is(err, context.Canceled) || got != (storetest.FanOutRunClaimMetadataStorage{}) {
				t.Fatalf("cancelled metadata read supplied a claim witness: %+v %v", got, err)
			}
			foreign := uuid.NewString()
			if changed, err := storetest.SetRetainedFanOutCardinality(p.ctx, p.selected.events, foreign, 4); err == nil || changed != 0 {
				t.Fatalf("absent intent supplied a metadata write receipt: %d %v", changed, err)
			}
			after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(p.ctx, p.selected.events)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("reversible metadata faults changed source/history/data/claim/clocks: %v", err)
			}
		})
	}
}
