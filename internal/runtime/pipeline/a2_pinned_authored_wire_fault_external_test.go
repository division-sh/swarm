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

func TestA2PinnedAuthoredWireFaultKeepsOriginalOwnerScopeAndRestoresBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			f := newA2CorruptMapSource(t, backend.open(t))
			defer func() {
				if result, err := f.handoff.owner.ReleaseFanOutClaim(f.p.ctx, f.claim); err != nil || !result.Acknowledged {
					t.Errorf("release exact source proof claim: %+v %v", result, err)
				}
			}()
			restored := false
			defer func() {
				if !restored {
					if err := f.writeRaw(f.raw); err != nil {
						t.Error(err)
					}
				}
			}()
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.p.ctx, f.p.selected.events)
			if err != nil {
				t.Fatal(err)
			}
			probe := storetest.CollectTransactions(t, f.p.selected.events, storetest.TransactionProbeOptions{})
			if changed, err := storetest.SetPinnedAuthoredMutationWire(f.p.ctx, f.p.selected.events, f.intent.Source, []byte("null")); err != nil || changed != 1 {
				t.Fatalf("hostile wire missed exact real source: %d %v", changed, err)
			}
			if got, err := storetest.ReadPinnedAuthoredMutationStorage(f.p.ctx, f.p.selected.events, f.intent.Source); err != nil || !bytes.Equal(got, []byte("null")) {
				t.Fatalf("fault reconstructed rather than preserving hostile wire: %s %v", got, err)
			}
			if changed, err := storetest.SetPinnedAuthoredMutationWire(f.p.ctx, f.p.selected.events, f.intent.Source, f.raw); err != nil || changed != 1 {
				t.Fatalf("restore exact compiled writer source: %d %v", changed, err)
			}
			restored = true
			if got, err := storetest.ReadPinnedAuthoredMutationStorage(f.p.ctx, f.p.selected.events, f.intent.Source); err != nil || !bytes.Equal(got, f.raw) {
				t.Fatalf("restoration changed exact physical bytes: %s/%s %v", got, f.raw, err)
			}
			if counts := probe.Snapshot(); counts.Active != 0 || counts.Total.Begun != 4 || counts.Total.WriteCommits != 2 || counts.Total.ReadCommits != 2 {
				t.Fatalf("fault/read/restore/read escaped original coordinator cuts: %+v", counts)
			}
			for _, coordinate := range []string{"run", "entity", "mutation", "field"} {
				foreign := f.intent.Source
				switch coordinate {
				case "run":
					foreign.RunID = uuid.NewString()
				case "entity":
					foreign.EntityID = uuid.NewString()
				case "mutation":
					foreign.MutationID = uuid.NewString()
				case "field":
					foreign.Field += ".foreign"
				}
				if changed, err := storetest.SetPinnedAuthoredMutationWire(f.p.ctx, f.p.selected.events, foreign, []byte("null")); err == nil || changed != 0 {
					t.Fatalf("foreign %s acquired write receipt: %d %v", coordinate, changed, err)
				}
			}
			cancelled, cancel := context.WithCancel(f.p.ctx)
			cancel()
			if changed, err := storetest.SetPinnedAuthoredMutationWire(cancelled, f.p.selected.events, f.intent.Source, []byte("null")); !errors.Is(err, context.Canceled) || changed != 0 {
				t.Fatalf("cancelled source fault returned a write receipt: %d %v", changed, err)
			}
			after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(f.p.ctx, f.p.selected.events)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("restoration/refusals changed source, sibling, state, intent, history or clocks: %v", err)
			}
		})
	}
}
