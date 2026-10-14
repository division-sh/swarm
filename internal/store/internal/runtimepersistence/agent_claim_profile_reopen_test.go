package runtimepersistence

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestAgentFirstClaimProfileSurvivesNativeReopenAndRefusesClosedOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var open func() agentFixtureFlowStore
			if backend == "sqlite" {
				path := filepath.Join(t.TempDir(), "claim-profile.db")
				open = func() agentFixtureFlowStore {
					return newBootstrappedSQLiteRuntimeStoreForPath(t, path)
				}
			} else {
				dsn := testutil.StartPostgresDSN(t)
				open = func() agentFixtureFlowStore {
					selected, err := NewPostgresStore(dsn)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := selected.Close(); err != nil {
							t.Error(err)
						}
					})
					bootstrapTestPostgresStore(t, selected)
					return selected
				}
			}
			f := admitAgentClaimProfileFixture(t, testAuthorActivityContext(), open())
			_, first, original := f.claim(t)
			originalStorage := profileFixtureSnapshot(t, f)
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			reopened := f
			reopened.store = open()
			current, err := reopened.store.Snapshot(f.ctx, first.Claim.DeliveryID())
			if err != nil || !reflect.DeepEqual(first.Snapshot, current) {
				t.Fatalf("native reopen changed the unlaunched claim: before=%+v after=%+v err=%v", first.Snapshot, current, err)
			}
			captured, err := ReadFirstAgentClaimProfileForTest(f.ctx, reopened.store, current)
			if err != nil || captured != original {
				t.Fatalf("native reopen lost immutable first capture: %+v err=%v", captured, err)
			}
			// Bootstrap may write its own bookkeeping. Fence the attempted fault
			// after reopening, then conserve every physical application table.
			before := profileFixtureSnapshot(t, reopened)
			if !reflect.DeepEqual(originalStorage["event_delivery_attempts"], before["event_delivery_attempts"]) {
				t.Fatal("native reopen changed immutable attempt or capture bytes")
			}
			if err := f.ctx.Err(); err != nil {
				t.Fatal(err)
			}
			if err := ApplyAgentClaimProfileFaultForTest(f.ctx, f.store, first.Claim, original, AgentClaimProfileMissing, deliverylifecycle.Claim{}); err == nil {
				t.Fatal("closed original owner mutated durable capture")
			}
			if after := profileFixtureSnapshot(t, reopened); !reflect.DeepEqual(before, after) {
				t.Fatal("closed-owner refusal changed complete reopened storage")
			}
		})
	}
}
