package runtimepersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestWorkspaceMockInvocationStorageRetainsExactCountsBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			if got, err := ReadWorkspaceMockInvocationStorageForTest(ctx, fixture.store); err != nil || got != (WorkspaceMockInvocationStorage{}) {
				t.Fatalf("fresh invocation counts: %+v %v", got, err)
			}
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			at := time.Now().UTC()
			completed := eventtest.PersistedProjection(uuid.NewString(), "work.completed", "runtime", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, at)
			if err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, completed, []string{"worker", "pending-worker"}); err != nil {
				t.Fatal(err)
			}
			other := eventtest.PersistedProjection(uuid.NewString(), "work.other", "runtime", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, at.Add(time.Second))
			if err := commitSemanticEventFixtureWithAgents(ctx, fixture.store, other, nil); err != nil {
				t.Fatal(err)
			}
			claimed, err := claimDeliveryFixture(ctx, fixture.store, completed, testAgentDeliveryRoute(t, runID, "worker", "fixture/worker"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
				t.Fatal(err)
			}
			before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil {
				t.Fatal(err)
			}
			want := WorkspaceMockInvocationStorage{AgentDeliveries: 2, Delivered: 1, Emitted: 1}
			got, err := ReadWorkspaceMockInvocationStorageForTest(ctx, fixture.store)
			if err != nil || got != want {
				t.Fatalf("fixed invocation counts: %+v %v, want %+v", got, err, want)
			}
			after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("observation changed invocation storage: %v", err)
			}
			if _, err := fixture.db.ExecContext(ctx, `ALTER TABLE events RENAME TO inaccessible_invocation_events`); err != nil {
				t.Fatal(err)
			}
			if got, err := ReadWorkspaceMockInvocationStorageForTest(ctx, fixture.store); err == nil || got != (WorkspaceMockInvocationStorage{}) {
				t.Fatalf("failed observation returned partial counts: %+v %v", got, err)
			}
		})
	}
}
