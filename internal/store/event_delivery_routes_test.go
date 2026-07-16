package store

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_EventDeliveryRoutesPersistNodeTargetRows(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := testAuthorActivityContext()
	pg := newTestPostgresStore(t, db)
	evt := eventtest.PersistedProjectionForProducer(
		uuid.NewString(),
		events.EventType("child/output.done"),
		events.NodeProducer("workflow-runtime"),
		"",
		[]byte(`{}`),
		0,
		"",
		"",
		events.EnvelopeForTargetSet(events.EventEnvelope{}, []events.RouteIdentity{
			{EntityID: "ent-a", FlowInstance: "child-a/inst-1"},
			{EntityID: "ent-b", FlowInstance: "child-b/inst-1"},
		}),
		time.Now().UTC(),
	)

	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	routes := []events.DeliveryRoute{
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{EntityID: "ent-a", FlowInstance: "child-a/inst-1"}},
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{EntityID: "ent-b", FlowInstance: "child-b/inst-1"}},
	}
	if err := pg.InsertEventDeliveryRoutes(ctx, evt.ID(), routes); err != nil {
		t.Fatalf("InsertEventDeliveryRoutes: %v", err)
	}

	got, err := pg.ListEventDeliveryRoutes(ctx, evt.ID())
	if err != nil {
		t.Fatalf("ListEventDeliveryRoutes: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("delivery routes = %#v, want 2", got)
	}
	seen := map[string]struct{}{}
	for _, route := range got {
		if route.SubscriberType != "node" || route.SubscriberID != "workflow-runtime" {
			t.Fatalf("delivery route = %#v, want node/workflow-runtime", route)
		}
		seen[route.Target.EntityID] = struct{}{}
	}
	for _, want := range []string{"ent-a", "ent-b"} {
		if _, ok := seen[want]; !ok {
			t.Fatalf("missing delivery target %q in %#v", want, got)
		}
	}
}

func TestDeliveryRouteSyntheticProjectionRoundTripsOnBothBackends(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error)
	}{
		{name: "postgres", setup: setupPostgresReplyContextStoreTest},
		{name: "sqlite", setup: setupSQLiteReplyContextStoreTest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, seed := tc.setup(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			eventID := uuid.NewString()
			if err := seed(ctx, runID, eventID); err != nil {
				t.Fatalf("seed event: %v", err)
			}
			projection, err := events.NewDeliveryPayloadProjection(map[string]string{
				"validation_case_id": uuid.NewString(),
			})
			if err != nil {
				t.Fatalf("NewDeliveryPayloadProjection: %v", err)
			}
			want := events.DeliveryRoute{
				SubscriberType:    "node",
				SubscriberID:      "validator-node",
				Target:            events.RouteIdentity{FlowID: "validator", FlowInstance: "validator/one", EntityID: uuid.NewString()},
				PayloadProjection: projection,
			}
			if err := store.InsertEventDeliveryRoutes(ctx, eventID, []events.DeliveryRoute{want}); err != nil {
				t.Fatalf("InsertEventDeliveryRoutes: %v", err)
			}
			got, err := store.ListEventDeliveryRoutes(ctx, eventID)
			if err != nil {
				t.Fatalf("ListEventDeliveryRoutes: %v", err)
			}
			if len(got) != 1 || got[0] != want.Normalized() {
				t.Fatalf("delivery routes = %#v, want %#v", got, want.Normalized())
			}
		})
	}
}
