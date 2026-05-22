package store

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	"swarm/internal/testutil"
)

func TestPostgresStore_EventDeliveryRoutesPersistNodeTargetRows(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	pg := &PostgresStore{DB: db}
	evt := events.Event{
		ID:        uuid.NewString(),
		Type:      events.EventType("child/output.done"),
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}.WithTargetSet([]events.RouteIdentity{
		{EntityID: "ent-a", FlowInstance: "child-a/inst-1"},
		{EntityID: "ent-b", FlowInstance: "child-b/inst-1"},
	})
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	routes := []events.DeliveryRoute{
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{EntityID: "ent-a", FlowInstance: "child-a/inst-1"}},
		{SubscriberType: "node", SubscriberID: "workflow-runtime", Target: events.RouteIdentity{EntityID: "ent-b", FlowInstance: "child-b/inst-1"}},
	}
	if err := pg.InsertEventDeliveryRoutes(ctx, evt.ID, routes); err != nil {
		t.Fatalf("InsertEventDeliveryRoutes: %v", err)
	}

	got, err := pg.ListEventDeliveryRoutes(ctx, evt.ID)
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
