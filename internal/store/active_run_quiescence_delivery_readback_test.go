package store

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/google/uuid"
)

type activeRunDeliveryQuiescenceReadStore interface {
	authorActivityReceiptStore
	ApplyActiveRunQuiescence(context.Context, runtimerunquiescence.Request) (runtimerunquiescence.Result, error)
	ActiveRunDeliveryQuiesced(context.Context, string, string, string) (string, bool, error)
}

var _ activeRunDeliveryQuiescenceReadStore = (*PostgresStore)(nil)
var _ activeRunDeliveryQuiescenceReadStore = (*SQLiteRuntimeStore)(nil)

func TestActiveRunDeliveryQuiescenceReadbackParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(activeRunDeliveryQuiescenceReadStore)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 7, 22, 18, 0, 0, 0, time.UTC)
			runID := uuid.NewString()
			eventID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			event := eventtest.RunCreatingRootIngress(
				eventID, events.EventType("quiescence.requested"), "gateway", "", nil, 0,
				runID, "", events.EventEnvelope{}, now,
			)
			route := events.DeliveryRoute{
				SubscriberType: string(runtimedelivery.SubscriberAgent),
				SubscriberID:   "agent-a",
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("commit active-run delivery: %v", err)
			}
			claimed, err := selected.ClaimAgentDelivery(ctx, event, route)
			if err != nil {
				t.Fatalf("claim active-run delivery: %v", err)
			}
			if claimed.Snapshot.Status != runtimedelivery.StatusInProgress {
				t.Fatalf("claimed status = %q, want in_progress", claimed.Snapshot.Status)
			}

			result, err := selected.ApplyActiveRunQuiescence(ctx, runtimerunquiescence.Request{
				OperationName: "test_active_run_delivery_quiescence_readback",
				RequestedAt:   now.Add(time.Minute),
				RunIDs:        []string{runID},
				ReasonCode:    runtimerunquiescence.ServeAbandonReasonCode,
				ControlledBy:  "test",
				DeliveryNote:  "test active-run delivery quiescence",
			})
			if err != nil {
				t.Fatalf("ApplyActiveRunQuiescence: %v", err)
			}
			if len(result.Deliveries) != 1 || !result.Deliveries[0].Changed {
				t.Fatalf("quiesced deliveries = %#v, want one changed delivery", result.Deliveries)
			}

			reason, quiesced, err := selected.ActiveRunDeliveryQuiesced(ctx, eventID, string(runtimedelivery.SubscriberAgent), "agent-a")
			if err != nil || !quiesced || reason != runtimerunquiescence.ServeAbandonReasonCode {
				t.Fatalf("active-run delivery quiescence = reason:%q quiesced:%v err:%v", reason, quiesced, err)
			}
			if reason, quiesced, err := selected.ActiveRunDeliveryQuiesced(ctx, eventID, string(runtimedelivery.SubscriberAgent), "agent-b"); err != nil || quiesced || reason != "" {
				t.Fatalf("unrelated delivery quiescence = reason:%q quiesced:%v err:%v", reason, quiesced, err)
			}
		})
	}
}
