package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type boundedRunDebugDeliveryStore interface {
	authorActivityReceiptStore
	LoadRunDebugReport(context.Context, string, RunDebugQueryOptions) (RunDebugReport, error)
}

func TestRunDebugReportBoundsDeliveryHydrationParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(boundedRunDebugDeliveryStore)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			base := time.Date(2026, 7, 23, 5, 0, 0, 0, time.UTC)
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)

			selectedFailure := seedBoundedRunDebugDelivery(
				t, ctx, fixture, selected, runID, "diagnostic.selected", "selected-agent",
				runtimedelivery.StateExhausted,
				testFailureEnvelope(runtimefailures.ClassRetryExhausted, "selected_dead_letter", nil),
				base.Add(3*time.Second),
			)
			excludedFailure := seedBoundedRunDebugDelivery(
				t, ctx, fixture, selected, runID, "diagnostic.excluded", "excluded-agent",
				runtimedelivery.StateRetrying,
				testFailureEnvelope(runtimefailures.ClassConnectorFailure, "excluded_retry", nil),
				base.Add(2*time.Second),
			)
			delivered := seedBoundedRunDebugDelivery(
				t, ctx, fixture, selected, runID, "diagnostic.delivered", "delivered-agent",
				runtimedelivery.StateDelivered, runtimefailures.Envelope{}, base.Add(time.Second),
			)
			seedBoundedRunDebugDelivery(
				t, ctx, fixture, selected, runID, "diagnostic.delivered.sibling", "delivered-agent",
				runtimedelivery.StateDelivered, runtimefailures.Envelope{}, base,
			)

			corruptOperatorAgentDeliveryTail(t, ctx, fixture, excludedFailure.DeliveryID)
			corruptOperatorAgentDeliveryTail(t, ctx, fixture, delivered.DeliveryID)

			report, err := selected.LoadRunDebugReport(ctx, runID, RunDebugQueryOptions{DeadLetterLimit: 1})
			if err != nil {
				t.Fatalf("load bounded run debug report: %v", err)
			}
			wantCounts := map[string]int{
				"delivered-agent\x00delivered":  2,
				"excluded-agent\x00failed":      1,
				"selected-agent\x00dead_letter": 1,
			}
			if len(report.Deliveries) != len(wantCounts) {
				t.Fatalf("delivery counts = %#v, want %#v", report.Deliveries, wantCounts)
			}
			for _, count := range report.Deliveries {
				key := count.SubscriberID + "\x00" + count.Status
				if wantCounts[key] != count.Count {
					t.Fatalf("delivery count %q = %d, want %d", key, count.Count, wantCounts[key])
				}
				delete(wantCounts, key)
			}
			if len(wantCounts) != 0 {
				t.Fatalf("missing delivery counts: %#v", wantCounts)
			}
			if len(report.FailedDeliveries) != 1 || report.FailedDeliveries[0].DeliveryID != selectedFailure.DeliveryID {
				t.Fatalf("failed deliveries = %#v, want selected %s", report.FailedDeliveries, selectedFailure.DeliveryID)
			}

			corruptOperatorAgentDeliveryTail(t, ctx, fixture, selectedFailure.DeliveryID)
			if _, err := selected.LoadRunDebugReport(ctx, runID, RunDebugQueryOptions{DeadLetterLimit: 1}); err == nil || !strings.Contains(err.Error(), "decode delivery target") {
				t.Fatalf("selected malformed failure error = %v, want canonical route decode failure", err)
			}
		})
	}
}

func seedBoundedRunDebugDelivery(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	store boundedRunDebugDeliveryStore,
	runID string,
	eventName string,
	subscriberID string,
	state runtimedelivery.State,
	failure runtimefailures.Envelope,
	occurredAt time.Time,
) runtimedelivery.Snapshot {
	t.Helper()
	event := eventtest.PersistedProjection(
		uuid.NewString(), events.EventType(eventName), "runtime", "", json.RawMessage(`{"bounded":true}`), 0,
		runID, "", events.EventEnvelope{}, occurredAt.Add(-time.Minute),
	)
	route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: subscriberID}
	if err := commitSemanticEventFixtureWithRoutes(ctx, store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("commit bounded run-debug event %s: %v", eventName, err)
	}
	var failurePtr *runtimefailures.Envelope
	if state == runtimedelivery.StateRetrying || state == runtimedelivery.StateExhausted {
		failurePtr = &failure
	}
	snapshot := seedDeliveryStateFixture(t, ctx, store, event, route, state, failurePtr)
	if fixture.dialect == runtimeauthoractivity.DialectPostgres {
		setPostgresDeliveryFixtureTimes(t, ctx, fixture.db, snapshot, occurredAt.Add(-time.Minute), occurredAt)
	} else {
		setSQLiteDeliveryFixtureTimes(t, ctx, fixture.db, snapshot, occurredAt.Add(-time.Minute), occurredAt)
	}
	return snapshot
}
