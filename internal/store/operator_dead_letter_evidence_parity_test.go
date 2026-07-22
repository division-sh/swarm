package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/google/uuid"
)

type operatorDeadLetterEvidenceStore interface {
	authorActivityReceiptStore
	UpsertAgent(context.Context, runtimemanager.PersistedAgent) error
	LoadOperatorAgentDeliveryDiagnostics(context.Context, string, OperatorAgentDeliveryDiagnosticsOptions) (OperatorAgentDeliveryDiagnostics, error)
	LoadOperatorEvent(context.Context, string) (OperatorEventFull, error)
	LoadRunDebugReport(context.Context, string, RunDebugQueryOptions) (RunDebugReport, error)
}

func TestOperatorDeadLetterEvidenceIsScopedToExactDeliveryParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(operatorDeadLetterEvidenceStore)
			ctx := testAuthorActivityContext()
			now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
			runID := uuid.NewString()
			eventID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			if err := selected.UpsertAgent(ctx, runtimemanager.PersistedAgent{
				Config: runtimeactors.AgentConfig{
					ID: "agent-a", Role: "worker", Type: "managed", Model: "regular", ExecutionMode: "live",
					Memory: agentmemory.PlatformDefault(), Config: json.RawMessage(`{"system_prompt":"delivery evidence"}`),
				},
				Status: "active", StartedAt: now,
			}); err != nil {
				t.Fatalf("upsert agent-a: %v", err)
			}
			event := eventtest.PersistedProjection(
				eventID, events.EventType("delivery.evidence"), "gateway", "", json.RawMessage(`{"message":"proof"}`), 0,
				runID, "", events.EventEnvelope{}, now,
			)
			routes := []events.DeliveryRoute{
				{
					SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: "agent-a",
					Target: events.RouteIdentity{FlowID: "flow-a", FlowInstance: "flow-a/one", EntityID: uuid.NewString()},
				},
				{
					SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: "agent-a",
					Target: events.RouteIdentity{FlowID: "flow-a", FlowInstance: "flow-a/two", EntityID: uuid.NewString()},
				},
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, routes); err != nil {
				t.Fatalf("commit event with sibling deliveries: %v", err)
			}

			snapshots := make(map[string]runtimedelivery.Snapshot, len(routes))
			for index, route := range routes {
				claimed, err := selected.ClaimAgentDelivery(ctx, event, route)
				if err != nil {
					t.Fatalf("claim %s: %v", route.SubscriberID, err)
				}
				failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "route_"+string(rune('a'+index))+"_failed", nil)
				snapshot, err := selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
					Disposition: runtimedelivery.FailureDeadLetter,
					ReasonCode:  failure.Detail.Code,
					Failure:     &failure,
				})
				if err != nil {
					t.Fatalf("dead-letter %s: %v", route.SubscriberID, err)
				}
				snapshots[snapshot.DeliveryID] = snapshot
			}

			diagnostics, err := selected.LoadOperatorAgentDeliveryDiagnostics(ctx, "agent-a", OperatorAgentDeliveryDiagnosticsOptions{})
			if err != nil {
				t.Fatalf("load agent diagnostics: %v", err)
			}
			if len(diagnostics.DeadLetters) != 2 {
				t.Fatalf("agent dead letters = %#v, want both route-level deliveries", diagnostics.DeadLetters)
			}
			for _, delivery := range diagnostics.DeadLetters {
				assertExactOperatorDeadLetterEvidence(t, delivery.DeadLetterRecords, snapshots[delivery.DeliveryID])
			}

			full, err := selected.LoadOperatorEvent(ctx, eventID)
			if err != nil {
				t.Fatalf("load operator event: %v", err)
			}
			if len(full.DeadLetters) != 2 || len(full.Deliveries) != 2 {
				t.Fatalf("operator event evidence = dead_letters:%#v deliveries:%#v", full.DeadLetters, full.Deliveries)
			}
			for _, delivery := range full.Deliveries {
				assertExactOperatorDeadLetterEvidence(t, delivery.DeadLetters, snapshots[delivery.DeliveryID])
			}

			report, err := selected.LoadRunDebugReport(ctx, runID, RunDebugQueryOptions{})
			if err != nil {
				t.Fatalf("load run debug report: %v", err)
			}
			if len(report.FailedDeliveries) != 2 {
				t.Fatalf("run failed deliveries = %#v, want two", report.FailedDeliveries)
			}
			for _, delivery := range report.FailedDeliveries {
				assertExactOperatorDeadLetterEvidence(t, delivery.DeadLetters, snapshots[delivery.DeliveryID])
			}
		})
	}
}

func assertExactOperatorDeadLetterEvidence(t *testing.T, records []OperatorDeadLetterRecord, snapshot runtimedelivery.Snapshot) {
	t.Helper()
	if len(records) != 1 {
		t.Fatalf("dead-letter records for %s = %#v, want one", snapshot.DeliveryID, records)
	}
	if got := records[0]; got.DeliveryID != snapshot.DeliveryID || got.ClaimVersion != snapshot.ClaimVersion || got.Failure.Detail.Code != snapshot.ReasonCode {
		t.Fatalf("dead-letter record = %#v, want delivery=%s claim=%d reason=%s", got, snapshot.DeliveryID, snapshot.ClaimVersion, snapshot.ReasonCode)
	}
}
