package store

import (
	"context"
	"encoding/json"
	"fmt"
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

type operatorAgentDeliveryPageStore interface {
	authorActivityReceiptStore
	UpsertAgent(context.Context, runtimemanager.PersistedAgent) error
	LoadOperatorAgentDeliveryLifecycle(context.Context, string, OperatorAgentDeliveryLifecycleOptions) (OperatorAgentDeliveryLifecycleList, error)
	LoadOperatorAgentDeliveryDiagnostics(context.Context, string, OperatorAgentDeliveryDiagnosticsOptions) (OperatorAgentDeliveryDiagnostics, error)
}

func TestOperatorAgentDeliveryPagesBoundHydrationParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(operatorAgentDeliveryPageStore)
			ctx := testAuthorActivityContext()
			now := time.Now().UTC().Truncate(time.Second)
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
			if err := selected.UpsertAgent(ctx, runtimemanager.PersistedAgent{
				Config: runtimeactors.AgentConfig{
					ID: "agent-a", Role: "worker", Type: "managed", Model: "regular", ExecutionMode: "live",
					Memory: agentmemory.PlatformDefault(), Config: json.RawMessage(`{"system_prompt":"bounded delivery pages"}`),
				},
				Status: "active", StartedAt: now,
			}); err != nil {
				t.Fatalf("upsert agent: %v", err)
			}

			route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: "agent-a"}
			failed := seedOperatorAgentDeliveryPageHistory(t, ctx, fixture, selected, route, runID, "failed", now)
			deadLetters := seedOperatorAgentDeliveryPageHistory(t, ctx, fixture, selected, route, runID, "dead_letter", now.Add(-10*time.Minute))

			firstLifecycle, err := selected.LoadOperatorAgentDeliveryLifecycle(ctx, "agent-a", OperatorAgentDeliveryLifecycleOptions{
				RunID: runID, Statuses: []string{"failed"}, Limit: 1,
			})
			if err != nil || len(firstLifecycle.Deliveries) != 1 || firstLifecycle.Deliveries[0].DeliveryID != failed[0] || firstLifecycle.NextCursor == "" {
				t.Fatalf("first lifecycle cursor page = %#v err=%v", firstLifecycle, err)
			}
			secondLifecycle, err := selected.LoadOperatorAgentDeliveryLifecycle(ctx, "agent-a", OperatorAgentDeliveryLifecycleOptions{
				RunID: runID, Statuses: []string{"failed"}, Limit: 1, Cursor: firstLifecycle.NextCursor,
			})
			if err != nil || len(secondLifecycle.Deliveries) != 1 || secondLifecycle.Deliveries[0].DeliveryID != failed[1] || secondLifecycle.NextCursor == "" {
				t.Fatalf("second lifecycle cursor page = %#v err=%v", secondLifecycle, err)
			}

			firstDiagnostics, err := selected.LoadOperatorAgentDeliveryDiagnostics(ctx, "agent-a", OperatorAgentDeliveryDiagnosticsOptions{
				FailureLimit: 1, DeadLetterLimit: 1,
			})
			if err != nil || firstDiagnostics.FailuresNextCursor == "" || firstDiagnostics.DeadLettersNextCursor == "" {
				t.Fatalf("first diagnostics cursor page = %#v err=%v", firstDiagnostics, err)
			}
			secondDiagnostics, err := selected.LoadOperatorAgentDeliveryDiagnostics(ctx, "agent-a", OperatorAgentDeliveryDiagnosticsOptions{
				FailureLimit: 1, FailureCursor: firstDiagnostics.FailuresNextCursor,
				DeadLetterLimit: 1, DeadLetterCursor: firstDiagnostics.DeadLettersNextCursor,
			})
			if err != nil || len(secondDiagnostics.Failures) != 1 || secondDiagnostics.Failures[0].DeliveryID != failed[1] ||
				len(secondDiagnostics.DeadLetters) != 1 || secondDiagnostics.DeadLetters[0].DeliveryID != deadLetters[1] {
				t.Fatalf("second diagnostics cursor page = %#v err=%v", secondDiagnostics, err)
			}

			corruptOperatorAgentDeliveryTail(t, ctx, fixture, failed[2])
			corruptOperatorAgentDeliveryTail(t, ctx, fixture, deadLetters[2])

			lifecycle, err := selected.LoadOperatorAgentDeliveryLifecycle(ctx, "agent-a", OperatorAgentDeliveryLifecycleOptions{
				RunID: runID, Statuses: []string{"failed"}, Limit: 1,
			})
			if err != nil {
				t.Fatalf("load bounded lifecycle page: %v", err)
			}
			if len(lifecycle.Deliveries) != 1 || lifecycle.Deliveries[0].DeliveryID != failed[0] || lifecycle.NextCursor == "" {
				t.Fatalf("bounded lifecycle page = %#v, want newest failure plus cursor", lifecycle)
			}

			diagnostics, err := selected.LoadOperatorAgentDeliveryDiagnostics(ctx, "agent-a", OperatorAgentDeliveryDiagnosticsOptions{
				FailureLimit: 1, DeadLetterLimit: 1,
			})
			if err != nil {
				t.Fatalf("load bounded diagnostics pages: %v", err)
			}
			if diagnostics.Summary.Failures24h != 3 || diagnostics.Summary.DeadLetters24h != 3 {
				t.Fatalf("bounded diagnostics summary = %#v, want three failures and three dead letters", diagnostics.Summary)
			}
			if len(diagnostics.Failures) != 1 || diagnostics.Failures[0].DeliveryID != failed[0] || diagnostics.FailuresNextCursor == "" {
				t.Fatalf("bounded failure page = %#v cursor=%q", diagnostics.Failures, diagnostics.FailuresNextCursor)
			}
			if len(diagnostics.DeadLetters) != 1 || diagnostics.DeadLetters[0].DeliveryID != deadLetters[0] || diagnostics.DeadLettersNextCursor == "" {
				t.Fatalf("bounded dead-letter page = %#v cursor=%q", diagnostics.DeadLetters, diagnostics.DeadLettersNextCursor)
			}
		})
	}
}

func seedOperatorAgentDeliveryPageHistory(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	store operatorAgentDeliveryPageStore,
	route events.DeliveryRoute,
	runID string,
	status string,
	base time.Time,
) []string {
	t.Helper()
	deliveryIDs := make([]string, 0, 3)
	for index := 0; index < 3; index++ {
		occurredAt := base.Add(-time.Duration(index) * time.Minute)
		event := eventtest.PersistedProjection(
			uuid.NewString(), events.EventType(fmt.Sprintf("page.%s.%d", status, index)), "gateway", "", json.RawMessage(`{}`), 0,
			runID, "", events.EventEnvelope{}, occurredAt.Add(-time.Minute),
		)
		if err := commitSemanticEventFixture(ctx, store, event); err != nil {
			t.Fatalf("commit %s page event %d: %v", status, index, err)
		}
		failure := testFailureEnvelope(runtimefailures.ClassConnectorFailure, fmt.Sprintf("page_%s_%d", status, index), nil)
		state := runtimedelivery.StateRetrying
		if status == "dead_letter" {
			state = runtimedelivery.StateExhausted
			failure = testFailureEnvelope(runtimefailures.ClassRetryExhausted, fmt.Sprintf("page_%s_%d", status, index), nil)
		}
		snapshot := seedAgentDeliveryStateFixture(t, ctx, store, event, route, state, &failure)
		if fixture.dialect == "postgres" {
			setPostgresDeliveryFixtureTimes(t, ctx, fixture.db, snapshot, occurredAt.Add(-time.Minute), occurredAt)
		} else {
			setSQLiteDeliveryFixtureTimes(t, ctx, fixture.db, snapshot, occurredAt.Add(-time.Minute), occurredAt)
		}
		deliveryIDs = append(deliveryIDs, snapshot.DeliveryID)
	}
	return deliveryIDs
}

func corruptOperatorAgentDeliveryTail(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, deliveryID string) {
	t.Helper()
	query := `UPDATE event_deliveries SET delivery_target_route = '[]' WHERE delivery_id = ?`
	args := []any{deliveryID}
	if fixture.dialect == "postgres" {
		query = `UPDATE event_deliveries SET delivery_target_route = '[]'::jsonb WHERE delivery_id = $1::uuid`
	}
	result, err := fixture.db.ExecContext(ctx, query, args...)
	if err != nil {
		t.Fatalf("corrupt out-of-page delivery %s: %v", deliveryID, err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("corrupt out-of-page delivery %s affected %d rows, err=%v", deliveryID, rows, rowsErr)
	}
}
