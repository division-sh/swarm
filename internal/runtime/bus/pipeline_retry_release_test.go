package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type retryReleaseInterceptor struct {
	eventID string
}

func (i retryReleaseInterceptor) Intercept(_ context.Context, event events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if event.ID() != i.eventID {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	return false, nil, runtimepipelineobligation.ReleaseForRetry("activity_contract_pin_unavailable", nil), nil
}

type retryReleaseSetInterceptor struct {
	eventIDs map[string]struct{}
}

func (i retryReleaseSetInterceptor) Intercept(_ context.Context, event events.Event) (bool, []events.Event, runtimepipelineobligation.ExecutionOutcome, error) {
	if _, retry := i.eventIDs[event.ID()]; !retry {
		return true, nil, runtimepipelineobligation.Continue(), nil
	}
	return false, nil, runtimepipelineobligation.ReleaseForRetry("activity_contract_pin_unavailable", nil), nil
}

func TestPipelineRetryReleasePreservesReplayAcrossDispatchSurfacesOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend+"/foreground", func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			event := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(time.Second))
			fixture.bus.SetInterceptors(retryReleaseInterceptor{eventID: event.ID()})

			if err := fixture.bus.Publish(fixture.ctx, event); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			assertRetryReleaseReplayable(t, fixture, event.ID())
		})

		t.Run(backend+"/post_commit", func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			fixture.bus.SetInterceptors(retryReleaseInterceptor{eventID: fixture.event.ID()})

			if err := fixture.bus.EngineDispatcher().DispatchPostCommit(fixture.ctx, []runtimeengine.EmitIntent{{Event: fixture.event}}); err != nil {
				t.Fatalf("DispatchPostCommit: %v", err)
			}
			assertRetryReleaseReplayable(t, fixture, fixture.event.ID())
		})

		t.Run(backend+"/recovery_fairness", func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			later := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(time.Second))
			storetest.CommitSemanticEventWithRoutes(
				t,
				fixture.ctx,
				fixture.store,
				later,
				nil,
				runtimepipelineobligation.ScopeSubscribed,
			)
			fixture.bus.SetInterceptors(retryReleaseInterceptor{eventID: fixture.event.ID()})

			processed, err := fixture.bus.SweepUndispatched(fixture.ctx, 10)
			if err != nil {
				t.Fatalf("SweepUndispatched: %v", err)
			}
			if processed != 1 {
				t.Fatalf("processed = %d, want later obligation only", processed)
			}
			if got := retryReleasePipelineReceiptCount(t, fixture, later.ID()); got != 1 {
				t.Fatalf("later event pipeline receipts = %d, want 1", got)
			}
			assertRetryReleaseReplayable(t, fixture, fixture.event.ID())
		})
	}
}

func TestStartupRecoveryDrainsPastFullRetryReleasePageOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := newCompleteEventDispatchFixture(t, backend, false)
			secondRetry := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(time.Microsecond))
			later := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(2*time.Microsecond))
			storetest.CommitSemanticEventWithRoutes(t, fixture.ctx, fixture.store, secondRetry, nil, runtimepipelineobligation.ScopeSubscribed)
			storetest.CommitSemanticEventWithRoutes(t, fixture.ctx, fixture.store, later, nil, runtimepipelineobligation.ScopeSubscribed)
			fixture.bus.SetInterceptors(retryReleaseSetInterceptor{eventIDs: map[string]struct{}{
				fixture.event.ID(): {},
				secondRetry.ID():   {},
			}})

			if err := runtimepipeline.NewRecoveryManagerWithLimit(fixture.bus, 2).Recover(fixture.ctx); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if got := retryReleasePipelineReceiptCount(t, fixture, later.ID()); got != 1 {
				t.Fatalf("later event pipeline receipts = %d, want 1", got)
			}
			assertRetryReleaseReplayable(t, fixture, fixture.event.ID())
			assertRetryReleaseReplayable(t, fixture, secondRetry.ID())
		})
	}
}

func newRetryReleaseTestEvent(fixture completeEventDispatchFixture, createdAt time.Time) events.Event {
	sourceRoute := events.RouteIdentity{
		FlowID:       "retry-source",
		FlowInstance: "retry-source/one",
		EntityID:     uuid.NewString(),
	}
	return eventtest.InExecutionMode(eventtest.PersistedChildForProducer(
		uuid.NewString(),
		events.EventType("custom.replay.checked"),
		eventtest.Producer(events.EventProducerNode, "retry-release-node"),
		"retry-release-task",
		[]byte(`{"text":"retry release"}`),
		fixture.event.ChainDepth()+1,
		fixture.event.RunID(),
		fixture.event.ID(),
		events.EnvelopeForSourceRoute(events.EventEnvelope{}, sourceRoute),
		createdAt.UTC().Truncate(time.Microsecond),
	), executionmode.Mock)
}

func assertRetryReleaseReplayable(t *testing.T, fixture completeEventDispatchFixture, eventID string) {
	t.Helper()
	if got := retryReleasePipelineReceiptCount(t, fixture, eventID); got != 0 {
		t.Fatalf("retry-release event pipeline receipts = %d, want 0", got)
	}
	work, err := fixture.store.PipelineObligations().ClaimEvent(
		fixture.ctx,
		eventID,
		runtimepipelineobligation.PurposeRecovery,
	)
	if err != nil {
		t.Fatalf("reclaim retry-release event: %v", err)
	}
	if err := fixture.store.PipelineObligations().Release(fixture.ctx, work.Claim); err != nil {
		t.Fatalf("release reclaimed retry-release event: %v", err)
	}
}

func retryReleasePipelineReceiptCount(t *testing.T, fixture completeEventDispatchFixture, eventID string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM event_receipts WHERE event_id = ? AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	if fixture.dialect == "postgres" {
		query = `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'`
	}
	var count int
	if err := fixture.db.QueryRowContext(fixture.ctx, query, eventID).Scan(&count); err != nil {
		t.Fatalf("count pipeline receipts: %v", err)
	}
	return count
}

var _ runtimebus.EventInterceptor = retryReleaseInterceptor{}
var _ runtimebus.EventInterceptor = retryReleaseSetInterceptor{}
