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

type recordingPipelineRecoveryOwner struct {
	bus     *runtimebus.EventBus
	results []runtimepipelineobligation.SweepResult
}

type blockedRunDispatchGate map[string]bool

func (g blockedRunDispatchGate) QueueableRunDispatchBlocked(_ context.Context, runID string) (bool, error) {
	return g[runID], nil
}

func (o *recordingPipelineRecoveryOwner) SweepPipelineObligations(ctx context.Context, limit int) (runtimepipelineobligation.SweepResult, error) {
	result, err := o.bus.SweepPipelineObligations(ctx, limit)
	o.results = append(o.results, result)
	return result, err
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
			retryEvents := []events.Event{fixture.event}
			for i := 1; i < 5; i++ {
				event := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(time.Duration(i)*time.Microsecond))
				storetest.CommitSemanticEventWithRoutes(t, fixture.ctx, fixture.store, event, nil, runtimepipelineobligation.ScopeSubscribed)
				retryEvents = append(retryEvents, event)
			}
			later := newRetryReleaseTestEvent(fixture, fixture.event.CreatedAt().Add(5*time.Microsecond))
			storetest.CommitSemanticEventWithRoutes(t, fixture.ctx, fixture.store, later, nil, runtimepipelineobligation.ScopeSubscribed)
			retryIDs := make(map[string]struct{}, len(retryEvents))
			for _, event := range retryEvents {
				retryIDs[event.ID()] = struct{}{}
			}
			fixture.bus.SetInterceptors(retryReleaseSetInterceptor{eventIDs: retryIDs})

			recovery := &recordingPipelineRecoveryOwner{bus: fixture.bus}
			if err := runtimepipeline.NewRecoveryManagerWithLimit(recovery, 2).Recover(fixture.ctx); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if len(recovery.results) < 4 {
				t.Fatalf("startup recovery passes = %d, want continuation across at least 4 bounded batches", len(recovery.results))
			}
			for i, result := range recovery.results {
				if result.Examined > 2 {
					t.Fatalf("startup recovery pass %d examined %d, want <= 2", i, result.Examined)
				}
			}
			last := recovery.results[len(recovery.results)-1]
			if !last.Exhausted || !last.Blocked {
				t.Fatalf("final startup recovery result = %#v, want exhausted with retained local blockage", last)
			}
			if got := retryReleasePipelineReceiptCount(t, fixture, later.ID()); got != 1 {
				t.Fatalf("later event pipeline receipts = %d, want 1", got)
			}
			for _, event := range retryEvents {
				assertRetryReleaseReplayable(t, fixture, event.ID())
			}
		})
	}
}

func TestPipelineScanRunLocalBlockDoesNotStarveLaterRunOnSQLiteAndPostgres(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, route := range []string{"ordinary", "decision"} {
			t.Run(backend+"/"+route, func(t *testing.T) {
				fixture := newCompleteEventDispatchFixture(t, backend, route == "decision")
				laterRunID := uuid.NewString()
				laterAt := fixture.event.CreatedAt().Add(time.Microsecond)
				seedCompleteEventDispatchRun(t, fixture.ctx, fixture.db, backend, laterRunID, laterAt.Add(-time.Second))
				later := newRetryReleaseRunRoot(laterRunID, laterAt)
				storetest.CommitSemanticEventWithRoutes(
					t,
					fixture.ctx,
					fixture.store,
					later,
					nil,
					runtimepipelineobligation.ScopeSubscribed,
				)
				if route == "decision" {
					fixture.insertDecisionObligationFor(t, later)
				}
				fixture.bus.SetRunDispatchGate(blockedRunDispatchGate{fixture.event.RunID(): true})

				result, err := fixture.bus.SweepPipelineObligations(fixture.ctx, 10)
				if err != nil {
					t.Fatalf("SweepPipelineObligations: %v", err)
				}
				if result.Settled != 1 || !result.Exhausted || !result.Blocked {
					t.Fatalf("sweep result = %#v, want one later settlement plus exhausted local block", result)
				}
				if got := retryReleasePipelineReceiptCount(t, fixture, fixture.event.ID()); got != 0 {
					t.Fatalf("blocked predecessor receipts = %d, want 0", got)
				}
				if got := retryReleasePipelineReceiptCount(t, fixture, later.ID()); got != 1 {
					t.Fatalf("later running-run receipts = %d, want 1", got)
				}
				if route == "decision" {
					if got := fixture.decisionObligationStatus(t, fixture.event.ID()); got != "pending" {
						t.Fatalf("blocked decision route status = %q, want pending", got)
					}
					if got := fixture.decisionObligationStatus(t, later.ID()); got != "completed" {
						t.Fatalf("later decision route status = %q, want completed", got)
					}
				}

			})
		}
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

func newRetryReleaseRunRoot(runID string, createdAt time.Time) events.Event {
	entityID := uuid.NewString()
	return eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("custom.replay.checked"),
		"api.v1",
		"",
		[]byte(`{"text":"later running run"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		createdAt.UTC().Truncate(time.Microsecond),
	)
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
