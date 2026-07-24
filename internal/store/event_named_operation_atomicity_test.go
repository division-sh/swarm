package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestEventNamedOperationAtomicityParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, failure := range []struct {
				name   string
				mutate func(*CommitSelectedForkEventRequest)
			}{
				{
					name: "lineage",
					mutate: func(req *CommitSelectedForkEventRequest) {
						// The event and request agree, but the declared source fact does not exist.
					},
				},
				{
					name: "delivery_manifest",
					mutate: func(req *CommitSelectedForkEventRequest) {
						first, _ := events.NewDeliveryPayloadProjection(map[string]string{"summary": "one"})
						second, _ := events.NewDeliveryPayloadProjection(map[string]string{"summary": "two"})
						req.Commit.DeliveryRoutes = []events.DeliveryRoute{
							{SubscriberType: "agent", SubscriberID: "worker", PayloadProjection: first},
							{SubscriberType: "agent", SubscriberID: "worker", PayloadProjection: second},
						}
					},
				},
				{
					name: "replay_scope",
					mutate: func(req *CommitSelectedForkEventRequest) {
						req.Commit.ReplayScope = "unsupported"
					},
				},
				{
					name: "pipeline_receipt",
					mutate: func(req *CommitSelectedForkEventRequest) {
						disposition := runtimepipelineobligation.Terminal("fixture_error", &runtimefailures.Envelope{})
						req.Commit.Disposition = &disposition
					},
				},
				{
					name: "dead_letter",
					mutate: func(req *CommitSelectedForkEventRequest) {
						req.Commit.DeadLetter = &runtimedeadletters.Record{OriginalEventID: uuid.NewString()}
					},
				},
			} {
				t.Run("rollback_"+failure.name, func(t *testing.T) {
					fixture := backend.open(t)
					ctx := testAuthorActivityContext()
					withSource := failure.name != "lineage"
					req := newSelectedForkAtomicityRequest(t, ctx, fixture.store.(eventRecordContractStore), withSource)
					failure.mutate(&req)
					if outcome, err := fixture.store.(eventRecordContractStore).CommitSelectedForkEvent(ctx, req); err == nil || outcome != runtimebus.EventAppendOutcomeUnknown {
						t.Fatalf("outcome=%v err=%v, want rollback failure", outcome, err)
					}
					assertSelectedForkOperationCounts(t, ctx, fixture, req.Commit.Event.ID(), selectedForkOperationCounts{})
				})
			}

			t.Run("exact_duplicate_stops_whole_operation", func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				store := fixture.store.(eventRecordContractStore)
				req := newSelectedForkAtomicityRequest(t, ctx, store, true)
				req.Commit.DeliveryRoutes = []events.DeliveryRoute{{SubscriberType: "agent", SubscriberID: "worker"}}
				outcome, err := store.CommitSelectedForkEvent(ctx, req)
				if err != nil || outcome != runtimebus.EventAppendInserted {
					t.Fatalf("initial outcome=%v err=%v", outcome, err)
				}
				want := selectedForkOperationCounts{event: 1, lineage: 1, deliveries: 1, stories: 1}
				assertSelectedForkOperationCounts(t, ctx, fixture, req.Commit.Event.ID(), want)

				duplicate := req
				duplicate.Commit.DeliveryRoutes = []events.DeliveryRoute{{SubscriberType: "agent", SubscriberID: "must-not-appear"}}
				disposition := runtimepipelineobligation.Terminal("fixture_error", &runtimefailures.Envelope{})
				duplicate.Commit.Disposition = &disposition
				duplicate.Commit.DeadLetter = &runtimedeadletters.Record{OriginalEventID: uuid.NewString()}
				outcome, err = store.CommitSelectedForkEvent(ctx, duplicate)
				if err != nil || outcome != runtimebus.EventAppendExactDuplicate {
					t.Fatalf("duplicate outcome=%v err=%v", outcome, err)
				}
				assertSelectedForkOperationCounts(t, ctx, fixture, req.Commit.Event.ID(), want)

				conflictEvent := selectedForkEventForRequest(t, req, []byte(`{"changed":true}`))
				conflict, err := events.AdmitForPersistence(conflictEvent, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
				if err != nil {
					t.Fatal(err)
				}
				conflicting := req
				conflicting.Commit.Event = conflict
				if outcome, err := store.CommitSelectedForkEvent(ctx, conflicting); !errors.Is(err, ErrEventIdentityConflict) || outcome != runtimebus.EventAppendOutcomeUnknown {
					t.Fatalf("conflict outcome=%v err=%v", outcome, err)
				}
				assertSelectedForkOperationCounts(t, ctx, fixture, req.Commit.Event.ID(), want)
			})
		})
	}
}

func TestCommitSelectedForkEventHostileRepeatPostgres(t *testing.T) {
	fixture := openPostgresAuthorActivityReceiptFixture(t)
	ctx := testAuthorActivityContext()
	store := fixture.store.(eventRecordContractStore)
	req := newSelectedForkAtomicityRequest(t, ctx, store, true)
	req.Commit.DeliveryRoutes = []events.DeliveryRoute{{SubscriberType: "agent", SubscriberID: "worker"}}

	const attempts = 12
	start := make(chan struct{})
	results := make(chan runtimebus.EventAppendOutcome, attempts)
	errorsSeen := make(chan error, attempts)
	var workers sync.WaitGroup
	for range attempts {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			outcome, err := store.CommitSelectedForkEvent(ctx, req)
			results <- outcome
			errorsSeen <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errorsSeen)
	inserted, duplicates := 0, 0
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("hostile repeat: %v", err)
		}
	}
	for outcome := range results {
		switch outcome {
		case runtimebus.EventAppendInserted:
			inserted++
		case runtimebus.EventAppendExactDuplicate:
			duplicates++
		default:
			t.Fatalf("unexpected outcome %v", outcome)
		}
	}
	if inserted != 1 || duplicates != attempts-1 {
		t.Fatalf("inserted=%d duplicates=%d, want 1/%d", inserted, duplicates, attempts-1)
	}
	assertSelectedForkOperationCounts(t, ctx, fixture, req.Commit.Event.ID(), selectedForkOperationCounts{event: 1, lineage: 1, deliveries: 1, stories: 1})
}

func newSelectedForkAtomicityRequest(t *testing.T, ctx context.Context, store eventRecordContractStore, persistSource bool) CommitSelectedForkEventRequest {
	t.Helper()
	createdAt := time.Date(2026, 7, 18, 19, 0, 0, 0, time.UTC)
	sourceRunID := uuid.NewString()
	sourceEventID := uuid.NewString()
	if persistSource {
		source := eventtest.RunCreatingRootIngress(sourceEventID, "atomic.source", "gateway", "source-task", []byte(`{"source":true}`), 0, sourceRunID, "", events.EventEnvelope{}, createdAt)
		if err := commitSemanticEventFixture(ctx, store, source); err != nil {
			t.Fatalf("commit source event: %v", err)
		}
	}
	forkRunID := uuid.NewString()
	forkTrigger := eventtest.RunCreatingRootIngress(uuid.NewString(), "atomic.fork_trigger", "gateway", "fork-task", []byte(`{"fork":true}`), 0, forkRunID, "", events.EventEnvelope{}, createdAt)
	if err := commitSemanticEventFixture(ctx, store, forkTrigger); err != nil {
		t.Fatalf("commit fork run trigger: %v", err)
	}
	lineage, err := events.NewSelectedForkLineage(forkRunID, sourceRunID, sourceEventID, "selection:atomic", "fork-task", executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.SelectedForkReplay(uuid.NewString(), "atomic.selected", eventtest.Producer(events.EventProducerNode, "fork-node"), "fork-task", []byte(`{"selected":true}`), 1, lineage, events.EventEnvelope{}, createdAt.Add(time.Second))
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	owner := pipelineObligationOwnerForFixture(store)
	claim, err := owner.ClaimPublication(ctx, event.ID())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Release(context.WithoutCancel(ctx), claim) })
	return CommitSelectedForkEventRequest{
		Commit: runtimebus.CommitPublishRequest{
			Event: admitted, ReplayScope: runtimepipelineobligation.ScopeDirect, PipelineClaim: claim,
		},
		Lineage: RunForkSelectedContractExecutionLineage{
			ForkRunID: forkRunID, SourceRunID: sourceRunID, SourceEventID: sourceEventID,
			ForkEventID: event.ID(), EventName: string(event.Type()), SelectionAuthority: lineage.AuthorityStamp(), CreatedAt: event.CreatedAt(),
		},
	}
}

func selectedForkEventForRequest(t *testing.T, req CommitSelectedForkEventRequest, payload []byte) events.Event {
	t.Helper()
	event := req.Commit.Event.Event()
	lineage, ok := event.SelectedForkLineage()
	if !ok {
		t.Fatal("selected-fork lineage is unavailable")
	}
	return eventtest.SelectedForkReplay(event.ID(), event.Type(), event.Producer(), event.TaskID(), payload, event.ChainDepth(), lineage, event.Envelope(), event.CreatedAt())
}

type selectedForkOperationCounts struct {
	event      int
	lineage    int
	deliveries int
	receipts   int
	deadLetter int
	stories    int
}

func assertSelectedForkOperationCounts(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, eventID string, want selectedForkOperationCounts) {
	t.Helper()
	placeholder := "?"
	cast := ""
	if fixture.dialect == "postgres" {
		placeholder = "$1"
		cast = "::uuid"
	}
	queries := []struct {
		label string
		query string
		want  int
	}{
		{"event", fmt.Sprintf("SELECT COUNT(*) FROM events WHERE event_id = %s%s", placeholder, cast), want.event},
		{"lineage", fmt.Sprintf("SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_event_id = %s%s", placeholder, cast), want.lineage},
		{"deliveries", fmt.Sprintf("SELECT COUNT(*) FROM event_deliveries WHERE event_id = %s%s", placeholder, cast), want.deliveries},
		{"receipts", fmt.Sprintf("SELECT COUNT(*) FROM event_receipts WHERE event_id = %s%s", placeholder, cast), want.receipts},
		{"dead_letters", fmt.Sprintf("SELECT COUNT(*) FROM dead_letters WHERE original_event_id = %s%s", placeholder, cast), want.deadLetter},
		{"stories", fmt.Sprintf("SELECT COUNT(*) FROM author_activity_occurrences WHERE source_identity = %s", placeholder), want.stories},
	}
	for _, query := range queries {
		var got int
		if err := fixture.db.QueryRowContext(ctx, query.query, eventID).Scan(&got); err != nil {
			t.Fatalf("count %s: %v", query.label, err)
		}
		if got != query.want {
			t.Fatalf("%s count=%d, want %d", query.label, got, query.want)
		}
	}
}
