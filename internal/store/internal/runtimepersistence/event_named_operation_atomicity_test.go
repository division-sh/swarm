package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func TestEventNamedOperationAtomicityParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, failure := range []struct {
				name   string
				mutate func(*runtimebus.CommitSelectedForkEventRequest)
			}{
				{
					name: "lineage",
					mutate: func(req *runtimebus.CommitSelectedForkEventRequest) {
						// The event and request agree, but the declared source fact does not exist.
					},
				},
				{
					name: "delivery_manifest",
					mutate: func(req *runtimebus.CommitSelectedForkEventRequest) {
						first, _ := events.NewDeliveryPayloadProjection(map[string]string{"summary": "one"})
						second, _ := events.NewDeliveryPayloadProjection(map[string]string{"summary": "two"})
						worker := testAgentDeliveryRoute(t, "worker", "fixture/worker")
						req.Commit.DeliveryRoutes = []events.DeliveryRoute{
							{Recipient: worker.Recipient, AgentIdentity: worker.AgentIdentity, PayloadProjection: first},
							{Recipient: worker.Recipient, AgentIdentity: worker.AgentIdentity, PayloadProjection: second},
						}
					},
				},
				{
					name: "target_projection",
					mutate: func(req *runtimebus.CommitSelectedForkEventRequest) {
						req.Commit.DeliveryRoutes = []events.DeliveryRoute{{
							Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("worker")),
							Target: events.MustExistingEntityTarget(events.RouteIdentity{
								FlowID: "worker", FlowInstance: "worker/one", EntityID: uuid.NewString(),
							}),
						}}
					},
				},
				{
					name: "replay_scope",
					mutate: func(req *runtimebus.CommitSelectedForkEventRequest) {
						req.Commit.ReplayScope = "unsupported"
					},
				},
				{
					name: "pipeline_receipt",
					mutate: func(req *runtimebus.CommitSelectedForkEventRequest) {
						disposition := runtimepipelineobligation.Terminal("fixture_error", &runtimefailures.Envelope{})
						req.Commit.Disposition = &disposition
					},
				},
				{
					name: "dead_letter",
					mutate: func(req *runtimebus.CommitSelectedForkEventRequest) {
						req.Commit.DeadLetter = &runtimedeadletters.Record{OriginalEventID: uuid.NewString()}
					},
				},
			} {
				t.Run("rollback_"+failure.name, func(t *testing.T) {
					fixture := backend.open(t)
					ctx := testAuthorActivityContext()
					withSource := failure.name != "lineage"
					req := newSelectedForkAtomicityRequest(t, ctx, fixture, withSource)
					failure.mutate(&req)
					setSelectedForkAtomicitySettlement(&req)
					if outcome, err := commitSelectedForkEventOutcome(ctx, fixture.store.(eventRecordContractStore), req); err == nil || outcome != runtimebus.EventAppendOutcomeUnknown {
						t.Fatalf("outcome=%v err=%v, want rollback failure", outcome, err)
					}
					assertSelectedForkOperationCounts(t, ctx, fixture, req.Commit.Event.ID(), selectedForkOperationCounts{})
				})
			}

			t.Run("exact_duplicate_stops_whole_operation", func(t *testing.T) {
				fixture := backend.open(t)
				ctx := testAuthorActivityContext()
				store := fixture.store.(eventRecordContractStore)
				req := newSelectedForkAtomicityRequest(t, ctx, fixture, true)
				req.Commit.DeliveryRoutes = []events.DeliveryRoute{testAgentDeliveryRoute(t, "worker", "fixture/worker")}
				setSelectedForkAtomicitySettlement(&req)
				outcome, err := commitSelectedForkEventOutcome(ctx, store, req)
				if err != nil || outcome != runtimebus.EventAppendInserted {
					t.Fatalf("initial outcome=%v err=%v", outcome, err)
				}
				want := selectedForkOperationCounts{event: 1, lineage: 1, deliveries: 1, stories: 1}
				assertSelectedForkOperationCounts(t, ctx, fixture, req.Commit.Event.ID(), want)

				duplicate := req
				duplicate.Commit.DeliveryRoutes = []events.DeliveryRoute{testAgentDeliveryRoute(t, "must-not-appear", "fixture/must-not-appear")}
				setSelectedForkAtomicitySettlement(&duplicate)
				disposition := runtimepipelineobligation.Terminal("fixture_error", &runtimefailures.Envelope{})
				duplicate.Commit.Disposition = &disposition
				duplicate.Commit.DeadLetter = &runtimedeadletters.Record{OriginalEventID: uuid.NewString()}
				outcome, err = commitSelectedForkEventOutcome(ctx, store, duplicate)
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
				if outcome, err := commitSelectedForkEventOutcome(ctx, store, conflicting); !errors.Is(err, events.ErrEventIdentityConflict) || outcome != runtimebus.EventAppendOutcomeUnknown {
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
	req := newSelectedForkAtomicityRequest(t, ctx, fixture, true)
	req.Commit.DeliveryRoutes = []events.DeliveryRoute{testAgentDeliveryRoute(t, "worker", "fixture/worker")}
	setSelectedForkAtomicitySettlement(&req)

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
			outcome, err := commitSelectedForkEventOutcome(ctx, store, req)
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

func TestSQLiteCommitSelectedForkEventSerializesWithClaimAbandonment(t *testing.T) {
	fixture := openSQLiteAuthorActivityReceiptFixture(t)
	ctx := testAuthorActivityContext()
	selected := fixture.store.(*SQLiteRuntimeStore)
	owner := selected.PipelineObligations()
	req := newSelectedForkAtomicityRequest(t, ctx, fixture, true)
	commitLocked := make(chan struct{})
	allowCommit := make(chan struct{})
	releaseAttempted := make(chan struct{})
	var lockAttempts atomic.Int32
	var blockCommit sync.Once
	before := func() {
		if lockAttempts.Add(1) == 2 {
			close(releaseAttempted)
		}
	}
	after := func() {
		blockCommit.Do(func() {
			close(commitLocked)
			<-allowCommit
		})
	}
	if err := selected.pipelineSQLiteOwner.SetSQLiteClaimOperationHooksForTest(req.Commit.PipelineClaim, before, after); err != nil {
		t.Fatalf("set SQLite pipeline claim hooks: %v", err)
	}

	type commitResult struct {
		outcome runtimebus.EventAppendOutcome
		err     error
	}
	commitDone := make(chan commitResult, 1)
	go func() {
		outcome, err := commitSelectedForkEventOutcome(ctx, selected, req)
		commitDone <- commitResult{outcome: outcome, err: err}
	}()
	<-commitLocked

	releaseDone := make(chan error, 1)
	go func() {
		releaseDone <- owner.Release(ctx, req.Commit.PipelineClaim)
	}()
	select {
	case <-releaseAttempted:
	case <-time.After(2 * time.Second):
		t.Fatal("claim abandonment did not reach the selected-fork operation lock")
	}
	select {
	case err := <-releaseDone:
		t.Fatalf("claim abandonment completed before selected-fork commit: %v", err)
	default:
	}

	close(allowCommit)
	result := <-commitDone
	if result.err != nil || result.outcome != runtimebus.EventAppendInserted {
		t.Fatalf("selected-fork commit outcome=%v err=%v", result.outcome, result.err)
	}
	if err := <-releaseDone; err != nil {
		t.Fatalf("release after selected-fork commit: %v", err)
	}
	if err := selected.pipelineSQLiteOwner.SetSQLiteClaimOperationHooksForTest(req.Commit.PipelineClaim, nil, nil); !errors.Is(err, runtimepipelineobligation.ErrStaleClaim) {
		t.Fatalf("claim after serialized abandonment = %v, want ErrStaleClaim", err)
	}
	assertSelectedForkOperationCounts(t, ctx, fixture, req.Commit.Event.ID(), selectedForkOperationCounts{
		event:   1,
		lineage: 1,
		stories: 2,
	})
}

func newSelectedForkAtomicityRequest(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, persistSource bool) runtimebus.CommitSelectedForkEventRequest {
	t.Helper()
	store := fixture.store.(eventRecordContractStore)
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
	var deliveryAuthority runtimedelivery.ExecutionAuthority
	if persistSource {
		bindingID := uuid.NewString()
		if fixture.dialect == authoractivityfixture.DialectSQLite {
			if _, err := fixture.db.ExecContext(ctx, `
				INSERT INTO run_fork_selected_contract_bindings (
					binding_id,fork_run_id,source_run_id,fork_event_id,mode,created_at
				) VALUES (?,?,?,?,'selected_contracts',?)`,
				bindingID, forkRunID, sourceRunID, sourceEventID, createdAt,
			); err != nil {
				t.Fatalf("seed selected-contract binding: %v", err)
			}
		} else {
			if _, err := fixture.db.ExecContext(ctx, `
				INSERT INTO run_fork_selected_contract_bindings (
					binding_id,fork_run_id,source_run_id,fork_event_id,mode,created_at
				) VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,'selected_contracts',$5)`,
				bindingID, forkRunID, sourceRunID, sourceEventID, createdAt,
			); err != nil {
				t.Fatalf("seed selected-contract binding: %v", err)
			}
		}
		authorityStore, ok := fixture.store.(interface {
			IssueRunForkSelectedContractRuntimeExecution(context.Context, runfork.SelectedContractRuntimeExecutionIssueRequest) (runfork.SelectedContractRuntimeExecution, error)
		})
		if !ok {
			t.Fatal("selected-fork atomicity fixture has no selected execution authority owner")
		}
		selection := runfork.RunForkContractSelection{Mode: "selected_contracts"}
		issued, err := authorityStore.IssueRunForkSelectedContractRuntimeExecution(ctx, runfork.SelectedContractRuntimeExecutionIssueRequest{
			Admission: runfork.RunForkSelectedContractExecutionAdmission{
				Owner: runfork.RunForkSelectedContractExecutionAdmissionOwner, FutureExecutionOwner: runfork.RunForkSelectedContractExecutionOwner,
				NonMutating: true, ExecutionSupported: false, ForkRunID: forkRunID, SourceRunID: sourceRunID, ForkEventID: sourceEventID,
				ContractSelection: selection, ContractBindingOwner: runfork.RunForkSelectedContractBindingOwner,
				AdmissionOwner: "runtime.run_fork.frontier", AdmissionUse: runfork.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
				ExecutionModelOwner: runfork.RunForkSelectedContractExecutionModelOwner, SourceWorkflowName: "workflow", SourceWorkflowVersion: "v1",
				DeferredWorkAdmissionOwner: runfork.RunForkSelectedContractDeferredWorkAdmissionOwner,
			},
			ContainerPlanFingerprint:   "sha256:container",
			ActorCensusFingerprint:     "sha256:actors",
			EffectiveConfigFingerprint: "sha256:config",
			ExecutionMode:              executionmode.Live,
			Now:                        createdAt,
		})
		if err != nil {
			t.Fatalf("issue selected execution authority: %v", err)
		}
		sourceFact, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx)
		if !ok {
			t.Fatal("selected-fork atomicity context has no bundle source fact")
		}
		deliveryAuthority, err = runtimedelivery.NewSelectedExecutionAuthority(sourceFact, issued.ExecutionID, forkRunID, issued.Generation)
		if err != nil {
			t.Fatalf("construct selected delivery authority: %v", err)
		}
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
	return runtimebus.CommitSelectedForkEventRequest{
		Commit: runtimebus.CommitPublishRequest{
			Event: admitted, RouteSettlement: testRouteSettlement(admitted.Event(), nil), ReplayScope: runtimepipelineobligation.ScopeDirect, PipelineClaim: claim,
			DeliveryAuthority: deliveryAuthority,
		},
		Lineage: runfork.RunForkSelectedContractExecutionLineage{
			ForkRunID: forkRunID, SourceRunID: sourceRunID, SourceEventID: sourceEventID,
			ForkEventID: event.ID(), EventName: string(event.Type()), SelectionAuthority: lineage.AuthorityStamp(), CreatedAt: event.CreatedAt(),
		},
	}
}

func setSelectedForkAtomicitySettlement(req *runtimebus.CommitSelectedForkEventRequest) {
	if req == nil {
		return
	}
	req.Commit.RouteSettlement = testRouteSettlement(req.Commit.Event.Event(), req.Commit.DeliveryRoutes)
}

func selectedForkEventForRequest(t *testing.T, req runtimebus.CommitSelectedForkEventRequest, payload []byte) events.Event {
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
