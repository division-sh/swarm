package pipeline_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type recoveryActivityCell struct {
	ctx         context.Context
	coordinator *runtimepipeline.PipelineCoordinator
	intent      runtimeengine.ActivityIntent
	fact        correlation.SourceArtifactFact
	journal     runtimepipeline.ActivityAttemptJournal
	request     events.Event
	started     runtimepipeline.ActivityAttemptRecord
}

func newRecoveryActivityCell(t *testing.T, selected gateRecoveryStoreCase, destination, runtimeID string) recoveryActivityCell {
	t.Helper()
	tool := runtimecontracts.MustToolSchemaEntry(
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: destination, TimeoutSeconds: 10}),
	)
	source := activityJournalProofSource(t, map[string]runtimecontracts.ToolSchemaEntry{"provider_write": tool})
	runID := uuid.NewString()
	ctx := requireActivityReplayRun(t, selected, source, runID)
	fact, present := correlation.SourceArtifactFactFromContext(ctx)
	if !present {
		t.Fatal("activity recovery cell has no admitted artifact")
	}
	ctx = correlation.WithRuntimeInstanceID(ctx, runtimeID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runtimeID, fact.BundleHash()))
	bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: fact, RuntimeInstanceID: runtimeID,
	}, "research/entity-1/research.scanner_provider_write.succeeded", "research/entity-1/research.scanner_provider_write.failed")
	if err != nil {
		t.Fatal(err)
	}
	pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
	intent := seedSelectedActivitySource(t, ctx, selected, pc, runtimepipeline.NonIdempotentActivityIntentForTest(runID, uuid.NewString(), uuid.NewString()))
	intent.BundleHash, intent.WorkflowVersion = fact.BundleHash(), source.WorkflowVersion()
	return recoveryActivityCell{ctx: ctx, coordinator: pc, intent: intent, fact: fact}
}

func TestDisplacedHTTPActivityRecoveryRoutesSiblingBundlesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, _ := openActivityReplayStore(t, backend)
			launched, release := make(chan struct{}, 2), make(chan struct{})
			var calls atomic.Int32
			var releaseOnce sync.Once
			finish := func() { releaseOnce.Do(func() { close(release) }) }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				launched <- struct{}{}
				<-release
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			t.Cleanup(server.Close)
			t.Cleanup(finish)
			originalRequest := startupownership.AcquireRequest{OwnerID: "sibling-activity-original", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}
			cells := []recoveryActivityCell{
				newRecoveryActivityCell(t, selected, server.URL+"/first", originalRequest.RuntimeInstanceID),
				newRecoveryActivityCell(t, selected, server.URL+"/sibling", originalRequest.RuntimeInstanceID),
			}
			if cells[0].fact.BundleHash() == cells[1].fact.BundleHash() {
				t.Fatal("sibling recovery witness requires two distinct real artifacts")
			}
			store := selected.events.(startupownership.Store)
			ctx := authoractivity.WithScope(context.Background(), authoractivity.RuntimeScope(originalRequest.RuntimeInstanceID))
			original, err := store.AcquireProcessCapability(ctx, originalRequest)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = original.Release(context.Background()) })
			plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{
				{BundleHash: cells[0].fact.BundleHash()}, {BundleHash: cells[1].fact.BundleHash()},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := original.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
				t.Fatal(err)
			}
			for i := range cells {
				cell := &cells[i]
				grant, err := original.IssueGenerationGrant(ctx, startupownership.GrantRequest{
					BundleHash: cell.fact.BundleHash(), RuntimeInstanceID: originalRequest.RuntimeInstanceID,
					RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
					t.Fatal(err)
				}
				if _, err := grant.AdmitExecution(ctx); err != nil {
					t.Fatal(err)
				}
				evidence, err := grant.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				cell.journal, err = startupownership.CaptureActivityJournal(ctx, grant, evidence)
				if err != nil {
					t.Fatal(err)
				}
				if err := cell.coordinator.InstallActivityJournal(cell.journal); err != nil {
					t.Fatal(err)
				}
				cell.request, err = runtimepipeline.ActivityRecoveryRequestForTest(cell.intent)
				if err != nil {
					t.Fatal(err)
				}
				storetest.CommitSemanticEvent(t, cell.ctx, selected.events, cell.request)
				workCtx, cancel := context.WithCancel(cell.ctx)
				done := make(chan error, 1)
				go func() {
					done <- runtimepipeline.ExecuteActivityIntentForTest(workCtx, cell.coordinator, nil, cell.intent)
				}()
				select {
				case <-launched:
				case err := <-done:
					cancel()
					t.Fatalf("sibling original request did not launch: %v", err)
				case <-time.After(10 * time.Second):
					cancel()
					t.Fatal("sibling original request launch was not observed")
				}
				var found bool
				cell.started, found, err = cell.journal.LoadActivityAttempt(cell.ctx, cell.request.ID())
				cancel()
				select {
				case callErr := <-done:
					if !errors.Is(callErr, context.Canceled) {
						t.Fatalf("sibling caller did not retain its stop: %v", callErr)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("sibling caller did not join")
				}
				if err != nil || !found || cell.started.Status != runtimepipeline.ActivityAttemptStatusStarted || cell.started.ExecutionBinding.IsZero() {
					t.Fatalf("sibling original native evidence missing: found=%t err=%v", found, err)
				}
			}
			if current, err := startupownership.ReconcileDisplacedHTTPActivities(ctx, original); err != nil || len(current.Results) != 0 {
				t.Fatalf("current sibling work was treated as displaced: %v", err)
			}
			if err := original.Release(ctx); err != nil {
				t.Fatal(err)
			}
			successorRequest := startupownership.AcquireRequest{OwnerID: "sibling-activity-successor", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}
			successor, err := store.AcquireProcessCapability(ctx, successorRequest)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = successor.Release(context.Background()) })
			ctx = authoractivity.WithScope(context.Background(), authoractivity.RuntimeScope(successorRequest.RuntimeInstanceID))
			before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected.events)
			if err != nil {
				t.Fatal(err)
			}
			// Native recovery orders request IDs. Let its first row pass scope
			// admission, then prove the second row's refusal rolls both back.
			first := &cells[0]
			if cells[1].request.ID() < first.request.ID() {
				first = &cells[1]
			}
			wrongCtx := authoractivity.WithScope(ctx, authoractivity.BundleScope(successorRequest.RuntimeInstanceID, first.fact.BundleHash()))
			if result, err := startupownership.ReconcileDisplacedHTTPActivities(wrongCtx, successor); err == nil || len(result.Results) != 0 {
				t.Fatal("preselected foreign sibling scope escaped native refusal")
			}
			afterRefusal, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected.events)
			if err != nil || !reflect.DeepEqual(before, afterRefusal) {
				t.Fatalf("one sibling's scope refusal retained partial native rows: %v", err)
			}
			summary, err := startupownership.ReconcileDisplacedHTTPActivities(ctx, successor)
			if err != nil || len(summary.Results) != 2 || calls.Load() != 2 {
				t.Fatalf("runtime-scoped sibling recovery lost once-only results: count=%d calls=%d err=%v", len(summary.Results), calls.Load(), err)
			}
			for _, recovered := range summary.Results {
				proveRecoveredActivitySibling(t, selected, cells, successorRequest.RuntimeInstanceID, recovered)
			}
			if repeated, err := startupownership.ReconcileDisplacedHTTPActivities(ctx, successor); err != nil || len(repeated.Results) != 0 || calls.Load() != 2 {
				t.Fatalf("published sibling results retained work or repeated HTTP: %v", err)
			}
			finish()
		})
	}
}

func proveRecoveredActivitySibling(t *testing.T, selected gateRecoveryStoreCase, cells []recoveryActivityCell, runtimeID string, recovered runtimepipeline.RecoveredHTTPActivity) {
	t.Helper()
	var cell *recoveryActivityCell
	for i := range cells {
		if cells[i].request.ID() == recovered.RequestEventID {
			cell = &cells[i]
		}
	}
	if cell == nil || recovered.BundleHash != cell.fact.BundleHash() || recovered.Status != runtimepipeline.ActivityAttemptStatusUncertain ||
		recovered.ExecutionBinding != cell.started.ExecutionBinding || recovered.CompletedAt == nil {
		t.Fatal("recovered sibling differs from its original source/execution")
	}
	ctx := authoractivity.WithScope(cell.ctx, authoractivity.BundleScope(runtimeID, cell.fact.BundleHash()))
	ctx = correlation.WithRuntimeInstanceID(ctx, runtimeID)
	page, err := selected.events.(authoractivity.Reader).ListAuthorActivity(ctx, authoractivity.ListOptions{RunID: cell.intent.SourceRunID, Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	matching := 0
	for _, occurrence := range page.Occurrences {
		if occurrence.Kind == authoractivity.KindActivityLifecycle && occurrence.Transition == runtimepipeline.ActivityAttemptStatusUncertain && occurrence.SourceIdentity == recovered.RequestEventID {
			matching++
			if occurrence.Scope != authoractivity.BundleScope(runtimeID, cell.fact.BundleHash()) {
				t.Fatal("sibling uncertainty story has foreign source/runtime attribution")
			}
		}
	}
	if matching != 1 {
		t.Fatalf("sibling uncertainty has %d original story occurrences", matching)
	}
	source := cell.coordinator.SemanticSource()
	bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
		ContractBundle: source, SourceArtifactFact: cell.fact, RuntimeInstanceID: runtimeID,
	}, recovered.ResultEventType)
	if err != nil {
		t.Fatal(err)
	}
	pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
	if err := pc.PublishRecoveredHTTPActivity(ctx, recovered); err != nil {
		t.Fatalf("successor publication of exact sibling result: %v", err)
	}
	if storetest.ObserveEventCardinality(t, ctx, selected.events, recovered.ResultEventID) != 1 || !loadActivityResultForProof(t, ctx, selected, recovered.ResultEventID).Event.Event().CreatedAt().Equal(*recovered.CompletedAt) {
		t.Fatal("sibling result lost once-only identity or its native timestamp")
	}
	before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected.events)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.PublishRecoveredHTTPActivity(ctx, recovered); err != nil {
		t.Fatal(err)
	}
	if _, changed, err := cell.journal.CompleteActivityAttempt(ctx, recovered.ActivityAttemptRecord); err == nil || changed {
		t.Fatal("displaced sibling writer retained terminal mutation permission")
	}
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected.events)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("sibling result replay or displaced writer changed native rows: %v", err)
	}
}
