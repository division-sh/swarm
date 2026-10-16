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

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestDisplacedHTTPActivityOriginalSessionRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, _ := openActivityReplayStore(t, backend)
			var calls atomic.Int32
			launched, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			finishProvider := func() { releaseOnce.Do(func() { close(release) }) }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					close(launched)
				}
				<-release
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			t.Cleanup(server.Close)
			t.Cleanup(finishProvider)
			tool := runtimecontracts.MustToolSchemaEntry(
				runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
				runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
				runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: server.URL, TimeoutSeconds: 10}),
			)
			source := activityJournalProofSource(t, map[string]runtimecontracts.ToolSchemaEntry{"provider_write": tool})
			runID := uuid.NewString()
			ctx := requireActivityReplayRun(t, selected, source, runID)
			bus := newActivityJournalProofBus(t, selected, source)
			pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
			intent := seedSelectedActivitySource(t, ctx, selected, pc, runtimepipeline.NonIdempotentActivityIntentForTest(runID, uuid.NewString(), uuid.NewString()))
			fact, found := correlation.SourceArtifactFactFromContext(ctx)
			if !found {
				t.Fatal("activity fixture has no admitted source fact")
			}
			intent.BundleHash, intent.WorkflowVersion = fact.BundleHash(), source.WorkflowVersion()
			store := selected.events.(startupownership.Store)
			request := startupownership.AcquireRequest{OwnerID: "activity-recovery-original", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()}
			original, err := store.AcquireProcessCapability(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = original.Release(context.Background()) })
			plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: intent.BundleHash}}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := original.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}); err != nil {
				t.Fatal(err)
			}
			grant, err := original.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: intent.BundleHash, RuntimeInstanceID: request.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: plan.Revision})
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
			journal, err := startupownership.CaptureActivityJournal(ctx, grant, evidence)
			if err != nil {
				t.Fatalf("capture original native activity journal: %v", err)
			}
			if err := pc.InstallActivityJournal(journal); err != nil {
				t.Fatal(err)
			}
			requestEvent, err := runtimepipeline.ActivityRecoveryRequestForTest(intent)
			if err != nil {
				t.Fatal(err)
			}
			storetest.CommitSemanticEvent(t, ctx, selected.events, requestEvent)
			firstCtx, cancelFirst := context.WithCancel(ctx)
			t.Cleanup(cancelFirst)
			firstDone := make(chan error, 1)
			go func() { firstDone <- runtimepipeline.ExecuteActivityIntentForTest(firstCtx, pc, nil, intent) }()
			select {
			case <-launched:
			case err := <-firstDone:
				t.Fatalf("original activity did not launch: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("original activity launch was not observed")
			}
			started, found, err := journal.LoadActivityAttempt(ctx, requestEvent.ID())
			if err != nil || !found || started.Status != runtimepipeline.ActivityAttemptStatusStarted || started.ExecutionBinding.IsZero() || calls.Load() != 1 {
				t.Fatalf("original native start evidence unavailable: found=%t err=%v", found, err)
			}
			if current, err := startupownership.ReconcileDisplacedHTTPActivities(ctx, original); err != nil || len(current.Results) != 0 {
				t.Fatalf("current execution was treated as displaced: %v", err)
			}
			cancelFirst()
			select {
			case err := <-firstDone:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("original caller did not retain cancellation: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("original caller did not join")
			}
			if err := original.Release(ctx); err != nil {
				t.Fatal(err)
			}
			successor, err := store.AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "activity-recovery-successor", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = successor.Release(context.Background()) })
			summary, err := startupownership.ReconcileDisplacedHTTPActivities(ctx, successor)
			if err != nil || len(summary.Results) != 1 {
				t.Fatalf("displaced native recovery: results=%d err=%v", len(summary.Results), err)
			}
			terminal := summary.Results[0]
			if terminal.Status != runtimepipeline.ActivityAttemptStatusUncertain || terminal.Failure == nil || terminal.CompletedAt == nil ||
				terminal.RequestEventID != started.RequestEventID || terminal.ExecutionBinding != started.ExecutionBinding || calls.Load() != 1 {
				t.Fatal("takeover lost original identity or repeated the HTTP call")
			}
			if err := runtimepipeline.ValidateActivityAttemptExecutionIdentity(terminal.ActivityAttemptRecord, started); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				replayed, err := startupownership.ReconcileDisplacedHTTPActivities(ctx, successor)
				if err != nil || len(replayed.Results) != 1 || !reflect.DeepEqual(replayed.Results[0], terminal) || calls.Load() != 1 {
					t.Fatalf("recovery replay changed the original result: %v", err)
				}
			}
			if err := pc.PublishRecoveredHTTPActivity(ctx, terminal); err != nil {
				t.Fatalf("acknowledged native result publication: %v", err)
			}
			result := loadActivityResultForProof(t, ctx, selected, terminal.ResultEventID)
			if !result.Event.Event().CreatedAt().Equal(*terminal.CompletedAt) {
				t.Fatal("recovery publication changed the original terminal timestamp")
			}
			beforeReplay := storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)
			if err := pc.PublishRecoveredHTTPActivity(ctx, terminal); err != nil {
				t.Fatalf("exact recovered result replay: %v", err)
			}
			if !reflect.DeepEqual(result, loadActivityResultForProof(t, ctx, selected, terminal.ResultEventID)) ||
				!reflect.DeepEqual(beforeReplay, storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)) ||
				storetest.ObserveEventCardinality(t, ctx, selected.events, terminal.ResultEventID) != 1 || calls.Load() != 1 {
				t.Fatal("result replay changed durable publication or repeated the HTTP call")
			}
			if settled, err := startupownership.ReconcileDisplacedHTTPActivities(ctx, successor); err != nil || len(settled.Results) != 0 {
				t.Fatalf("published original result retained a recovery obligation: %v", err)
			}
			if _, changed, err := journal.CompleteActivityAttempt(ctx, terminal.ActivityAttemptRecord); err == nil || changed {
				t.Fatal("late predecessor writer retained permission after displacement")
			}
			finishProvider()
		})
	}
}
