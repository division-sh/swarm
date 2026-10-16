package pipeline_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestHTTPActivityReopenedStartedAttemptNeedsRecoveryOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, closeStore, reopen := openActivityReplayStore(t, backend)
			var calls atomic.Int32
			launched := make(chan struct{})
			providerJoined := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
				if calls.Add(1) == 1 {
					close(launched)
					<-request.Context().Done()
					close(providerJoined)
				}
			}))
			t.Cleanup(server.Close)
			tool := runtimecontracts.MustToolSchemaEntry(
				runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),

				runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
				runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodPost, URL: server.URL, TimeoutSeconds: 10}),
			)
			source := activityJournalProofSource(t, map[string]runtimecontracts.ToolSchemaEntry{"provider_write": tool})
			runID := uuid.NewString()
			ctx := requireActivityReplayRun(t, selected, source, runID)
			bus := newActivityJournalProofBus(t, selected, source)
			coordinator := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
			intent := runtimepipeline.NonIdempotentActivityIntentForTest(runID, uuid.NewString(), uuid.NewString())
			intent = seedSelectedActivitySource(t, ctx, selected, coordinator, intent)
			requestID := runtimepipeline.ActivityAttemptStartForTest(intent).RequestEventID
			firstCtx, cancelFirst := context.WithCancel(ctx)
			t.Cleanup(cancelFirst)
			firstDone := make(chan error, 1)
			go func() {
				firstDone <- runtimepipeline.ExecuteActivityIntentForTest(firstCtx, coordinator, nil, intent)
			}()
			select {
			case <-launched:
			case <-time.After(10 * time.Second):
				t.Fatal("provider did not observe the physical HTTP request")
			}
			started, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, requestID)
			if err != nil || !found || started.Status != runtimepipeline.ActivityAttemptStatusStarted || calls.Load() != 1 {
				t.Fatalf("live attempt: found=%t record=%+v calls=%d err=%v", found, started, calls.Load(), err)
			}
			before := storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)
			if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, coordinator, nil, intent); err != nil {
				t.Fatalf("live duplicate: %v", err)
			}
			live, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, requestID)
			if err != nil || !found || !reflect.DeepEqual(started, live) || calls.Load() != 1 || !reflect.DeepEqual(before, storetest.ObserveActivityResultPublicationStorage(t, ctx, selected.events)) {
				t.Fatalf("live duplicate changed its predecessor: found=%t record=%+v calls=%d err=%v", found, live, calls.Load(), err)
			}
			cancelFirst()
			select {
			case err := <-firstDone:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("postlaunch cancellation did not retain its cause: %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("canceled activity caller did not join")
			}
			select {
			case <-providerJoined:
			case <-time.After(10 * time.Second):
				t.Fatal("canceled provider request did not join")
			}
			stopped, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, requestID)
			if err != nil || !found || !reflect.DeepEqual(started, stopped) {
				t.Fatalf("expected retained postlaunch start after canceled settlement: found=%t record=%+v err=%v", found, stopped, err)
			}
			if err := bus.WaitForQuiescence(ctx); err != nil {
				t.Fatal(err)
			}
			if err := closeStore(); err != nil {
				t.Fatal(err)
			}
			selected = reopen()
			replayBus := newActivityJournalProofBus(t, selected, source)
			replay := newGateRecoveryCoordinator(replayBus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
			replayErr := runtimepipeline.ExecuteActivityIntentForTest(ctx, replay, nil, intent)
			after, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, requestID)
			if err != nil || !found || calls.Load() != 1 {
				t.Fatalf("reopen redispatched or lost the attempt: found=%t record=%+v calls=%d err=%v", found, after, calls.Load(), err)
			}
			failure, uncertain := runtimefailures.EnvelopeFromError(replayErr)
			if !uncertain || failure.Class != runtimefailures.ClassOutcomeUncertain {
				t.Errorf("reopened postlaunch attempt did not expose typed uncertainty: err=%v record=%+v identical_to_live=%t publications=%d calls=%d", replayErr, after, reflect.DeepEqual(started, after), len(replayBus.publishes), calls.Load())
			}
		})
	}
}
