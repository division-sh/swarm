package pipeline_test

import (
	"errors"
	"io"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type fixedHTTPInterruptedBody struct {
	cause  error
	closed atomic.Bool
}

func (b *fixedHTTPInterruptedBody) Read([]byte) (int, error) { return 0, b.cause }
func (b *fixedHTTPInterruptedBody) Close() error {
	b.closed.Store(true)
	return nil
}

func TestKeyFreeHTTPActivityFixedSettlementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			for _, outcome := range []string{"success", "forbidden", "unavailable", "transport", "refused_transport", "body", "deadline"} {
				t.Run(backend+"/"+method+"/"+outcome, func(t *testing.T) {
					selected, closeStore, reopen := openActivityReplayStore(t, backend)
					tool := runtimecontracts.MustToolSchemaEntry(
						runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
						runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject)),
						runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: method, URL: "https://provider.invalid/fixed-policy", TimeoutSeconds: 1}),
					)
					source := activityJournalProofSource(t, map[string]runtimecontracts.ToolSchemaEntry{"provider_write": tool})
					runID := uuid.NewString()
					ctx := requireActivityReplayRun(t, selected, source, runID)
					bus := newActivityJournalProofBus(t, selected, source)
					pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
					intent := runtimepipeline.NonIdempotentActivityIntentForTest(runID, uuid.NewString(), uuid.NewString())
					intent = seedSelectedActivitySource(t, ctx, selected, pc, intent)
					_, releaseJournal := installActivityReplayJournal(t, ctx, selected, bus, pc)
					var calls atomic.Int32
					interrupted := &fixedHTTPInterruptedBody{cause: errors.New("provider body interrupted")}
					client := &http.Client{Timeout: time.Millisecond, Transport: activityTerminalRoundTripFunc(func(request *http.Request) (*http.Response, error) {
						calls.Add(1)
						if request.Method != method {
							t.Errorf("physical method = %s, want %s", request.Method, method)
						}
						if _, ok := request.Context().Deadline(); !ok {
							t.Error("authored HTTP budget is absent from the physical request")
						}
						switch outcome {
						case "transport":
							return nil, errors.New("connection lost after dispatch")
						case "refused_transport":
							return nil, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
						case "deadline":
							<-request.Context().Done()
							return nil, request.Context().Err()
						case "body":
							return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: interrupted}, nil
						case "forbidden":
							return &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":"not allowed"}`))}, nil
						case "unavailable":
							return &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"error":"unavailable"}`))}, nil
						default:
							return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
						}
					})}
					if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, pc, client, intent); err != nil {
						t.Fatalf("fixed HTTP settlement: %v", err)
					}
					journal, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, runtimepipeline.ActivityAttemptStartForTest(intent).RequestEventID)
					want := runtimepipeline.ActivityAttemptStatusUncertain
					if outcome == "success" {
						want = runtimepipeline.ActivityAttemptStatusSucceeded
					} else if outcome == "forbidden" || outcome == "unavailable" {
						want = runtimepipeline.ActivityAttemptStatusFailed
					}
					if err != nil || !found || journal.Status != want || journal.Attempt != 1 || calls.Load() != 1 || journal.CompletedAt == nil || len(bus.publishes) != 1 {
						t.Fatalf("fixed HTTP receipt: found=%t record=%+v calls=%d publications=%d err=%v", found, journal, calls.Load(), len(bus.publishes), err)
					}
					if want == runtimepipeline.ActivityAttemptStatusUncertain && (journal.Failure == nil || journal.Failure.Class != runtimefailures.ClassOutcomeUncertain || journal.Failure.Detail.Code != "activity_provider_outcome_uncertain") {
						t.Fatalf("interrupted HTTP result is not the canonical uncertain result: %+v", journal.Failure)
					}
					if outcome == "forbidden" && (journal.Failure == nil || journal.Failure.Class != runtimefailures.ClassAuthorizationDenied || journal.Failure.Detail.Code != "provider_forbidden") {
						t.Fatalf("complete provider refusal lost its known outcome: %+v", journal.Failure)
					}
					if outcome == "unavailable" && (journal.Failure == nil || journal.Failure.Class != runtimefailures.ClassConnectorFailure || journal.Failure.Detail.Code != "provider_http_status") {
						t.Fatalf("complete provider 503 lost its known outcome: %+v", journal.Failure)
					}
					if outcome == "body" && !interrupted.closed.Load() {
						t.Fatal("interrupted HTTP response body was not closed")
					}
					if client.Timeout != time.Millisecond {
						t.Fatal("activity mutated the shared client's timeout")
					}
					first := loadActivityResultForProof(t, ctx, selected, journal.ResultEventID)
					if first.Event.Event().ID() != bus.publishes[0].ID() || !first.Event.Event().CreatedAt().Equal(*journal.CompletedAt) {
						t.Fatal("published result does not use the exact durable receipt")
					}
					if err := bus.WaitForQuiescence(ctx); err != nil {
						t.Fatal(err)
					}
					releaseJournal()
					if err := closeStore(); err != nil {
						t.Fatal(err)
					}
					selected = reopen()
					replayBus := newActivityJournalProofBus(t, selected, source)
					replay := newGateRecoveryCoordinator(replayBus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
					for range 2 {
						if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, replay, client, intent); err != nil {
							t.Fatalf("reopened terminal result: %v", err)
						}
					}
					stored, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, journal.RequestEventID)
					if err != nil || !found || !reflect.DeepEqual(journal, stored) || calls.Load() != 1 || len(replayBus.publishes) != 2 {
						t.Fatalf("terminal reuse changed receipt or redispatched: found=%t record=%+v calls=%d publications=%d err=%v", found, stored, calls.Load(), len(replayBus.publishes), err)
					}
					for _, publication := range replayBus.publishes {
						if publication.ID() != journal.ResultEventID || !publication.CreatedAt().Equal(*journal.CompletedAt) {
							t.Fatal("terminal reuse reminted result identity or completion time")
						}
					}
					if !reflect.DeepEqual(first, loadActivityResultForProof(t, ctx, selected, journal.ResultEventID)) {
						t.Fatal("reopened terminal reuse changed immutable result facts")
					}
				})
			}
		}
	}
}
