package pipeline

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type capturedActivityJournalProbe struct {
	ActivityAttemptJournal
	claims    atomic.Int32
	terminals atomic.Int32
	mu        sync.Mutex
	record    *ActivityAttemptRecord
}

func (j *capturedActivityJournalProbe) LoadActivityAttempt(_ context.Context, id string) (ActivityAttemptRecord, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.record == nil || j.record.RequestEventID != id {
		return ActivityAttemptRecord{}, false, nil
	}
	return *j.record, true, nil
}

func (j *capturedActivityJournalProbe) ClaimActivityAttemptForLoopGeneration(_ context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	j.claims.Add(1)
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.record != nil {
		return *j.record, false, nil
	}
	record.StartedAt = time.Now().UTC()
	j.record = &record
	return record, true, nil
}

func (j *capturedActivityJournalProbe) CompleteActivityAttempt(_ context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	j.terminals.Add(1)
	j.mu.Lock()
	defer j.mu.Unlock()
	completed := time.Now().UTC()
	record.CompletedAt = &completed
	j.record = &record
	return record, true, nil
}

func (j *capturedActivityJournalProbe) MarkActivityAttemptUncertain(ctx context.Context, record ActivityAttemptRecord) (ActivityAttemptRecord, bool, error) {
	return j.CompleteActivityAttempt(ctx, record)
}

type unusedActivityInstanceReader struct {
	WorkflowInstancePersistenceReader
}

func TestActivityDispatchRetainsInstalledJournalAcrossReplacement(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "uncertain"}[interrupted], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			original, successor := &capturedActivityJournalProbe{}, &capturedActivityJournalProbe{}
			tool := runtimecontracts.MustToolSchemaEntry(
				runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
				runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{Method: http.MethodGet, URL: "https://proof.invalid"}),
				runtimecontracts.WithToolSchemas(runtimecontracts.MustToolInputSchema(runtimecontracts.ToolSchemaObject), runtimecontracts.ToolInputSchema{}),
			)
			bus := &recordingPipelineBus{}
			pc := &PipelineCoordinator{
				bus: bus, executionPosture: executionposture.Live,
				module:        staticSemanticWorkflowModule{source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"provider_write": tool}})},
				workflowStore: &workflowInstanceStore{instanceReader: &unusedActivityInstanceReader{}},
			}
			if err := pc.InstallActivityJournal(original); err != nil {
				t.Fatal(err)
			}
			launched, release := make(chan struct{}), make(chan struct{})
			client := &http.Client{Transport: activityRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				close(launched)
				select {
				case <-release:
				case <-request.Context().Done():
					return nil, request.Context().Err()
				}
				if interrupted {
					return nil, errors.New("provider connection ended after launch")
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"ok":true}`))}, nil
			})}
			intent := NonIdempotentActivityIntentForTest(uuid.NewString(), uuid.NewString(), uuid.NewString())
			done := make(chan error, 1)
			go func() {
				done <- (pipelineActivityDispatcher{coordinator: pc, client: client}).executeActivityIntent(ctx, intent)
			}()
			select {
			case <-launched:
			case err := <-done:
				t.Fatalf("activity ended before launch: %v", err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if err := pc.InstallActivityJournal(successor); err != nil {
				t.Fatal(err)
			}
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if original.claims.Load() != 1 || original.terminals.Load() != 1 || successor.claims.Load() != 0 || successor.terminals.Load() != 0 || len(bus.publishes) != 1 {
				t.Fatalf("callback changed installed owner: original=%d/%d successor=%d/%d publications=%d", original.claims.Load(), original.terminals.Load(), successor.claims.Load(), successor.terminals.Load(), len(bus.publishes))
			}
		})
	}
}
