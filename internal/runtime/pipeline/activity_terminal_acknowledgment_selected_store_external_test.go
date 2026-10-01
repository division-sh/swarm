package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestActivityTerminalTimestampPostCommitErrorBothStores(t *testing.T) {
	for _, tc := range activityTerminalReplayStoreCases() {
		for _, uncertain := range []bool{false, true} {
			name := "complete"
			if uncertain {
				name = "uncertain"
			}
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				ctx := testAuthorActivityContext(t, context.Background())
				runID := uuid.NewString()
				selected := tc.open(t)
				requireActivityReplayRun(t, ctx, selected, runID)
				fault := errors.New("injected activity post-commit cleanup fault")
				persistence, faultCount := storetest.ActivityJournalCleanupPersistenceFault(t, selected.events, fault)
				selected.persistence = persistence

				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					calls.Add(1)
					_, _ = io.WriteString(w, `{"ok":true,"result":{"message_id":42}}`)
				}))
				defer server.Close()
				tool := runtimepipeline.CompiledChannelActivityToolForTest(server.URL)
				source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{Tools: map[string]runtimecontracts.ToolSchemaEntry{"channel.ops.deliver": tool}})
				bus := newActivityJournalProofBus(t, selected, source)
				pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{Module: gateRecoveryModule{source: source}})
				var client *http.Client
				if uncertain {
					client = &http.Client{Transport: activityTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
						calls.Add(1)
						return nil, fmt.Errorf("connection reset after dispatch")
					})}
				}
				intent := runtimepipeline.NonIdempotentActivityIntentForTest(runID, uuid.NewString(), uuid.NewString())
				intent.Tool = "channel.ops.deliver"
				intent.ActivityID = "channel_deliver"
				intent.SuccessEvent = "channel.deliver.succeeded"
				intent.FailureEvent = "channel.deliver.failed"
				intent.Input = runtimepipeline.ActivityInputForTest(map[string]any{"message_id": 42})
				intent = intent.Normalized()
				seedSelectedActivitySource(t, ctx, selected, intent)
				if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, pc, client, intent); !errors.Is(err, fault) {
					t.Fatalf("terminal execution error = %v, want post-commit fault", err)
				}
				if faultCount() != 1 || calls.Load() != 1 || len(bus.publishes) != 1 {
					t.Fatalf("committed activity follow-up: faults=%d provider_calls=%d publications=%d", faultCount(), calls.Load(), len(bus.publishes))
				}
				stored, found, err := activityReplayJournal(selected).LoadActivityAttempt(ctx, runtimepipeline.ActivityAttemptStartForTest(intent).RequestEventID)
				if err != nil || !found {
					t.Fatalf("load terminal attempt: found=%t err=%v", found, err)
				}
				wantStatus := runtimepipeline.ActivityAttemptStatusSucceeded
				if uncertain {
					wantStatus = runtimepipeline.ActivityAttemptStatusUncertain
				}
				if stored.Status != wantStatus || bus.publishes[0].ID() != stored.ResultEventID || stored.CompletedAt == nil || !bus.publishes[0].CreatedAt().Equal(stored.CompletedAt.UTC().Truncate(time.Microsecond)) {
					t.Fatalf("journaled publication = status %q event %q, persisted = status %q event %q", wantStatus, bus.publishes[0].ID(), stored.Status, stored.ResultEventID)
				}
				if err := runtimepipeline.ExecuteActivityIntentForTest(ctx, pc, client, intent); err != nil {
					t.Fatalf("replay terminal attempt: %v", err)
				}
				if calls.Load() != 1 || len(bus.publishes) != 2 || bus.publishes[1].ID() != stored.ResultEventID || !bus.publishes[1].CreatedAt().Equal(bus.publishes[0].CreatedAt()) {
					t.Fatalf("terminal replay: provider_calls=%d publications=%d", calls.Load(), len(bus.publishes))
				}
			})
		}
	}
}
