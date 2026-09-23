package apiv1

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
)

func TestOperatorRunForkActivatedPostCommitErrorReplaysWithoutReexecution(t *testing.T) {
	cleanupErr := errors.New("selected fork postcommit cleanup failed")
	selectedCalls := 0
	selected := SelectedContractRunForkExecutor{
		ExecuteSelectedContractRunFork: func(_ context.Context, req runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
			selectedCalls++
			if req.SourceRunID != runForkTestSourceRunID || req.At != runForkTestEventID || req.ExpectedBundleHash != runForkTestBundleHash || !req.AllowSourceFreeze {
				t.Fatalf("selected executor request = %#v", req)
			}
			return runforkexecution.SelectedContractExecutionResult{
				Owner: "runtime.run_fork.selected_contract_execution",
				Materialization: runfork.RunForkMaterialization{
					SourceRunID:    runForkTestSourceRunID,
					ForkRunID:      runForkTestForkRunID,
					ForkPoint:      runfork.RunForkPoint{EventID: runForkTestEventID},
					ForkRunStatus:  "paused",
					ExecutionReady: true,
				},
				Activation: runfork.RunForkActivation{
					SourceRunID:     runForkTestSourceRunID,
					ForkRunID:       runForkTestForkRunID,
					ForkPoint:       runfork.RunForkPoint{EventID: runForkTestEventID},
					Activated:       true,
					ForkRunStatus:   "running",
					SourceRunStatus: "forked",
					SourceFrozen:    true,
				},
				ExecutedEventCount: 2,
			}, cleanupErr
		},
	}
	idempotency := newMutatingProbeIdempotencyStore()
	opts := RunForkHandlerOptions{
		Availability: &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
			runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
		}},
		Executor:    selected,
		Idempotency: idempotency,
	}
	request := Request{
		Method:       "run.fork",
		ActorTokenID: "fork-postcommit-test",
		RequestHash:  "fork-postcommit-request-hash",
		Params: map[string]any{
			"source_run_id":       runForkTestSourceRunID,
			"fork_event_id":       runForkTestEventID,
			"allow_source_freeze": true,
			"idempotency_key":     "activated-fork-postcommit",
		},
	}
	now := time.Unix(1700000000, 0).UTC()
	first, firstErr := executeRunFork(context.Background(), request, opts, now)
	if !errors.Is(firstErr, cleanupErr) {
		t.Fatalf("first run.fork error = %v, want postcommit diagnostic", firstErr)
	}
	second, secondErr := executeRunFork(context.Background(), request, opts, now)
	if selectedCalls != 1 {
		t.Fatalf("same-key retry reexecuted activated fork: selected executor calls = %d, want 1; first=%#v second=%#v secondErr=%v completions=%d", selectedCalls, first, second, secondErr, len(idempotency.records))
	}
	if len(idempotency.records) != 1 {
		t.Fatalf("idempotency completions = %d, want activated fork completion", len(idempotency.records))
	}
	if secondErr != nil {
		t.Fatalf("same-key replay error = %v", secondErr)
	}
	for label, value := range map[string]any{"first": first, "replay": second} {
		got, ok := value.(RunForkExecutionResult)
		if !ok || got.SourceRunID != runForkTestSourceRunID || got.ForkRunID != runForkTestForkRunID || got.ForkEventID != runForkTestEventID || got.ForkRunStatus != "running" || got.SourceRunStatus != "forked" || !got.SourceFrozen || got.ExecutedEventCount != 2 || got.BundleHash != runForkTestBundleHash {
			t.Fatalf("%s activated run.fork result = %#v", label, value)
		}
	}
}
