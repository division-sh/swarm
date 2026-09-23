package apiv1

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
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
	if firstErr != nil {
		t.Fatalf("first run.fork error = %v, want acknowledged completion", firstErr)
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

func TestOperatorRunForkAcknowledgedCleanupCompletesSelectedStoreIdempotencyBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) APIIdempotencyStore
	}{
		{"sqlite", func(t *testing.T) APIIdempotencyStore {
			return storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())
		}},
		{"postgres", func(t *testing.T) APIIdempotencyStore {
			_, db, _ := testutil.StartPostgres(t)
			return storetest.AdmitPostgresRuntimeStore(t, db)
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			calls := 0
			cleanup := errors.New("selected fork cleanup failed after activation")
			executor := SelectedContractRunForkExecutor{ExecuteSelectedContractRunFork: func(_ context.Context, _ runforkexecution.SelectedContractExecutionRequest) (runforkexecution.SelectedContractExecutionResult, error) {
				calls++
				return runforkexecution.SelectedContractExecutionResult{
					Owner: runfork.RunForkSelectedContractExecutionOwner,
					Materialization: runfork.RunForkMaterialization{
						SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID,
						ForkPoint: runfork.RunForkPoint{EventID: runForkTestEventID},
					},
					Activation: runfork.RunForkActivation{
						SourceRunID: runForkTestSourceRunID, ForkRunID: runForkTestForkRunID,
						ForkPoint: runfork.RunForkPoint{EventID: runForkTestEventID},
						Activated: true, ForkRunStatus: runfork.RunForkActivatedStatus,
						SourceRunStatus: runfork.RunForkSourceFrozenStatus, SourceFrozen: true,
					},
					ExecutedEventCount: 2,
				}, cleanup
			}}
			opts := RunForkHandlerOptions{
				Availability: &recordingRunForkAvailability{rows: map[string]runbundle.Availability{
					runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
				}},
				Executor: executor, Idempotency: backend.open(t),
			}
			req := Request{
				Method: "run.fork", ActorTokenID: "selected-fork-store-ack",
				RequestHash: "selected-fork-store-ack-hash",
				Params: map[string]any{
					"source_run_id": runForkTestSourceRunID, "fork_event_id": runForkTestEventID,
					"allow_source_freeze": true, "idempotency_key": "selected-fork-store-ack-key",
				},
			}
			now := time.Unix(1700000000, 0).UTC()
			for attempt := 0; attempt < 2; attempt++ {
				value, err := executeRunFork(context.Background(), req, opts, now)
				if err != nil {
					t.Fatalf("attempt %d: %v", attempt, err)
				}
				result, ok := value.(RunForkExecutionResult)
				if !ok || result.ForkRunID != runForkTestForkRunID || result.ForkRunStatus != runfork.RunForkActivatedStatus || result.ExecutedEventCount != 2 {
					t.Fatalf("attempt %d result: %#v", attempt, value)
				}
			}
			if calls != 1 {
				t.Fatalf("selected execution repeated after committed API completion: %d calls", calls)
			}
		})
	}
}
