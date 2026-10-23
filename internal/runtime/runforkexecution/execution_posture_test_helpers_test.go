package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type selectedRetainedExecutionObservation struct {
	ExecutedEventCount int
	ForkEvents         []SelectedContractExecutionForkEvent
}

// The activation ACK is not completion. Join its existing retained owner and
// read canonical publication evidence without adding fields to that ACK.
func observeSelectedRetainedExecution(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, selected any, child string) (selectedRetainedExecutionObservation, error) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, selectedForkCapabilityProofQuiescenceTimeout)
	defer cancel()
	contexts := owner.ports.contexts
	contexts.mu.Lock()
	var retained *selectedForkContext
	for _, entry := range contexts.entries {
		if entry.binding.ForkRunID == child {
			if retained != nil {
				contexts.mu.Unlock()
				return selectedRetainedExecutionObservation{}, errors.New("ambiguous retained selected execution")
			}
			retained = entry
		}
	}
	contexts.mu.Unlock()
	var outcome error
	if retained != nil {
		select {
		case <-wait.Done():
			return selectedRetainedExecutionObservation{}, context.Cause(wait)
		case <-retained.done:
		}
		contexts.mu.Lock()
		outcome = retained.err
		contexts.mu.Unlock()
	}
	storage, err := storetest.ReadSelectedExecutionStorage(wait, selected, child)
	if err != nil || storage.State != "closed" || storage.Occurrences != 1 {
		return selectedRetainedExecutionObservation{}, errors.Join(outcome, err, fmt.Errorf("selected execution is not exactly closed: %+v", storage))
	}
	header, err := selected.(interface {
		LoadRunHeader(context.Context, string) (operatorread.RunHeader, error)
	}).LoadRunHeader(wait, child)
	if err != nil {
		return selectedRetainedExecutionObservation{}, errors.Join(outcome, err)
	}
	if header.Failure != nil {
		outcome = errors.Join(outcome, failures.FromEnvelope(*header.Failure))
	}
	snapshot, err := storetest.ReadSelectedForkApplicationStorageSnapshot(wait, selected)
	if err != nil {
		return selectedRetainedExecutionObservation{}, errors.Join(outcome, err)
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(snapshot["run_fork_selected_contract_executions"])
	if err != nil {
		return selectedRetainedExecutionObservation{}, errors.Join(outcome, err)
	}
	var observed selectedRetainedExecutionObservation
	for _, row := range rows {
		runID, err := row["fork_run_id"].Text()
		if err != nil {
			return observed, errors.Join(outcome, err)
		}
		if runID != child {
			continue
		}
		var event SelectedContractExecutionForkEvent
		for name, destination := range map[string]*string{"source_event_id": &event.SourceEventID, "fork_event_id": &event.ForkEventID, "event_name": &event.EventName} {
			*destination, err = row[name].Text()
			if err != nil {
				return observed, errors.Join(outcome, err)
			}
		}
		observed.ForkEvents = append(observed.ForkEvents, event)
	}
	sort.Slice(observed.ForkEvents, func(i, j int) bool {
		return observed.ForkEvents[i].SourceEventID < observed.ForkEvents[j].SourceEventID
	})
	observed.ExecutedEventCount = len(observed.ForkEvents)
	return observed, outcome
}

func executeLiveSelectedContractRunFork(ctx context.Context, req SelectedContractExecutionRequest) (SelectedContractExecutionResult, error) {
	if !req.AgentRuntime.ExecutionPosture.Valid() {
		req.AgentRuntime.ExecutionPosture = executionposture.Live
	}
	return executeSelectedContractRunForkForTest(ctx, req)
}

func activateLiveSelectedContractRunFork(ctx context.Context, req SelectedContractActivationGateRequest) (SelectedContractActivationGateResult, error) {
	if !req.AgentRuntime.ExecutionPosture.Valid() {
		req.AgentRuntime.ExecutionPosture = executionposture.Live
	}
	ctx, err := selectedFixtureEntryContext(ctx, req.ExecutionOwner)
	if err != nil {
		return SelectedContractActivationGateResult{}, err
	}
	return ActivateSelectedContractRunFork(ctx, req)
}

func liveTestEffectController(store runtimeeffects.Store) *runtimeeffects.Controller {
	return runtimeeffects.NewController(store).WithExecutionPosture(executionposture.Live)
}

func liveTestCompletionController(store runtimeeffects.Store, completion runtimeeffects.CompletionStore, heartbeat runtimeeffects.CompletionHeartbeatStore, projector runtimeeffects.CompletionSpendProjector) *runtimeeffects.Controller {
	return runtimeeffects.NewCompletionController(store, completion, heartbeat, projector).WithExecutionPosture(executionposture.Live)
}
