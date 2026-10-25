package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type selectedRetainedExecutionObservation struct {
	ExecutedEventCount int
	ForkEvents         []SelectedContractExecutionForkEvent
}

func retireQuiescentSelectedTestContext(ctx context.Context, owner SelectedContractExecutionOwner, child string) error {
	contexts := owner.ports.contexts
	contexts.mu.Lock()
	var prepared *PreparedSelectedFork
	for _, entry := range contexts.entries {
		if entry.binding.ForkRunID == child {
			if prepared != nil {
				contexts.mu.Unlock()
				return errors.New("ambiguous retained selected execution")
			}
			prepared = entry.retained
		}
	}
	contexts.mu.Unlock()
	if prepared == nil {
		return errors.New("selected test shutdown requires its retained execution")
	}
	prepared.bindMu.Lock()
	runtime := prepared.retainedRuntime
	complete, closeErr := prepared.cleanupComplete, prepared.closeErr
	prepared.bindMu.Unlock()
	if complete {
		return closeErr
	}
	if runtime == nil {
		return errors.New("selected test shutdown requires its retained runtime")
	}
	runtime.mu.Lock()
	retainedContext, eventBus := runtime.cleanupContext, runtime.bus
	runtime.mu.Unlock()
	if retainedContext == nil {
		return errors.New("selected test shutdown lacks its original execution context")
	}
	owned, cancel := bindSelectedDecisionCaller(ctx, retainedContext)
	defer cancel()
	_, authority, err := selectedContractManagedExecutionAuthority(owned)
	if err != nil {
		return err
	}
	for {
		if err := runtime.WaitForQuiescence(owned, eventBus); err != nil {
			return err
		}
		err := owner.ports.runtimeExecution.RequireRunForkSelectedContractExecutionSettlement(owned, authority,
			selectedContractExecutionFrontierEventIDs(prepared.frontier.FrontierEvents), prepared.loadedSource.Source)
		if err == nil {
			return owner.disposePreparation(prepared)
		}
		select {
		case <-owned.Done():
			return errors.Join(err, context.Cause(owned))
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func selectedTestInstanceSettled(t *testing.T, ctx context.Context, owner SelectedContractExecutionOwner, selected any, instance flowidentity.RunScopedFlowInstance, state string) bool {
	t.Helper()
	header, found, err := owner.ports.workflow.LoadWorkflowInstance(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := storetest.ReadServedIncompletePipelineHandoffCount(ctx, selected, instance.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || header.CurrentState != state || pending != 0 {
		return false
	}
	if err := retireQuiescentSelectedTestContext(ctx, owner, instance.RunID); err != nil {
		t.Fatalf("shutdown settled selected execution: %v", err)
	}
	return true
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
		storage, err := storetest.ReadSelectedExecutionStorage(wait, selected, child)
		if err != nil {
			return selectedRetainedExecutionObservation{}, err
		}
		for storage.State == "running" {
			published := true
			if retained.retained != nil {
				for _, source := range selectedContractExecutionFrontierEventIDs(retained.retained.frontier.FrontierEvents) {
					frontier, err := storetest.ReadCausalDeliveryFrontier(wait, selected, child, activityidentity.ForkLineageEventID(child, source))
					if err != nil {
						return selectedRetainedExecutionObservation{}, err
					}
					published = published && frontier.Observed != 0
				}
			}
			pending, err := storetest.ReadServedIncompletePipelineHandoffCount(wait, selected, child)
			if err != nil {
				return selectedRetainedExecutionObservation{}, err
			}
			if published && pending == 0 {
				break
			}
			select {
			case <-wait.Done():
				return selectedRetainedExecutionObservation{}, context.Cause(wait)
			case <-time.After(10 * time.Millisecond):
			}
			storage, err = storetest.ReadSelectedExecutionStorage(wait, selected, child)
			if err != nil {
				return selectedRetainedExecutionObservation{}, err
			}
		}
		if storage.State == "running" {
			outcome = retireQuiescentSelectedTestContext(wait, owner, child)
		}
		contexts.mu.Lock()
		outcome = errors.Join(outcome, retained.err)
		contexts.mu.Unlock()
	}
	storage, err := storetest.ReadSelectedExecutionStorage(wait, selected, child)
	if err != nil || storage.Occurrences != 1 {
		return selectedRetainedExecutionObservation{}, errors.Join(outcome, err, fmt.Errorf("selected execution occurrence changed: %+v", storage))
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
	wantState := "running"
	if header.Status != "running" || errors.Is(outcome, context.Canceled) {
		wantState = "closed"
	}
	if storage.State != wantState {
		return selectedRetainedExecutionObservation{}, errors.Join(outcome, fmt.Errorf("selected execution state=%s want=%s for run status=%s", storage.State, wantState, header.Status))
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
