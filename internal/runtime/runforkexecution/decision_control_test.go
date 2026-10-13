package runforkexecution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func TestSelectedDecisionCallerPreservesOwnedContextAndCancellation(t *testing.T) {
	type key struct{}
	owned, retire := context.WithCancelCause(context.WithValue(context.Background(), key{}, "fork-owned"))
	defer retire(nil)
	caller, cancel := context.WithTimeout(context.WithValue(context.Background(), key{}, "source-caller"), time.Second)
	defer cancel()
	ctx, release := bindSelectedDecisionCaller(caller, owned)
	defer release()
	if ctx.Value(key{}) != "fork-owned" {
		t.Fatal("operator context replaced the child's execution authority")
	}
	deadline, bounded := ctx.Deadline()
	want, _ := caller.Deadline()
	if !bounded || deadline != want {
		t.Fatal("operator deadline was removed or extended")
	}
	cancel()
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), context.Canceled) {
			t.Fatalf("caller cancellation changed: %v", context.Cause(ctx))
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not reach the selected decision turn")
	}
	if owned.Err() != nil {
		t.Fatal("one canceled operator turn retired the selected executor")
	}
	other, otherRelease := bindSelectedDecisionCaller(context.Background(), owned)
	defer otherRelease()
	cause := errors.New("selected execution retired")
	retire(cause)
	if !errors.Is(context.Cause(other), cause) {
		t.Fatalf("retained execution cancellation was lost: %v", context.Cause(other))
	}
}

func TestSelectedDecisionControlJoinsOnlyOwnRetainedOccurrence(t *testing.T) {
	runID := uuid.NewString()
	binding := testSelectedContractBinding(runID)
	binding.BindingID = uuid.NewString()
	admission, err := managedexecution.New(managedexecution.KindSelectedContractFork, uuid.NewString(), 1,
		runID, "decision-census", runForkTestBundleHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	process := worklifetime.NewProcess()
	ctx := worklifetime.WithProcess(context.Background(), process)
	occurrence, err := process.NewSelectedFork(ctx, worklifetime.SelectedForkIdentity{
		ExecutionID: admission.ExecutionAuthorityID, RunID: runID, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := occurrence.RetireAndWait(context.Background()); err != nil {
			t.Error(err)
		}
		if process.ActiveCount() != 0 {
			t.Fatalf("decision control leaked work: %d", process.ActiveCount())
		}
	})
	owned := worklifetime.WithOccurrence(managedexecution.WithAdmission(ctx, admission), occurrence)
	runtime := &selectedContractAgentRuntime{controlContext: owned, timerLifecycle: &pipeline.PipelineCoordinator{}}
	entry := &selectedForkContext{binding: binding, retained: &PreparedSelectedFork{retainedRuntime: runtime}}
	contexts := &selectedForkContexts{recovered: true, entries: map[*selectedContractOperation]*selectedForkContext{{}: entry}}
	owner := SelectedContractExecutionOwner{ports: &selectedContractExecutionPorts{contexts: contexts}}
	use, err := owner.acquireDecisionControl(context.Background(), binding, runfork.ControlMailboxDefer)
	if err != nil {
		t.Fatal(err)
	}
	if actual, found := managedexecution.FromContext(use.ctx); !found || actual.ID != admission.ID {
		t.Fatal("control did not retain its own execution admission")
	}
	if err := use.close(); err != nil {
		t.Fatal(err)
	}
	if err := use.close(); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"preactivation", "retiring", "closed", "unreconciled", "process_retired", "no_context", "wrong_occurrence", "wrong_generation"} {
		t.Run(phase, func(t *testing.T) {
			entry.retained, entry.retiring, entry.closed = &PreparedSelectedFork{retainedRuntime: runtime}, false, false
			contexts.recovered, contexts.retired = true, false
			runtime.controlContext = owned
			switch phase {
			case "preactivation":
				entry.retained = nil
			case "retiring":
				entry.retiring = true
			case "closed":
				entry.closed = true
			case "unreconciled":
				contexts.recovered = false
			case "process_retired":
				contexts.retired = true
			case "no_context":
				runtime.controlContext = nil
			case "wrong_occurrence":
				runtime.controlContext = worklifetime.WithOccurrence(managedexecution.WithAdmission(ctx, admission), process)
			case "wrong_generation":
				next := admission
				next.Generation++
				runtime.controlContext = managedexecution.WithAdmission(owned, next)
			}
			if use, err := owner.acquireDecisionControl(context.Background(), binding, runfork.ControlMailboxDefer); err == nil {
				_ = use.close()
				t.Fatal("decision admitted without its exact live retained occurrence")
			}
		})
	}
}
