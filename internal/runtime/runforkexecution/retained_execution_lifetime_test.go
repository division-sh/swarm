package runforkexecution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func TestSelectedRetainedExecutionOutlivesOrchestration(t *testing.T) {
	process := worklifetime.NewProcess()
	request, cancelRequest := context.WithCancel(worklifetime.WithProcess(context.Background(), process))
	operation := selectedContractOperationForTest(t, request)
	if err := operation.Bind(worklifetime.SelectedForkIdentity{ExecutionID: "execution", RunID: "child", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	orchestration := operation.Context()
	execution, err := operation.beginRetainedExecution()
	if err != nil {
		t.Fatal(err)
	}
	defer execution.Done()
	if err := operation.releaseOrchestration(); err != nil {
		t.Fatal(err)
	}
	cancelRequest()
	if orchestration.Err() == nil {
		t.Fatal("orchestration lease still accepted after handoff")
	}
	if execution.Context().Err() != nil {
		t.Fatal("acknowledgment canceled retained execution")
	}
	owner, ok := worklifetime.OccurrenceFromContext(execution.Context())
	if !ok || owner != operation.selected {
		t.Fatal("retained execution lost exact child generation")
	}
	if _, err := operation.beginRetainedExecution(); err == nil {
		t.Fatal("released orchestration acquired another executor")
	}
	process.Retire()
	<-execution.Context().Done()
	if !errors.Is(context.Cause(execution.Context()), worklifetime.ErrRetired) {
		t.Fatalf("process retirement lost retained execution: %v", context.Cause(execution.Context()))
	}
}

func TestSelectedRetainedRuntimeShutdownJoinsBeforeResourceRelease(t *testing.T) {
	operation := selectedContractOperationForTest(t, worklifetime.WithProcess(context.Background(), worklifetime.NewProcess()))
	if err := operation.Bind(worklifetime.SelectedForkIdentity{ExecutionID: "execution", RunID: "child", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	lease, err := operation.beginRetainedExecution()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(lease.Context())
	stopping, allowExit, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
	runtime := &selectedContractAgentRuntime{
		executionLease: lease, cancelExecution: cancel, executionDone: make(chan error, 1),
		cleanup: func() { close(released) },
	}
	go func() {
		<-ctx.Done()
		close(stopping)
		<-allowExit
		runtime.executionDone <- nil
	}()
	finished := make(chan error, 1)
	go func() { finished <- runtime.Shutdown() }()
	<-stopping
	select {
	case <-released:
		t.Fatal("projection released before retained serving joined")
	default:
	}
	close(allowExit)
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown failed to join retained execution")
	}
	<-released
	if err := runtime.Shutdown(); err != nil {
		t.Fatalf("joined shutdown was not idempotent: %v", err)
	}
}
