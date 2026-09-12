package runforkexecution

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func selectedContractOperationForTest(t testing.TB, ctx context.Context) *selectedContractOperation {
	t.Helper()
	operation, err := beginSelectedContractOperation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := operation.Finish(); err != nil {
			t.Error(err)
		}
	})
	return operation
}

func TestSelectedContractOperationLifetime(t *testing.T) {
	ctx, cancel := context.WithCancel(runForkTestContext(t))
	operation := selectedContractOperationForTest(t, ctx)
	if _, err := testGatewayWorkOwner(t).RetireAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	identity := worklifetime.SelectedForkIdentity{ExecutionID: "execution", RunID: "fork", Generation: 1}
	if err := operation.Bind(identity); err != nil {
		t.Fatal(err)
	}
	cancel()
	if !errors.Is(operation.PreparationContext().Err(), context.Canceled) {
		t.Fatal("preparation did not preserve client cancellation")
	}
	if err := operation.Context().Err(); err != nil {
		t.Fatalf("client cancellation retired accepted work: %v", err)
	}
	if owner, ok := worklifetime.OccurrenceFromContext(operation.Context()); !ok || owner != operation.selected {
		t.Fatal("operation lost exact selected occurrence")
	}
	// No finite work exists: even an already-cancelled waiter must succeed.
	// Counting orchestration as finite work must fail here, not self-join.
	quiescent, stopWaiting := context.WithCancel(context.Background())
	stopWaiting()
	if err := operation.selected.WaitForQuiescence(quiescent); err != nil {
		t.Fatal(err)
	}
	if err := operation.Bind(identity); err == nil {
		t.Fatal("accepted second binding")
	}
	selected := operation.selected
	if err := operation.Finish(); err != nil {
		t.Fatal(err)
	}
	if _, err := selected.Begin(context.Background()); !errors.Is(err, worklifetime.ErrRetired) {
		t.Fatalf("admission after retirement: %v", err)
	}
}

func TestSelectedContractOperationShutdownBeforeBinding(t *testing.T) {
	ctx := runForkTestContext(t)
	operation := selectedContractOperationForTest(t, ctx)
	process, _ := worklifetime.ProcessFromContext(ctx)
	process.Retire()
	<-operation.PreparationContext().Done()
	if !errors.Is(context.Cause(operation.PreparationContext()), worklifetime.ErrRetired) {
		t.Fatalf("preparation ignored process retirement: %v", context.Cause(operation.PreparationContext()))
	}
	if err := operation.Bind(worklifetime.SelectedForkIdentity{ExecutionID: "execution", RunID: "fork", Generation: 1}); !errors.Is(err, worklifetime.ErrRetired) {
		t.Fatalf("binding after shutdown: %v", err)
	}
	if process.ActiveCount() == 0 {
		t.Fatal("shutdown released preparation before failure cleanup")
	}
	if err := operation.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestSelectedContractOperationRejectsCanceledPreparation(t *testing.T) {
	ctx, cancel := context.WithCancel(runForkTestContext(t))
	cancel()
	if _, err := beginSelectedContractOperation(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled preparation: %v", err)
	}
}

func TestSelectedContractOperationJoinsAcceptedHandoff(t *testing.T) {
	ctx := runForkTestContext(t)
	operation := selectedContractOperationForTest(t, ctx)
	if _, err := testGatewayWorkOwner(t).RetireAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := operation.Bind(worklifetime.SelectedForkIdentity{ExecutionID: "execution", RunID: "fork", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	handoff, err := operation.selected.Begin(operation.Context())
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- operation.Finish() }()
	<-handoff.Context().Done()
	process, _ := worklifetime.ProcessFromContext(ctx)
	if process.ActiveCount() == 0 {
		t.Error("selected retirement released process before accepted handoff settled")
	}
	select {
	case err := <-finished:
		t.Fatalf("retirement skipped accepted handoff: %v", err)
	default:
	}
	if err := handoff.Done(); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if process.ActiveCount() != 0 {
		t.Fatalf("settled operation retained %d leases", process.ActiveCount())
	}
}

func TestPreparedSelectedForkCloseJoinsBeforeProjectionCleanup(t *testing.T) {
	ctx := runForkTestContext(t)
	process, _ := worklifetime.ProcessFromContext(ctx)
	baseline := process.ActiveCount()
	operation := selectedContractOperationForTest(t, ctx)
	if err := operation.Bind(worklifetime.SelectedForkIdentity{ExecutionID: "prepared-execution", RunID: "fork", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	work, err := operation.selected.Begin(operation.Context())
	if err != nil {
		t.Fatal(err)
	}
	cleaned := make(chan struct{})
	sentinel := errors.New("projection cleanup failed")
	cleanupCalls := 0
	prepared := &PreparedSelectedFork{operation: operation, loadedSource: LoadedSelectedContractSource{Cleanup: func() error {
		if process.ActiveCount() <= baseline {
			t.Error("process released before projection cleanup")
		}
		cleanupCalls++
		if cleanupCalls == 1 {
			close(cleaned)
			return sentinel
		}
		return nil
	}}}
	finished := make(chan error, 1)
	go func() { finished <- prepared.Close() }()
	<-work.Context().Done()
	select {
	case <-cleaned:
		t.Fatal("projection released before accepted selected work settled")
	default:
	}
	if err := work.Done(); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; !errors.Is(err, sentinel) {
		t.Fatalf("cleanup error lost: %v", err)
	}
	if process.ActiveCount() <= baseline || prepared.cleanupComplete {
		t.Fatal("failed cleanup released process responsibility")
	}
	if err := prepared.Close(); err != nil || cleanupCalls != 2 {
		t.Fatalf("cleanup retry did not settle resource: calls=%d err=%v", cleanupCalls, err)
	}
	if err := prepared.Close(); err != nil || cleanupCalls != 2 {
		t.Fatalf("successful cleanup repeated: calls=%d err=%v", cleanupCalls, err)
	}
	if _, err := prepared.MaterializationRequest(); err == nil {
		t.Fatal("closed preparation remained consumable")
	}
	if process.ActiveCount() != baseline {
		t.Fatal("terminal cleanup retained accepted work")
	}
	control, err := testGatewayWorkOwner(t).Begin(ctx)
	if err != nil {
		t.Fatalf("selected cleanup retired normal runtime: %v", err)
	}
	if err := control.Done(); err != nil {
		t.Fatal(err)
	}
}
