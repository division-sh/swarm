package destructivereset

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestCoordinatorExecutesNamedResetWorkflowUnderOneLease(t *testing.T) {
	now := time.Date(2026, 8, 9, 19, 0, 0, 0, time.UTC)
	locks := &recordingLockManager{acquired: true}
	var calls []string
	var cleanupRequest CleanupRequest
	coord := &Coordinator{
		Planner: InventoryPlanner{Reader: inventoryReaderFunc(func(context.Context) (Inventory, error) {
			calls = append(calls, "plan")
			return Inventory{CleanupRunSetKnown: true}, nil
		})},
		Locks: locks,
		RuntimeContexts: runtimeContextQuiescerFunc(func(context.Context) error {
			calls = append(calls, "runtime")
			return nil
		}),
		Quiescer: quiescenceApplierFunc(func(context.Context, QuiescenceRequest) (QuiescenceResult, error) {
			calls = append(calls, "quiescence")
			if locks.lease == nil || locks.lease.releases != 0 {
				t.Fatal("lease released before quiescence")
			}
			return QuiescenceResult{OperationName: DefaultOperationName, AppliedAt: now}, nil
		}),
		Cleaner: cleanupApplierFunc(func(_ context.Context, req CleanupRequest) (CleanupResult, error) {
			calls = append(calls, "cleanup")
			cleanupRequest = req
			return CleanupResult{OperationName: DefaultOperationName, AppliedAt: now}, nil
		}),
		Containers: containerStopperFunc(func(context.Context, ContainerResetRequest) (ContainerResetResult, error) {
			calls = append(calls, "containers")
			return ContainerResetResult{OperationName: DefaultOperationName, AppliedAt: now}, nil
		}),
		Now: func() time.Time { return now },
	}

	got, err := coord.Execute(context.Background(), Request{OperationID: destructiveResetOperationID, ActorTokenID: "operator", RequestHash: "hash"})
	if err != nil {
		t.Fatalf("Execute error = %v", err)
	}
	if !slices.Equal(calls, []string{"plan", "runtime", "quiescence", "cleanup", "containers"}) {
		t.Fatalf("calls = %#v", calls)
	}
	if got.Plan.OperationName != DefaultOperationName || got.Quiescence.OperationName != DefaultOperationName || got.Cleanup.OperationName != DefaultOperationName || got.Containers.OperationName != DefaultOperationName {
		t.Fatalf("execution result = %#v", got)
	}
	if locks.lease == nil || locks.lease.releases != 1 {
		t.Fatalf("lease = %#v, want one terminal release", locks.lease)
	}
	if cleanupRequest.OperationID != destructiveResetOperationID {
		t.Fatalf("cleanup operation id = %q, want %q", cleanupRequest.OperationID, destructiveResetOperationID)
	}
}

func TestCoordinatorRejectsMissingOperationIdentityBeforeOwnerCalls(t *testing.T) {
	locks := &recordingLockManager{acquired: true}
	coord := &Coordinator{Planner: successfulPlanner(), Locks: locks, Quiescer: successfulQuiescer(), Cleaner: successfulCleaner(), Containers: successfulContainers()}
	_, err := coord.Execute(context.Background(), Request{ActorTokenID: "operator", RequestHash: "hash"})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Execute error = %v, want ErrInvalidRequest", err)
	}
	if locks.acquires != 0 {
		t.Fatalf("lock acquisitions = %d, want none", locks.acquires)
	}
}

func TestCoordinatorReleasesLeaseAtEveryWorkflowFailure(t *testing.T) {
	stageErr := errors.New("stage failed")
	tests := []struct {
		name       string
		planner    Planner
		quiescer   QuiescenceApplier
		cleaner    CleanupApplier
		containers ContainerStopper
	}{
		{name: "plan", planner: plannerFunc(func(context.Context, Request) (Plan, error) { return Plan{}, stageErr }), quiescer: successfulQuiescer(), cleaner: successfulCleaner(), containers: successfulContainers()},
		{name: "quiescence", planner: successfulPlanner(), quiescer: quiescenceApplierFunc(func(context.Context, QuiescenceRequest) (QuiescenceResult, error) {
			return QuiescenceResult{}, stageErr
		}), cleaner: successfulCleaner(), containers: successfulContainers()},
		{name: "cleanup", planner: successfulPlanner(), quiescer: successfulQuiescer(), cleaner: cleanupApplierFunc(func(context.Context, CleanupRequest) (CleanupResult, error) { return CleanupResult{}, stageErr }), containers: successfulContainers()},
		{name: "containers", planner: successfulPlanner(), quiescer: successfulQuiescer(), cleaner: successfulCleaner(), containers: containerStopperFunc(func(context.Context, ContainerResetRequest) (ContainerResetResult, error) {
			return ContainerResetResult{}, stageErr
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			locks := &recordingLockManager{acquired: true}
			coord := &Coordinator{Planner: test.planner, Locks: locks, Quiescer: test.quiescer, Cleaner: test.cleaner, Containers: test.containers}
			_, err := coord.Execute(context.Background(), Request{OperationID: destructiveResetOperationID, ActorTokenID: "operator", RequestHash: "hash", DryRun: true})
			if !errors.Is(err, stageErr) {
				t.Fatalf("Execute error = %v, want stage failure", err)
			}
			if locks.lease == nil || locks.lease.releases != 1 {
				t.Fatalf("lease = %#v, want one release", locks.lease)
			}
		})
	}
}

const destructiveResetOperationID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

type inventoryReaderFunc func(context.Context) (Inventory, error)

func (f inventoryReaderFunc) ReadResetInventory(ctx context.Context) (Inventory, error) {
	return f(ctx)
}

type plannerFunc func(context.Context, Request) (Plan, error)

func (f plannerFunc) BuildPlan(ctx context.Context, req Request) (Plan, error) { return f(ctx, req) }

type quiescenceApplierFunc func(context.Context, QuiescenceRequest) (QuiescenceResult, error)

func (f quiescenceApplierFunc) Apply(ctx context.Context, req QuiescenceRequest) (QuiescenceResult, error) {
	return f(ctx, req)
}

type cleanupApplierFunc func(context.Context, CleanupRequest) (CleanupResult, error)

func (f cleanupApplierFunc) Apply(ctx context.Context, req CleanupRequest) (CleanupResult, error) {
	return f(ctx, req)
}

type containerStopperFunc func(context.Context, ContainerResetRequest) (ContainerResetResult, error)

func (f containerStopperFunc) Apply(ctx context.Context, req ContainerResetRequest) (ContainerResetResult, error) {
	return f(ctx, req)
}

type runtimeContextQuiescerFunc func(context.Context) error

func (f runtimeContextQuiescerFunc) QuiesceAllRuntimeContexts(ctx context.Context) error {
	return f(ctx)
}

func successfulPlanner() Planner {
	return plannerFunc(func(context.Context, Request) (Plan, error) { return Plan{CleanupRunSetKnown: true}, nil })
}

func successfulQuiescer() QuiescenceApplier {
	return quiescenceApplierFunc(func(context.Context, QuiescenceRequest) (QuiescenceResult, error) { return QuiescenceResult{}, nil })
}

func successfulCleaner() CleanupApplier {
	return cleanupApplierFunc(func(context.Context, CleanupRequest) (CleanupResult, error) { return CleanupResult{}, nil })
}

func successfulContainers() ContainerStopper {
	return containerStopperFunc(func(context.Context, ContainerResetRequest) (ContainerResetResult, error) {
		return ContainerResetResult{}, nil
	})
}
