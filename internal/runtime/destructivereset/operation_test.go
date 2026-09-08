package destructivereset

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

type operationMemoryStore struct{ operation Operation }

func (s *operationMemoryStore) AdmitResetOperation(_ context.Context, req Request) (Operation, error) {
	if s.operation.Revision != 0 {
		if !s.operation.Matches(req) {
			return Operation{}, ErrOperationConflict
		}
		return s.operation, nil
	}
	op, err := NewOperation(req)
	if err == nil {
		s.operation = op
	}
	return op, err
}
func (s *operationMemoryStore) ReadResetOperation(context.Context, string) (Operation, error) {
	return s.operation, nil
}
func (s *operationMemoryStore) PendingResetOperations(context.Context) ([]Operation, error) {
	if s.operation.Revision == 0 || s.operation.Phase == PhaseCompleted {
		return nil, nil
	}
	return []Operation{s.operation}, nil
}
func (s *operationMemoryStore) AdvanceResetOperation(_ context.Context, before, after Operation) error {
	if !reflect.DeepEqual(s.operation, before) {
		return ErrOperationChanged
	}
	if err := ValidateOperationTransition(before, after); err != nil {
		return err
	}
	s.operation = after
	return nil
}

// Coordinator-only fixtures model atomic effect receipts. Real transaction
// rollback, commit and readback are proven separately against both databases.
func installCoordinatorOperationFixture(t *testing.T, c *Coordinator) *operationMemoryStore {
	t.Helper()
	store := &operationMemoryStore{}
	c.Operations = store
	quiescer, cleaner := c.Quiescer, c.Cleaner
	c.Quiescer = quiescenceApplierFunc(func(ctx context.Context, req QuiescenceRequest) (QuiescenceResult, error) {
		out, err := quiescer.Apply(ctx, req)
		if err != nil || req.Result.DryRun {
			return out, err
		}
		out.OperationName = DefaultOperationName
		out.AppliedAt = req.RequestedAt
		next := store.operation
		next.Phase, next.Revision, next.Quiescence = PhaseQuiesced, next.Revision+1, &out
		return out, store.AdvanceResetOperation(ctx, store.operation, next)
	})
	c.Cleaner = cleanupApplierFunc(func(ctx context.Context, req CleanupRequest) (CleanupResult, error) {
		out, err := cleaner.Apply(ctx, req)
		if err != nil || req.Result.DryRun {
			return out, err
		}
		out.OperationName, out.IncludeSourceArtifacts, out.AppliedAt = DefaultOperationName, req.Result.IncludeSourceArtifacts, req.RequestedAt
		next := store.operation
		next.Phase, next.Revision, next.Cleanup = PhaseCleanupCommitted, next.Revision+1, &out
		return out, store.AdvanceResetOperation(ctx, store.operation, next)
	})
	return store
}

func TestCoordinatorHistoricalReplayDoesNotWithdrawOrInventoryLaterWork(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "completed"
		if partial {
			name = "partial outcome remains pending"
		}
		t.Run(name, func(t *testing.T) { proveCoordinatorHistoricalReplay(t, partial) })
	}
}

func proveCoordinatorHistoricalReplay(t *testing.T, partial bool) {
	lifecycle := &recordingResetLifecycle{}
	plans := 0
	c := &Coordinator{
		Planner: plannerFunc(func(_ context.Context, req Request) (Plan, error) {
			plans++
			return Plan{CleanupRunSetKnown: true, IncludeSourceArtifacts: req.IncludeSourceArtifacts}, nil
		}),
		Locks: &recordingLockManager{acquired: true}, RuntimeContexts: lifecycle,
		Quiescer: successfulQuiescer(), Cleaner: successfulCleaner(),
		Containers: containerStopperFunc(func(context.Context, ContainerResetRequest) (ContainerResetResult, error) {
			if partial {
				return ContainerResetResult{OperationName: DefaultOperationName, Failed: []ContainerStopFailure{{Error: "container acknowledgment uncertain"}}}, nil
			}
			return ContainerResetResult{OperationName: DefaultOperationName}, nil
		}),
	}
	journal := installCoordinatorOperationFixture(t, c)
	req := Request{OperationID: uuid.NewString(), ActorTokenID: "operator", IdempotencyKey: "key", RequestHash: "hash"}
	first, err := c.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.OperationID = uuid.NewString()
	second, err := c.Execute(context.Background(), req)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("replay = %+v, %v", second, err)
	}
	wantPhase := PhaseCompleted
	if partial {
		wantPhase = PhaseCleanupCommitted
	}
	if lifecycle.begins != 1 || plans != 1 || journal.operation.Phase != wantPhase {
		t.Fatal("historical replay touched current execution")
	}
	req.RequestHash = "conflict"
	if _, err := c.Execute(context.Background(), req); !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("conflict = %v", err)
	}
	if lifecycle.begins != 1 || plans != 1 {
		t.Fatal("conflicting request touched execution")
	}
	prior := journal.operation
	tampered := prior
	tampered.Revision++
	tampered.Request.RequestHash = "rewrite"
	if err := journal.AdvanceResetOperation(context.Background(), prior, tampered); err == nil {
		t.Fatal("historical outcome could be rewritten")
	}
}
