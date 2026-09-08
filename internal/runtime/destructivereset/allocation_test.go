package destructivereset

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

func allocationOperation(t *testing.T) (Operation, SourceProjection) {
	t.Helper()
	allocation := SourceProjection{Cleanup: sourceartifact.RuntimeProjectionCleanup{BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), Identity: "runtime-projection-v1:" + strings.Repeat("b", 32), Root: filepath.Join(t.TempDir(), "swarm-source-planned")}, ManagedContainers: true}
	operation, err := NewOperation(Request{OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "allocation", RequestedAt: time.Now().UTC(), IncludeSourceArtifactsSet: true})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: allocation.Cleanup.BundleHash}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	operation.SourceSet = &plan
	operation.Phase, operation.Revision = PhaseContainersSettled, 5
	operation.Plan = &Result{OperationName: DefaultOperationName, PlannedAt: operation.Request.RequestedAt, Plan: Plan{CleanupRunSetKnown: true}}
	operation.Quiescence = &QuiescenceResult{OperationName: DefaultOperationName}
	operation.Cleanup = &CleanupResult{OperationName: DefaultOperationName}
	operation.Containers = &ContainerResetResult{OperationName: DefaultOperationName}
	if err := operation.Validate(); err != nil {
		t.Fatal(err)
	}
	return operation, allocation
}

type allocationAcknowledgmentStore struct {
	operationMemoryStore
	commit bool
	reads  int
}

func (s *allocationAcknowledgmentStore) AdvanceResetOperation(ctx context.Context, before, after Operation) error {
	if s.commit {
		if err := s.operationMemoryStore.AdvanceResetOperation(ctx, before, after); err != nil {
			return err
		}
	}
	return errors.New("allocation acknowledgment unavailable")
}

func (s *allocationAcknowledgmentStore) ReadResetOperation(ctx context.Context, id string) (Operation, error) {
	s.reads++
	if s.reads > 1 {
		if _, bounded := ctx.Deadline(); !bounded {
			return Operation{}, errors.New("unbounded allocation reconciliation")
		}
	}
	return s.operationMemoryStore.ReadResetOperation(ctx, id)
}

func TestProjectionAllocationRequiresDurableExactAcknowledgment(t *testing.T) {
	for _, commit := range []bool{false, true} {
		operation, allocation := allocationOperation(t)
		store := &allocationAcknowledgmentStore{operationMemoryStore: operationMemoryStore{operation: operation}, commit: commit}
		err := RecordProjectionAllocation(context.Background(), store, operation.Request.OperationID, allocation)
		if (err == nil) != commit || store.reads != 2 {
			t.Fatalf("commit=%t: allocation permission=%v, reads=%d", commit, err, store.reads)
		}
		if !reflect.DeepEqual(store.operation.Request, operation.Request) {
			t.Fatal("allocation mutated admission")
		}
		if commit && (len(store.operation.Allocations) != 1 || store.operation.Allocations[0] != allocation) {
			t.Fatal("readback accepted different allocation evidence")
		}
	}
}

func TestProjectionAllocationHistoryIsAppendOnlyAndSourceBound(t *testing.T) {
	before, allocation := allocationOperation(t)
	store := &operationMemoryStore{operation: before}
	if err := RecordProjectionAllocation(context.Background(), store, before.Request.OperationID, allocation); err != nil {
		t.Fatal(err)
	}
	before = store.operation
	for _, mutation := range []string{"same-identity", "same-root", "foreign-source", "rewrite-prefix", "rewrite-request", "rewrite-effects"} {
		t.Run(mutation, func(t *testing.T) {
			next := allocation
			next.Cleanup.Identity = "runtime-projection-v1:" + strings.Repeat("c", 32)
			next.Cleanup.Root += "-next"
			after := before
			after.Revision++
			after.Allocations = append(append([]SourceProjection{}, before.Allocations...), next)
			switch mutation {
			case "same-identity":
				after.Allocations[1].Cleanup.Identity = allocation.Cleanup.Identity
			case "same-root":
				after.Allocations[1].Cleanup.Root = allocation.Cleanup.Root
			case "foreign-source":
				after.Allocations[1].Cleanup.BundleHash = "bundle-v2:sha256:" + strings.Repeat("d", 64)
			case "rewrite-prefix":
				after.Allocations[0].ManagedContainers = false
			case "rewrite-request":
				after.Request.RequestHash = "rewritten"
			case "rewrite-effects":
				effect := *after.Cleanup
				effect.AppliedAt = time.Now()
				after.Cleanup = &effect
			}
			if err := ValidateOperationTransition(before, after); err == nil {
				t.Fatal("rewritten allocation evidence accepted")
			}
		})
	}
	after := before
	after.Phase, after.Revision = PhaseCompleted, before.Revision+1
	after.Response = &ExecutionResult{Plan: *before.Plan, Quiescence: *before.Quiescence, Cleanup: *before.Cleanup, Containers: *before.Containers}
	if err := ValidateOperationTransition(before, after); err != nil {
		t.Fatal(err)
	}
	store.operation = after
	if err := RecordProjectionAllocation(context.Background(), store, before.Request.OperationID, allocation); err == nil {
		t.Fatal("historical completed operation admitted allocation")
	}
}
