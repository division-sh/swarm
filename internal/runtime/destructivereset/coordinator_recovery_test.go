package destructivereset

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func (*recoveryResetLifecycle) SettleResources(context.Context) error { return nil }

func (*recoveryResetLifecycle) ResetSourceProjections(context.Context) ([]SourceProjection, error) {
	return nil, nil
}

func TestCoordinatorPendingRecoveryUsesOnlyUncommittedPhases(t *testing.T) {
	for _, phase := range []OperationPhase{PhaseAdmitted, PhasePlanned, PhaseQuiesced, PhaseCleanupCommitted, PhaseContainersSettled} {
		for _, retain := range []bool{false, true} {
			name := string(phase) + "/clear"
			if retain {
				name = string(phase) + "/retain"
			}
			t.Run(name, func(t *testing.T) {
				pause := errors.New("interrupted before next phase")
				interrupted := true
				lifecycle := &recoveryResetLifecycle{}
				c := receiptTestCoordinator(lifecycle)
				journal := installCoordinatorOperationFixture(t, c)
				plans, quiescences, cleanups, stops := 0, 0, 0, 0
				planner, quiescer, cleaner, stopper := c.Planner, c.Quiescer, c.Cleaner, c.Containers
				c.Planner = plannerFunc(func(ctx context.Context, req Request) (Plan, error) {
					if interrupted && phase == PhaseAdmitted {
						return Plan{}, pause
					}
					plans++
					return planner.BuildPlan(ctx, req)
				})
				c.Quiescer = quiescenceApplierFunc(func(ctx context.Context, req QuiescenceRequest) (QuiescenceResult, error) {
					if interrupted && phase == PhasePlanned {
						return QuiescenceResult{}, pause
					}
					quiescences++
					return quiescer.Apply(ctx, req)
				})
				c.Cleaner = cleanupApplierFunc(func(ctx context.Context, req CleanupRequest) (CleanupResult, error) {
					if interrupted && phase == PhaseQuiesced {
						return CleanupResult{}, pause
					}
					cleanups++
					return cleaner.Apply(ctx, req)
				})
				c.Containers = containerStopperFunc(func(ctx context.Context, req ContainerResetRequest) (ContainerResetResult, error) {
					if interrupted && phase == PhaseCleanupCommitted {
						return ContainerResetResult{}, pause
					}
					stops++
					return stopper.Apply(ctx, req)
				})
				if phase == PhaseContainersSettled {
					lifecycle.completeErr = pause
				}
				req := receiptTestRequest()
				req.IdempotencyKey = "recover"
				req.IncludeSourceArtifacts, req.IncludeSourceArtifactsSet = !retain, true
				if _, err := c.Execute(context.Background(), req); !errors.Is(err, pause) {
					t.Fatalf("initial interruption = %v", err)
				}
				if journal.operation.Phase != phase {
					t.Fatalf("saved phase = %s, want %s", journal.operation.Phase, phase)
				}
				before := journal.operation
				// Exercise serialized journal evidence, not an in-memory result alias.
				encoded, err := json.Marshal(before)
				if err != nil {
					t.Fatal(err)
				}
				journal.operation = Operation{}
				if err := json.Unmarshal(encoded, &journal.operation); err != nil {
					t.Fatal(err)
				}
				interrupted, lifecycle.completeErr = false, nil
				recovery, err := c.RecoverPending(context.Background())
				if err != nil || recovery == nil {
					t.Fatal(err)
				}
				if journal.operation.Phase != PhaseContainersSettled || len(lifecycle.retained) != 0 {
					t.Fatal("effect recovery completed before local reconstruction")
				}
				if err := recovery.Complete(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := recovery.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				if journal.operation.Phase != PhaseCompleted || plans != 1 || quiescences != 1 || cleanups != 1 || stops != 1 {
					t.Fatalf("recovery repeated work: phase=%s effects=%d/%d/%d/%d", journal.operation.Phase, plans, quiescences, cleanups, stops)
				}
				if len(lifecycle.retained) != 1 || lifecycle.retained[0] != retain {
					t.Fatalf("reconstruction disposition = %v", lifecycle.retained)
				}
				for _, id := range lifecycle.operationIDs {
					if id != before.Request.OperationID {
						t.Fatalf("recovery changed operation identity: %s", id)
					}
				}
				begins := len(lifecycle.operationIDs)
				if recovery, err := c.RecoverPending(context.Background()); err != nil || recovery != nil {
					t.Fatalf("completed operation returned pending recovery: %v", err)
				}
				req.OperationID = uuid.NewString()
				out, err := c.Execute(context.Background(), req)
				if err != nil || !reflect.DeepEqual(journal.operation.Response, &out) || len(lifecycle.operationIDs) != begins {
					t.Fatalf("historical replay affected later execution: %v", err)
				}
			})
		}
	}
}

func TestCoordinatorPendingRecoveryPreservesHistoricalPartialOutcome(t *testing.T) {
	lifecycle := &recoveryResetLifecycle{}
	c := receiptTestCoordinator(lifecycle)
	failed := true
	stops := 0
	c.Containers = containerStopperFunc(func(context.Context, ContainerResetRequest) (ContainerResetResult, error) {
		stops++
		out := ContainerResetResult{OperationName: DefaultOperationName}
		if failed {
			out.Failed = []ContainerStopFailure{{Error: "target still running"}}
		}
		return out, nil
	})
	journal := installCoordinatorOperationFixture(t, c)
	req := receiptTestRequest()
	req.IdempotencyKey = "partial"
	first, err := c.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	revision := journal.operation.Revision
	if _, err := c.RecoverPending(context.Background()); !errors.Is(err, ErrOperationInProgress) {
		t.Fatalf("unsettled recovery admitted startup: %v", err)
	}
	if journal.operation.Revision != revision || len(lifecycle.retained) != 0 {
		t.Fatal("repeated failure rewrote history or reconstructed execution")
	}
	failed = false
	c.Planner = plannerFunc(func(context.Context, Request) (Plan, error) {
		t.Fatal("partial recovery inventoried later work")
		return Plan{}, nil
	})
	c.Quiescer = quiescenceApplierFunc(func(context.Context, QuiescenceRequest) (QuiescenceResult, error) {
		t.Fatal("partial recovery repeated quiescence")
		return QuiescenceResult{}, nil
	})
	c.Cleaner = cleanupApplierFunc(func(context.Context, CleanupRequest) (CleanupResult, error) {
		t.Fatal("partial recovery repeated committed destruction")
		return CleanupResult{}, nil
	})
	recovery, err := c.RecoverPending(context.Background())
	if err != nil || recovery == nil {
		t.Fatal(err)
	}
	if err := recovery.Complete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if journal.operation.Phase != PhaseCompleted || len(lifecycle.retained) != 1 || stops != 3 {
		t.Fatal("pending resource settlement did not converge")
	}
	second, err := c.Execute(context.Background(), req)
	if err != nil || !reflect.DeepEqual(first, second) || stops != 3 {
		t.Fatalf("recovery replaced the historical response: %v", err)
	}
}

type recoveryResetLifecycle struct {
	operationIDs []string
	retained     []bool
	completeErr  error
}

func (l *recoveryResetLifecycle) BeginDestructiveReset(_ context.Context, id string) (RuntimeReset, error) {
	l.operationIDs = append(l.operationIDs, id)
	return l, nil
}

func (l *recoveryResetLifecycle) Complete(_ context.Context, retain bool) error {
	if l.completeErr != nil {
		return l.completeErr
	}
	l.retained = append(l.retained, retain)
	return nil
}

func (l *recoveryResetLifecycle) Release() {}

func TestPendingRecoveryInterruptionDoesNotCompleteOrReleaseSuccessor(t *testing.T) {
	lifecycle := &recoveryResetLifecycle{completeErr: errors.New("boot interrupted")}
	c := receiptTestCoordinator(lifecycle)
	journal := installCoordinatorOperationFixture(t, c)
	if _, err := c.Execute(context.Background(), receiptTestRequest()); err == nil {
		t.Fatal("expected interruption after effect settlement")
	}
	locks := &recordingLockManager{acquired: true}
	c.Locks = locks
	recovery, err := c.RecoverPending(context.Background())
	if err != nil || recovery == nil {
		t.Fatalf("recover effects: %v", err)
	}
	if locks.lease.releases != 0 || journal.operation.Phase != PhaseContainersSettled {
		t.Fatal("recovery released serialization or completed before reconstruction")
	}
	before := journal.operation
	if err := recovery.Complete(context.Background()); err == nil {
		t.Fatal("failed reconstruction completed recovery")
	}
	if !reflect.DeepEqual(before, journal.operation) {
		t.Fatal("failed reconstruction changed durable effect evidence")
	}
	for i := 0; i < 2; i++ {
		if err := recovery.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if locks.lease.releases != 1 || !reflect.DeepEqual(before, journal.operation) {
		t.Fatal("closing an interrupted recovery repeated release or completed its receipt")
	}
	lifecycle.completeErr = nil
	next, err := c.RecoverPending(context.Background())
	if err != nil || next == nil {
		t.Fatalf("recover interrupted startup: %v", err)
	}
	defer func() { _ = next.Close(context.Background()) }()
	if err := recovery.Complete(context.Background()); err == nil {
		t.Fatal("closed recovery retained completion authority")
	}
	if err := recovery.Close(context.Background()); err != nil || locks.lease.releases != 0 {
		t.Fatal("old recovery released successor serialization")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := next.Complete(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled startup completed recovery: %v", err)
	}
	if err := next.Complete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := next.Complete(context.Background()); err != nil || len(lifecycle.retained) != 1 {
		t.Fatal("repeated completion reconstructed execution")
	}
}
