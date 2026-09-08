package destructivereset

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

type resetReceiptFaultStore struct {
	OperationStore
	failPhase OperationPhase
	readErr   error
	writeErr  error
}

func (s resetReceiptFaultStore) ReadResetOperation(ctx context.Context, id string) (Operation, error) {
	if s.readErr != nil {
		return Operation{}, s.readErr
	}
	return s.OperationStore.ReadResetOperation(ctx, id)
}

func (s resetReceiptFaultStore) AdvanceResetOperation(ctx context.Context, before, after Operation) error {
	if err := s.OperationStore.AdvanceResetOperation(ctx, before, after); err != nil {
		return err
	}
	if after.Phase == s.failPhase {
		return s.writeErr
	}
	return nil
}

func TestCoordinatorReconcilesExactCommittedReceiptAfterAcknowledgmentLoss(t *testing.T) {
	for _, phase := range []OperationPhase{PhasePlanned, PhaseQuiesced, PhaseCleanupCommitted, PhaseContainersSettled, PhaseCompleted} {
		t.Run(string(phase), func(t *testing.T) {
			lifecycle := &recordingResetLifecycle{}
			quiescences, cleanups, containers := 0, 0, 0
			c := receiptTestCoordinator(lifecycle)
			journal := installCoordinatorOperationFixture(t, c)
			lostAck := errors.New("commit acknowledgment lost")
			c.Operations = resetReceiptFaultStore{OperationStore: journal, failPhase: phase, writeErr: lostAck}
			quiescer, cleaner, stopper := c.Quiescer, c.Cleaner, c.Containers
			c.Quiescer = quiescenceApplierFunc(func(ctx context.Context, req QuiescenceRequest) (QuiescenceResult, error) {
				quiescences++
				out, err := quiescer.Apply(ctx, req)
				if err == nil && phase == PhaseQuiesced {
					return QuiescenceResult{}, lostAck
				}
				return out, err
			})
			c.Cleaner = cleanupApplierFunc(func(ctx context.Context, req CleanupRequest) (CleanupResult, error) {
				cleanups++
				out, err := cleaner.Apply(ctx, req)
				if err == nil && phase == PhaseCleanupCommitted {
					return CleanupResult{}, lostAck
				}
				return out, err
			})
			c.Containers = containerStopperFunc(func(ctx context.Context, req ContainerResetRequest) (ContainerResetResult, error) {
				containers++
				return stopper.Apply(ctx, req)
			})
			out, err := c.Execute(context.Background(), receiptTestRequest())
			if err != nil {
				t.Fatal(err)
			}
			if journal.operation.Phase != PhaseCompleted || !reflect.DeepEqual(journal.operation.Response, &out) ||
				quiescences != 1 || cleanups != 1 || containers != 1 || len(lifecycle.retained) != 1 {
				t.Fatalf("receipt recovery repeated or skipped work: %+v counts=%d/%d/%d lifecycle=%+v", journal.operation, quiescences, cleanups, containers, lifecycle)
			}
		})
	}
}

func TestCoordinatorUncertainEffectNeverCompensatesOrStartsLaterEffects(t *testing.T) {
	for _, stage := range []OperationPhase{PhaseQuiesced, PhaseCleanupCommitted} {
		for _, unreadable := range []bool{false, true} {
			name := string(stage) + "/missing"
			if unreadable {
				name = string(stage) + "/unreadable"
			}
			t.Run(name, func(t *testing.T) {
				lifecycle := &recordingResetLifecycle{}
				c := receiptTestCoordinator(lifecycle)
				journal := installCoordinatorOperationFixture(t, c)
				uncertain := errors.New("transaction result unknown")
				laterEffects := 0
				fail := func() {
					if unreadable {
						c.Operations = resetReceiptFaultStore{OperationStore: journal, readErr: errors.New("receipt unavailable")}
					}
				}
				if stage == PhaseQuiesced {
					c.Quiescer = quiescenceApplierFunc(func(context.Context, QuiescenceRequest) (QuiescenceResult, error) {
						fail()
						return QuiescenceResult{}, uncertain
					})
					c.Cleaner = cleanupApplierFunc(func(context.Context, CleanupRequest) (CleanupResult, error) {
						laterEffects++
						return CleanupResult{}, nil
					})
				} else {
					c.Cleaner = cleanupApplierFunc(func(context.Context, CleanupRequest) (CleanupResult, error) {
						fail()
						return CleanupResult{}, uncertain
					})
				}
				c.Containers = containerStopperFunc(func(context.Context, ContainerResetRequest) (ContainerResetResult, error) {
					laterEffects++
					return ContainerResetResult{}, nil
				})
				_, err := c.Execute(context.Background(), receiptTestRequest())
				if !errors.Is(err, uncertain) || len(lifecycle.retained) != 0 || laterEffects != 0 || lifecycle.releases != 1 {
					t.Fatalf("uncertain effect escaped fence: err=%v lifecycle=%+v later=%d", err, lifecycle, laterEffects)
				}
			})
		}
	}
}

func receiptTestCoordinator(lifecycle RuntimeContextLifecycle) *Coordinator {
	return &Coordinator{
		Planner: plannerFunc(func(_ context.Context, req Request) (Plan, error) {
			return Plan{CleanupRunSetKnown: true, IncludeSourceArtifacts: req.IncludeSourceArtifacts}, nil
		}),
		Locks: &recordingLockManager{acquired: true}, RuntimeContexts: lifecycle,
		Quiescer: successfulQuiescer(), Cleaner: successfulCleaner(),
		Containers: containerStopperFunc(func(context.Context, ContainerResetRequest) (ContainerResetResult, error) {
			return ContainerResetResult{OperationName: DefaultOperationName}, nil
		}),
	}
}

func receiptTestRequest() Request {
	return Request{OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "hash"}
}
