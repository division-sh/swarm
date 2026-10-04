package destructivereset

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type pendingResetReaderFunc func(context.Context) ([]Operation, error)

func (f pendingResetReaderFunc) PendingResetOperations(ctx context.Context) ([]Operation, error) {
	return f(ctx)
}

func TestInspectPendingResetOperationsUsesCanonicalLedgerWithoutEffects(t *testing.T) {
	operation, err := NewOperation(Request{
		OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "pending-inspection",
		RequestedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		rows []Operation
		fail bool
	}{
		{name: "inspected_empty"},
		{name: "pending", rows: []Operation{operation}},
		{name: "multiple", rows: []Operation{operation, operation}, fail: true},
		{name: "invalid", rows: []Operation{{}}, fail: true},
		{name: "malformed_completed", rows: []Operation{{Request: operation.Request, Revision: 1, Phase: PhaseCompleted}}, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			reader := pendingResetReaderFunc(func(context.Context) ([]Operation, error) {
				calls++
				return tc.rows, nil
			})
			got, err := InspectPendingOperations(context.Background(), reader)
			if (err != nil) != tc.fail || calls != 1 {
				t.Fatalf("rows=%+v err=%v calls=%d", got, err, calls)
			}
			if tc.fail {
				failure, ok := failures.As(err)
				if !ok || failure.Failure.Class != failures.ClassLifecycleConflict {
					t.Fatalf("ledger invalidity did not preserve its refusal class: %v", err)
				}
			}
			if !tc.fail && !reflect.DeepEqual(got, tc.rows) {
				t.Fatalf("inspector changed canonical ledger evidence: got=%+v want=%+v", got, tc.rows)
			}
			// The port supplied above has no lock, effect, advance, or capability method.
		})
	}
}

func TestInspectPendingResetOperationsMissingFailedCanceledReadsNeverMeanEmpty(t *testing.T) {
	if _, err := InspectPendingOperations(context.Background(), nil); err == nil {
		t.Fatal("missing ledger reader became an empty successful observation")
	}
	cause := errors.New("required reset ledger read failed")
	if _, err := InspectPendingOperations(context.Background(), pendingResetReaderFunc(func(context.Context) ([]Operation, error) {
		return nil, cause
	})); !errors.Is(err, cause) {
		t.Fatalf("required failed read lost: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectPendingOperations(ctx, pendingResetReaderFunc(func(context.Context) ([]Operation, error) {
		t.Fatal("canceled observation reached the ledger")
		return nil, nil
	})); !errors.Is(err, context.Canceled) {
		t.Fatalf("entry cancellation lost: %v", err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	if _, err := InspectPendingOperations(ctx, pendingResetReaderFunc(func(context.Context) ([]Operation, error) {
		cancel()
		return nil, nil
	})); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation during a successful-looking read became success: %v", err)
	}
}
