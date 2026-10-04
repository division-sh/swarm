package channelonboarding

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type retainedInspectionReader struct {
	operations  []Operation
	activations []ConnectedChannelActivation
	teardowns   []TeardownOperation
	failure     string
	reads       []string
}

func (r *retainedInspectionReader) read(ctx context.Context, name string) error {
	r.reads = append(r.reads, name)
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.failure == name {
		return errors.New("selected " + name + " read failed")
	}
	return nil
}

func (r *retainedInspectionReader) ListChannelOnboardingOperations(ctx context.Context) ([]Operation, error) {
	return r.operations, r.read(ctx, "operations")
}

func (r *retainedInspectionReader) ListCurrentConnectedChannelActivations(ctx context.Context) ([]ConnectedChannelActivation, error) {
	return r.activations, r.read(ctx, "activations")
}

func (r *retainedInspectionReader) ListChannelTeardowns(ctx context.Context) ([]TeardownOperation, error) {
	return r.teardowns, r.read(ctx, "teardowns")
}

func TestRetainedChannelInspectionPreservesExactOwnershipAndRequiredReads(t *testing.T) {
	operation := Operation{OperationID: "operation", SlotKey: "slot", Coordinate: testCoordinate()}
	activation := ConnectedChannelActivation{ActivationID: "activation", OperationID: operation.OperationID, SlotKey: operation.SlotKey, Coordinate: operation.Coordinate}
	for _, cause := range []string{"valid", "unconnected", "operations", "activations", "duplicate", "missing_owner", "crossed_slot", "crossed_coordinate", "canceled"} {
		t.Run(cause, func(t *testing.T) {
			reader := &retainedInspectionReader{operations: []Operation{operation}, activations: []ConnectedChannelActivation{activation}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch cause {
			case "unconnected":
				reader.operations, reader.activations = nil, nil
			case "operations", "activations":
				reader.failure = cause
			case "duplicate":
				reader.operations = append(reader.operations, operation)
			case "missing_owner":
				reader.operations = nil
			case "crossed_slot":
				reader.activations[0].SlotKey = "other-slot"
			case "crossed_coordinate":
				reader.activations[0].Coordinate.RuntimeInstanceID = "other-runtime"
			case "canceled":
				cancel()
			}
			before := RetainedActivationInventory{Operations: append([]Operation(nil), reader.operations...), Activations: append([]ConnectedChannelActivation(nil), reader.activations...)}
			inventory, err := InspectRetainedActivations(ctx, reader)
			if cause == "valid" || cause == "unconnected" {
				if err != nil || !reflect.DeepEqual(before, inventory) || !reflect.DeepEqual(reader.reads, []string{"operations", "activations"}) {
					t.Fatalf("durable facts were reconstructed or omitted: %+v err=%v reads=%v", inventory, err, reader.reads)
				}
			} else if err == nil || len(inventory.Operations) != 0 || len(inventory.Activations) != 0 {
				t.Fatalf("partial or crossed inventory became success: %+v err=%v", inventory, err)
			}
			if cause == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation erased: %v", err)
			}
			if strings.HasPrefix(cause, "crossed_") || cause == "missing_owner" || cause == "duplicate" {
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("durable ownership conflict lost its classification: %v", err)
				}
			}
		})
	}
	if _, err := InspectRetainedActivations(context.Background(), nil); err == nil {
		t.Fatal("missing read port became empty inventory")
	}
}

func TestPendingChannelTeardownInspectionSharesRecoveryKindAdmission(t *testing.T) {
	for _, kind := range []TeardownKind{TeardownUnbind, TeardownProofRevoke, TeardownInterfaceRetirement, TeardownContextRetirement, "unknown"} {
		t.Run(string(kind), func(t *testing.T) {
			reader := &retainedInspectionReader{teardowns: []TeardownOperation{
				{TeardownID: "finished", Phase: TeardownSucceeded, Kind: kind},
				{TeardownID: "pending", Phase: TeardownAuthorityRetired, Kind: kind},
			}}
			pending, err := InspectPendingTeardowns(context.Background(), reader)
			if kind.Valid() {
				if err != nil || len(pending) != 1 || pending[0].TeardownID != "pending" {
					t.Fatalf("pending recovery facts: %+v err=%v", pending, err)
				}
			} else if !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "unsupported teardown kind") {
				t.Fatalf("unknown recovery kind escaped admission: %+v err=%v", pending, err)
			}
		})
	}
	reader := &retainedInspectionReader{failure: "teardowns"}
	if _, err := InspectPendingTeardowns(context.Background(), reader); err == nil {
		t.Fatal("teardown read failure became empty inventory")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InspectPendingTeardowns(ctx, reader); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled teardown read: %v", err)
	}
}
