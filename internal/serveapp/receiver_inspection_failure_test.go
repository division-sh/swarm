package serveapp

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type receiverCleanupFailureReader struct {
	receiverProofStateReader
	failure error
}

func (r receiverCleanupFailureReader) InspectSnapshot(ctx context.Context, read func(context.Context) error) error {
	return errors.Join(r.receiverProofStateReader.InspectSnapshot(ctx, read), r.failure)
}

func requireReceiverInspectionCleanupFailures(t testing.TB, reader receiverProofStateReader, runID, flow, path, entityID string) {
	t.Helper()
	if record, instance, err := readReceiverTargetState(context.Background(), reader, runID, flow, path, entityID); err != nil || record.Presence != pipeline.WorkflowTargetPersistenceComplete || len(instance.Fields) == 0 {
		t.Fatalf("native successful read control: record=%+v instance=%+v err=%v", record, instance, err)
	}
	cleanup := errors.New("independent inspection cleanup failure")
	fault := receiverCleanupFailureReader{receiverProofStateReader: reader, failure: cleanup}
	for _, cancelled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if cancelled {
			cancel()
		}
		record, instance, err := readReceiverTargetState(ctx, fault, runID, flow, path, entityID)
		cancel()
		if !errors.Is(err, cleanup) || (cancelled && !errors.Is(err, context.Canceled)) || !reflect.DeepEqual(record, pipeline.WorkflowTargetPersistenceRecord{}) || !reflect.DeepEqual(instance, pipeline.WorkflowInstance{}) {
			t.Fatalf("failed inspection leaked evidence or lost a joined error: cancelled=%t record=%+v instance=%+v err=%v", cancelled, record, instance, err)
		}
	}
}
