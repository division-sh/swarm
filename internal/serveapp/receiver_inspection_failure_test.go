package serveapp

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

type receiverCleanupFailureReader struct {
	receiverProofStateReader
	failure error
}

func (r receiverCleanupFailureReader) InspectSnapshot(ctx context.Context, read func(context.Context) error) error {
	return errors.Join(r.receiverProofStateReader.InspectSnapshot(ctx, read), r.failure)
}

func TestReceiverInspectionCleanupFailureDiscardsTargetEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			_, start := lifecycleRestartHarness(t, backend, canonicalrouting.CopyServedReceiverInitialization(t))
			_, rt := start()
			params := receiverInitializationPublishParams(rt, "inspection-cleanup", "inspection-cleanup", `{"active":true,"label":"kept","attributes":{}}`)
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, params)
			waitPublicationSiteCompletion(t, rt, seed.RunID)
			_, receiver := requireSingleReceiverTargetState(t, rt.ReceiverStateReader, seed.RunID, "account")
			if record, instance, err := readReceiverTargetState(context.Background(), rt.ReceiverStateReader, seed.RunID, "account", receiver.StorageRef, receiver.EntityID); err != nil || record.Presence != pipeline.WorkflowTargetPersistenceComplete || !reflect.DeepEqual(instance.Fields, receiver.Fields) {
				t.Fatalf("native successful read control: record=%+v instance=%+v err=%v", record, instance, err)
			}
			cleanup := errors.New("independent inspection cleanup failure")
			fault := receiverCleanupFailureReader{receiverProofStateReader: rt.ReceiverStateReader, failure: cleanup}
			for _, cancelled := range []bool{false, true} {
				ctx, cancel := context.WithCancel(context.Background())
				if cancelled {
					cancel()
				}
				record, instance, err := readReceiverTargetState(ctx, fault, seed.RunID, "account", receiver.StorageRef, receiver.EntityID)
				cancel()
				if !errors.Is(err, cleanup) || (cancelled && !errors.Is(err, context.Canceled)) || !reflect.DeepEqual(record, pipeline.WorkflowTargetPersistenceRecord{}) || !reflect.DeepEqual(instance, pipeline.WorkflowInstance{}) {
					t.Fatalf("failed inspection leaked evidence or lost a joined error: cancelled=%t record=%+v instance=%+v err=%v", cancelled, record, instance, err)
				}
			}
		})
	}
}
