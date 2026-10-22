package runforkpersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/google/uuid"
)

func TestHistoricalConstructionHeaderHonorsCancellationDuringBirthSelection(t *testing.T) {
	source, child, entity := uuid.NewString(), uuid.NewString(), uuid.NewString()
	const path = "review/one"
	projected, err := runfork.ProjectEntityOwnership(source, child, entity, path)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := &runForkRevisionSnapshot{RunID: source, Revision: 2, EntityMetadata: []runForkRevisionEntityMetadata{{
		EntityID: entity, FlowInstance: path, FlowTemplate: "review", ConstructionKind: "constructed",
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reads := 0
	result, found, err := historicalFlowInstanceConstructionCutForHeader(ctx, runlifecycle.Snapshot{RunID: child}, pipeline.WorkflowInstance{StorageRef: projected.Fork.FlowInstance}, snapshot,
		func(context.Context, pipeline.WorkflowInstance) (runForkReceiverBirth, error) {
			reads++
			cancel()
			return runForkReceiverBirth{}, nil
		})
	if reads != 1 || !errors.Is(err, context.Canceled) || found || !reflect.DeepEqual(result, historicalFlowInstanceConstructionCut{}) {
		t.Fatalf("per-header cached work ignored cancellation: reads=%d found=%t result=%+v err=%v", reads, found, result, err)
	}
}
