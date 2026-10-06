package serveapp

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type receiverProofStateReader interface {
	pipeline.WorkflowTargetPersistenceReader
	ListWorkflowInstances(context.Context, string) ([]pipeline.WorkflowInstance, error)
}

func requireReceiverProofStateReader(t testing.TB, selected any) receiverProofStateReader {
	t.Helper()
	reader, ok := selected.(receiverProofStateReader)
	if !ok || reader == nil {
		t.Fatal("receiver proof requires the original selected workflow reader")
	}
	return reader
}

func requireReceiverTargetState(t testing.TB, reader receiverProofStateReader, runID, flow, path, entityID string) (pipeline.WorkflowTargetPersistenceRecord, pipeline.WorkflowInstance) {
	t.Helper()
	if reader == nil {
		t.Fatal("receiver proof requires an owned workflow reader")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(flow, flowidentity.LogicalInstanceID(path), path))
	if err != nil {
		t.Fatal(err)
	}
	record, err := reader.LoadWorkflowTargetPersistence(context.Background(), owner, identity.NormalizeEntityID(entityID))
	if err != nil {
		t.Fatal(err)
	}
	instance, err := record.DecodeComplete(owner.Route, identity.NormalizeEntityID(entityID))
	if err != nil {
		t.Fatal(err)
	}
	if instance.WorkflowName != flow || instance.StorageRef != path || instance.EntityID != entityID {
		t.Fatalf("receiver readback lost exact template/route/entity: %+v", instance)
	}
	return record, instance
}

func requireSingleReceiverTargetState(t testing.TB, reader receiverProofStateReader, runID, flow string) (pipeline.WorkflowTargetPersistenceRecord, pipeline.WorkflowInstance) {
	t.Helper()
	if reader == nil {
		t.Fatal("receiver proof requires an owned workflow reader")
	}
	instances, err := reader.ListWorkflowInstances(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	var receiver pipeline.WorkflowInstance
	count := 0
	for _, instance := range instances {
		if instance.WorkflowName == flow {
			receiver = instance
			count++
		}
	}
	if count != 1 {
		t.Fatalf("receiver companions=%d, want exactly one %s", count, flow)
	}
	return requireReceiverTargetState(t, reader, runID, flow, receiver.StorageRef, receiver.EntityID)
}
