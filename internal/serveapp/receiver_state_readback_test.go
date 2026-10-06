package serveapp

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type receiverProofStateReader interface {
	pipeline.WorkflowTargetPersistenceReader
	ListWorkflowInstances(context.Context, string) ([]pipeline.WorkflowInstance, error)
	InspectSnapshot(context.Context, func(context.Context) error) error
}

func requireReceiverApplicationSnapshot(t testing.TB, reader receiverProofStateReader) map[string][]string {
	t.Helper()
	var snapshot map[string]storetest.SelectedForkStorageTableSnapshot
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		snapshot, err = storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, reader)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string][]string, 2*len(snapshot))
	for table, evidence := range snapshot {
		columns, err := json.Marshal(evidence.Columns)
		if err != nil {
			t.Fatal(err)
		}
		out[table] = evidence.Rows
		out[table+"/columns"] = []string{string(columns)}
	}
	return out
}

func requireReceiverEventIdempotencyCardinality(t testing.TB, reader receiverProofStateReader, key string) int {
	t.Helper()
	var count int
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		count, err = storetest.ReadEventIdempotencyCardinality(ctx, reader, key)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func requireReceiverConstructionPublicationFields(t testing.TB, reader receiverProofStateReader, owner flowidentity.RunScopedFlowInstance, entityID, eventID string) map[string]any {
	t.Helper()
	var fields map[string]any
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		fields, err = storetest.ReadReceiverConstructionPublicationFields(ctx, reader, owner, entityID, eventID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return fields
}

func requireReceiverConstructionStorage(t testing.TB, reader receiverProofStateReader, runID, instance, entityID, entityType string) storetest.ReceiverConstructionStorage {
	t.Helper()
	var physical storetest.ReceiverConstructionStorage
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		physical, err = storetest.ReadReceiverConstructionStorage(ctx, reader, runID, instance, entityID, entityType)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return physical
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
	record, instance, err := readReceiverTargetState(context.Background(), reader, runID, flow, path, entityID)
	if err != nil {
		t.Fatal(err)
	}
	return record, instance
}

func readReceiverTargetState(ctx context.Context, reader receiverProofStateReader, runID, flow, path, entityID string) (pipeline.WorkflowTargetPersistenceRecord, pipeline.WorkflowInstance, error) {
	var record pipeline.WorkflowTargetPersistenceRecord
	var instance pipeline.WorkflowInstance
	err := reader.InspectSnapshot(ctx, func(ctx context.Context) error {
		var err error
		record, instance, err = loadReceiverTargetState(ctx, reader, runID, flow, path, entityID)
		return err
	})
	if err != nil {
		return pipeline.WorkflowTargetPersistenceRecord{}, pipeline.WorkflowInstance{}, err
	}
	return record, instance, nil
}

func loadReceiverTargetState(ctx context.Context, reader receiverProofStateReader, runID, flow, path, entityID string) (pipeline.WorkflowTargetPersistenceRecord, pipeline.WorkflowInstance, error) {
	owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(flow, flowidentity.LogicalInstanceID(path), path))
	if err != nil {
		return pipeline.WorkflowTargetPersistenceRecord{}, pipeline.WorkflowInstance{}, err
	}
	record, err := reader.LoadWorkflowTargetPersistence(ctx, owner, identity.NormalizeEntityID(entityID))
	if err != nil {
		return pipeline.WorkflowTargetPersistenceRecord{}, pipeline.WorkflowInstance{}, err
	}
	instance, err := record.DecodeComplete(owner.Route, identity.NormalizeEntityID(entityID))
	if err != nil {
		return pipeline.WorkflowTargetPersistenceRecord{}, pipeline.WorkflowInstance{}, err
	}
	if instance.WorkflowName != flow || instance.StorageRef != path || instance.EntityID != entityID {
		return pipeline.WorkflowTargetPersistenceRecord{}, pipeline.WorkflowInstance{}, fmt.Errorf("receiver readback lost exact template/route/entity: %+v", instance)
	}
	return record, instance, nil
}

func requireSingleReceiverTargetState(t testing.TB, reader receiverProofStateReader, runID, flow string) (pipeline.WorkflowTargetPersistenceRecord, pipeline.WorkflowInstance) {
	t.Helper()
	if reader == nil {
		t.Fatal("receiver proof requires an owned workflow reader")
	}
	var record pipeline.WorkflowTargetPersistenceRecord
	var receiver pipeline.WorkflowInstance
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		instances, err := reader.ListWorkflowInstances(ctx, runID)
		if err != nil {
			return err
		}
		count := 0
		for _, instance := range instances {
			if instance.WorkflowName == flow {
				receiver = instance
				count++
			}
		}
		if count != 1 {
			return fmt.Errorf("receiver companions=%d, want exactly one %s", count, flow)
		}
		record, receiver, err = loadReceiverTargetState(ctx, reader, runID, flow, receiver.StorageRef, receiver.EntityID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return record, receiver
}
