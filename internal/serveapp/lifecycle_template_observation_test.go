package serveapp

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type lifecycleTemplateObservation struct {
	Endpoint, BundleHash string
	ReceiverStateReader  receiverProofStateReader
}

// Keep the original persisted values, not their decoded map representations.
func lifecycleTemplateSnapshot(t *testing.T, reader receiverProofStateReader, runID string) string {
	t.Helper()
	type persistedLifecycle struct {
		EntityID, CurrentState      string
		Revision                    int64
		HasFields                   bool
		Fields, Config, Accumulator string
	}
	var records []persistedLifecycle
	err := reader.InspectSnapshot(t.Context(), func(ctx context.Context) error {
		instances, err := reader.ListWorkflowInstances(ctx, runID)
		if err != nil {
			return err
		}
		sort.Slice(instances, func(i, j int) bool { return instances[i].EntityID < instances[j].EntityID })
		for _, instance := range instances {
			owner, err := flowidentity.NewRunScopedFlowInstance(runID, flowidentity.StoredRoute(instance.WorkflowName, flowidentity.LogicalInstanceID(instance.StorageRef), instance.StorageRef))
			if err != nil {
				return err
			}
			record, err := reader.LoadWorkflowTargetPersistence(ctx, owner, identity.NormalizeEntityID(instance.EntityID))
			if err != nil {
				return err
			}
			if err := record.Validate(owner.Route, identity.NormalizeEntityID(instance.EntityID)); err != nil {
				return err
			}
			header := record.Lifecycle.State
			records = append(records, persistedLifecycle{
				EntityID: header.EntityID, CurrentState: header.CurrentState, Revision: header.Revision,
				HasFields: record.Presence.HasState(), Fields: string(record.State.Fields),
				Config: string(record.Lifecycle.Config), Accumulator: string(header.Accumulator),
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) == 0 {
		t.Fatal("no persisted lifecycle proof rows")
	}
	var encoded string
	for _, row := range records {
		encoded += fmt.Sprintf("%s/%s/%d/%t:%s/%s/%s\n", row.EntityID, row.CurrentState, row.Revision, row.HasFields, row.Fields, row.Config, row.Accumulator)
	}
	return encoded
}

func requireLifecycleTemplateEventCount(t *testing.T, reader receiverProofStateReader, runID, event string, want int) {
	t.Helper()
	stored, err := storetest.ReadSelectedForkSourceDomain(t.Context(), reader, runID)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := storetest.DecodeSelectedForkSnapshotRows(stored["events"])
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		name, err := row["event_name"].Text()
		if err != nil {
			t.Fatal(err)
		}
		if name == event {
			count++
		}
	}
	if count != want {
		t.Fatalf("%s count=%d, want %d", event, count, want)
	}
}
