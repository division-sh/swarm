package bus

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

type joinConstructedReceiverStore struct {
	EventStore
	record pipeline.WorkflowTargetPersistenceRecord
	owner  flowidentity.RunScopedFlowInstance
	entity identity.EntityID
}

func (s *joinConstructedReceiverStore) LoadWorkflowTargetPersistence(_ context.Context, owner flowidentity.RunScopedFlowInstance, entity identity.EntityID) (pipeline.WorkflowTargetPersistenceRecord, error) {
	s.owner, s.entity = owner, entity
	return s.record, nil
}

func TestJoinReceiverReadConsumesHeaderAndOptionalFields(t *testing.T) {
	fixture, _ := topologyOperationFixture(t)
	constructed := ConstructedFlowInstanceIdentityFixture(fixture.Source, "workers", "one", busInternalTestRunID)
	target := events.RouteIdentity{FlowID: constructed.TemplateID, FlowInstance: constructed.InstancePath, EntityID: constructed.EntityID}
	config, err := pipeline.WorkflowInstanceHeaderPayloadForIdentity(constructed, "v1")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := canonicaljson.Bytes(config)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	header := pipeline.WorkflowEntityStatePersistenceRecord{EntityID: constructed.EntityID, FlowInstance: constructed.InstancePath,
		CurrentState: "ready", Revision: 1, EnteredStageAt: at, CreatedAt: at, UpdatedAt: at,
		Gates: json.RawMessage(`{}`), Bookkeeping: json.RawMessage(`{}`), Accumulator: json.RawMessage(`{}`)}
	complete := pipeline.WorkflowTargetPersistenceRecord{Presence: pipeline.WorkflowTargetPersistenceCompleteFieldless,
		Lifecycle: pipeline.WorkflowLifecycleCompanionPersistenceRecord{FlowInstance: constructed.InstancePath,
			WorkflowName: constructed.TemplateID, WorkflowVersion: "v1", Mode: "template", Status: "active", Config: raw,
			CreatedAt: at, State: header}}
	for _, fields := range []bool{false, true} {
		t.Run(map[bool]string{false: "fieldless", true: "fields"}[fields], func(t *testing.T) {
			record := complete
			if fields {
				record.Presence = pipeline.WorkflowTargetPersistenceComplete
				record.Lifecycle.State.EntityType = "worker"
				record.State = record.Lifecycle.State
				record.State.Fields = json.RawMessage(`{"value":2}`)
			}
			store := &joinConstructedReceiverStore{record: record}
			bus := &EventBus{store: store, semanticSource: fixture.Source}
			got, err := bus.loadJoinAdmissionInstance(context.Background(), busInternalTestRunID, target)
			if err != nil || got == nil || got.ParentFlowID != constructed.ParentRoute.FlowID ||
				got.ParentFlowInstance != constructed.ParentRoute.FlowInstance || got.ParentEntityID != constructed.ParentEntityID ||
				got.InstanceID != constructed.InstanceID || got.StorageRef != constructed.InstancePath ||
				store.owner.Route != constructed.Route() || store.owner.RunID != busInternalTestRunID || store.entity.String() != constructed.EntityID {
				t.Fatalf("complete receiver read = %#v lookup=%#v err=%v", got, store.owner, err)
			}
			if fields && len(got.Fields) != 1 || !fields && len(got.Fields) != 0 {
				t.Fatalf("field companion changed: %#v", got.Fields)
			}
		})
	}
	for _, presence := range []pipeline.WorkflowTargetPersistencePresence{
		pipeline.WorkflowTargetPersistenceAbsent, pipeline.WorkflowTargetPersistenceStateOnly, pipeline.WorkflowTargetPersistenceLifecycleOnly,
	} {
		record := complete
		record.Presence = presence
		store := &joinConstructedReceiverStore{record: record}
		bus := &EventBus{store: store, semanticSource: fixture.Source}
		got, err := bus.loadJoinAdmissionInstance(context.Background(), busInternalTestRunID, target)
		if presence == pipeline.WorkflowTargetPersistenceAbsent {
			if err != nil || got != nil {
				t.Fatalf("absent receiver = %#v err=%v", got, err)
			}
		} else if err == nil {
			t.Fatalf("partial persistence admitted: %v", presence)
		}
	}
	complete.Lifecycle.State.EntityID = "foreign"
	store := &joinConstructedReceiverStore{record: complete}
	bus := &EventBus{store: store, semanticSource: fixture.Source}
	if _, err := bus.loadJoinAdmissionInstance(context.Background(), busInternalTestRunID, target); err == nil {
		t.Fatal("foreign header admitted")
	}
}
