package runforkpersistence

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestFanOutExecutionOwnershipProjectionRoles(t *testing.T) {
	for _, flow := range []string{".", "worker", "parent/worker"} {
		for _, kind := range []string{"existing", "materializing", "entityless"} {
			t.Run(flow+"/"+kind, func(t *testing.T) {
				plan, capsule := fanOutOwnershipFixture(t, flow, kind)
				before, _ := json.Marshal(capsule)
				got, err := projectRunForkFanOutExecutionOwnership(plan, "child-run", capsule)
				if err != nil {
					t.Fatal(err)
				}
				assertFanOutCapsuleFieldPartition(t, capsule, got)
				wantEntity, wantPath := capsule.EntityID, capsule.Route.InstancePath
				if flow == "." {
					wantPath = "child-run"
					if kind != "entityless" {
						wantEntity = "child-run"
					}
				}
				if got.EntityID != wantEntity || got.Route.InstancePath != wantPath || got.Receiver.Target.Route().EntityID != wantEntity || got.Receiver.Target.Route().FlowInstance != wantPath || got.Receiver.Target.Code() != capsule.Receiver.Target.Code() {
					t.Fatalf("wrong child receiver projection: %+v", got)
				}
				if flow == "." {
					if got.ProducerSource.Route().EntityID != "child-run" {
						t.Fatal("root producer retained source owner")
					}
				} else if got.ProducerSource != capsule.ProducerSource {
					t.Fatal("non-root producer rehomed")
				}
				again, err := projectRunForkFanOutExecutionOwnership(plan, "child-run", capsule)
				if err != nil || !again.Equal(got) {
					t.Fatalf("repeat differs: %v", err)
				}
				after, _ := json.Marshal(capsule)
				if string(before) != string(after) {
					t.Fatal("source capsule changed")
				}
				plan.SourceRunID = "child-run"
				if len(plan.Entities) != 0 {
					plan.Entities[0].EntityID = got.EntityID
					plan.Entities[0].MaterializationMetadata = &runfork.RunForkMaterializedEntitySnapshotMetadata{Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, FlowInstance: got.Route.InstancePath}
				}
				grandchild, err := projectRunForkFanOutExecutionOwnership(plan, "grandchild-run", got)
				if err != nil {
					t.Fatalf("fork of fork: %v", err)
				}
				assertFanOutCapsuleFieldPartition(t, got, grandchild)
			})
		}
	}
}

func TestFanOutExecutionOwnershipSeparatesProducerAndReceiver(t *testing.T) {
	plan, capsule := fanOutOwnershipFixture(t, "worker", "existing")
	capsule.ProducerSource = eventtest.RootRoutingSource(plan.SourceRunID)
	got, err := projectRunForkFanOutExecutionOwnership(plan, "child-run", capsule)
	if err != nil || got.EntityID != capsule.EntityID || got.ProducerSource.Route().EntityID != "child-run" || got.Route != capsule.Route {
		t.Fatalf("producer state became receiver authority: %+v %v", got, err)
	}
}

func TestFanOutExecutionOwnershipRejectsContradictions(t *testing.T) {
	for _, name := range []string{"scope", "root_route", "missing_entity", "duplicate_entity", "rehome_entity", "foreign_root_producer", "receiver_entity", "receiver_flow", "receiver_kind", "receiver_node"} {
		t.Run(name, func(t *testing.T) {
			plan, capsule := fanOutOwnershipFixture(t, ".", "existing")
			switch name {
			case "scope":
				capsule.ExecutionFlowID = "foreign"
			case "root_route":
				capsule.Route.InstanceID = "foreign"
			case "missing_entity":
				plan.Entities = nil
			case "duplicate_entity":
				plan.Entities = append(plan.Entities, plan.Entities[0])
			case "rehome_entity":
				plan.Entities[0].MaterializationMetadata.FlowInstance = "foreign"
			case "foreign_root_producer":
				capsule.ProducerSource = eventtest.RootRoutingSource("foreign")
			case "receiver_entity":
				target := capsule.Receiver.Target.Route()
				target.EntityID = "foreign"
				capsule.Receiver.Target = events.MustExistingEntityTarget(target)
			case "receiver_flow":
				target := capsule.Receiver.Target.Route()
				target.FlowID = "foreign"
				capsule.Receiver.Target = events.MustExistingEntityTarget(target)
			case "receiver_kind":
				capsule.Receiver.Target = events.DeliveryTargetOwnership{}
			case "receiver_node":
				node, err := identity.AdmitExecutableNodeDeclaration(".", "other")
				if err != nil {
					t.Fatal(err)
				}
				capsule.Receiver.Node = node
			}
			before, _ := json.Marshal(capsule)
			if _, err := projectRunForkFanOutExecutionOwnership(plan, "child-run", capsule); err == nil {
				t.Fatal("contradictory execution owner accepted")
			}
			after, _ := json.Marshal(capsule)
			if string(before) != string(after) {
				t.Fatal("rejection changed source evidence")
			}
		})
	}
}

func fanOutOwnershipFixture(t *testing.T, flow, kind string) (runfork.RunForkPlan, fanoutobligation.Capsule) {
	t.Helper()
	node, err := identity.AdmitExecutableNodeDeclaration(flow, "scatter")
	if err != nil {
		t.Fatal(err)
	}
	entity, path := "receiver-entity", flow
	producer := eventtest.StaticFlowRoutingSource(flow, path, entity)
	if flow == "." {
		entity, path = "source-run", "source-run"
		producer = eventtest.RootRoutingSource("source-run")
	}
	if kind == "entityless" {
		entity = ""
	}
	targetRoute := events.RouteIdentity{FlowID: flow, FlowInstance: path, EntityID: entity}
	var target events.DeliveryTargetOwnership
	switch kind {
	case "existing":
		target, err = events.NewExistingEntityTarget(targetRoute)
	case "materializing":
		target, err = events.NewMaterializingEntityTarget(targetRoute)
	case "entityless":
		target, err = events.NewEntitylessReceiverTarget(targetRoute)
	}
	if err != nil {
		t.Fatal(err)
	}
	plan := runfork.RunForkPlan{SourceRunID: "source-run"}
	if kind == "existing" {
		plan.Entities = []runfork.RunForkEntityState{{EntityID: entity, MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, FlowInstance: path}}}
	}
	frozen := func() map[string]any {
		return map[string]any{"source_run_id": "source-run", "precise": json.Number("9007199254740993"), "nested": map[string]any{"entity_id": "source-run"}}
	}
	return plan, fanoutobligation.Capsule{
		NodeKey: node.Key(), ExecutionFlowID: flow, EntityID: entity, Route: flowidentity.StoredRoute(flow, path, path), HandlerEventKey: "scatter.requested",
		CurrentState: "working", ChainDepth: 2, ProducerSource: producer,
		Receiver: &fanoutobligation.ExecutionReceiver{Node: node, Target: target},
		Lineage:  events.EventLineage{RunID: "ancestor-run", ParentEventID: "original-trigger", ExecutionMode: executionmode.Live},
		Entity:   frozen(), PlatformEntity: frozen(), Computed: frozen(), Accumulated: frozen(), Join: frozen(), StateFields: frozen(), StateBookkeeping: frozen(), StateGates: map[string]bool{"ready": true},
	}
}

func assertFanOutCapsuleFieldPartition(t *testing.T, source, child fanoutobligation.Capsule) {
	t.Helper()
	immutable := []string{"NodeKey", "ExecutionFlowID", "HandlerEventKey", "CurrentState", "ChainDepth", "Lineage", "Entity", "PlatformEntity", "Computed", "Accumulated", "Join", "StateFields", "StateBookkeeping", "StateGates"}
	executable := []string{"EntityID", "Route", "ProducerSource", "Receiver", "Loop"}
	if len(immutable)+len(executable) != reflect.TypeOf(source).NumField() {
		t.Fatal("capsule field census changed; classify every new field")
	}
	for _, field := range immutable {
		if !reflect.DeepEqual(reflect.ValueOf(source).FieldByName(field).Interface(), reflect.ValueOf(child).FieldByName(field).Interface()) {
			t.Fatalf("immutable capsule field %s changed", field)
		}
	}
	// Ownership projection does not itself select generations. The composed
	// writer/evaluator proof separately checks the declared Loop correspondence.
	if !reflect.DeepEqual(source.Loop, child.Loop) {
		t.Fatal("ownership inferred loop correspondence")
	}
}
