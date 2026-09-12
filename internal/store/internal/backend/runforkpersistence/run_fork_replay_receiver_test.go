package runforkpersistence

import (
	"crypto/sha256"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestReplayReceiverRequiresFixedPublicationAndInitializedState(t *testing.T) {
	for _, supplier := range []string{"node", "flow"} {
		for _, hostile := range []string{"valid", "unfinished", "failed", "terminal", "missing_state", "wrong_owner", "foreign_delivery", "erased_supplier", "missing_materializer", "changed_live_route", "duplicate_source", "absent_source"} {
			if supplier == "flow" && (hostile == "unfinished" || hostile == "failed" || hostile == "terminal" || hostile == "missing_materializer") {
				continue
			}
			t.Run(supplier+"/"+hostile, func(t *testing.T) {
				snapshot, event, source := replayReceiverProjectionFixture(t, supplier)
				childRun := eventtest.UUID("replay-child")
				switch hostile {
				case "unfinished":
					snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusPending
				case "failed":
					snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusFailed
				case "terminal":
					snapshot.Deliveries[0].Snapshot.Status = deliverylifecycle.StatusDeadLetter
				case "duplicate_source":
					snapshot.Deliveries = append(snapshot.Deliveries, snapshot.Deliveries[len(snapshot.Deliveries)-1])
				case "absent_source":
					snapshot.Deliveries = snapshot.Deliveries[:len(snapshot.Deliveries)-1]
				case "missing_state":
					snapshot.EntityMetadata = nil
				case "wrong_owner":
					snapshot.EntityMetadata[0].FlowInstance = "foreign/receiver"
				case "foreign_delivery":
					snapshot.Deliveries[0].Snapshot.RunID = childRun
				case "erased_supplier":
					for i := range snapshot.Deliveries {
						snapshot.Deliveries[i].Snapshot.Route.Initialization = events.ReceiverInitialization{}
						snapshot.Deliveries[i].Snapshot.Route.Materialization = events.ReceiverMaterializationPlan{}
					}
					source = snapshot.Deliveries[len(snapshot.Deliveries)-1].Snapshot
				case "missing_materializer":
					snapshot.Deliveries = snapshot.Deliveries[1:]
				case "changed_live_route":
					source.Route.ConnectClaim = events.ConnectExecutionClaim{}
				}
				before := source.Route
				child, err := projectRunForkReplayInitializedReceiver(snapshot, event, source, childRun)
				if hostile != "valid" {
					if err == nil {
						t.Fatalf("accepted %s source evidence: %+v", hostile, child)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !child.Target.ExistingEntity() || !child.Initialization.Empty() || !child.Materialization.Empty() || child.AgentIdentity.RunID != childRun {
					t.Fatalf("replay retained source permission: %+v", child)
				}
				if !reflect.DeepEqual(before, source.Route) || child.Target.Route() != source.Route.Target.Route() || child.ConnectClaim != source.Route.ConnectClaim {
					t.Fatal("receiver projection changed frozen source or exact routing evidence")
				}
				if _, err := child.Identity(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func replayReceiverProjectionFixture(t *testing.T, supplier string) (*runForkRevisionSnapshot, events.Event, deliverylifecycle.Snapshot) {
	t.Helper()
	runID := eventtest.UUID("replay-source")
	event := eventtest.ExistingRunRootIngress(eventtest.UUID("replay-publication"), "work.ready", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	target := events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "receiver", FlowInstance: "receiver", EntityID: eventtest.UUID("replay-receiver")})
	node := identitytest.FlowNode(t, "receiver", "initialize")
	initializer := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: target}
	var err error
	if supplier == "node" {
		initializer.Initialization, err = events.AdmitNodeReceiverInitialization(event, target, node)
	} else {
		initializer.Initialization, err = events.AdmitFlowReceiverInitialization(event, target)
	}
	if err != nil {
		t.Fatal(err)
	}
	pin := sha256.Sum256([]byte("receiver-pin"))
	initializer.ConnectClaim, err = events.AdmitConnectExecutionClaim(sha256.Sum256([]byte("node-edge")), pin, initializer.Recipient, node, "work.ready")
	if err != nil {
		t.Fatal(err)
	}
	name, err := agentidentity.DeclaredName("worker", "receiver/agents.yaml")
	if err != nil {
		t.Fatal(err)
	}
	route, err := agentidentity.PresentRoute("receiver", "receiver", "receiver")
	if err != nil {
		t.Fatal(err)
	}
	agent, err := agentidentity.New(runID, name, route)
	if err != nil {
		t.Fatal(err)
	}
	dependent := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: agent, Target: target, Initialization: initializer.Initialization}
	dependent.ConnectClaim, err = events.AdmitConnectExecutionClaim(sha256.Sum256([]byte("agent-edge")), pin, dependent.Recipient, identity.ExecutableNode{}, "work.ready")
	if err != nil {
		t.Fatal(err)
	}
	publication := []events.DeliveryRoute{dependent}
	if supplier == "node" {
		publication = []events.DeliveryRoute{initializer, dependent}
		plan, err := events.AdmitReceiverMaterializationPlan(event, initializer, []events.DeliveryRoute{dependent}, publication)
		if err != nil {
			t.Fatal(err)
		}
		dependent, err = plan.BindDependent(dependent)
		if err != nil {
			t.Fatal(err)
		}
		publication[1] = dependent
	}
	if err := events.ValidateReceiverMaterializations(event, publication); err != nil {
		t.Fatal(err)
	}
	snapshot := &runForkRevisionSnapshot{RunID: runID, Revision: 1, EntityMetadata: []runForkRevisionEntityMetadata{{EntityID: target.Route().EntityID, FlowInstance: "receiver", EntityType: "receipt"}}}
	for _, route := range publication {
		id, err := deliverylifecycle.DeliveryID(event.ID(), route)
		if err != nil {
			t.Fatal(err)
		}
		state := deliverylifecycle.StatusPending
		if route.Recipient.IsNode() {
			state = deliverylifecycle.StatusDelivered
		}
		snapshot.Deliveries = append(snapshot.Deliveries, runForkRevisionDelivery{Snapshot: deliverylifecycle.Snapshot{DeliveryID: id, RunID: runID, EventID: event.ID(), Route: route, Status: state}})
	}
	return snapshot, event, snapshot.Deliveries[len(snapshot.Deliveries)-1].Snapshot
}

func TestReplayExistingRootReceiverProjectsCanonicalChildOwnership(t *testing.T) {
	snapshot, event, delivery := replayReceiverProjectionFixture(t, "flow")
	name, err := agentidentity.DeclaredName("worker", "agents.yaml")
	if err != nil {
		t.Fatal(err)
	}
	delivery.Route.AgentIdentity, err = agentidentity.New(snapshot.RunID, name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	delivery.Route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowInstance: snapshot.RunID, EntityID: snapshot.RunID})
	delivery.Route.Initialization = events.ReceiverInitialization{}
	delivery.Route.Materialization = events.ReceiverMaterializationPlan{}
	delivery.Route.ConnectClaim = events.ConnectExecutionClaim{}
	delivery.DeliveryID, err = deliverylifecycle.DeliveryID(event.ID(), delivery.Route)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Deliveries = []runForkRevisionDelivery{{Snapshot: delivery}}
	snapshot.EntityMetadata = []runForkRevisionEntityMetadata{{EntityID: snapshot.RunID, FlowInstance: snapshot.RunID, EntityType: "receipt"}}
	childRun := eventtest.UUID("replay-child")
	child, err := projectRunForkReplayInitializedReceiver(snapshot, event, delivery, childRun)
	if err != nil {
		t.Fatal(err)
	}
	if got := child.Target.Route(); got.EntityID != childRun || got.FlowInstance != childRun || !child.Target.ExistingEntity() {
		t.Fatalf("replay retained source root ownership: %+v", child)
	}
}

func TestReplayUntargetedReceiverPreservesAbsence(t *testing.T) {
	snapshot, event, delivery := replayReceiverProjectionFixture(t, "flow")
	delivery.Route.Target = events.DeliveryTargetOwnership{}
	delivery.Route.Initialization = events.ReceiverInitialization{}
	delivery.Route.Materialization = events.ReceiverMaterializationPlan{}
	var err error
	delivery.DeliveryID, err = deliverylifecycle.DeliveryID(event.ID(), delivery.Route)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Deliveries = []runForkRevisionDelivery{{Snapshot: delivery}}
	snapshot.EntityMetadata = nil
	childRun := eventtest.UUID("replay-child")
	before := delivery.Route
	child, err := projectRunForkReplayInitializedReceiver(snapshot, event, delivery, childRun)
	if err != nil {
		t.Fatal(err)
	}
	want := before
	want.AgentIdentity.RunID = childRun
	if !reflect.DeepEqual(want, child) || !reflect.DeepEqual(before, delivery.Route) {
		t.Fatalf("targetless replay invented receiver evidence: %+v", child)
	}
}
