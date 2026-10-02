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
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestReplayReceiverRequiresFixedPublicationAndInitializedState(t *testing.T) {
	for _, supplier := range []string{"observer", "agent_only"} {
		for _, hostile := range []string{"valid", "unfinished", "failed", "terminal", "missing_state", "imported_state", "missing_history", "contradictory_history", "wrong_owner", "foreign_delivery", "erased_supplier", "missing_materializer", "changed_live_route", "duplicate_source", "absent_source"} {
			if supplier == "agent_only" && (hostile == "unfinished" || hostile == "failed" || hostile == "terminal" || hostile == "missing_materializer") {
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
				case "imported_state":
					snapshot.EntityMetadata[0].ConstructionKind = "imported_state"
				case "missing_history":
					snapshot.EntityMutations = nil
				case "contradictory_history":
					snapshot.EntityMutations[0].NewValue = []byte(`"foreign"`)
				case "wrong_owner":
					snapshot.EntityMetadata[0].FlowInstance = "foreign/receiver"
				case "foreign_delivery":
					snapshot.Deliveries[0].Snapshot.RunID = childRun
				case "erased_supplier":
					for i := range snapshot.Deliveries {
						snapshot.Deliveries[i].Snapshot.Route.Initialization = events.ReceiverInitialization{}
					}
					source = snapshot.Deliveries[len(snapshot.Deliveries)-1].Snapshot
				case "missing_materializer":
					snapshot.Deliveries = snapshot.Deliveries[1:]
				case "changed_live_route":
					source.Route.ConnectClaim = events.ConnectExecutionClaim{}
				}
				before := source.Route
				child, err := projectRunForkReplayInitializedReceiver(snapshot, event, source, childRun)
				if hostile != "valid" && hostile != "unfinished" && hostile != "failed" && hostile != "terminal" && hostile != "missing_materializer" {
					if err == nil {
						t.Fatalf("accepted %s source evidence: %+v", hostile, child)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if !child.Target.ExistingEntity() || !child.Initialization.Empty() || child.AgentIdentity.RunID != childRun {
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
	initializer.Initialization, err = events.AdmitFlowReceiverInitialization(event, target)
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
	if supplier == "observer" {
		publication = []events.DeliveryRoute{initializer, dependent}
	}
	if err := events.ValidateReceiverMaterializations(event, publication); err != nil {
		t.Fatal(err)
	}
	snapshot := &runForkRevisionSnapshot{RunID: runID, Revision: 1}
	setReplayReceiverConstructedHistory(t, snapshot, target.Route().EntityID, "receiver", "receiver", event.CreatedAt())
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
	delivery.Route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: snapshot.RunID, EntityID: snapshot.RunID})
	delivery.Route.Initialization = events.ReceiverInitialization{}
	delivery.Route.ConnectClaim = events.ConnectExecutionClaim{}
	delivery.DeliveryID, err = deliverylifecycle.DeliveryID(event.ID(), delivery.Route)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Deliveries = []runForkRevisionDelivery{{Snapshot: delivery}}
	setReplayReceiverConstructedHistory(t, snapshot, snapshot.RunID, snapshot.RunID, ".", event.CreatedAt())
	childRun := eventtest.UUID("replay-child")
	child, err := projectRunForkReplayInitializedReceiver(snapshot, event, delivery, childRun)
	if err != nil {
		t.Fatal(err)
	}
	if got := child.Target.Route(); got.EntityID != childRun || got.FlowInstance != childRun || !child.Target.ExistingEntity() {
		t.Fatalf("replay retained source root ownership: %+v", child)
	}
}

// Fixed-snapshot component data, not selected-store construction qualification.
func setReplayReceiverConstructedHistory(t *testing.T, snapshot *runForkRevisionSnapshot, entityID, path, flowID string, at time.Time) {
	t.Helper()
	snapshot.EntityMetadata = []runForkRevisionEntityMetadata{{
		EntityID: entityID, FlowInstance: path, EntityType: "receipt", ConstructionKind: "constructed",
		FlowTemplate: flowID, Mode: "static", FlowConfig: []byte(`{}`), Status: "active", CurrentState: "pending",
		StageDefined: true, CreatedAt: at, UpdatedAt: at, EnteredStateAt: at,
	}}
	snapshot.EntityMutations = []runForkRevisionEntityMutation{{
		EntityID: entityID, Domain: "lifecycle_state", NewValue: []byte(`"pending"`), CreatedAt: at,
	}}
}

func TestRunForkConstructedHistoryRequiresExactSnapshotState(t *testing.T) {
	for _, consumer := range []string{"decoder", "producer"} {
		for _, hostile := range []string{"valid", "imported_state", "missing_history", "contradictory_history", "missing_config", "duplicate_header"} {
			t.Run(consumer+"/"+hostile, func(t *testing.T) {
				snapshot, event, _ := replayReceiverProjectionFixture(t, "agent_only")
				setReplayReceiverConstructedHistory(t, snapshot, snapshot.RunID, snapshot.RunID, ".", event.CreatedAt())
				input := runfork.RunForkSelectedContractSourceEvent{
					SourceEventID: event.ID(), EventName: string(event.Type()), Payload: event.Payload(),
					RoutingSource: eventtest.RootRoutingSource(snapshot.RunID),
				}
				snapshot.Events = []runForkRevisionEvent{{EventID: input.SourceEventID, EventName: input.EventName, Payload: input.Payload, RoutingSource: input.RoutingSource}}
				switch hostile {
				case "imported_state":
					snapshot.EntityMetadata[0].ConstructionKind = "imported_state"
				case "missing_history":
					snapshot.EntityMutations = nil
				case "contradictory_history":
					snapshot.EntityMutations[0].NewValue = []byte(`"foreign"`)
				case "missing_config":
					snapshot.EntityMetadata[0].FlowConfig = nil
				case "duplicate_header":
					snapshot.EntityMetadata = append(snapshot.EntityMetadata, snapshot.EntityMetadata[0])
				}
				var err error
				if consumer == "decoder" {
					var state runfork.RunForkEntityState
					state, err = loadRunForkConstructedEntityState(snapshot, snapshot.RunID)
					if err == nil && (state.CurrentState != "pending" || state.MaterializationMetadata == nil || state.MaterializationMetadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance) {
						t.Fatalf("constructed history lost exact state: %+v", state)
					}
				} else {
					forkRun := eventtest.UUID("producer-child")
					admission := runForkSourceStateAdmission{snapshot: snapshot, forkRunID: forkRun}
					projected, state, failure := admission.project(input)
					err = failure
					if err == nil && (state == nil || state.Source.EntityID != snapshot.RunID || state.Fork.EntityID != forkRun || projected.RoutingSource.Route().EntityID != forkRun) {
						t.Fatalf("producer lost exact ownership: %+v %+v", projected, state)
					}
					if err == nil && (state.history.EntityID != snapshot.RunID || state.history.CurrentState != "pending" || state.history.MaterializationMetadata == nil || state.history.MaterializationMetadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance) {
						t.Fatalf("producer dropped fixed-revision construction history: %+v", state.history)
					}
				}
				if (err == nil) != (hostile == "valid") {
					t.Fatalf("history %s: err=%v", hostile, err)
				}
			})
		}
	}
}

func TestReplayUntargetedReceiverPreservesAbsence(t *testing.T) {
	snapshot, event, delivery := replayReceiverProjectionFixture(t, "flow")
	delivery.Route.Target = events.DeliveryTargetOwnership{}
	delivery.Route.Initialization = events.ReceiverInitialization{}
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
