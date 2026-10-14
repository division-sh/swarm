package events

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/google/uuid"
)

func receiverMaterializationFixture(t testing.TB) (Event, DeliveryRoute, []DeliveryRoute) {
	t.Helper()
	event, err := NewExistingRunRootIngressEvent(ExistingRunRootIngressEventInput{Facts: validFacts(), RunID: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewMaterializingEntityTarget(RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	node := identitytest.FlowNode(t, "consumer", "observer")
	observer := DeliveryRoute{Recipient: MustNodeDeliveryRecipient(node), Target: target}
	receiver, err := agentidentity.PresentRoute("consumer", "consumer", "consumer")
	if err != nil {
		t.Fatal(err)
	}
	observer.Initialization, err = AdmitFlowReceiverInitialization(event, target, receiver)
	if err != nil {
		t.Fatal(err)
	}
	pin := sha256.Sum256([]byte("exact-compiled-receiver-pin"))
	observer.ConnectClaim, err = AdmitConnectExecutionClaim(sha256.Sum256([]byte("node-edge")), pin, observer.Recipient, node, "item.received")
	if err != nil {
		t.Fatal(err)
	}
	var agents []DeliveryRoute
	for _, label := range []string{"observer", "renamed-observer"} {
		name, err := agentidentity.DeclaredName(label, "consumer/agents.yaml")
		if err != nil {
			t.Fatal(err)
		}
		route, err := agentidentity.PresentRoute("consumer", "consumer", "consumer")
		if err != nil {
			t.Fatal(err)
		}
		actor, err := agentidentity.New(event.RunID(), name, route)
		if err != nil {
			t.Fatal(err)
		}
		agent := DeliveryRoute{Recipient: MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor, Target: target, Initialization: observer.Initialization}
		agent.ConnectClaim, err = AdmitConnectExecutionClaim(sha256.Sum256([]byte(label)), pin, agent.Recipient, identity.ExecutableNode{}, "item.received")
		if err != nil {
			t.Fatal(err)
		}
		agents = append(agents, agent)
	}
	return event, observer, agents
}

func TestReceiverInitializationObserversDoNotBecomeMaterializers(t *testing.T) {
	event, observer, agents := receiverMaterializationFixture(t)
	// Removing, reordering or adding ordinary node deliveries cannot create a
	// dependency on their execution. Construction has its own exact receipt.
	for _, nodes := range [][]DeliveryRoute{nil, {observer}, {observer, observer}} {
		publication := append(append([]DeliveryRoute(nil), nodes...), agents...)
		for _, reverse := range []bool{false, true} {
			if reverse {
				for i, j := 0, len(publication)-1; i < j; i, j = i+1, j-1 {
					publication[i], publication[j] = publication[j], publication[i]
				}
			}
			if err := ValidateReceiverMaterializations(event, publication); err != nil {
				t.Fatal(err)
			}
			for _, route := range publication {
				full, err := json.Marshal(route)
				if err != nil {
					t.Fatal(err)
				}
				var restored DeliveryRoute
				if err := json.Unmarshal(full, &restored); err != nil || !reflect.DeepEqual(restored, route.Normalized()) {
					t.Fatalf("public route roundtrip: %s %v", full, err)
				}
				raw, err := EncodeReceiverMaterializationRecord(route)
				if err != nil {
					t.Fatal(err)
				}
				bare := route
				bare.Initialization = ReceiverInitialization{}
				restored, err = RestoreReceiverMaterializationRecord(bare, raw)
				if err != nil || !reflect.DeepEqual(restored, route) || !SameDeliveryRouteIdentity(restored, route) {
					t.Fatalf("durable construction receipt roundtrip: %v", err)
				}
			}
		}
	}
}

func TestReceiverInitializationSeparatesStaticActorFromConstruction(t *testing.T) {
	event, _, agents := receiverMaterializationFixture(t)
	agent := agents[0]
	construction := agent.Target.Route()
	actor := construction
	actor.EntityID = ""
	var err error
	agent.Target, err = NewEntitylessReceiverTarget(actor)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateReceiverMaterializations(event, []DeliveryRoute{agent}); err != nil {
		t.Fatalf("entityless actor lost its independent construction dependency: %v", err)
	}
	raw, err := EncodeReceiverMaterializationRecord(agent)
	if err != nil {
		t.Fatal(err)
	}
	bare := agent
	bare.Initialization = ReceiverInitialization{}
	restored, err := RestoreReceiverMaterializationRecord(bare, raw)
	if err != nil || !SameDeliveryRouteIdentity(agent, restored) {
		t.Fatalf("independent construction roundtrip: %v", err)
	}
	for _, crossed := range []string{"instance", "flow", "run", "scope", "logical_instance"} {
		hostile := agent
		switch crossed {
		case "instance":
			actor.FlowInstance = "consumer/other"
			hostile.Target, _ = NewEntitylessReceiverTarget(actor)
		case "flow":
			actor.FlowID = "other"
			hostile.Target, _ = NewEntitylessReceiverTarget(actor)
		case "run":
			hostile.Initialization.runID = uuid.NewString()
		case "scope":
			hostile.AgentIdentity.Route.ScopeKey = "foreign"
		case "logical_instance":
			hostile.AgentIdentity.Route.InstanceID = "foreign"
		}
		if err := ValidateReceiverMaterializations(event, []DeliveryRoute{hostile}); err == nil {
			t.Fatalf("accepted crossed %s construction dependency", crossed)
		}
		if _, err := hostile.Identity(); err == nil {
			t.Fatalf("accepted crossed %s durable identity", crossed)
		}
		if _, err := json.Marshal(hostile); err == nil {
			t.Fatalf("serialized crossed %s receiver", crossed)
		}
		actor = construction
		actor.EntityID = ""
	}
}

func TestReceiverInitializationReusedConstructionKeepsOriginalPublication(t *testing.T) {
	original, _, agents := receiverMaterializationFixture(t)
	agent := agents[0]
	construction := agent.Target.Route()
	var err error
	agent.Initialization.target, err = NewExistingEntityTarget(construction)
	if err != nil {
		t.Fatal(err)
	}
	actor := construction
	actor.EntityID = ""
	agent.Target, err = NewEntitylessReceiverTarget(actor)
	if err != nil {
		t.Fatal(err)
	}
	later := original.Clone()
	later.id = uuid.NewString()
	if err := ValidateReceiverMaterializations(later, []DeliveryRoute{agent}); err != nil {
		t.Fatalf("later publication cannot retain original construction: %v", err)
	}
	if agent.Initialization.CreatingEventID() != original.ID() {
		t.Fatal("later publication replaced original constructor identity")
	}
	foreign := later.Clone()
	foreign.runID = uuid.NewString()
	if err := ValidateReceiverMaterializations(foreign, []DeliveryRoute{agent}); err == nil {
		t.Fatal("accepted original construction from another run")
	}
}

func TestReceiverInitializationPreservesRootAndNestedKeyedCoordinates(t *testing.T) {
	event, _, agents := receiverMaterializationFixture(t)
	for _, shape := range []struct {
		name, flow, path string
		receiver         agentidentity.Route
	}{
		{"root", ".", event.RunID(), agentidentity.RootRoute()},
		{"nested_keyless_under_keyed", "templ/child", "templ/42/child", agentidentity.Route{Presence: agentidentity.RoutePresent, ScopeKey: "templ/child", InstanceID: "child", InstancePath: "templ/42/child"}},
	} {
		for _, stored := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stored=%t", shape.name, stored), func(t *testing.T) {
				identity, err := agentidentity.New(event.RunID(), agents[0].AgentIdentity.Name, shape.receiver)
				if err != nil {
					t.Fatal(err)
				}
				target := RouteIdentity{FlowID: shape.flow, FlowInstance: shape.path, EntityID: uuid.NewString()}
				var dependency ReceiverInitialization
				if stored {
					dependency, err = AdmitStoredFlowReceiverInitialization(event.RunID(), "", MustExistingEntityTarget(target), shape.receiver)
				} else {
					dependency, err = AdmitFlowReceiverInitialization(event, MustMaterializingEntityTarget(target), shape.receiver)
				}
				if err != nil {
					t.Fatal(err)
				}
				target.EntityID = ""
				route := DeliveryRoute{Recipient: MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity, Target: MustEntitylessReceiverTarget(target), Initialization: dependency}
				if err := ValidateReceiverMaterializations(event, []DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				raw, err := json.Marshal(route)
				if err != nil {
					t.Fatal(err)
				}
				var restored DeliveryRoute
				if err := json.Unmarshal(raw, &restored); err != nil || !SameDeliveryRouteIdentity(route, restored) {
					t.Fatalf("captured coordinate roundtrip: %v", err)
				}
			})
		}
	}
}

func TestReceiverInitializationStoredStartupRetainsTypedAbsence(t *testing.T) {
	event, _, agents := receiverMaterializationFixture(t)
	agent := agents[0]
	flow := agent.Target.Route()
	dependency, err := NewExistingEntityTarget(flow)
	if err != nil {
		t.Fatal(err)
	}
	agent.Initialization, err = AdmitStoredFlowReceiverInitialization(event.RunID(), "", dependency, agent.AgentIdentity.Route)
	if err != nil || agent.Initialization.Empty() || agent.Initialization.CreatingEventID() != "" {
		t.Fatalf("startup construction absence was erased or rejected: %v", err)
	}
	flow.EntityID = ""
	agent.Target, err = NewEntitylessReceiverTarget(flow)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateReceiverMaterializations(event, []DeliveryRoute{agent}); err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeReceiverMaterializationRecord(agent)
	if err != nil {
		t.Fatal(err)
	}
	bare := agent
	bare.Initialization = ReceiverInitialization{}
	restored, err := RestoreReceiverMaterializationRecord(bare, raw)
	if err != nil || !SameDeliveryRouteIdentity(restored, agent) {
		t.Fatalf("startup absence roundtrip: %v", err)
	}
}

func TestReceiverInitializationRootWireRequiresEveryCoordinate(t *testing.T) {
	event, _, _ := receiverMaterializationFixture(t)
	target := MustExistingEntityTarget(RouteIdentity{FlowID: ".", FlowInstance: event.RunID(), EntityID: uuid.NewString()})
	dependency, err := AdmitStoredFlowReceiverInitialization(event.RunID(), "", target, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(dependency)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"presence", "scope_key", "instance_id", "instance_path"} {
		for _, variant := range []string{"missing", "wrong_case", "null", "wrong_type"} {
			t.Run(key+"/"+variant, func(t *testing.T) {
				var document map[string]json.RawMessage
				if err := json.Unmarshal(raw, &document); err != nil {
					t.Fatal(err)
				}
				var coordinates map[string]json.RawMessage
				if err := json.Unmarshal(document["receiver_route"], &coordinates); err != nil {
					t.Fatal(err)
				}
				switch variant {
				case "missing":
					delete(coordinates, key)
				case "wrong_case":
					coordinates[strings.ToUpper(key)] = coordinates[key]
					delete(coordinates, key)
				case "null":
					coordinates[key] = json.RawMessage(`null`)
				case "wrong_type":
					coordinates[key] = json.RawMessage(`1`)
				}
				document["receiver_route"], err = json.Marshal(coordinates)
				if err != nil {
					t.Fatal(err)
				}
				hostile, err := json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
				var restored ReceiverInitialization
				if err := json.Unmarshal(hostile, &restored); err == nil {
					t.Fatal("partial or alternate coordinate admitted")
				}
			})
		}
	}
}

func TestReceiverInitializationClosedWireAndIdentity(t *testing.T) {
	event, node, _ := receiverMaterializationFixture(t)
	bare := node
	bare.Initialization = ReceiverInitialization{}
	if SameDeliveryRouteIdentity(node, bare) {
		t.Fatal("construction receipt omitted from identity")
	}
	if err := ValidateDeliveryRoutes([]DeliveryRoute{node, bare}); err == nil {
		t.Fatal("same execution accepted conflicting receipt identities")
	}
	raw, err := json.Marshal(node.Initialization)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		[]byte(`null`), []byte(`[]`),
		bytes.Replace(raw, []byte(`"kind":`), []byte(`"node":null,"kind":`), 1),
		bytes.Replace(raw, []byte(`"kind":`), []byte(`"kind":"flow_lifecycle","kind":`), 1),
		bytes.Replace(raw, []byte(`"flow_lifecycle"`), []byte(`"node_delivery"`), 1),
		bytes.Replace(raw, []byte(`"event_id":`), []byte(`"EVENT_ID":`), 1),
	} {
		before := node.Initialization
		restored := before
		if err := json.Unmarshal(bad, &restored); err == nil || restored != before {
			t.Fatalf("accepted or mutated on invalid receipt: %s", bad)
		}
	}
	foreign := event.Clone()
	foreign.id = uuid.NewString()
	if err := node.Initialization.ValidateEvent(foreign); err == nil {
		t.Fatal("accepted foreign publication")
	}
}

func TestReceiverInitializationRejectsErasureAndForgedSupplier(t *testing.T) {
	for _, variant := range []string{"erase_all", "event", "run", "target", "absent_supplier"} {
		t.Run(variant, func(t *testing.T) {
			event, node, agents := receiverMaterializationFixture(t)
			publication := append([]DeliveryRoute{node}, agents...)
			switch variant {
			case "erase_all":
				for i := range publication {
					publication[i].Initialization = ReceiverInitialization{}
				}
			case "event":
				publication[1].Initialization.eventID = uuid.NewString()
			case "run":
				publication[1].Initialization.runID = uuid.NewString()
			case "target":
				target := node.Target.Route()
				target.EntityID = uuid.NewString()
				publication[1].Initialization.target, _ = NewMaterializingEntityTarget(target)
			case "absent_supplier":
				publication[1].Initialization = ReceiverInitialization{}
			}
			if err := ValidateReceiverMaterializations(event, publication); err == nil {
				t.Fatal("accepted corrupted construction evidence")
			}
		})
	}
}

func TestReceiverInitializationDurableCodecRejectsOldAndPartialRecords(t *testing.T) {
	_, _, agents := receiverMaterializationFixture(t)
	agent := agents[0]
	raw, err := EncodeReceiverMaterializationRecord(agent)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		[]byte(`{}`), []byte(`{"initialization":null}`),
		bytes.Replace(raw, []byte(`"flow_lifecycle"`), []byte(`"node_delivery"`), 1),
		bytes.Replace(raw, []byte(`"initialization":`), []byte(`"dependency":null,"initialization":`), 1),
		bytes.Replace(raw, []byte(`"initialization":`), []byte(`"INITIALIZATION":`), 1),
		append(append([]byte(nil), raw...), []byte(` {}`)...),
	} {
		bare := agent
		bare.Initialization = ReceiverInitialization{}
		got, err := RestoreReceiverMaterializationRecord(bare, bad)
		if err == nil || !reflect.DeepEqual(got, DeliveryRoute{}) {
			t.Fatalf("accepted malformed or retired record: %s", bad)
		}
	}
	if _, err := RestoreReceiverMaterializationRecord(agent, []byte("null")); err == nil {
		t.Fatal("erased construction evidence")
	}
	foreign := agent
	foreign.Initialization.eventID = uuid.NewString()
	foreignRaw, err := EncodeReceiverMaterializationRecord(foreign)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreReceiverMaterializationRecord(agent, foreignRaw); err == nil {
		t.Fatal("replaced construction evidence")
	}
	full, err := json.Marshal(agent)
	if err != nil {
		t.Fatal(err)
	}
	var route DeliveryRoute
	if err := json.Unmarshal(bytes.Replace(full, []byte(`"subscriber_type":`), []byte(`"receiver_materialization_plan":{},"subscriber_type":`), 1), &route); err == nil {
		t.Fatal("accepted retired node dependency in a public route")
	}
}
