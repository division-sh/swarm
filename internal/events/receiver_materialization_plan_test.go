package events

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/google/uuid"
)

func TestReceiverMaterializationPlanDurableRouteRoundTrip(t *testing.T) {
	event, node, agents := receiverMaterializationFixture(t)
	publication := append([]DeliveryRoute{node}, agents...)
	plan, err := AdmitReceiverMaterializationPlan(event, node, agents, publication)
	if err != nil {
		t.Fatal(err)
	}
	for i := range agents {
		baseID, err := agents[i].Identity()
		if err != nil {
			t.Fatal(err)
		}
		bound, err := plan.BindDependent(agents[i])
		if err != nil {
			t.Fatal(err)
		}
		boundID, err := bound.Identity()
		if err != nil || boundID == baseID {
			t.Fatalf("dependency missing from durable identity: %v", err)
		}
		raw, err := json.Marshal(bound)
		if err != nil {
			t.Fatal(err)
		}
		var restored DeliveryRoute
		if err := json.Unmarshal(raw, &restored); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(restored, bound.Normalized()) {
			t.Fatal("route codec lost or changed admitted dependency")
		}
		publication[i+1] = restored
	}
	if err := ValidateReceiverMaterializations(event, publication); err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{1, 2} {
		corrupt := append([]DeliveryRoute(nil), publication...)
		corrupt[index].Materialization = ReceiverMaterializationPlan{}
		if err := ValidateReceiverMaterializations(event, corrupt); err == nil {
			t.Fatal("aggregate accepted erased dependent plan")
		}
	}
	corrupt := append([]DeliveryRoute(nil), publication[1:]...)
	if err := ValidateReceiverMaterializations(event, corrupt); err == nil {
		t.Fatal("aggregate accepted missing materializer")
	}
	otherEvent := event
	otherEvent.id = uuid.NewString()
	if err := ValidateReceiverMaterializations(otherEvent, publication); err == nil {
		t.Fatal("aggregate accepted another publication")
	}
	if _, err := plan.BindDependent(node); err == nil {
		t.Fatal("bound dependency to materializer")
	}
}

func TestReceiverMaterializationPlanStrictDurableCodec(t *testing.T) {
	event, node, agents := receiverMaterializationFixture(t)
	plan, err := AdmitReceiverMaterializationPlan(event, node, agents, append([]DeliveryRoute{node}, agents...))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"unknown", "missing", "null_field", "duplicate_key", "duplicate_escaped_key", "trailing", "empty_agents", "duplicate_agents", "unordered_agents", "foreign_run", "different_target"} {
		t.Run(variant, func(t *testing.T) {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "unknown":
				fields["legacy"] = json.RawMessage(`true`)
			case "missing":
				delete(fields, "routing_source")
			case "null_field":
				fields["routing_source"] = json.RawMessage(`null`)
			case "empty_agents":
				fields["dependent_route_identities"] = json.RawMessage(`[]`)
			case "duplicate_agents", "unordered_agents":
				var ids []string
				if err := json.Unmarshal(fields["dependent_route_identities"], &ids); err != nil {
					t.Fatal(err)
				}
				if variant == "duplicate_agents" {
					ids[1] = ids[0]
				} else {
					ids[0], ids[1] = ids[1], ids[0]
				}
				fields["dependent_route_identities"], _ = json.Marshal(ids)
			case "foreign_run":
				fields["run_id"], _ = json.Marshal(uuid.NewString())
			case "different_target":
				target := node.Target.Route()
				target.EntityID = uuid.NewString()
				owner, err := NewMaterializingEntityTarget(target)
				if err != nil {
					t.Fatal(err)
				}
				fields["target"], _ = json.Marshal(owner)
			}
			bad, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "duplicate_key":
				bad = append(bytes.TrimSuffix(bad, []byte("}")), append([]byte(`,"run_id":`), append(fields["run_id"], '}')...)...)
			case "duplicate_escaped_key":
				bad = append(bytes.TrimSuffix(bad, []byte("}")), append([]byte(`,"run_\u0069d":`), append(fields["run_id"], '}')...)...)
			case "trailing":
				bad = append(bad, []byte(` {}`)...)
			}
			if _, err := RestoreDeliveryMaterialization(agents[0], bad); err == nil {
				t.Fatalf("accepted %s", variant)
			}
		})
	}
}

func receiverMaterializationFixture(t *testing.T) (Event, DeliveryRoute, []DeliveryRoute) {
	t.Helper()
	event, err := NewExistingRunRootIngressEvent(ExistingRunRootIngressEventInput{Facts: validFacts(), RunID: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	target, err := NewMaterializingEntityTarget(RouteIdentity{FlowID: "consumer", FlowInstance: "consumer", EntityID: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	node := identitytest.FlowNode(t, "consumer", "materializer")
	materializer := DeliveryRoute{Recipient: MustNodeDeliveryRecipient(node), Target: target}
	pin := sha256.Sum256([]byte("exact-compiled-receiver-pin"))
	materializer.ConnectClaim, err = AdmitConnectExecutionClaim(sha256.Sum256([]byte("node-edge-generation")), pin, materializer.Recipient, node, "item.received")
	if err != nil {
		t.Fatal(err)
	}
	var agents []DeliveryRoute
	for _, label := range []string{"materializer", "renamed-observer"} {
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
		agent := DeliveryRoute{Recipient: MustAgentDeliveryRecipient(actor.AgentID()), AgentIdentity: actor, Target: target}
		agent.ConnectClaim, err = AdmitConnectExecutionClaim(sha256.Sum256([]byte(label)), pin, agent.Recipient, identity.ExecutableNode{}, "item.received")
		if err != nil {
			t.Fatal(err)
		}
		agents = append(agents, agent)
	}
	return event, materializer, agents
}

func TestReceiverMaterializationPlanExactBindingAndOrder(t *testing.T) {
	event, node, agents := receiverMaterializationFixture(t)
	publication := append([]DeliveryRoute{node}, agents...)
	plan, err := AdmitReceiverMaterializationPlan(event, node, agents, publication)
	if err != nil {
		t.Fatal(err)
	}
	for _, ordered := range [][]DeliveryRoute{
		{node, agents[0], agents[1]}, {agents[1], node, agents[0]}, {agents[0], agents[1], node},
		{agents[0], node, agents[0], agents[1]},
	} {
		if err := plan.ValidatePublication(event, ordered); err != nil {
			t.Fatalf("recipient order changed exact binding: %v", err)
		}
		other, err := AdmitReceiverMaterializationPlan(event, node, []DeliveryRoute{agents[1], agents[0]}, ordered)
		if err != nil || !plan.Equal(other) {
			t.Fatalf("dependent order changed plan: %v", err)
		}
	}
	if plan.RunID() != event.RunID() || plan.EventID() != event.ID() || !SameDeliveryTargetOwnership(plan.Target(), node.Target) {
		t.Fatal("publication or target binding lost")
	}
	nodeID, _ := node.Identity()
	if plan.Materializer() != nodeID || len(plan.Dependents()) != 2 {
		t.Fatal("exact dependent or materializer identity lost")
	}
	if !plan.Target().MaterializingEntity() || plan.Target().ExistingEntity() {
		t.Fatal("future dependency became existing ownership")
	}
	copyIDs := plan.Dependents()
	copyIDs[0] = DeliveryRouteIdentity{}
	if !plan.Dependents()[0].Valid() {
		t.Fatal("caller mutated admitted dependency")
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var forged ReceiverMaterializationPlan
	if err := json.Unmarshal(encoded, &forged); err != nil {
		t.Fatal(err)
	}
	if err := forged.ValidatePublication(event, publication); err == nil {
		t.Fatal("read projection manufactured admission")
	}
}

func TestReceiverMaterializationPlanRejectsHostileAdmission(t *testing.T) {
	for _, variant := range []string{
		"missing_run", "missing_event", "malformed_event", "zero_run", "agent_materializer", "existing_materializer",
		"entityless_materializer", "missing_node_claim", "absent_node", "absent_agent", "missing_agents", "duplicate_agent",
		"foreign_agent_run", "foreign_agent_instance", "other_entity", "existing_agent_target", "missing_agent_claim",
		"other_receiver_pin", "other_receiver_event", "contradictory_recipient", "ambiguous_node", "same_node_different_projection",
		"zero_node_edge", "zero_receiver_pin", "zero_agent_edge",
		"omitted_agent",
	} {
		t.Run(variant, func(t *testing.T) {
			event, node, agents := receiverMaterializationFixture(t)
			publication := []DeliveryRoute{node, agents[0], agents[1]}
			switch variant {
			case "missing_run":
				event.runID = ""
			case "missing_event":
				event.id = ""
			case "malformed_event":
				event.id = "not-an-event-id"
			case "zero_run":
				event.runID = uuid.Nil.String()
			case "agent_materializer":
				node = agents[0]
			case "existing_materializer":
				node.Target, _ = NewExistingEntityTarget(node.Target.Route())
			case "entityless_materializer":
				target := node.Target.Route()
				target.EntityID = ""
				node.Target, _ = NewEntitylessReceiverTarget(target)
			case "missing_node_claim":
				node.ConnectClaim = ConnectExecutionClaim{}
			case "zero_node_edge":
				node.ConnectClaim.digest = [sha256.Size]byte{}
			case "zero_receiver_pin":
				node.ConnectClaim.receiverPinDigest = [sha256.Size]byte{}
				agents[0].ConnectClaim.receiverPinDigest = [sha256.Size]byte{}
				agents[1].ConnectClaim.receiverPinDigest = [sha256.Size]byte{}
			case "zero_agent_edge":
				agents[0].ConnectClaim.digest = [sha256.Size]byte{}
			case "absent_node":
				publication = publication[1:]
			case "absent_agent":
				publication = publication[:2]
			case "missing_agents":
				agents = nil
			case "omitted_agent":
				agents = agents[:1]
			case "duplicate_agent":
				agents = append(agents, agents[0])
			case "foreign_agent_run":
				agents[0].AgentIdentity.RunID = uuid.NewString()
			case "foreign_agent_instance":
				agents[0].AgentIdentity.Route, _ = agentidentity.PresentRoute("consumer", "consumer/other", "consumer/other")
			case "other_entity":
				target := agents[0].Target.Route()
				target.EntityID = uuid.NewString()
				agents[0].Target, _ = NewMaterializingEntityTarget(target)
			case "existing_agent_target":
				agents[0].Target, _ = NewExistingEntityTarget(agents[0].Target.Route())
			case "missing_agent_claim":
				agents[0].ConnectClaim = ConnectExecutionClaim{}
			case "other_receiver_pin":
				agents[0].ConnectClaim.receiverPinDigest = sha256.Sum256([]byte("unrelated-receiver"))
			case "other_receiver_event":
				agents[0].ConnectClaim.handlerEvent = "other.received"
			case "contradictory_recipient":
				agents[0].ConnectClaim.recipientID = "foreign-agent"
			case "ambiguous_node":
				other := node
				nodeID := identitytest.FlowNode(t, "consumer", "second-materializer")
				other.Recipient = MustNodeDeliveryRecipient(nodeID)
				other.ConnectClaim.handlerNode = nodeID
				other.ConnectClaim.recipientID = nodeID.Key()
				publication = append(publication, other)
			case "same_node_different_projection":
				other := node
				other.PayloadProjection, _ = NewDeliveryPayloadProjection(map[string]string{"key": "event.payload.other"})
				publication = append(publication, other)
			}
			// Hostile facts are in the publication too; rejection must not rely
			// merely on the caller and persisted route list disagreeing.
			if len(publication) == 3 && variant != "absent_node" && variant != "absent_agent" {
				publication[0] = node
				if len(agents) >= 2 {
					publication[1], publication[2] = agents[0], agents[1]
				}
			}
			before := append([]DeliveryRoute(nil), publication...)
			if _, err := AdmitReceiverMaterializationPlan(event, node, agents, publication); err == nil {
				t.Fatal("hostile receiver dependency admitted")
			}
			if !reflect.DeepEqual(before, publication) {
				t.Fatal("rejected admission mutated caller routes")
			}
		})
	}
}

func TestReceiverMaterializationPlanPreservesDistinctInstances(t *testing.T) {
	event, node, agents := receiverMaterializationFixture(t)
	publication := append([]DeliveryRoute{node}, agents...)
	other := node
	route := node.Target.Route()
	route.FlowInstance = "consumer/other"
	route.EntityID = uuid.NewString()
	other.Target, _ = NewMaterializingEntityTarget(route)
	publication = append(publication, other)
	plan, err := AdmitReceiverMaterializationPlan(event, node, agents, publication)
	if err != nil {
		t.Fatalf("another concrete receiver became a competing materializer: %v", err)
	}
	if err := plan.ValidatePublication(event, publication); err != nil {
		t.Fatal(err)
	}
	otherID, _ := other.Identity()
	if plan.Materializer() == otherID {
		t.Fatal("dependency redirected to another instance")
	}
}

func TestReceiverMaterializationPlanRejectsChangedPublication(t *testing.T) {
	for _, variant := range []string{"run", "event", "source", "node_edge_generation", "agent_edge_generation", "agent_owner", "payload_projection", "missing_node", "missing_agent", "empty_plan"} {
		t.Run(variant, func(t *testing.T) {
			event, node, agents := receiverMaterializationFixture(t)
			publication := append([]DeliveryRoute{node}, agents...)
			plan, err := AdmitReceiverMaterializationPlan(event, node, agents, publication)
			if err != nil {
				t.Fatal(err)
			}
			switch variant {
			case "run":
				event.runID = uuid.NewString()
			case "event":
				event.id = uuid.NewString()
			case "source":
				event.routingSource, _ = NewRootRoutingSource(uuid.NewString())
			case "node_edge_generation":
				publication[0].ConnectClaim.digest = sha256.Sum256([]byte("other-node-generation"))
			case "agent_edge_generation":
				publication[1].ConnectClaim.digest = sha256.Sum256([]byte("other-agent-generation"))
			case "agent_owner":
				publication[1].AgentIdentity.Name, _ = agentidentity.DeclaredName("materializer", "other/agents.yaml")
			case "payload_projection":
				publication[1].PayloadProjection, _ = NewDeliveryPayloadProjection(map[string]string{"key": "event.payload.other"})
			case "missing_node":
				publication = publication[1:]
			case "missing_agent":
				publication = publication[:2]
			case "empty_plan":
				plan = ReceiverMaterializationPlan{}
			}
			if err := plan.ValidatePublication(event, publication); err == nil {
				t.Fatal("changed publication retained old receiver authority")
			}
		})
	}
}
