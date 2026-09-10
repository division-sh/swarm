package events

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/google/uuid"
)

func TestReceiverInitializationObserversDoNotBecomeMaterializers(t *testing.T) {
	for _, flowOwned := range []bool{false, true} {
		event, node, agents := receiverMaterializationFixture(t)
		observer := node
		identity := identitytest.FlowNode(t, "consumer", "observer")
		observer.Recipient = MustNodeDeliveryRecipient(identity)
		observer.ConnectClaim.handlerNode = identity
		observer.ConnectClaim.recipientID = identity.Key()
		publication := append([]DeliveryRoute{node, observer}, agents...)
		if flowOwned {
			supplier, err := AdmitFlowReceiverInitialization(event, node.Target)
			if err != nil {
				t.Fatal(err)
			}
			for index := range publication {
				publication[index].Initialization = supplier
			}
		} else {
			plan, err := AdmitReceiverMaterializationPlan(event, node, agents, publication)
			if err != nil {
				t.Fatal(err)
			}
			for index := 2; index < len(publication); index++ {
				publication[index], err = plan.BindDependent(publication[index])
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		for _, reverse := range []bool{false, true} {
			if reverse {
				for i, j := 0, len(publication)-1; i < j; i, j = i+1, j-1 {
					publication[i], publication[j] = publication[j], publication[i]
				}
			}
			if err := ValidateReceiverMaterializations(event, publication); err != nil {
				t.Fatalf("flow=%v reverse=%v: %v", flowOwned, reverse, err)
			}
			for _, route := range publication {
				full, err := json.Marshal(route)
				if err != nil {
					t.Fatal(err)
				}
				var fullRestored DeliveryRoute
				if err := json.Unmarshal(full, &fullRestored); err != nil || !reflect.DeepEqual(fullRestored, route.Normalized()) {
					t.Fatalf("public route wire lost supplier: %s %v", full, err)
				}
				raw, err := EncodeReceiverMaterializationRecord(route)
				if err != nil {
					t.Fatal(err)
				}
				bare := route
				bare.Initialization, bare.Materialization = ReceiverInitialization{}, ReceiverMaterializationPlan{}
				restored, err := RestoreReceiverMaterializationRecord(bare, raw)
				if err != nil || !reflect.DeepEqual(restored, route) {
					t.Fatalf("durable supplier roundtrip: %v", err)
				}
				if !SameDeliveryRouteIdentity(restored, route) {
					t.Fatal("supplier disappeared from route identity")
				}
			}
		}
	}
}

func TestReceiverInitializationClosedWireAndIdentity(t *testing.T) {
	event, node, _ := receiverMaterializationFixture(t)
	lifecycle, err := AdmitFlowReceiverInitialization(event, node.Target)
	if err != nil {
		t.Fatal(err)
	}
	other := node
	other.Initialization = lifecycle
	if SameDeliveryRouteIdentity(node, other) {
		t.Fatal("supplier omitted from identity")
	}
	if err := ValidateDeliveryRoutes([]DeliveryRoute{node, other}); err == nil {
		t.Fatal("same execution accepted conflicting supplier identities")
	}
	raw, err := json.Marshal(lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{
		[]byte(`null`), []byte(`[]`),
		bytes.Replace(raw, []byte(`"kind":`), []byte(`"node":null,"kind":`), 1),
		bytes.Replace(raw, []byte(`"kind":`), []byte(`"kind":"flow_lifecycle","kind":`), 1),
	} {
		var restored ReceiverInitialization
		if err := json.Unmarshal(bad, &restored); err == nil {
			t.Fatalf("accepted noncanonical union: %s", bad)
		}
	}
	record, err := EncodeReceiverMaterializationRecord(other)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RestoreReceiverMaterializationRecord(node, record); err == nil {
		t.Fatal("record replaced admitted supplier")
	}
}

func TestReceiverInitializationRejectsErasureAndForgedSupplier(t *testing.T) {
	for _, variant := range []string{"erase_all", "erase_plan", "fake_lifecycle", "event", "run", "target", "node", "absent_supplier"} {
		t.Run(variant, func(t *testing.T) {
			event, node, agents := receiverMaterializationFixture(t)
			plan, err := AdmitReceiverMaterializationPlan(event, node, agents, append([]DeliveryRoute{node}, agents...))
			if err != nil {
				t.Fatal(err)
			}
			publication := []DeliveryRoute{node}
			for _, agent := range agents {
				agent, err = plan.BindDependent(agent)
				if err != nil {
					t.Fatal(err)
				}
				publication = append(publication, agent)
			}
			switch variant {
			case "erase_all":
				for i := range publication {
					publication[i].Initialization = ReceiverInitialization{}
					publication[i].Materialization = ReceiverMaterializationPlan{}
				}
			case "erase_plan":
				for i := range publication {
					publication[i].Materialization = ReceiverMaterializationPlan{}
				}
			case "fake_lifecycle":
				publication[1].Initialization, _ = AdmitFlowReceiverInitialization(event, node.Target)
				publication[1].Materialization = ReceiverMaterializationPlan{}
			case "event":
				publication[1].Initialization.eventID = uuid.NewString()
			case "run":
				publication[1].Initialization.runID = uuid.NewString()
			case "target":
				target := node.Target.Route()
				target.EntityID = uuid.NewString()
				publication[1].Initialization.target, _ = NewMaterializingEntityTarget(target)
			case "node":
				publication[1].Initialization.node = identitytest.FlowNode(t, "consumer", "fake")
			case "absent_supplier":
				publication[1].Initialization = ReceiverInitialization{}
			}
			if err := ValidateReceiverMaterializations(event, publication); err == nil {
				t.Fatal("accepted corrupted supplier evidence")
			}
		})
	}
}

func TestReceiverInitializationDurableCodecRejectsOldAndPartialRecords(t *testing.T) {
	event, node, agents := receiverMaterializationFixture(t)
	plan, err := AdmitReceiverMaterializationPlan(event, node, agents, append([]DeliveryRoute{node}, agents...))
	if err != nil {
		t.Fatal(err)
	}
	agent, err := plan.BindDependent(agents[0])
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeReceiverMaterializationRecord(agent)
	if err != nil {
		t.Fatal(err)
	}
	old, _ := json.Marshal(plan)
	for _, bad := range [][]byte{old, []byte(`{}`), []byte(`{"initialization":null,"dependency":null}`), bytes.Replace(raw, []byte(`"node_delivery"`), []byte(`"unknown"`), 1), append(append([]byte(nil), raw...), []byte(` {}`)...)} {
		bare := agent
		bare.Materialization, bare.Initialization = ReceiverMaterializationPlan{}, ReceiverInitialization{}
		if _, err := RestoreReceiverMaterializationRecord(bare, bad); err == nil {
			t.Fatalf("accepted malformed supplier record: %s", bad)
		}
	}
}
