package events

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestHistoricalReceiverInitializationKeepsClosedFixedCut(t *testing.T) {
	event, _, agents := receiverMaterializationFixture(t)
	route := agents[0]
	construction := route.Target.Route()
	actor := construction
	actor.EntityID = ""
	route.Target = MustEntitylessReceiverTarget(actor)
	fixed := HistoricalReceiverConstruction{SourceRunID: uuid.NewString(), SourceRevision: 7, SourceReceiver: route.AgentIdentity.Route}
	var err error
	route.Initialization, err = AdmitHistoricalFlowReceiverInitialization(event.RunID(), MustExistingEntityTarget(construction), route.AgentIdentity.Route, fixed)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateReceiverMaterializations(event, []DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeReceiverMaterializationRecord(route)
	if err != nil {
		t.Fatal(err)
	}
	bare := route
	bare.Initialization = ReceiverInitialization{}
	restored, err := RestoreReceiverMaterializationRecord(bare, raw)
	if err != nil || !restored.Initialization.Equal(route.Initialization) || !SameDeliveryRouteIdentity(restored, route) {
		t.Fatalf("fixed-cut dependency changed through durable restoration: %v", err)
	}
	if restored.Initialization.CreatingEventID() != "" {
		t.Fatal("historical construction fabricated a native creating publication")
	}
	admitted, present := restored.Initialization.HistoricalConstruction()
	if !present || admitted != fixed {
		t.Fatal("historical construction became native startup absence")
	}
	admitted.SourceRevision++
	unchanged, _ := restored.Initialization.HistoricalConstruction()
	if unchanged != fixed {
		t.Fatal("returned fixed-cut facts mutate the admitted dependency")
	}
	for _, field := range []string{"source_run_id", "source_revision", "source_receiver"} {
		for _, remove := range []bool{true, false} {
			t.Run(field+map[bool]string{true: "/missing", false: "/null"}[remove], func(t *testing.T) {
				encoded, err := json.Marshal(route.Initialization)
				if err != nil {
					t.Fatal(err)
				}
				var wire map[string]any
				if err := json.Unmarshal(encoded, &wire); err != nil {
					t.Fatal(err)
				}
				history := wire["historical"].(map[string]any)
				if remove {
					delete(history, field)
				} else {
					history[field] = nil
				}
				hostile, err := json.Marshal(wire)
				if err != nil {
					t.Fatal(err)
				}
				var decoded ReceiverInitialization
				if err := json.Unmarshal(hostile, &decoded); err == nil {
					t.Fatal("partial fixed-cut facts became receiver authority")
				}
			})
		}
	}
	for _, alter := range []func(*HistoricalReceiverConstruction){
		func(value *HistoricalReceiverConstruction) { value.SourceRunID = event.RunID() },
		func(value *HistoricalReceiverConstruction) { value.SourceRevision = 0 },
		func(value *HistoricalReceiverConstruction) { value.SourceReceiver.ScopeKey = "other" },
		func(value *HistoricalReceiverConstruction) { value.SourceReceiver.InstanceID = "other" },
	} {
		hostile := fixed
		alter(&hostile)
		if _, err := AdmitHistoricalFlowReceiverInitialization(event.RunID(), MustExistingEntityTarget(construction), route.AgentIdentity.Route, hostile); err == nil {
			t.Fatal("contradictory fixed-cut coordinates acquired receiver authority")
		}
	}
}
