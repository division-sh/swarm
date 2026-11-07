package events

import (
	"encoding/json"
	"testing"
)

func TestFlowOwnedControlPreservesOptionalEntityThroughEveryCodec(t *testing.T) {
	for _, entity := range []string{"", "entity"} {
		t.Run("entity="+entity, func(t *testing.T) {
			route := RouteIdentity{FlowID: "child", FlowInstance: "outer/child/one", EntityID: entity}
			source, err := NewFlowOwnedControlRoutingSource(route)
			if err != nil {
				t.Fatal(err)
			}
			if source.Route() != route {
				t.Fatalf("source route = %#v", source.Route())
			}
			wire, err := json.Marshal(source)
			if err != nil {
				t.Fatal(err)
			}
			var decoded RoutingSource
			if err := json.Unmarshal(wire, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded != source {
				t.Fatalf("JSON changed source: %#v", decoded)
			}
			restored, err := RestoreRoutingSource(source.Kind().StorageCode(), route, "", source.Life())
			if err != nil || restored != source {
				t.Fatalf("storage changed source: %#v, %v", restored, err)
			}
			if got, err := AdmitRuntimeControlEventType("platform.human_task.approved", restored); err != nil || got != "platform.human_task.approved" {
				t.Fatalf("event admission = %q, %v", got, err)
			}
			later, err := source.WithLife(7)
			if err != nil || later.Route() != source.Route() || source.Life() != 1 {
				t.Fatalf("life changed source identity or original value: %#v %v", later, err)
			}
			wire, err = json.Marshal(later)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(wire, &decoded); err != nil || decoded != later {
				t.Fatalf("JSON lost captured life: %#v %v", decoded, err)
			}
			restored, err = RestoreRoutingSource(later.Kind().StorageCode(), route, "", later.Life())
			if err != nil || restored != later {
				t.Fatalf("storage lost captured life: %#v %v", restored, err)
			}
			for _, invalid := range []int64{0, -1} {
				if _, err := RestoreRoutingSource(source.Kind().StorageCode(), route, "", invalid); err == nil {
					t.Fatalf("storage repaired invalid life %d", invalid)
				}
			}
			if err := json.Unmarshal([]byte(`{"kind":"flow_owned_control","route":{"flow_id":"child","flow_instance":"outer/child/one"}}`), &decoded); err == nil {
				t.Fatal("JSON repaired missing life")
			}
		})
	}
}

func TestFlowOwnedControlRejectsMissingFlowAuthority(t *testing.T) {
	for _, route := range []RouteIdentity{{}, {EntityID: "entity"}, {FlowID: "child"}, {FlowInstance: "child/one"}} {
		if _, err := NewFlowOwnedControlRoutingSource(route); err == nil {
			t.Fatalf("constructor accepted %#v", route)
		}
		if _, err := RestoreRoutingSource("flow_owned_control", route, "", 1); err == nil {
			t.Fatalf("storage accepted %#v", route)
		}
		wire, err := json.Marshal(map[string]any{"kind": "flow_owned_control", "route": route})
		if err != nil {
			t.Fatal(err)
		}
		var source RoutingSource
		if err := json.Unmarshal(wire, &source); err == nil {
			t.Fatalf("JSON accepted %#v", route)
		}
	}
	if _, err := NewRootRoutingSource(""); err == nil {
		t.Fatal("root source lost its entity requirement")
	}
}
