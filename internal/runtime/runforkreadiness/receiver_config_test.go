package runforkreadiness

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedContractReceiverConfigMissingHistoricalEvidenceFailsClosed(t *testing.T) {
	for _, leaf := range []string{"item", "ti-not-the-business-key"} {
		for _, frontier := range []string{"agent", "activity"} {
			t.Run(leaf+"/"+frontier, func(t *testing.T) {
				req := templateAdmissionRequest(t)
				req.Plan.Entities[0].MaterializationMetadata.FlowConfig = nil
				path := "consumer/" + leaf
				req.Plan.Entities[0].MaterializationMetadata.FlowInstance = path
				// Neither an entity field nor an identically named state bucket
				// proves the immutable receiver configuration at the fork revision.
				req.Plan.Entities[0].Fields = map[string]any{"vertical_id": "business-key", "config": map[string]any{"vertical_id": "business-key"}}
				for i := range req.RecipientPlanning.RecipientPlanEvents {
					event := &req.RecipientPlanning.RecipientPlanEvents[i]
					for j := range event.Recipients {
						recipient := &event.Recipients[j]
						recipient.Path = path
						recipient.AgentPlan.Route.InstanceID = leaf
						recipient.AgentPlan.Route.InstancePath = path
					}
					if frontier == "activity" {
						event.EventName = runfork.RunForkSelectedContractPlatformActivityEvent
						event.Recipients = nil
					}
				}
				for i := range req.Plan.PendingWork {
					pending := &req.Plan.PendingWork[i]
					pending.FlowInstance = path
					pending.RoutingSource = eventtest.ConcreteTemplateRoutingSource("consumer", path, req.Plan.Entities[0].EntityID)
				}
				projection, err := Project(req.Plan, req.Source, req.RecipientPlanning, req.SourceModes, req.ModelOptions)
				if err == nil || !strings.Contains(err.Error(), "exact fixed-revision receiver configuration") || projection != nil {
					t.Fatalf("missing config acquired readiness: projection=%#v err=%v", projection, err)
				}
			})
		}
	}
}

func TestSelectedContractReceiverConfigEqualityPreservesNumericKindsAndPresence(t *testing.T) {
	integer := runfork.RunForkSelectedContractWorkflowState{Config: map[string]any{"nested": []any{int64(7)}}}
	double := integer
	double.Config = map[string]any{"nested": []any{float64(7)}}
	if selectedContractWorkflowStatesEqual(integer, double) {
		t.Fatal("integer and double configuration merged")
	}
	absent, empty := integer, integer
	absent.Config = nil
	empty.Config = map[string]any{}
	if selectedContractWorkflowStatesEqual(absent, empty) {
		t.Fatal("missing config merged with explicit empty config")
	}
	if !selectedContractWorkflowStatesEqual(integer, integer) {
		t.Fatal("exact config did not compare equal")
	}
}

func TestSelectedContractReceiverConfigSealedReadbackPreservesNumericKinds(t *testing.T) {
	projection := Projection{States: []runfork.RunForkSelectedContractWorkflowState{{
		Config: map[string]any{"key": "business-key", "nested": []any{int64(7), float64(7), nil}},
	}}}
	raw, err := canonicaljson.MarshalPreservingNumberKinds(projection)
	if err != nil {
		t.Fatal(err)
	}
	admitted := Admission{sealed: &admittedProjection{projection: raw}}
	for i := 0; i < 2; i++ {
		readback, err := admitted.Projection()
		if err != nil {
			t.Fatal(err)
		}
		values := readback.States[0].Config["nested"].([]any)
		if values[0] != json.Number("7") || values[1] != json.Number("7.0") || values[2] != nil {
			t.Fatalf("sealed config lost numeric kinds: %#v", values)
		}
		values[0] = "mutated readback"
	}
}

func TestSelectedContractReceiverConfigAdmissionSealsRecordedBusinessConfig(t *testing.T) {
	req := templateAdmissionRequestWithVariables(t, map[string]contracts.FlowVariable{
		"nested": {Type: "json"}, "status": {Type: "boolean"}, "flow_path": {Type: "json"},
	})
	req.Plan.Entities[0].MaterializationMetadata.FlowConfig = json.RawMessage(`{"instance_id":"item","storage_ref":"consumer/item","flow_path":"consumer/item","status":"active","config":{"vertical_id":"original-business-key","nested":[7,7.0,null],"status":false,"flow_path":["business","path"]}}`)
	admitted, err := Admit(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := admitted.ValidateAgainst(req.Binding); err != nil {
		t.Fatal(err)
	}
	want := `{"flow_path":["business","path"],"nested":[7,7.0,null],"status":false,"vertical_id":"original-business-key"}`
	for i := 0; i < 2; i++ {
		projection, err := admitted.Projection()
		if err != nil {
			t.Fatal(err)
		}
		for _, config := range []map[string]any{projection.States[0].Config, projection.Flows[0].Config} {
			wire, err := canonicaljson.MarshalPreservingNumberKinds(config)
			if err != nil || string(wire) != want {
				t.Fatalf("sealed business config = %s: %v", wire, err)
			}
			config["nested"].([]any)[0] = "mutated readback"
		}
		flow := projection.Flows[0]
		if flow.ActivationVariables["vertical_id"] != "original-business-key" || string(flow.Agents[0].Config.Config) != want {
			t.Fatalf("first activation consumers lost exact config: %#v, %s", flow.ActivationVariables, flow.Agents[0].Config.Config)
		}
	}
	req.Plan.Entities[0].MaterializationMetadata.FlowConfig = json.RawMessage(strings.Replace(string(req.Plan.Entities[0].MaterializationMetadata.FlowConfig), "[7,7.0,null]", "[7,7,null]", 1))
	if err := admitted.ValidateAgainst(req.Binding); err == nil {
		t.Fatal("numeric-kind-only config change retained the original admission binding")
	}
	req.Plan.Entities[0].MaterializationMetadata.FlowConfig = json.RawMessage(`{"config":{"vertical_id":"incoming"}}`)
	if err := admitted.ValidateAgainst(req.Binding); err == nil {
		t.Fatal("different recorded config retained the original admission binding")
	}
}

func TestSelectedContractReceiverConfigRejectsContradictoryRecordedEnvelope(t *testing.T) {
	for _, raw := range []string{
		`{"instance_id":"other","flow_path":"consumer/item","config":{}}`,
		`{"instance_id":"item","flow_path":"consumer/other","config":{}}`,
		`{"instance_id":"item","storage_ref":"consumer/other","flow_path":"consumer/item","config":{}}`,
		`{"instance_id":"item","flow_path":"consumer/item","vertical_id":"flat-business-key"}`,
		`{"instance_id":"item","flow_path":"consumer/item","config":null}`,
		`{"instance_id":"item","flow_path":"consumer/item","config":{},"config":{}}`,
		`{"instance_id":"item","flow_path":"consumer/item","config":{}} {}`,
		"{\"config\":{\"bad\":\"\xff\"}}",
	} {
		t.Run(raw, func(t *testing.T) {
			req := templateAdmissionRequest(t)
			req.Plan.Entities[0].MaterializationMetadata.FlowConfig = json.RawMessage(raw)
			if _, err := Admit(req); err == nil {
				t.Fatal("contradictory recorded config admitted")
			}
		})
	}
}
