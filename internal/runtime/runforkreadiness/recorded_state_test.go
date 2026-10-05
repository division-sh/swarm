package runforkreadiness

import (
	"encoding/json"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"strings"
	"testing"
)

func TestSelectedContractHeaderMissingHistoricalEvidenceFailsClosed(t *testing.T) {
	for _, leaf := range []string{"item", "ti-not-the-business-key"} {
		for _, frontier := range []string{"agent", "activity"} {
			t.Run(leaf+"/"+frontier, func(t *testing.T) {
				req := templateAdmissionRequest(t)
				req.Plan.Entities[0].MaterializationMetadata.FlowConfig = nil
				path := "consumer/" + leaf
				req.Plan.Entities[0].MaterializationMetadata.FlowInstance = path
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
				if err == nil || !strings.Contains(err.Error(), runfork.RunForkMaterializedEntitySnapshotMetadataOwner) || projection != nil {
					t.Fatalf("missing header acquired readiness: projection=%#v err=%v", projection, err)
				}
			})
		}
	}
}

func TestSelectedContractAdmissionBindsHistoricalStateKindsAndPresence(t *testing.T) {
	for _, changed := range []map[string]any{
		{"nested": []any{int64(7), int64(7), nil}},
		{"nested": []any{float64(7), float64(7), nil}},
		nil, {}, {"nested": []any{int64(7), float64(7)}},
	} {
		req := templateAdmissionRequest(t)
		req.Plan.Entities[0].Fields = map[string]any{"nested": []any{int64(7), float64(7), nil}}
		admitted, err := Admit(req)
		if err != nil {
			t.Fatal(err)
		}
		if err := admitted.ValidateAgainst(req.Binding); err != nil {
			t.Fatal(err)
		}
		req.Plan.Entities[0].Fields = changed
		if err := admitted.ValidateAgainst(req.Binding); err == nil {
			t.Fatalf("changed historical state retained admission: %#v", changed)
		}
	}
}

func TestSelectedContractRejectsContradictoryRecordedHeader(t *testing.T) {
	for _, raw := range []string{
		`{"instance_id":"other","flow_path":"consumer/item"}`,
		`{"instance_id":"item","flow_path":"consumer/other"}`,
		`{"instance_id":"item","storage_ref":"consumer/other","flow_path":"consumer/item"}`,
		`{"instance_id":"item","flow_path":"consumer/item","vertical_id":"flat-business-key"}`,
		`{"instance_id":"item","flow_path":"consumer/item","config":{}}`,
		`{"instance_id":"item","flow_path":"consumer/item","config":null}`,
		`{"instance_id":7,"flow_path":"consumer/item"}`,
		`{"instance_id":"item","flow_path":"consumer/item"} {}`,
		"{\"config\":{\"bad\":\"\xff\"}}",
	} {
		t.Run(raw, func(t *testing.T) {
			req := templateAdmissionRequest(t)
			req.Plan.Entities[0].MaterializationMetadata.FlowConfig = json.RawMessage(raw)
			if _, err := Admit(req); err == nil {
				t.Fatal("contradictory recorded header admitted")
			}
		})
	}
}
