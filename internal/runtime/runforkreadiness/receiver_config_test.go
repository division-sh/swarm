package runforkreadiness

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestSelectedContractReceiverConfigMissingHistoricalEvidenceFailsClosed(t *testing.T) {
	for _, leaf := range []string{"item", "ti-not-the-business-key"} {
		for _, frontier := range []string{"agent", "activity"} {
			t.Run(leaf+"/"+frontier, func(t *testing.T) {
				req := templateAdmissionRequest(t)
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
