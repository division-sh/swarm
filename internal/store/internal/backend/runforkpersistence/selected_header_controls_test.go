package runforkpersistence

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestSelectedWorkflowConfigPreservesRecordedControls(t *testing.T) {
	state := selectedWorkflowConstructionFixture(t, selectedContractWorkflowState{
		RunID: "00000000-0000-0000-0000-000000000227", EntityID: "00000000-0000-0000-0000-000000000228",
		WorkflowName: "orders", Route: "orders/order-1", WorkflowVersion: "source-version", Mode: "template", ExecutionMode: executionmode.Mock,
	})
	var controls map[string]any
	if err := json.Unmarshal(state.History.MaterializationMetadata.FlowConfig, &controls); err != nil {
		t.Fatal(err)
	}
	controls["instance_kind"], controls["template_version"], controls["status"] = "template", "retained-template", "active"
	raw, err := canonicaljson.MarshalPreservingNumberKinds(controls)
	if err != nil {
		t.Fatal(err)
	}
	state.History.MaterializationMetadata.FlowConfig = raw
	state.WorkflowVersion = "selected-version"
	before := append([]byte(nil), raw...)
	for attempt := 0; attempt < 3; attempt++ {
		projected, err := selectedContractWorkflowStateConfig(state)
		if err != nil {
			t.Fatal(err)
		}
		var actual map[string]any
		if err := json.Unmarshal(projected, &actual); err != nil {
			t.Fatal(err)
		}
		for key, want := range map[string]any{
			"instance_kind": "template", "template_version": "retained-template", "status": "active",
			"workflow_version": "selected-version", "instance_id": "order-1", "storage_ref": "orders/order-1", "flow_path": "orders/order-1",
		} {
			if !reflect.DeepEqual(actual[key], want) {
				t.Errorf("header control %s: got %#v want %#v", key, actual[key], want)
			}
		}
		if !bytes.Equal(before, state.History.MaterializationMetadata.FlowConfig) {
			t.Fatal("selected header projection changed the retained source controls")
		}
	}
}
