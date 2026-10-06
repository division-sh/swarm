package cataloge2e

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestFlowInstanceCreatedExpectationAdmission(t *testing.T) {
	valid := func() map[string]any {
		return map[string]any{"template": "worker", "instance_id": "ti-worker", "fields": map[string]any{"worker_id": "worker-1"}}
	}
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"retired_config", func(want map[string]any) { want["config"] = map[string]any{"worker_id": "worker-1"} }},
		{"unknown_key", func(want map[string]any) { want["field"] = want["fields"] }},
		{"fields_scalar", func(want map[string]any) { want["fields"] = "worker-1" }},
		{"fields_null", func(want map[string]any) { want["fields"] = nil }},
		{"fields_list", func(want map[string]any) { want["fields"] = []any{"worker-1"} }},
		{"empty_field_name", func(want map[string]any) { want["fields"] = map[string]any{"": "worker-1"} }},
		{"missing_template", func(want map[string]any) { delete(want, "template") }},
		{"missing_instance", func(want map[string]any) { delete(want, "instance_id") }},
		{"template_type", func(want map[string]any) { want["template"] = 1 }},
		{"instance_type", func(want map[string]any) { want["instance_id"] = false }},
		{"auto_emitted_type", func(want map[string]any) { want["auto_emitted"] = []any{"worker.ready"} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			want := valid()
			test.mutate(want)
			if err := validateFlowInstanceCreatedExpectation(want); err == nil {
				t.Fatalf("invalid expectation accepted: %#v", want)
			}
		})
	}
	if err := validateFlowInstanceCreatedExpectation(valid()); err != nil {
		t.Fatal(err)
	}
	if err := validateFlowInstanceCreatedExpectation(nil); err != nil {
		t.Fatal(err)
	}
}

func TestFlowInstanceCreatedExpectationRejectsUnreadOrWrongValues(t *testing.T) {
	const mutationEnv = "SWARM_CATALOG_EXPECTATION_MUTATION"
	if mutation := os.Getenv(mutationEnv); mutation != "" {
		requireCatalogCreationHandlerOrders(t, "test-create-flow-instance-duplicate", "worker-flow/ti-bc9c6acffc914a7ed5a2793b", "flow.spawned", "worker.ready", "awaiting_ready", "awaiting_spawned", func(expected *catalogExpectedDocument) {
			want := expected.Expected.FlowInstanceCreated
			switch mutation {
			case "wrong_value", "entities_with_wrong_value":
				want["fields"].(map[string]any)["worker_id"] = "IMPOSSIBLE_EXPECTATION"
				if mutation == "entities_with_wrong_value" {
					expected.Expected.Entities = map[string]catalogEntityExpected{catalogRuntimeRunID: {EntityState: "done"}}
				}
			case "unknown_key":
				want["config"] = want["fields"]
				delete(want, "fields")
			default:
				t.Fatalf("unknown expectation mutation %q", mutation)
			}
		})
		return
	}
	for _, test := range []struct{ name, marker string }{
		{"wrong_value", `flow instance fields "worker_id" =`},
		{"entities_with_wrong_value", `flow instance fields "worker_id" =`},
		{"unknown_key", "unknown key flow_instance_created.config"},
	} {
		t.Run(test.name, func(t *testing.T) {
			command := exec.Command(os.Args[0], "-test.run=^TestFlowInstanceCreatedExpectationRejectsUnreadOrWrongValues$", "-test.count=1", "-test.timeout=2m")
			command.Env = append(os.Environ(), mutationEnv+"="+test.name)
			output, err := command.CombinedOutput()
			if err == nil || strings.Count(string(output), test.marker) != 4 {
				t.Fatalf("expectation must fail on both stores and handler orders: err=%v\n%s", err, output)
			}
		})
	}
}
