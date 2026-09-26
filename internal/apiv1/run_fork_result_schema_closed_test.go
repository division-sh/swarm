package apiv1

import (
	"testing"
)

func TestRunForkResultSchemaKeepsPointArmsClosed(t *testing.T) {
	root := repoRoot(t)
	doc, _ := loadComplianceOpenRPC(t, complianceOpenRPCPath(root))
	validator := newOpenRPCResultSchemaValidator(t, doc)
	method := validator.methods["run.fork"]
	if method.Result == nil {
		t.Fatal("run.fork result schema is missing")
	}
	schema := method.Result.Schema
	event, ok := successfulRuntimeResult(t, "run.fork").(map[string]any)
	if !ok {
		t.Fatal("run.fork probe did not return an object")
	}
	if err := validator.validateValue("$.run.fork.result", schema, event); err != nil {
		t.Fatalf("event fork result rejected: %v", err)
	}
	deployment := copyRunForkResultFields(event)
	deployment["fork_point_kind"] = "deployment_revision"
	delete(deployment, "fork_event_id")
	if err := validator.validateValue("$.run.fork.result", schema, deployment); err != nil {
		t.Fatalf("eventless deployment fork result rejected: %v", err)
	}
	for _, test := range []struct {
		name   string
		base   map[string]any
		mutate func(map[string]any)
	}{
		{"event_without_id", event, func(value map[string]any) { delete(value, "fork_event_id") }},
		{"deployment_with_event_id", deployment, func(value map[string]any) { value["fork_event_id"] = event["fork_event_id"] }},
		{"deployment_with_null_event_id", deployment, func(value map[string]any) { value["fork_event_id"] = nil }},
		{"missing_shared_owner", deployment, func(value map[string]any) { delete(value, "owner") }},
		{"extra_field", event, func(value map[string]any) { value["unexpected"] = true }},
		{"zero_revision", event, func(value map[string]any) { value["fork_revision"] = float64(0) }},
		{"unknown_kind", event, func(value map[string]any) { value["fork_point_kind"] = "unknown" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := copyRunForkResultFields(test.base)
			test.mutate(value)
			if err := validator.validateValue("$.run.fork.result", schema, value); err == nil {
				t.Fatalf("contradictory run.fork response passed closed schema: %#v", value)
			}
		})
	}
}

func copyRunForkResultFields(source map[string]any) map[string]any {
	copy := make(map[string]any, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}
