package apispec

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/division-sh/swarm/internal/platform"
)

func TestRunForkResultHasClosedTypedPoint(t *testing.T) {
	artifact, err := os.ReadFile(platform.DefaultOpenRPCFile(repoRoot(t)))
	if err != nil {
		t.Fatal(err)
	}
	var doc OpenRPCDocument
	if err := json.Unmarshal(artifact, &doc); err != nil {
		t.Fatal(err)
	}
	schema, ok := doc.Components.Schemas["RunForkResult"].(map[string]any)
	if !ok {
		t.Fatalf("RunForkResult schema = %#v", doc.Components.Schemas["RunForkResult"])
	}
	if schema["required"] != nil || schema["properties"] != nil || schema["additionalProperties"] != nil {
		t.Fatal("oneOf root has sibling constraints ignored by the result validator")
	}
	arms, ok := schema["oneOf"].([]any)
	if !ok || len(arms) != 2 {
		t.Fatalf("RunForkResult oneOf = %#v, want event and deployment arms", schema["oneOf"])
	}
	common := []string{"owner", "source_run_id", "source_run_status", "source_frozen", "fork_run_id", "fork_point_kind", "fork_revision", "fork_run_status", "bundle_hash", "executed_event_count", "data_pins"}
	seenKinds := map[string]bool{}
	for _, armValue := range arms {
		arm := armValue.(map[string]any)
		if arm["type"] != "object" || arm["additionalProperties"] != false {
			t.Errorf("fork result arm is not a closed object: %#v", arm)
		}
		required := stringSetFromSchema(t, arm["required"])
		properties := arm["properties"].(map[string]any)
		for _, field := range common {
			if !required[field] || properties[field] == nil {
				t.Errorf("fork result arm omits required %s", field)
			}
		}
		armKind := properties["fork_point_kind"].(map[string]any)["const"]
		kind, ok := armKind.(string)
		if !ok || seenKinds[kind] {
			t.Fatalf("duplicate or malformed fork point arm: %#v", armKind)
		}
		seenKinds[kind] = true
		if revision := properties["fork_revision"].(map[string]any); revision["minimum"] != float64(1) {
			t.Errorf("%s fork_revision minimum = %#v, want 1", kind, revision["minimum"])
		}
		switch armKind {
		case "event":
			if !required["fork_event_id"] || properties["fork_event_id"] == nil {
				t.Error("event fork arm does not require fork_event_id")
			}
		case "deployment_revision":
			if required["fork_event_id"] || properties["fork_event_id"] != nil {
				t.Error("deployment fork arm does not forbid fork_event_id")
			}
		default:
			t.Errorf("unexpected fork point arm: %#v", armKind)
		}
		if len(properties) != len(required) {
			t.Errorf("%s fork arm properties=%d required=%d; unexpected optional field", kind, len(properties), len(required))
		}
	}
	if !seenKinds["event"] || !seenKinds["deployment_revision"] {
		t.Errorf("fork point arms = %#v", seenKinds)
	}
}

func stringSetFromSchema(t *testing.T, value any) map[string]bool {
	t.Helper()
	values, ok := value.([]any)
	if !ok {
		t.Fatalf("schema string list = %#v", value)
	}
	result := make(map[string]bool, len(values))
	for _, item := range values {
		name, ok := item.(string)
		if !ok || result[name] {
			t.Fatalf("invalid or duplicate schema string %#v", item)
		}
		result[name] = true
	}
	return result
}
