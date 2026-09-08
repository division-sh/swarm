package bootverify

import (
	"context"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestLocalWildcardPayloadReaderConsumesExactProducerSchema(t *testing.T) {
	source := wildcardPayloadProofSource(t, "task.*", "text")
	report := Run(context.Background(), source, Options{})
	if errors := report.Errors(); len(errors) != 0 {
		t.Fatalf("wildcard payload verification: %#v", errors)
	}
	resolved, err := executablePayloadStructuralType(source, identitytest.FlowNode(t, "worker", "observer"), "task.*")
	if err != nil {
		t.Fatal(err)
	}
	field, ok := resolved.Field("work_id")
	if !ok || field.IsOptional || field.TypeRef != "text" {
		t.Fatalf("wildcard work_id = %#v, exists=%t", field, ok)
	}
}

func TestLocalWildcardPayloadReaderRejectsMissingAndIncompatibleProducerSchemas(t *testing.T) {
	for _, tc := range []struct{ name, pattern, secondType, want string }{
		{"missing finite expansion", "missing.*", "text", "no finite producer schema expansion"},
		{"incompatible type", "task.*", "integer", "incompatible producer schemas"},
		{"incompatible presence", "task.*", "text?", "incompatible producer schemas"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			report := Run(context.Background(), wildcardPayloadProofSource(t, tc.pattern, tc.secondType), Options{})
			if !reportContains(report.Errors(), "condition_expression_validation", tc.want) {
				t.Fatalf("verification errors = %#v, want %q", report.Errors(), tc.want)
			}
		})
	}
}

func wildcardPayloadProofSource(t *testing.T, pattern, secondType string) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	secondValue := "\"work-2\""
	if secondType == "integer" {
		secondValue = "7"
	}
	files := map[string]string{
		"schema.yaml":          "name: wildcard-payload-proof\n",
		"worker/schema.yaml":   "name: worker\nmode: static\ninitial_state: active\nstates: [active]\npins:\n  inputs:\n    events:\n      - event: start\n        source: external\n",
		"worker/entities.yaml": "work: {}\n",
		"worker/events.yaml":   "start: {}\ntask.done:\n  work_id: text\ntask.failed:\n  work_id: " + secondType + "\n",
		"worker/nodes.yaml":    "observer:\n  id: observer\n  execution_type: system_node\n  subscribes_to: [\"" + pattern + "\"]\n  event_handlers:\n    \"" + pattern + "\":\n      rules:\n        accept:\n          condition: payload.work_id != \"\"\n",
	}
	files["worker/nodes.yaml"] += "producer:\n  id: producer\n  execution_type: system_node\n  subscribes_to: [start, task.done]\n  produces: [task.done, task.failed]\n  event_handlers:\n    start:\n      emit:\n        event: task.done\n        fields:\n          work_id: {literal: work-1}\n    task.done:\n      emit:\n        event: task.failed\n        fields:\n          work_id: {literal: " + secondValue + "}\n"
	for path, contents := range files {
		writeBootverifyFixtureFile(t, filepath.Join(root, path), contents)
	}
	repo := repoRootForBootverifyTest(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load wildcard proof: %v", err)
	}
	return semanticview.Wrap(bundle)
}
