package cliapp

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunVerifyCommand_PolicyLookupExecutablePathBoundary(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")
	for _, tc := range []struct {
		name, fieldType, path, key string
		want                       bool
	}{
		{"required-json", "json", "payload.selector", "selected", true},
		{"nested-json", "json", "payload.selector.code", "selected", false},
		{"nested-map", "map[text]json", "payload.selector.code", "selected", false},
		{"nested-list", "[text]", "payload.selector.code", "selected", false},
		{"optional-boolean", "boolean?", "payload.selector", "true", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), `name: lookup-path-boundary
stages:
  active: {}
  done: {final: true}
pins:
  inputs: [requested]
  outputs: [completed]
`)
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "state.yaml"), "case: {}\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "requested:\n  selector: "+tc.fieldType+"\ncompleted:\n  choice: text\n")
			// The lookup has one value, chosen, and fails on a missing key; its
			// successful branch is therefore the effectful structural default.
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), `router:
  execution_type: system_node
  subscribes_to: [requested]
  produces: [completed]
  event_handlers:
    requested:
      rules:
        - lookup:
            on: `+tc.path+`
            entries: [{key: `+tc.key+`, value: chosen}]
            into: computed.choice
            default: fail
        - advances_to: done
          emit:
            event: completed
            fields:
              choice: computed.choice
`)
			var stdout, stderr bytes.Buffer
			code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
			if (code == 0) != tc.want {
				t.Fatalf("verify code=%d, want admitted=%v; stdout=%s stderr=%s", code, tc.want, stdout.String(), stderr.String())
			}
			if tc.want && !strings.Contains(stdout.String(), "verify ok: source=") {
				t.Fatalf("missing actual verify success: %s", stdout.String())
			}
			if !tc.want && !strings.Contains(stderr.String(), "[BLOCKER] executable_reader_expression_validation") {
				t.Fatalf("unsupported read did not reach executable-reader refusal: %s", stderr.String())
			}
		})
	}
}
