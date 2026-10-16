package cliapp

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunVerifyPolicyCaseShapes2287(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")
	for _, tc := range []struct {
		name, row, refusal string
	}{
		{"singular", "selector: payload.mode, equals: gold", ""},
		{"plural", "selectors: [payload.mode, payload.ready, payload.count], equals: [gold, true, 1]", ""},
		{"different_values", "selectors: [payload.mode, payload.mode], equals: [gold, blue]", ""},
		{"same_values", "selectors: [payload.mode, payload.target], equals: [gold, gold]", ""},
		{"singleton", "selectors: [payload.mode], equals: [gold]", "one selector: write `selector:` and a scalar `equals:`"},
		{"singular_list", "selector: payload.mode, equals: [gold]", "case is either singular (selector/equals) or plural (selectors/equals), not mixed"},
		{"plural_scalar", "selectors: [payload.mode, payload.target], equals: gold", "case is either singular (selector/equals) or plural (selectors/equals), not mixed"},
		{"scalar_selectors", "selectors: payload.mode, equals: gold", "`selectors` is a list; for one selector write `selector`"},
		{"duplicate", "selectors: [payload.mode, payload.mode], equals: [gold, gold]", "duplicate case pair at positions 1 and 2"},
		{"blank", `selectors: [payload.mode, ""], equals: [gold, blue]`, "must be non-empty"},
		{"type_mismatch", `selectors: [payload.mode, payload.ready], equals: [gold, "true"]`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: case-shape\nstages:\n  active: {}\n  done: {final: true}\npins:\n  inputs: [requested]\n  outputs: [completed]\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "entities.yaml"), "case: {}\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "events.yaml"), "requested:\n  mode: text\n  target: text\n  ready: boolean\n  count: integer\ncompleted:\n")
			writeWorkflowValidationFixtureFile(t, filepath.Join(root, "nodes.yaml"), "router:\n  execution_type: system_node\n  subscribes_to: [requested]\n  produces: [completed]\n  event_handlers:\n    requested:\n      rules:\n        - case: {"+tc.row+"}\n          advances_to: done\n          emit: completed\n        - advances_to: done\n          emit: completed\n")
			var stdout, stderr bytes.Buffer
			code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), RepoRoot(), root, &stdout, &stderr)
			if tc.name == "type_mismatch" {
				if code == 0 || !strings.Contains(stderr.String(), "[BLOCKER] condition_expression_validation") || !strings.Contains(stderr.String(), "(bool, string)") {
					t.Fatalf("typed comparison did not reach expression refusal: code=%d stdout=%s stderr=%s", code, &stdout, &stderr)
				}
			} else if tc.refusal != "" {
				if code == 0 || !strings.Contains(stderr.String(), tc.refusal) || !strings.Contains(stderr.String(), "nodes.yaml:") {
					t.Fatalf("want located refusal %q: code=%d stdout=%s stderr=%s", tc.refusal, code, &stdout, &stderr)
				}
			} else if code != 0 || !strings.Contains(stdout.String(), "verify ok: source=") {
				t.Fatalf("canonical verify refused: code=%d stdout=%s stderr=%s", code, &stdout, &stderr)
			}
		})
	}
}
