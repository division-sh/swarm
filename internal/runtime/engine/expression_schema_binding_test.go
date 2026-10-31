package engine

import (
	"os"
	"path/filepath"
	"testing"

	rc "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func TestExecutionFrameSchemaBindingUsesExactDeclarationScope(t *testing.T) {
	root := t.TempDir()
	for path, raw := range map[string]string{
		"schema.yaml":       "stages: []\n",
		"state.yaml":        "threshold: boolean\n",
		"left/schema.yaml":  "stages: []\n",
		"left/state.yaml":   "threshold: integer\n",
		"right/schema.yaml": "stages: []\n",
		"right/state.yaml":  "threshold: text\n",
		"empty/schema.yaml": "stages: []\n", "manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n", "left/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n", "right/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n", "empty/manifest.yaml": "version: 1.0.0\nplatform_version: '*'\n",
	} {
		full := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := rc.LoadWorkflowContractBundleWithOverrides(repo, root, rc.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	exec := &Executor{deps: RuntimeDependencies{Source: semanticview.Wrap(bundle)}}
	for _, tc := range []struct {
		name, scope, execution, expression string
		entity                             map[string]any
		want                               any
		refuse                             bool
	}{
		{"root", ".", "", "state.threshold", map[string]any{"threshold": true}, true, false},
		{"child_integer", "left", "left", "state.threshold + 1", map[string]any{"threshold": int64(75)}, int64(76), false},
		{"concrete_execution", "left", "left/instance-proof", "state.threshold + 1", map[string]any{"threshold": int64(75)}, int64(76), false},
		{"same_name_sibling", "right", "right", `state.threshold + "!"`, map[string]any{"threshold": "ready"}, "ready!", false},
		{"wrong_sibling_type", "right", "left", "state.threshold + 1", map[string]any{"threshold": int64(75)}, nil, true},
		{"missing_schema", "empty", "left", "state.threshold + 1", map[string]any{"threshold": int64(75)}, nil, true},
		{"undeclared_field", "left", "left", "state.unknown + 1", map[string]any{"unknown": int64(75)}, nil, true},
		{"entityless", "empty", "empty", "1 + 1", nil, int64(2), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			frame := &executionFrame{req: ExecutionRequest{Node: identitytest.ExecutableNode(t, tc.scope, "worker"), ExecutionFlowID: identity.NormalizeFlowID(tc.execution)}}
			exec.bindFrameExpressionSchemas(frame)
			options := frameExpressionOptions(frame)
			got, err := workflowexpr.EvalValueExpressionWithOptions(tc.expression, workflowexpr.ValueContext{Entity: tc.entity}, options)
			if tc.refuse {
				if err == nil {
					t.Fatalf("borrowed schema or accepted undeclared field: %v", got)
				}
			} else if err != nil || got != tc.want {
				t.Fatalf("exact declaration execution got=%v want=%v err=%v", got, tc.want, err)
			}
			if options.EntityType != nil {
				options.EntityType.Fields[0].Name = "corrupted"
				if frameExpressionOptions(frame).EntityType.Fields[0].Name == "corrupted" {
					t.Fatal("expression options alias the pinned schema")
				}
			}
		})
	}
}
