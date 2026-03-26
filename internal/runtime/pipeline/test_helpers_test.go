package pipeline

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func contractComplianceRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve runtime caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func contractComplianceBundleRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writePipelineFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: contract-compliance-bundle
version: "1.0.0"
platform_version: ">=1.0.0"
entity_schema:
  groups:
    - name: item
      fields:
        - name: item_id
          type: string
          primary: true
        - name: status
          type: string
flows: []
`)
	writePipelineFixtureFile(t, filepath.Join(root, "schema.yaml"), `
initial_state: idle
terminal_states:
  - done
states:
  - idle
  - done
pins:
  inputs:
    events:
      - item.created
  outputs:
    events:
      - item.reviewed
`)
	writePipelineFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePipelineFixtureFile(t, filepath.Join(root, "agents.yaml"), `
control-plane:
  id: control-plane
  role: control-plane
  model_tier: sonnet
  conversation_mode: session
  subscriptions:
    - item.reviewed
`)
	writePipelineFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  payload:
    properties:
      entity_id:
        type: string
      item_id:
        type: string
    required:
      - entity_id
      - item_id
item.reviewed:
  payload:
    properties:
      entity_id:
        type: string
      outcome:
        type: string
    required:
      - entity_id
`)
	writePipelineFixtureFile(t, filepath.Join(root, "nodes.yaml"), `
review-node:
  id: review-node
  execution_type: workflow_node
  subscribes_to:
    - item.created
  produces:
    - item.reviewed
  event_handlers:
    item.created:
      guard:
        check: payload.item_id != ''
      emits: item.reviewed
`)
	return root
}

func writePipelineFixtureFile(t *testing.T, path string, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
