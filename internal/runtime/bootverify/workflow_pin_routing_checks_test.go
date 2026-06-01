package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPinTargetResolution_FailsClosedForPinOutputWithoutTargetMechanism(t *testing.T) {
	bundle := loadPinRoutingVerifyBundle(t, "emit:\n        event: result.ready\n")
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "target_required_missing") {
		t.Fatalf("expected pin_target_resolution target_required_missing, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_AllowsExplicitBroadcastOptOut(t *testing.T) {
	bundle := loadPinRoutingVerifyBundle(t, "emit:\n        event: result.ready\n        broadcast: true\n")
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "pin_target_resolution", "") {
		t.Fatalf("unexpected pin_target_resolution error: %#v", report.Errors())
	}
}

func loadPinRoutingVerifyBundle(t *testing.T, emitBlock string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writePinRoutingVerifyFile(t, filepath.Join(root, "package.yaml"), `
name: pin-routing-verify
version: "1.0.0"
platform_version: ">=1.6.0"
flows:
  - id: worker
    flow: worker
    mode: static
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "schema.yaml"), "name: pin-routing-verify\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "worker", "schema.yaml"), `
name: worker
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events:
      - task.started
  outputs:
    events:
      - result.ready
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "worker", "events.yaml"), `
task.started:
  entity_id: text
result.ready:
  entity_id: text
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "worker", "entities.yaml"), `
task:
  entity_id: text
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "worker", "nodes.yaml"), `
worker-node:
  id: worker-node
  execution_type: system_node
  event_handlers:
    task.started:
      create_entity: true
      advances_to: done
      `+emitBlock+`  state_schema:
    fields:
      seen: integer
`)
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func writePinRoutingVerifyFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
