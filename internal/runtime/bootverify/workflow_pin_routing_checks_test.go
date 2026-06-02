package bootverify

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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

func TestPinTargetResolution_FailsClosedForRootPinOutputWithoutTargetMechanism(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		rootNodes: pinRoutingVerifyNodeYAML("root-node", "root.start", "root.ready", false),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "root handler.emit on node root-node") {
		t.Fatalf("expected root pin_target_resolution error, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForNestedRootPinOutputWithoutTargetMechanism(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		rootNodes: pinRoutingVerifyRuleNodeYAML("root-node", "root.start", "root.ready", false),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "root handler.rules.emit on node root-node") {
		t.Fatalf("expected nested root pin_target_resolution error, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_FailsClosedForRootGuardEscalationPinOutput(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		rootNodes: pinRoutingVerifyGuardNodeYAML("root-node", "root.start", "root.ready"),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "root handler.guard.on_fail.escalate on node root-node") {
		t.Fatalf("expected root guard escalation pin_target_resolution error, got %#v", report.Errors())
	}
}

func TestPinTargetResolution_AllowsRootExplicitBroadcastOptOut(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		rootNodes: pinRoutingVerifyNodeYAML("root-node", "root.start", "root.ready", true),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if reportContains(report.Errors(), "pin_target_resolution", "root") {
		t.Fatalf("unexpected root pin_target_resolution error: %#v", report.Errors())
	}
}

func TestPinTargetResolution_ChecksRootNodeWhenFlowNodeIDCollides(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		rootNodes:        pinRoutingVerifyNodeYAML("shared-node", "root.start", "root.ready", false),
		supportFlowNodes: pinRoutingVerifyNodeYAML("shared-node", "support.start", "support.ready", true),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if countPinTargetFindings(report, "root handler.emit on node shared-node") != 1 {
		t.Fatalf("expected exactly one root colliding-node finding, got %#v", pinTargetFindingMessages(report))
	}
}

func TestPinTargetResolution_EvaluatesPackageBackedFlowOnceNotRoot(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		supportFlowNodes: pinRoutingVerifyNodeYAML("support-node", "support.start", "support.ready", false),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if countPinTargetFindings(report, "flow support handler.emit on node support-node") != 1 {
		t.Fatalf("expected exactly one flow support finding, got %#v", pinTargetFindingMessages(report))
	}
	if countPinTargetFindings(report, "root handler.emit on node support-node") != 0 {
		t.Fatalf("package-backed flow node was reported as root: %#v", pinTargetFindingMessages(report))
	}
}

func TestPinTargetResolution_EvaluatesSoleParentPackageUnderOwningFlow(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		extrasNodes: pinRoutingVerifyNodeYAML("extras-node", "support.start", "support.ready", false),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if countPinTargetFindings(report, "flow support handler.emit on node extras-node") != 1 {
		t.Fatalf("expected exactly one sole-parent package flow finding, got %#v", pinTargetFindingMessages(report))
	}
}

func TestPinTargetResolution_DistinctSameFlowNodeIDSourceSitesBothConsidered(t *testing.T) {
	bundle := loadPinRoutingVerifySourceFixture(t, pinRoutingVerifySourceFixture{
		supportFlowNodes: pinRoutingVerifyNodeYAML("support-node", "support.start", "support.ready", false),
		extrasNodes:      pinRoutingVerifyNodeYAML("support-node", "support.start", "support.ready", true),
	})
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if countPinTargetFindings(report, "flow support handler.emit on node support-node") != 1 {
		t.Fatalf("expected invalid flow-scope support-node site not to be suppressed, got %#v", pinTargetFindingMessages(report))
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

type pinRoutingVerifySourceFixture struct {
	rootNodes        string
	supportFlowNodes string
	extrasNodes      string
}

func loadPinRoutingVerifySourceFixture(t *testing.T, opts pinRoutingVerifySourceFixture) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := t.TempDir()
	writePinRoutingVerifyFile(t, filepath.Join(root, "package.yaml"), `
name: pin-routing-source-identity
version: "1.0.0"
platform_version: ">=1.6.0"
packages:
  - path: extras
flows:
  - id: support
    flow: support
    mode: static
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "schema.yaml"), `
name: pin-routing-source-identity
pins:
  inputs:
    events: [root.start]
  outputs:
    events: [root.ready]
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "events.yaml"), `
root.start: {}
root.ready: {}
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "entities.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "nodes.yaml"), defaultPinRoutingFixtureYAML(opts.rootNodes))
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
flows: []
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: pending
states: [pending, done]
terminal_states: [done]
pins:
  inputs:
    events: [support.start]
  outputs:
    events: [support.ready]
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support.start: {}
support.ready: {}
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), "{}\n")
	writePinRoutingVerifyFile(t, filepath.Join(root, "flows", "support", "nodes.yaml"), defaultPinRoutingFixtureYAML(opts.supportFlowNodes))
	writePinRoutingVerifyFile(t, filepath.Join(root, "extras", "package.yaml"), `
name: extras
version: "1.0.0"
flows: []
`)
	writePinRoutingVerifyFile(t, filepath.Join(root, "extras", "nodes.yaml"), defaultPinRoutingFixtureYAML(opts.extrasNodes))
	repoRoot := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func pinRoutingVerifyNodeYAML(nodeID, trigger, eventType string, broadcast bool) string {
	if strings.TrimSpace(nodeID) == "" {
		return "{}\n"
	}
	broadcastLine := ""
	if broadcast {
		broadcastLine = "        broadcast: true\n"
	}
	return `
` + nodeID + `:
  id: ` + nodeID + `
  execution_type: system_node
  event_handlers:
    ` + trigger + `:
      emit:
        event: ` + eventType + `
` + broadcastLine
}

func pinRoutingVerifyGuardNodeYAML(nodeID, trigger, eventType string) string {
	if strings.TrimSpace(nodeID) == "" {
		return "{}\n"
	}
	return `
` + nodeID + `:
  id: ` + nodeID + `
  execution_type: system_node
  event_handlers:
    ` + trigger + `:
      guard:
        id: guard-escalate
        check: "false"
        on_fail: "escalate:` + eventType + `"
`
}

func pinRoutingVerifyRuleNodeYAML(nodeID, trigger, eventType string, broadcast bool) string {
	if strings.TrimSpace(nodeID) == "" {
		return "{}\n"
	}
	broadcastLine := ""
	if broadcast {
		broadcastLine = "          broadcast: true\n"
	}
	return `
` + nodeID + `:
  id: ` + nodeID + `
  execution_type: system_node
  event_handlers:
    ` + trigger + `:
      rules:
        - id: emit-root
          condition: "true"
          emit:
            event: ` + eventType + `
` + broadcastLine
}

func defaultPinRoutingFixtureYAML(value string) string {
	if strings.TrimSpace(value) == "" {
		return "{}\n"
	}
	return value
}

func countPinTargetFindings(report Report, messagePart string) int {
	count := 0
	for _, finding := range report.Errors() {
		if finding.CheckID == "pin_target_resolution" && strings.Contains(finding.Message, messagePart) {
			count++
		}
	}
	return count
}

func pinTargetFindingMessages(report Report) []string {
	out := []string{}
	for _, finding := range report.Errors() {
		if finding.CheckID == "pin_target_resolution" {
			out = append(out, finding.Message)
		}
	}
	return out
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
