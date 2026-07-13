package requiredagentsparentconnect

import (
	"path/filepath"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

const AgentID = "analyzer"

func LoadBundle(t testing.TB) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := Write(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		canonicalrouting.RepoRoot(t), root, runtimecontracts.DefaultPlatformSpecFile(canonicalrouting.RepoRoot(t)),
	)
	if err != nil {
		t.Fatalf("load required-agent parent-connect overlay: %v", err)
	}
	return bundle
}

func Write(t testing.TB) string {
	// routing-example-census: different-concept issue=none owner=flow_required_agent_authority proof=internal/runtime/bootverify/report_test.go:TestRun_DoesNotWarnForFlowOwnedAgentEmissionsDeclaredAsFlowOutputs
	t.Helper()
	root := canonicalrouting.CopyExample(t, canonicalrouting.ParentConnect)
	producerSchema := filepath.Join(root, "flows", "producer", "schema.yaml")
	canonicalrouting.ReplaceFile(t, producerSchema, "pins:\n", `required_agents:
  - role: analyzer
    subscribes_to: [work.requested]
    emits: [producer/work.ready]
    description: Analyzes work before delivery
pins:
`)
	canonicalrouting.WriteFile(t, root, "flows/producer/agents.yaml", `
analyzer:
  model: regular
  subscriptions: [work.requested]
  emit_events: [producer/work.ready]
`)
	return root
}
