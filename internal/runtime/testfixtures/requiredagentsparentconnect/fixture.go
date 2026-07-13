package requiredagentsparentconnect

import (
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
	return canonicalrouting.CopyParentConnectRequiredAgent(t)
}
