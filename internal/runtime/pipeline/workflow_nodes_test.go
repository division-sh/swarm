package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
)

func TestEmpirePipelineWorkflowNodes_ExposeSubscriptions(t *testing.T) {
	subs := defaultPipelineSubscriptions()
	if len(subs) == 0 {
		t.Fatal("expected subscriptions")
	}
	want := map[events.EventType]struct{}{
		events.EventType("scan.requested"):         {},
		events.EventType("vertical.shortlisted"):   {},
		events.EventType("spec.validation_passed"): {},
		events.EventType("runtime.reset"):          {},
	}
	for evt := range want {
		found := false
		for _, sub := range subs {
			if sub == evt {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing subscription %s", evt)
		}
	}
}

func TestEmpirePipelineWorkflowNodes_ExposePolicies(t *testing.T) {
	policy, ok := defaultPipelineEventPolicy("brand.revision_needed")
	if !ok {
		t.Fatal("expected brand.revision_needed policy")
	}
	if policy.Consume || !policy.VisibleDownstream {
		t.Fatalf("brand.revision_needed should be dual_delivery under 2.2.0, got %+v", policy)
	}
	if policy.RequireVertical {
		t.Fatal("brand.revision_needed should not require vertical_id under the 2.2.0 policy model")
	}

	policy, ok = defaultPipelineEventPolicy("category.assessed")
	if !ok || !policy.Consume {
		t.Fatalf("expected category.assessed consume policy, got ok=%v consume=%v", ok, policy.Consume)
	}
}

func TestDefaultPipelineWorkflowNodes_UseFiveNodeRuntimeModel(t *testing.T) {
	nodes := DefaultPipelineWorkflowNodes()
	got := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		got[node.ID] = struct{}{}
	}
	want := map[string]struct{}{
		"scan-orchestrator":       {},
		"discovery-aggregator":    {},
		"validation-orchestrator": {},
		"lifecycle-orchestrator":  {},
		"scoring-node":            {},
	}
	if len(got) != len(want) {
		t.Fatalf("expected exactly %d runtime nodes, got %d: %+v", len(want), len(got), got)
	}
	for nodeID := range want {
		if _, ok := got[nodeID]; !ok {
			t.Fatalf("missing runtime node %s in %+v", nodeID, got)
		}
	}
	if _, legacy := got["pipeline-coordinator"]; legacy {
		t.Fatalf("unexpected legacy pipeline-coordinator in active runtime node model: %+v", got)
	}
}

func TestEmpirePipelineWorkflowNodes_CoverValidationAndScanEdgeEvents(t *testing.T) {
	for _, eventType := range []string{
		"cto.spec_vetoed",
		"opco.ceo_ready",
		"synthesis.resolved",
		"trend_research.scan_complete",
	} {
		policy, ok := defaultPipelineEventPolicy(eventType)
		if !ok {
			t.Fatalf("expected policy for %s", eventType)
		}
		switch eventType {
		case "opco.ceo_ready":
			if policy.Consume || !policy.VisibleDownstream {
				t.Fatalf("%s should now be dual_delivery under 2.2.0, got %+v", eventType, policy)
			}
		case "synthesis.resolved", "trend_research.scan_complete":
			if !policy.Consume || policy.VisibleDownstream {
				t.Fatalf("%s should remain runtime-consuming under the discovery/scan node model, got %+v", eventType, policy)
			}
		default:
			if policy.Consume || !policy.VisibleDownstream {
				t.Fatalf("%s should be dual_delivery under the 2.2.0 workflow model, got %+v", eventType, policy)
			}
		}
	}
}

func TestFactoryPipelineCoordinator_DispatchWorkflowNodeEvent(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)
	if handled := pc.dispatchWorkflowNodeEvent(context.Background(), events.Event{Type: events.EventType("synthesis.resolved")}); !handled {
		t.Fatal("expected synthesis.resolved to be handled by workflow node executor")
	}
	if handled := pc.dispatchWorkflowNodeEvent(context.Background(), events.Event{Type: events.EventType("unknown.event")}); handled {
		t.Fatal("expected unknown.event to remain unhandled")
	}
}

func TestLoadWorkflowNodes_V220UsesOwningNodeAndDualDeliveryPolicy(t *testing.T) {
	repoRoot := projectRootFromWorkflowNodesTest(t)
	tmp := t.TempDir()
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "workflow-schema.yaml"), filepath.Join(tmp, "contracts", "workflow-schema.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "guard-action-registry.yaml"), filepath.Join(tmp, "contracts", "guard-action-registry.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "system-nodes.yaml"), filepath.Join(tmp, "contracts", "system-nodes.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "event-catalog.yaml"), filepath.Join(tmp, "contracts", "event-catalog.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "agent-tools.yaml"), filepath.Join(tmp, "contracts", "agent-tools.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "tool-schemas.yaml"), filepath.Join(tmp, "contracts", "tool-schemas.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "prompt-variables.yaml"), filepath.Join(tmp, "contracts", "prompt-variables.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "docs", "specs", "empireai-v2_2_0", "contracts-v220", "platform", "platform-spec.yaml"), filepath.Join(tmp, "contracts", "platform", "platform-spec.yaml"))

	bundle, err := runtimecontracts.LoadWorkflowContractBundle(tmp)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	nodes, err := LoadWorkflowNodes(bundle)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes() error = %v", err)
	}
	index := make(map[string]WorkflowNode, len(nodes))
	for _, node := range nodes {
		index[node.ID] = node
	}
	for _, nodeID := range []string{"scan-orchestrator", "discovery-aggregator", "validation-orchestrator", "lifecycle-orchestrator", "scoring-node"} {
		if _, ok := index[nodeID]; !ok {
			t.Fatalf("expected %s in v2.2.0 workflow nodes", nodeID)
		}
	}
	buildPolicy, ok := index["lifecycle-orchestrator"].Policies["build_complete"]
	if !ok {
		t.Fatal("expected build_complete policy for lifecycle-orchestrator")
	}
	if buildPolicy.Consume || !buildPolicy.VisibleDownstream {
		t.Fatalf("expected dual_delivery policy for build_complete, got %+v", buildPolicy)
	}
	scanPolicy, ok := index["scan-orchestrator"].Policies["scan.requested"]
	if !ok {
		t.Fatal("expected scan.requested policy for scan-orchestrator")
	}
	if !scanPolicy.Consume || scanPolicy.VisibleDownstream {
		t.Fatalf("expected consuming policy for scan.requested, got %+v", scanPolicy)
	}
}

func copyWorkflowContractFixture(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(dst), err)
	}
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

func projectRootFromWorkflowNodesTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}
