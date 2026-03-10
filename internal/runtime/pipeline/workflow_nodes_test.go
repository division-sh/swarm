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

func TestLoadWorkflowNodes_CurrentRootUsesOwningNodeAndDualDeliveryPolicy(t *testing.T) {
	repoRoot := projectRootFromWorkflowNodesTest(t)
	tmp := t.TempDir()
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"), filepath.Join(tmp, "contracts", "workflow-schema.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"), filepath.Join(tmp, "contracts", "guard-action-registry.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"), filepath.Join(tmp, "contracts", "system-nodes.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"), filepath.Join(tmp, "contracts", "event-catalog.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"), filepath.Join(tmp, "contracts", "agent-tools.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"), filepath.Join(tmp, "contracts", "tool-schemas.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"), filepath.Join(tmp, "contracts", "prompt-variables.yaml"))
	copyWorkflowContractFixture(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"), filepath.Join(tmp, "contracts", "platform", "platform-spec.yaml"))

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
			t.Fatalf("expected %s in current root workflow nodes", nodeID)
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
	if _, ok := index["validation-orchestrator"].Policies["spec.approved"]; !ok {
		t.Fatal("expected spec.approved policy for validation-orchestrator from semantic handler ownership")
	}
}

func TestLoadWorkflowNodes_CurrentRootUsesFlowPinSemanticsForOwnedFlowEvents(t *testing.T) {
	repoRoot := workflowNodesProjectRoot(t)
	tmp := t.TempDir()
	copyWorkflowNodesFixture(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"), filepath.Join(tmp, "contracts", "workflow-schema.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"), filepath.Join(tmp, "contracts", "guard-action-registry.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"), filepath.Join(tmp, "contracts", "system-nodes.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"), filepath.Join(tmp, "contracts", "event-catalog.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"), filepath.Join(tmp, "contracts", "agent-tools.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"), filepath.Join(tmp, "contracts", "tool-schemas.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"), filepath.Join(tmp, "contracts", "prompt-variables.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"), filepath.Join(tmp, "contracts", "platform", "platform-spec.yaml"))
	base := filepath.Join(repoRoot, "docs", "specs", "empireai-v2_6_0", "contracts-v250", "empire")
	copyWorkflowNodesFixture(t, filepath.Join(base, "package.yaml"), filepath.Join(tmp, "contracts", "empire", "package.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(base, "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "nodes.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(base, "events.yaml"), filepath.Join(tmp, "contracts", "empire", "events.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(base, "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "agents.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(base, "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "policy.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(base, "tools.yaml"), filepath.Join(tmp, "contracts", "empire", "tools.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(base, "runtime", "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "nodes.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(base, "runtime", "events.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "events.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(base, "runtime", "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "agents.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(base, "runtime", "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "policy.yaml"))
	copyWorkflowNodesFixture(t, filepath.Join(base, "runtime", "tools.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "tools.yaml"))
	for _, flow := range []string{"discovery", "scoring", "validation", "operating"} {
		copyWorkflowNodesFixture(t, filepath.Join(base, "flows", flow, "schema.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "schema.yaml"))
		copyWorkflowNodesFixture(t, filepath.Join(base, "flows", flow, "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "nodes.yaml"))
		copyWorkflowNodesFixture(t, filepath.Join(base, "flows", flow, "events.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "events.yaml"))
		copyWorkflowNodesFixture(t, filepath.Join(base, "flows", flow, "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "agents.yaml"))
		copyWorkflowNodesFixture(t, filepath.Join(base, "flows", flow, "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "policy.yaml"))
		if _, err := os.Stat(filepath.Join(base, "flows", flow, "tools.yaml")); err == nil {
			copyWorkflowNodesFixture(t, filepath.Join(base, "flows", flow, "tools.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "tools.yaml"))
		}
	}

	bundle, err := runtimecontracts.LoadWorkflowContractBundle(tmp)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	nodes, err := LoadWorkflowNodes(bundle)
	if err != nil {
		t.Fatalf("LoadWorkflowNodes() error = %v", err)
	}
	byID := map[string]WorkflowNode{}
	for _, node := range nodes {
		byID[node.ID] = node
	}
	validationNode, ok := byID["validation-orchestrator"]
	if !ok {
		t.Fatal("expected validation-orchestrator node")
	}
	if _, ok := validationNode.Policies["brand.candidates_ready"]; !ok {
		t.Fatal("expected validation-orchestrator to see brand.candidates_ready through flow pin semantics")
	}
}

func TestWorkflowNodeTransitionTriggers_UsesDerivedHandlerAdvancesTo(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			HandlerTransitions: []runtimecontracts.HandlerTransitionSemantic{{
				ID:         "validation-orchestrator:spec.approved",
				NodeID:     "validation-orchestrator",
				EventType:  "spec.approved",
				AdvancesTo: "cto_spec_review",
			}},
		},
	}
	got := workflowNodeTransitionTriggers(bundle, "validation-orchestrator")
	if !got["spec.approved"] {
		t.Fatal("expected derived handler advances_to event to be treated as a transition trigger")
	}
}

func TestWorkflowRuntimeNodeIDs_UsesDerivedHandlerAdvancesTo(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"validation-orchestrator": {ID: "validation-orchestrator"},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			HandlerTransitions: []runtimecontracts.HandlerTransitionSemantic{{
				ID:         "validation-orchestrator:spec.approved",
				NodeID:     "validation-orchestrator",
				EventType:  "spec.approved",
				AdvancesTo: "cto_spec_review",
			}},
		},
	}
	got := workflowRuntimeNodeIDs(bundle)
	if len(got) != 1 || got[0] != "validation-orchestrator" {
		t.Fatalf("workflowRuntimeNodeIDs() = %v, want [validation-orchestrator]", got)
	}
}

func workflowNodesProjectRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func copyWorkflowNodesFixture(t *testing.T, src, dst string) {
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
