package pipeline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPipelineArchitecture_EmpireBoundary(t *testing.T) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(".", "*.go"))
	if err != nil {
		t.Fatalf("glob pipeline files: %v", err)
	}
	for _, path := range matches {
		base := filepath.Base(path)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", base, err)
		}
		if !strings.Contains(string(data), "internal/runtime/pipeline/empire") {
			continue
		}
		t.Fatalf("%s imports pipeline/empire; keep Empire-specific policy out of generic pipeline files", base)
	}
}

func TestPipelineArchitecture_GenericTestsOnlyUseEmpireViaDefaultModuleBridge(t *testing.T) {
	t.Helper()

	matches, err := filepath.Glob(filepath.Join(".", "*_test.go"))
	if err != nil {
		t.Fatalf("glob pipeline test files: %v", err)
	}
	for _, path := range matches {
		base := filepath.Base(path)
		if base == "pipeline_architecture_test.go" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", base, err)
		}
		if !strings.Contains(string(data), "internal/runtime/pipeline/empire") {
			continue
		}
		if base == "module_default_test.go" {
			continue
		}
		t.Fatalf("%s imports pipeline/empire; keep Empire-specific tests under pipeline/empire and reserve module_default_test.go for the generic test bridge", base)
	}
}

func TestPipelineArchitecture_SplitExecutorsDoNotRouteBackThroughCoordinatorWrappers(t *testing.T) {
	t.Helper()

	checks := map[string][]string{
		"scan_orchestrator.go": {
			".coordinator.handleScanRequested(",
			".coordinator.handleScanCompletion(",
		},
		"discovery_aggregator.go": {
			".scanCoordinator.handleDiscoveryReport(",
			".scanCoordinator.handleDedupResolved(",
			".scanCoordinator.handleSynthesisResolved(",
		},
		"validation_orchestrator.go": {
			".validationGate.handleValidationStarted(",
			".validationGate.handleValidationGate(",
			".validationGate.handleCTOApproved(",
			".validationGate.handleSpecValidationPassed(",
			".validationGate.handleSpecValidationFailed(",
			".validationGate.handleCTORevisionNeeded(",
			".validationGate.handleValidationRejected(",
			".validationGate.handleValidationPackaged(",
			".validationGate.handleValidationMoreData(",
			".validationGate.handleBrandRevision(",
			".validationGate.handleSpecRevisionRequested(",
			".validationGate.handleInnerSpecRevision(",
		},
		"lifecycle_orchestrator.go": {
			".validationGate.handleVerticalApproved(",
			".validationGate.handleVerticalKilled(",
			".validationGate.handleVerticalResumed(",
			".validationGate.handleOpCoCEOReady(",
			".coordinator.forwardPortfolioDigestTimer(",
			".coordinator.resetWorkflowRuntimeState(",
			".coordinator.handleLifecycleBudgetThreshold(",
			".coordinator.handleLifecycleMailboxDecision(",
			".coordinator.persistLifecycleEvidence(",
			".coordinator.forwardSystemDirective(",
			".coordinator.applyLifecycleStageEvent(",
			".coordinator.applyLifecycleTeardownEvent(",
			".coordinator.forwardMarginalReviewTimer(",
		},
	}

	for file, forbidden := range checks {
		data, err := os.ReadFile(filepath.Join(".", file))
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		src := string(data)
		for _, needle := range forbidden {
			if strings.Contains(src, needle) {
				t.Fatalf("%s routes back through legacy coordinator wrapper %q; call the split executor's node-owned method directly", file, needle)
			}
		}
	}
}

func TestPipelineArchitecture_ScoringExceptionIsExplicit(t *testing.T) {
	t.Helper()

	workflowNodesRuntime, err := os.ReadFile(filepath.Join(".", "workflow_nodes_runtime.go"))
	if err != nil {
		t.Fatalf("read workflow_nodes_runtime.go: %v", err)
	}
	if !strings.Contains(string(workflowNodesRuntime), "Scoring still keeps a dedicated background subscriber") {
		t.Fatal("workflow_nodes_runtime.go must document the current reduced scoring architectural exception")
	}

	scoringNode, err := os.ReadFile(filepath.Join(".", "scoring_node.go"))
	if err != nil {
		t.Fatalf("read scoring_node.go: %v", err)
	}
	if !strings.Contains(string(scoringNode), "newBackgroundWorkflowNode(executor, bus, db)") {
		t.Fatal("scoring_node.go must build on the generic background workflow node wrapper until scoring is fully unified")
	}
	scoringRuntime, err := os.ReadFile(filepath.Join(".", "workflow_node_scoring.go"))
	if err != nil {
		t.Fatalf("read workflow_node_scoring.go: %v", err)
	}
	if !strings.Contains(string(scoringRuntime), "workflowNodeSubscriptions(ScoringNodeID)") {
		t.Fatal("scoring scoring executor must still derive its subscription surface from the scoring node contract until scoring is unified")
	}

	runtimeMain, err := os.ReadFile(filepath.Join("..", "runtime.go"))
	if err != nil {
		t.Fatalf("read ../runtime.go: %v", err)
	}
	if !strings.Contains(string(runtimeMain), "rt.Pipeline.BackgroundNodes(rt.Bus, stores.SQLDB)") {
		t.Fatal("runtime.go must source background system nodes from pipeline-owned assembly until scoring is fully unified")
	}
	if strings.Contains(string(runtimeMain), "NewScoringNode(rt.Bus, rt.Pipeline, stores.SQLDB)") {
		t.Fatal("runtime.go should no longer construct a dedicated scoring node directly in production")
	}
	workflowNodesRuntime, err = os.ReadFile(filepath.Join(".", "workflow_nodes_runtime.go"))
	if err != nil {
		t.Fatalf("read workflow_nodes_runtime.go: %v", err)
	}
	if !strings.Contains(string(workflowNodesRuntime), `strings.TrimSpace(node.ExecutionType) != "workflow_node"`) {
		t.Fatal("workflow_nodes_runtime.go must assemble background nodes from contract execution_type rather than scoring-specific checks")
	}
	if strings.Contains(string(workflowNodesRuntime), "case ScoringNodeID:") {
		t.Fatal("workflow_nodes_runtime.go should no longer special-case scoring when resolving background workflow executors")
	}
}

func TestPipelineArchitecture_CompatibilityBucketsStayIsolated(t *testing.T) {
	t.Helper()

	type bucketCheck struct {
		token   string
		allowed map[string]struct{}
	}

	checks := []bucketCheck{
		{
			token: `"scoring-restore"`,
			allowed: map[string]struct{}{
				"workflow_instance_projection.go": {},
			},
		},
		{
			token: `"scoring-state"`,
			allowed: map[string]struct{}{},
		},
		{
			token: `"pipeline-coordinator"`,
			allowed: map[string]struct{}{
				"coordinator.go":                   {},
				"coordinator_projection.go":        {},
				"coordinator_validation.go":        {},
				"coordinator_workflow_projection.go": {},
				"workflow_contract_validation.go":  {},
				"workflow_node_scan.go":            {},
				"workflow_node_validation.go":      {},
				"workflow_nodes.go":                {},
				"workflow_nodes_runtime.go":        {},
			},
		},
	}

	matches, err := filepath.Glob(filepath.Join(".", "*.go"))
	if err != nil {
		t.Fatalf("glob pipeline files: %v", err)
	}
	for _, path := range matches {
		base := filepath.Base(path)
		if strings.HasSuffix(base, "_test.go") {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", base, err)
		}
		src := string(data)
		for _, check := range checks {
			if !strings.Contains(src, check.token) {
				continue
			}
			if _, ok := check.allowed[base]; !ok {
				t.Fatalf("%s still references deprecated compatibility token %s outside the allowed migration files", base, check.token)
			}
		}
	}
}
