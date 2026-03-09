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
	if !strings.Contains(string(workflowNodesRuntime), "Scoring is still a dedicated runtime node") {
		t.Fatal("workflow_nodes_runtime.go must document the current scoring-node architectural exception")
	}

	scoringNode, err := os.ReadFile(filepath.Join(".", "scoring_node.go"))
	if err != nil {
		t.Fatalf("read scoring_node.go: %v", err)
	}
	for _, needle := range []string{
		`"vertical.discovered"`,
		`"vertical.derived"`,
		`"score.dimension_complete"`,
		`"scoring.contest_resolved"`,
	} {
		if !strings.Contains(string(scoringNode), needle) {
			t.Fatalf("scoring_node.go must remain the explicit owner of %s until scoring is unified", needle)
		}
	}

	runtimeMain, err := os.ReadFile(filepath.Join("..", "runtime.go"))
	if err != nil {
		t.Fatalf("read ../runtime.go: %v", err)
	}
	if !strings.Contains(string(runtimeMain), "NewScoringNode(rt.Bus, rt.Pipeline, stores.SQLDB)") {
		t.Fatal("runtime.go must wire the dedicated scoring node until the architecture is intentionally changed")
	}
}
