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

func TestPipelineArchitecture_NoEmpireTaxonomyInGenericProductionFiles(t *testing.T) {
	t.Helper()

	forbidden := []string{
		"empire-",
		"empire_",
		"Empire",
		"saas_gap",
		"saas_trend",
		"local_services",
		"automation_micro",
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
		text := string(data)
		for _, token := range forbidden {
			if strings.Contains(text, token) {
				t.Fatalf("%s still contains Empire/taxonomy token %q; move product logic into pipeline/empire or productpolicy/empire", base, token)
			}
		}
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
	if !strings.Contains(string(workflowNodesRuntime), "scoringTransitionExecutor") {
		t.Fatal("workflow_nodes_runtime.go must keep an explicit scoring transition executor in the active runtime model")
	}

	scoringRuntime, err := os.ReadFile(filepath.Join(".", "workflow_node_scoring.go"))
	if err != nil {
		t.Fatalf("read workflow_node_scoring.go: %v", err)
	}
	if !strings.Contains(string(scoringRuntime), "newBackgroundWorkflowNode(executor, bus, db)") {
		t.Fatal("workflow_node_scoring.go must build on the generic background workflow node wrapper until scoring is fully unified")
	}
	if !strings.Contains(string(scoringRuntime), "type scoringTransitionExecutor struct") {
		t.Fatal("workflow_node_scoring.go must keep an explicit scoring transition executor to separate intercept handling from background scoring")
	}
	if !strings.Contains(string(scoringRuntime), "type scoringBackgroundExecutor struct") {
		t.Fatal("workflow_node_scoring.go must keep an explicit scoring background executor for non-intercept scoring events")
	}
	if !strings.Contains(string(scoringRuntime), "func scoringTransitionSubscriptions()") || !strings.Contains(string(scoringRuntime), "func scoringBackgroundSubscriptions()") {
		t.Fatal("workflow_node_scoring.go must keep explicit transition/background scoring subscription sets while scoring remains split")
	}
	if !strings.Contains(string(scoringRuntime), `case "vertical.scored":`) {
		t.Fatal("scoring transition executor must still own vertical.scored interception")
	}
	if strings.Contains(string(scoringRuntime), "func scoringTransitionSubscriptions() []events.EventType {\n\treturn []events.EventType{\n\t\tevents.EventType(\"vertical.derived\")") {
		t.Fatal("scoring transition executor subscriptions should no longer include vertical.derived once derivation is background-owned")
	}
	if !strings.Contains(string(scoringRuntime), `case "vertical.discovered":`) || !strings.Contains(string(scoringRuntime), `case "score.dimension_complete":`) || !strings.Contains(string(scoringRuntime), `case "scoring.contest_resolved":`) {
		t.Fatal("scoring background executor must own the background scoring events")
	}
	if !strings.Contains(string(scoringRuntime), `case "vertical.derived":`) {
		t.Fatal("scoring background executor must own vertical.derived while the derivation loop remains background-driven")
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
	if !strings.Contains(string(workflowNodesRuntime), "newScoringTransitionExecutor(pc)") {
		t.Fatal("workflow_nodes_runtime.go must resolve scoring through the explicit transition executor in the split model")
	}
	if strings.Contains(string(workflowNodesRuntime), "out = append(out, pc.scoringState)") {
		t.Fatal("workflow_nodes_runtime.go must not append ScoringState directly into workflowNodeExecutors; use the explicit scoring transition executor")
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
			allowed: map[string]struct{}{},
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
