package pipeline

import (
	"os"
	"path/filepath"
	"testing"

	runtimecontracts "empireai/internal/runtime/contracts"
)

func TestValidateWorkflowContracts_CurrentBundle(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(pipelineTestBus{}, nil)
	if err := pc.ValidateWorkflowContracts(); err != nil {
		t.Fatalf("expected current workflow contracts to validate: %v", err)
	}
}

func TestValidateWorkflowContracts_CurrentRootBundleFixture(t *testing.T) {
	repoRoot := workflowValidationProjectRoot(t)
	tmp := t.TempDir()
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"), filepath.Join(tmp, "contracts", "workflow-schema.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"), filepath.Join(tmp, "contracts", "guard-action-registry.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"), filepath.Join(tmp, "contracts", "system-nodes.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"), filepath.Join(tmp, "contracts", "event-catalog.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"), filepath.Join(tmp, "contracts", "agent-tools.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"), filepath.Join(tmp, "contracts", "tool-schemas.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"), filepath.Join(tmp, "contracts", "prompt-variables.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"), filepath.Join(tmp, "contracts", "platform", "platform-spec.yaml"))

	bundle, err := runtimecontracts.LoadWorkflowContractBundle(tmp)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	if err := ValidateWorkflowContracts(bundle); err != nil {
		t.Fatalf("expected current root workflow contracts to validate: %v", err)
	}
}

func TestValidateWorkflowContracts_FailsWhenOwningNodeHasNoRuntimeExecutor(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(workflowValidationProjectRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	entry := bundle.Events["build_complete"]
	entry.OwningNode = "unsupported-orchestrator"
	bundle.Events["build_complete"] = entry
	bundle.Nodes["unsupported-orchestrator"] = runtimecontracts.SystemNodeContract{
		ID:             "unsupported-orchestrator",
		ExecutionType:  "system_node",
		Implementation: "internal/runtime/pipeline/unsupported_orchestrator.go",
		SubscribesTo:   []string{"build_complete"},
	}
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for unsupported runtime owning_node")
	}
}

func TestValidateWorkflowContracts_FailsWhenOwningNodeMissingSemanticEventHandler(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(workflowValidationProjectRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	entry := bundle.Events["build_complete"]
	entry.OwningNode = "lifecycle-orchestrator"
	bundle.Events["build_complete"] = entry
	handlers := bundle.Semantics.NodeHandlers["lifecycle-orchestrator"]
	delete(handlers, "build_complete")
	bundle.Semantics.NodeHandlers["lifecycle-orchestrator"] = handlers
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for missing semantic event_handler on owning_node")
	}
}

func TestValidateWorkflowContracts_FailsWhenDataAccumulationWritesFieldOutsideEntitySchema(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(workflowValidationProjectRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	transitions := bundle.Workflow.Workflow.Transitions
	if len(transitions) == 0 {
		t.Fatal("expected workflow transitions")
	}
	transitions[0].DataAccumulation.Writes = append(transitions[0].DataAccumulation.Writes, "not_in_entity_schema")
	bundle.Workflow.Workflow.Transitions = transitions
	bundle.Semantics.Transitions = append([]runtimecontracts.WorkflowTransitionContract{}, transitions...)
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for undeclared entity_schema write")
	}
}

func TestValidateWorkflowContracts_FailsWhenWritePinHasMultipleOwners(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(workflowValidationProjectRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	bundle.Semantics.WritePinOwners["shared_pin"] = []string{"validation", "operating"}
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for duplicate write-pin ownership")
	}
}

func TestValidateWorkflowContracts_FailsWhenRequiredAgentMissing(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(workflowValidationProjectRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	bundle.Semantics.FlowAgents["validation"] = append(bundle.Semantics.FlowAgents["validation"], runtimecontracts.FlowRequiredAgent{
		Role:         "missing_role",
		SubscribesTo: []string{"validation.started"},
		Emits:        []string{"research.completed"},
	})
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for missing required agent role")
	}
}

func TestValidateWorkflowContracts_FailsWhenRequiredAgentDoesNotFulfillSubscriptionsAndEmits(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(workflowValidationProjectRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	bundle.Semantics.FlowAgents["validation"] = []runtimecontracts.FlowRequiredAgent{{
		Role:         "researcher",
		SubscribesTo: []string{"validation.started", "missing.event"},
		Emits:        []string{"research.completed", "missing.emit"},
	}}
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for unmet required agent contract")
	}
}

func TestValidateWorkflowContracts_FailsWhenFlowInitialStateMissingFromStates(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(packageAwareWorkflowValidationFixtureRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	bundle.Semantics.FlowInitial["validation"] = "missing_state"
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for invalid flow initial_state")
	}
}

func TestValidateWorkflowContracts_FailsWhenFlowOutputEventMissingFromEventCatalog(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(packageAwareWorkflowValidationFixtureRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	bundle.Semantics.FlowOutputs["validation"] = append(bundle.Semantics.FlowOutputs["validation"], "missing.output.event")
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for missing flow output event")
	}
}

func TestValidateWorkflowContracts_FailsWhenFlowNamespaceMissing(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(packageAwareWorkflowValidationFixtureRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	bundle.Semantics.FlowNamespace["validation"] = ""
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for missing flow namespace")
	}
}

func TestValidateWorkflowContracts_FailsWhenHandlerAdvancesOutsideFlowStates(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(packageAwareWorkflowValidationFixtureRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	handlers := bundle.Semantics.NodeHandlers["validation-orchestrator"]
	handler := handlers["spec.approved"]
	handler.AdvancesTo = "missing_state"
	handlers["spec.approved"] = handler
	bundle.Semantics.NodeHandlers["validation-orchestrator"] = handlers
	transition := bundle.Semantics.HandlerTransitionIndex["validation-orchestrator"]["spec.approved"]
	transition.AdvancesTo = "missing_state"
	bundle.Semantics.HandlerTransitionIndex["validation-orchestrator"]["spec.approved"] = transition
	for i := range bundle.Semantics.HandlerTransitions {
		if bundle.Semantics.HandlerTransitions[i].ID == transition.ID {
			bundle.Semantics.HandlerTransitions[i].AdvancesTo = "missing_state"
		}
	}
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for handler advances_to outside flow states")
	}
}

func TestValidateWorkflowContracts_FailsWhenHandlerSourceEventMismatches(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(packageAwareWorkflowValidationFixtureRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	handlers := bundle.Semantics.NodeHandlers["validation-orchestrator"]
	handler := handlers["spec.approved"]
	handler.DataAccumulation.SourceEvent = "wrong.event"
	handlers["spec.approved"] = handler
	bundle.Semantics.NodeHandlers["validation-orchestrator"] = handlers
	transition := bundle.Semantics.HandlerTransitionIndex["validation-orchestrator"]["spec.approved"]
	transition.DataAccumulation.SourceEvent = "wrong.event"
	bundle.Semantics.HandlerTransitionIndex["validation-orchestrator"]["spec.approved"] = transition
	for i := range bundle.Semantics.HandlerTransitions {
		if bundle.Semantics.HandlerTransitions[i].ID == transition.ID {
			bundle.Semantics.HandlerTransitions[i].DataAccumulation.SourceEvent = "wrong.event"
		}
	}
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for handler source_event mismatch")
	}
}

func TestValidateWorkflowContracts_FailsWhenHandlerSetsUnknownGate(t *testing.T) {
	bundle, err := runtimecontracts.LoadWorkflowContractBundle(packageAwareWorkflowValidationFixtureRoot(t))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundle() error = %v", err)
	}
	handlers := bundle.Semantics.NodeHandlers["validation-orchestrator"]
	handler := handlers["spec.approved"]
	handler.SetsGate = "missing_gate"
	handlers["spec.approved"] = handler
	bundle.Semantics.NodeHandlers["validation-orchestrator"] = handlers
	transition := bundle.Semantics.HandlerTransitionIndex["validation-orchestrator"]["spec.approved"]
	transition.SetsGate = "missing_gate"
	bundle.Semantics.HandlerTransitionIndex["validation-orchestrator"]["spec.approved"] = transition
	for i := range bundle.Semantics.HandlerTransitions {
		if bundle.Semantics.HandlerTransitions[i].ID == transition.ID {
			bundle.Semantics.HandlerTransitions[i].SetsGate = "missing_gate"
		}
	}
	if err := ValidateWorkflowContracts(bundle); err == nil {
		t.Fatal("expected validation failure for unknown handler gate")
	}
}

func copyWorkflowValidationFixture(t *testing.T, src, dst string) {
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

func workflowValidationProjectRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func packageAwareWorkflowValidationFixtureRoot(t *testing.T) string {
	t.Helper()
	repoRoot := workflowValidationProjectRoot(t)
	tmp := t.TempDir()
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "workflow-schema.yaml"), filepath.Join(tmp, "contracts", "workflow-schema.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "guard-action-registry.yaml"), filepath.Join(tmp, "contracts", "guard-action-registry.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "system-nodes.yaml"), filepath.Join(tmp, "contracts", "system-nodes.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "event-catalog.yaml"), filepath.Join(tmp, "contracts", "event-catalog.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "agent-tools.yaml"), filepath.Join(tmp, "contracts", "agent-tools.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "tool-schemas.yaml"), filepath.Join(tmp, "contracts", "tool-schemas.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "prompt-variables.yaml"), filepath.Join(tmp, "contracts", "prompt-variables.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(repoRoot, "contracts", "platform", "platform-spec.yaml"), filepath.Join(tmp, "contracts", "platform", "platform-spec.yaml"))
	base := filepath.Join(repoRoot, "docs", "specs", "empireai-v2_6_0", "contracts-v250", "empire")
	copyWorkflowValidationFixture(t, filepath.Join(base, "package.yaml"), filepath.Join(tmp, "contracts", "empire", "package.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(base, "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "nodes.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(base, "events.yaml"), filepath.Join(tmp, "contracts", "empire", "events.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(base, "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "agents.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(base, "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "policy.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(base, "tools.yaml"), filepath.Join(tmp, "contracts", "empire", "tools.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(base, "runtime", "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "nodes.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(base, "runtime", "events.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "events.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(base, "runtime", "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "agents.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(base, "runtime", "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "policy.yaml"))
	copyWorkflowValidationFixture(t, filepath.Join(base, "runtime", "tools.yaml"), filepath.Join(tmp, "contracts", "empire", "runtime", "tools.yaml"))
	for _, flow := range []string{"discovery", "scoring", "validation", "operating"} {
		copyWorkflowValidationFixture(t, filepath.Join(base, "flows", flow, "schema.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "schema.yaml"))
		copyWorkflowValidationFixture(t, filepath.Join(base, "flows", flow, "nodes.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "nodes.yaml"))
		copyWorkflowValidationFixture(t, filepath.Join(base, "flows", flow, "events.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "events.yaml"))
		copyWorkflowValidationFixture(t, filepath.Join(base, "flows", flow, "agents.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "agents.yaml"))
		copyWorkflowValidationFixture(t, filepath.Join(base, "flows", flow, "policy.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "policy.yaml"))
		if _, err := os.Stat(filepath.Join(base, "flows", flow, "tools.yaml")); err == nil {
			copyWorkflowValidationFixture(t, filepath.Join(base, "flows", flow, "tools.yaml"), filepath.Join(tmp, "contracts", "empire", "flows", flow, "tools.yaml"))
		}
	}
	return tmp
}
