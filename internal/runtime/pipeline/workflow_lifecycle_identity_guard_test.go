package pipeline

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestWorkflowLifecycleIdentityConsumersDoNotReintroduceFallbacks(t *testing.T) {
	files := []string{
		"workflow_lifecycle_plan.go",
		"workflow_timer_owner.go",
		"workflow_join_lifecycle.go",
		"workflow_gate_lifecycle.go",
		"workflow_gate_decision.go",
		"workflow_gate_terminal.go",
		"workflow_state_persistence.go",
		"select_entity.go",
		"workflow_instance_route_recovery.go",
	}
	forbidden := []string{
		"StoredFlowInstance(",
		"workflowTimerCanonicalEntityID",
		"FlowInstanceEntityID(selected.StorageRef)",
		"func (pc *PipelineCoordinator) applyWorkflowJoinIntents",
		"func (pc *PipelineCoordinator) applyWorkflowGateIntents",
		"func (pc *PipelineCoordinator) reconcileClosedJoinSchedules",
		"func (pc *PipelineCoordinator) reconcileSupersededLoopSchedules",
		"func (l *WorkflowTimerLifecycle) CancelSupersededGenerations",
	}
	for _, name := range files {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		for _, token := range forbidden {
			if strings.Contains(string(raw), token) {
				t.Errorf("%s reintroduced non-authoritative lifecycle identity path %q", name, token)
			}
		}
	}

	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate workflow lifecycle identity guard")
	}
	pipelineDir := filepath.Dir(current)
	checks := []struct {
		file      string
		function  string
		forbidden []string
		required  []string
	}{
		{file: filepath.Join(pipelineDir, "workflow_join_lifecycle.go"), function: "workflowJoinPlansForStage", forbidden: []string{"WorkflowName"}, required: []string{"runtimeflowidentity.ScopeKey"}},
		{file: filepath.Join(pipelineDir, "workflow_join_lifecycle.go"), function: "joinSchedule", forbidden: []string{"WorkflowName", "NewJoinRef", "ParseJoinRef"}, required: []string{"activation.TimerHandle()", "handle.JoinRef"}},
		{file: filepath.Join(pipelineDir, "workflow_lifecycle_plan.go"), function: "planWorkflowJoinEffect", forbidden: []string{"instance.WorkflowName"}, required: []string{"NewJoinRefForGeneration", "joinruntime.NewActivation"}},
		{file: filepath.Join(pipelineDir, "workflow_join_resolution.go"), function: "resolveWorkflowJoinOccurrence", forbidden: []string{"WorkflowName", "ParseTimerHandle", "WorkflowJoinPlanForHandler"}, required: []string{"ParseJoinHandle", "WorkflowJoinPlanForRef"}},
		{file: filepath.Join(pipelineDir, "workflow_join_resolution.go"), function: "ResolveWorkflowJoinOccurrenceDeliveryTarget", forbidden: []string{"WorkflowName", "NodeContractSource", "RuntimeEventOwners"}, required: []string{"resolveWorkflowJoinOccurrence", "RootExecutionFlowID", "NewDeliveryTargetHandler"}},
		{file: filepath.Join(pipelineDir, "workflow_nodes.go"), function: "workflowNodeEventHandlerResolutionForDeliveryContext", forbidden: []string{"NodeContractSource", "RuntimeEventOwners"}, required: []string{"resolveWorkflowJoinOccurrence", "RootExecutionFlowID"}},
		{file: filepath.Join(pipelineDir, "delivery_target_ownership.go"), function: "ClassifyDeliveryTargetOwnership", forbidden: []string{"ParseJoinHandle", "WorkflowJoinPlanForHandler"}, required: []string{"ResolveWorkflowJoinOccurrenceDeliveryTarget"}},
		{file: filepath.Join(pipelineDir, "delivery_target_application.go"), function: "prepareDeliveryTargetApplication", forbidden: []string{"ParseJoinHandle", "WorkflowJoinPlanForHandler"}, required: []string{"resolveWorkflowJoinOccurrence", "declarationBoundTarget"}},
		{file: filepath.Join(pipelineDir, "..", "bus", "delivery_planner.go"), function: "planAtGeneration", forbidden: []string{"ParseJoinHandle", "WorkflowJoinPlanForHandler", "RuntimeEventOwners"}, required: []string{"ResolveWorkflowJoinOccurrenceDeliveryTarget"}},
		{file: filepath.Join(pipelineDir, "workflow_join_resolution.go"), function: "workflowJoinDeclarationRef", forbidden: []string{"WorkflowName", "NodeContractSource", "candidates"}, required: []string{"WorkflowJoinPlanForExecutionHandler", "WorkflowJoinPlanForRef"}},
		{file: filepath.Join(pipelineDir, "workflow_join_resolution.go"), function: "workflowJoinDeclarationForExecution", forbidden: []string{"WorkflowName", "WorkflowJoinPlanForHandler"}, required: []string{"resolveWorkflowJoinOccurrence", "workflowJoinDeclarationRef"}},
		{file: filepath.Join(pipelineDir, "engine_bridge.go"), function: "executeNodeContractHandler", forbidden: []string{"WorkflowJoinPlanForHandler", "ParseJoinHandle"}, required: []string{"workflowJoinDeclarationForExecution", "JoinDeclaration:"}},
		{file: filepath.Join(pipelineDir, "node_declarative.go"), function: "ExecuteHandlerSteps", forbidden: []string{"WorkflowJoinPlanForHandler", "ParseJoinHandle"}, required: []string{"workflowJoinDeclarationForExecution", "JoinDeclaration:"}},
		{file: filepath.Join(pipelineDir, "..", "engine", "executor.go"), function: "joinPlan", forbidden: []string{"WorkflowName", "WorkflowJoinPlanForHandler", "NewJoinRef"}, required: []string{"WorkflowJoinPlanForRef", "JoinDeclaration.Valid"}},
	}
	for _, check := range checks {
		t.Run(filepath.Base(check.file)+"/"+check.function, func(t *testing.T) {
			body := workflowLifecycleFunctionSource(t, check.file, check.function)
			for _, token := range check.forbidden {
				if strings.Contains(body, token) {
					t.Fatalf("%s reintroduced forbidden join identity authority %q:\n%s", check.function, token, body)
				}
			}
			for _, token := range check.required {
				if !strings.Contains(body, token) {
					t.Fatalf("%s stopped consuming canonical join identity owner %q:\n%s", check.function, token, body)
				}
			}
		})
	}
}

func workflowLifecycleFunctionSource(t *testing.T, path, name string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != name {
			continue
		}
		start := set.Position(function.Pos()).Offset
		end := set.Position(function.End()).Offset
		return string(raw[start:end])
	}
	t.Fatalf("function %s not found in %s", name, path)
	return ""
}
