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

func TestDurableHandlerConsumersStayOnDeliveryTargetApplication(t *testing.T) {
	_, current, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate delivery target application guard")
	}
	dir := filepath.Dir(current)
	checks := []struct {
		file      string
		function  string
		required  []string
		forbidden []string
	}{
		{
			file: filepath.Join(dir, "coordinator.go"), function: "executeNodeHandlerPlanResultWithEmissionPlan",
			required:  []string{"prepareDeliveryTargetApplication", "withDeliveryTargetApplication", "application.Event()", "application.State()"},
			forbidden: []string{"currentWorkflowState(executionCtx", "resolveHandlerEntityIDForFlow", "ensureHandlerEntityID"},
		},
		{
			file: filepath.Join(dir, "engine_bridge.go"), function: "executeNodeContractHandler",
			required:  []string{"deliveryTargetApplicationFromContext", "prepareDeliveryTargetApplication", "application.Event()", "application.State()"},
			forbidden: []string{"prepareStampedSelectOrCreateState", "handler.SelectEntity", "handler.SelectOrCreateEntity"},
		},
		{
			file: filepath.Join(dir, "engine_adapter.go"), function: "CommitEngineMutation",
			required:  []string{"deliveryTargetApplicationFromContext", "application.Validate()", "application.Route()", "application.EntityID()", "targetApplications..."},
			forbidden: []string{"resolveHandlerEntityIDForFlow", "ensureHandlerEntityID"},
		},
		{
			file: filepath.Join(dir, "engine_adapter.go"), function: "prepareMutation",
			required:  []string{"evaluatedWorkflowInstance", "engineMutation.EvaluatedState", "cloneWorkflowInstanceForEngineMutation(evaluated.instance)", "application.Validate()", "application.Route()", "application.EntityID()", "application.previewOnly()"},
			forbidden: []string{"loadCurrentDeliveryTargetState", "LoadTargetPersistence", "LoadState(", "workflowStore.Load(", "persistedInstance", "resolveHandlerEntityIDForFlow", "ensureHandlerEntityID"},
		},
		{
			file: filepath.Join(dir, "engine_adapter.go"), function: "LoadState",
			required:  []string{"deliveryTargetApplicationFromContext", "application.Validate()", "loadCurrentDeliveryTargetState", "application.Route()", "application.EntityID()"},
			forbidden: []string{"persistedSnapshot", "resolveHandlerEntityIDForFlow", "ensureHandlerEntityID"},
		},
		{
			file: filepath.Join(dir, "action_result_context.go"), function: "workflowNodeProducerSource",
			required:  []string{"deliveryTargetApplicationFromContext", "application.Validate()", "application.Owner()"},
			forbidden: []string{"resolveHandlerEntityIDForFlow", "ensureHandlerEntityID"},
		},
	}
	for _, check := range checks {
		t.Run(filepath.Base(check.file)+"/"+check.function, func(t *testing.T) {
			body := workflowLifecycleFunctionSource(t, check.file, check.function)
			for _, token := range check.required {
				if !strings.Contains(body, token) {
					t.Errorf("%s stopped consuming canonical delivery target application %q", check.function, token)
				}
			}
			for _, token := range check.forbidden {
				if strings.Contains(body, token) {
					t.Errorf("%s reintroduced stamped-target interpretation %q", check.function, token)
				}
			}
		})
	}
}

func TestReceiverElectionCannotReturnToHandlerExecution(t *testing.T) {
	for _, filename := range []string{"delivery_target_ownership.go", "delivery_target_application.go", "node_declarative.go", "engine_bridge.go"} {
		raw, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		for _, retired := range []string{"acquireDeliveryTargetByDeclaredKey", "acquireSelectOrCreateMaterializingTarget", "selectOrCreateEntityInstanceID", "SelectActiveWorkflowEntityStates", "handler.SelectEntity", "handler.SelectOrCreateEntity"} {
			if strings.Contains(string(raw), retired) {
				t.Errorf("%s restored retired receiver election %s", filename, retired)
			}
		}
	}
	if _, err := os.Stat("select_entity.go"); !os.IsNotExist(err) {
		t.Fatalf("retired selector interpreter exists: %v", err)
	}
}

func TestStampedTargetReaderCallsitesRemainFinite(t *testing.T) {
	allowed := map[string]struct{}{
		"engine_adapter.go": {},
		"engine_bridge.go":  {},
	}
	found := map[string]struct{}{}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if !ok || name.Name != "stampedDeliveryTargetOwnership" {
				return true
			}
			base := filepath.Base(path)
			found[base] = struct{}{}
			if _, ok := allowed[base]; !ok {
				t.Errorf("%s reads stamped target outside the finite application-consumer boundary", path)
			}
			return true
		})
	}
	for file := range allowed {
		if _, ok := found[file]; !ok {
			t.Errorf("expected stamped target consumer %s is missing; update the finite ledger deliberately", file)
		}
	}
}
