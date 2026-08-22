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
			required:  []string{"deliveryTargetApplicationFromContext", "prepareDeliveryTargetApplication", "application.Event()", "application.State()", "if !exactDelivery && handler.SelectEntity"},
			forbidden: []string{"prepareStampedSelectOrCreateState"},
		},
		{
			file: filepath.Join(dir, "node_declarative.go"), function: "ExecuteHandlerSteps",
			required:  []string{"deliveryTargetApplicationFromContext", "prepareDeliveryTargetApplication", "application.Event()", "application.State()", "if !exactDelivery && handler.SelectEntity"},
			forbidden: []string{"prepareStampedSelectOrCreateState"},
		},
		{
			file: filepath.Join(dir, "engine_adapter.go"), function: "CommitEngineMutation",
			required:  []string{"deliveryTargetApplicationFromContext", "application.Validate()", "application.Route()", "application.EntityID()", "targetApplications..."},
			forbidden: []string{"resolveHandlerEntityIDForFlow", "ensureHandlerEntityID"},
		},
		{
			file: filepath.Join(dir, "engine_adapter.go"), function: "prepareMutation",
			required:  []string{"application.persistedInstance()", "application.Route()", "application.EntityID()"},
			forbidden: []string{"resolveHandlerEntityIDForFlow", "ensureHandlerEntityID"},
		},
		{
			file: filepath.Join(dir, "engine_adapter.go"), function: "LoadState",
			required:  []string{"deliveryTargetApplicationFromContext", "application.Validate()", "application.persistedSnapshot()", "application.Route()", "application.EntityID()"},
			forbidden: []string{"resolveHandlerEntityIDForFlow", "ensureHandlerEntityID"},
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

func TestDeliveryTargetOwnershipRetiredFallbacksStayAbsent(t *testing.T) {
	raw, err := os.ReadFile("delivery_target_ownership.go")
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, retired := range []string{"handlerEntitylessSafe", "prepareStampedSelectOrCreateState", "sameFlow"} {
		if strings.Contains(source, retired) {
			t.Errorf("delivery target owner reintroduced retired interpreter %q", retired)
		}
	}
	classifier := workflowLifecycleFunctionSource(t, "delivery_target_ownership.go", "ClassifyDeliveryTargetOwnership")
	for _, required := range []string{"CompileDeliveryTargetCompatibilityPolicy", "acquireDeliveryTargetByDeclaredKey", "matchingDeliveryTargetOwnerCandidates"} {
		if !strings.Contains(classifier, required) {
			t.Errorf("target classifier stopped consuming canonical owner %q", required)
		}
	}
}

func TestRootExecutionIdentityConsumersStayCanonical(t *testing.T) {
	checks := []struct {
		file     string
		required []string
	}{
		{file: "workflow_instance_store.go", required: []string{"AdmitRootExecutionCoordinate"}},
		{file: "delivery_target_ownership.go", required: []string{"AdmitRootExecutionCoordinate"}},
		{file: "engine_adapter.go", required: []string{"AdmitRootExecutionCoordinate", "RootExecutionFlowID"}},
		{file: "workflow_gate_lifecycle.go", required: []string{"RootExecutionFlowID"}},
		{file: "workflow_loop_generation.go", required: []string{"RootExecutionFlowID"}},
		{file: "workflow_timer_owner.go", required: []string{"RootExecutionFlowID"}},
		{file: filepath.Join("..", "bus", "delivery_planner.go"), required: []string{"AdmitRootExecutionCoordinate"}},
		{file: filepath.Join("..", "bus", "eventbus_publish.go"), required: []string{"validateEventRootTargetCoordinates"}},
		{file: filepath.Join("..", "bus", "connect_"+"route_plan_dispatch.go"), required: []string{"AdmitRootExecutionCoordinate"}},
		{file: filepath.Join("..", "bus", "target_owner_projection.go"), required: []string{"AdmitRootExecutionCoordinate"}},
	}
	for _, check := range checks {
		t.Run(filepath.Base(check.file), func(t *testing.T) {
			raw, err := os.ReadFile(check.file)
			if err != nil {
				t.Fatal(err)
			}
			source := string(raw)
			for _, token := range check.required {
				if !strings.Contains(source, token) {
					t.Errorf("root identity consumer stopped using canonical owner %q", token)
				}
			}
			for _, retired := range []string{".WorkflowName()", "uuid.Parse"} {
				if strings.Contains(source, retired) {
					t.Errorf("root identity consumer reintroduced primitive authority %q", retired)
				}
			}
		})
	}
}

func TestDeclaredKeyAcquisitionConsumesEntityStateAuthority(t *testing.T) {
	acquisition := workflowLifecycleFunctionSource(t, "delivery_target_ownership.go", "acquireDeliveryTargetByDeclaredKey")
	for _, required := range []string{"SelectActiveWorkflowEntityStates", "decodeDeliveryTargetWorkflowEntityState"} {
		if !strings.Contains(acquisition, required) {
			t.Errorf("declared-key acquisition stopped consuming state authority %q", required)
		}
	}
	if strings.Contains(acquisition, "SelectActiveWorkflowInstances") {
		t.Error("declared-key acquisition reintroduced lifecycle-required selection")
	}
	classification := workflowLifecycleFunctionSource(t, "delivery_target_ownership.go", "ClassifyDeliveryTargetOwnership")
	if !strings.Contains(classification, "!req.Event.HasTargetRoute()") {
		t.Error("declared-key acquisition stopped requiring explicit target absence")
	}

	selectOrCreate := workflowLifecycleFunctionSource(t, "delivery_target_ownership.go", "acquireSelectOrCreateMaterializingTarget")
	for _, required := range []string{"LoadWorkflowInstance", "LoadWorkflowEntityState", "decodeDeliveryTargetWorkflowEntityState"} {
		if !strings.Contains(selectOrCreate, required) {
			t.Errorf("select-or-create exact-target validation stopped consuming %q", required)
		}
	}
}

func TestStampedTargetReaderCallsitesRemainFinite(t *testing.T) {
	allowed := map[string]struct{}{
		"engine_adapter.go":   {},
		"engine_bridge.go":    {},
		"node_declarative.go": {},
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
