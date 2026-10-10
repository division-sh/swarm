package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ast/astutil"
)

func nativeHandlerEmissionWorkload(t *testing.T, source string) string {
	t.Helper()
	body := projectionShapeFunction(t, source).Body
	astutil.Apply(body, func(cursor *astutil.Cursor) bool {
		if statement, ok := cursor.Node().(ast.Stmt); ok {
			text := formattedNativeReadNode(statement)
			if strings.HasPrefix(text, "pc, bus := newDeclarativeEmitContractCoordinator(") ||
				strings.HasPrefix(text, "pc, bus := newDeclarativeEmitContractCoordinatorWithBundle(") ||
				strings.HasPrefix(text, "fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, func(") {
				cursor.Delete()
				return false
			}
			if text == "bus := &recordingPipelineBus{}" ||
				text == "ctx := seedHandlerEngineExistingEntity(t, pc, entityID)" ||
				text == "fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityForTest(t, open, entityID)" ||
				text == "fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, open, module, entityID, nil)" ||
				text == formattedNativeReadNode(projectionShapeStatement(t, `pc := newPreviewPipelineCoordinatorForTest(bus,PipelineCoordinatorOptions{Module:canonicalPreviewWorkflowModuleForTest(&previewWorkflowModule{bundle:additiveOnSuccessContractBundle(t)})})`)) ||
				text == formattedNativeReadNode(projectionShapeStatement(t, `pc := newPreviewPipelineCoordinatorForTest(bus,PipelineCoordinatorOptions{Module:canonicalPreviewWorkflowModuleForTest(&previewWorkflowModule{bundle:rulesEmitTemplateContractBundle(t)})})`)) ||
				text == formattedNativeReadNode(projectionShapeStatement(t, `module := handlerTestWorkflowModuleWithBundle(additiveOnSuccessContractBundle(t),".","node-a")`)) ||
				text == formattedNativeReadNode(projectionShapeStatement(t, `module := handlerTestWorkflowModuleWithBundle(rulesEmitTemplateContractBundle(t),".","node-a")`)) ||
				text == formattedNativeReadNode(projectionShapeStatement(t, `pc := &PipelineCoordinator{bus:bus,expressionEval:newWorkflowExpressionEvaluator(),entityLocks:map[string]*sync.Mutex{},module:handlerEngineProjectNodeModule(t)}`)) ||
				text == formattedNativeReadNode(projectionShapeStatement(t, `if err := dispatchNativeHandlerFollowUpForTest(pc, ctx, bus, result.FollowUp); err != nil {t.Fatalf("dispatch committed follow-up: %v",err)}`)) ||
				text == formattedNativeReadNode(projectionShapeStatement(t, `if got := bus.publishedCount(); got != 1 {t.Fatalf("bus published count = %d, want 1 after deferred dispatch",got)}`)) {
				cursor.Delete()
				return false
			}
		}
		call, ok := cursor.Node().(*ast.CallExpr)
		if !ok {
			return true
		}
		switch formattedNativeReadNode(call.Fun) {
		case "executeNodeContractHandlerWithHandoff":
			call.Fun = ast.NewIdent("executeOriginalHandler")
		case "executeNativeHandlerEngineWithHandoffForTest":
			call.Fun = ast.NewIdent("executeOriginalHandler")
			args := []ast.Expr{call.Args[0], call.Args[2], call.Args[3], call.Args[5], call.Args[6], call.Args[7], ast.NewIdent("false")}
			call.Args = append(args, call.Args[8:]...)
		case "seedHandlerEngineExistingEntity":
			cursor.Replace(ast.NewIdent("ctx"))
			return false
		}
		return true
	}, nil)
	return formattedNativeReadNode(body)
}

func TestNativeHandlerEmissionRecipesRetainEnvelopeFieldsAndDeferredCut(t *testing.T) {
	for _, family := range []string{"native-handler-envelope-emission", "native-handler-deferred-emission", "native-handler-nested-emission", "native-handler-additive-emission", "native-handler-rule-template-emission", "native-handler-explicit-payload", "native-handler-escalation-empty", "native-handler-escalation-fields", "native-handler-undeclared-payload"} {
		row := nativeMissingHeaderRecipe(t, family)
		if nativeHandlerEmissionWorkload(t, row.Before) != nativeHandlerEmissionWorkload(t, row.After) {
			t.Fatalf("original complete handler, trigger, state, errors or dispatch assertions changed: %s", family)
		}
	}
	row := nativeMissingHeaderRecipe(t, "native-handler-deferred-emission")
	for _, required := range []string{"result.Committed", "len(result.FollowUp.Emissions) != 1", "want 0 before deferred dispatch", "dispatchNativeHandlerFollowUpForTest(pc, ctx, bus, result.FollowUp)", "want 1 after deferred dispatch"} {
		if !strings.Contains(row.After, required) {
			t.Fatalf("lost deferred cut: %s", required)
		}
	}
}

func TestNativeHandlerEmissionOracleRejectsLostWorkloadAndAssertions(t *testing.T) {
	for _, probe := range []struct{ family, before, after string }{
		{"native-handler-envelope-emission", "got != entityID", "got == entityID"},
		{"native-handler-envelope-emission", "!result.Handled", "result.Handled"},
		{"native-handler-nested-emission", `payload["summary"]`, `payload["other"]`},
		{"native-handler-nested-emission", `got != "queued"`, `got != "active"`},
		{"native-handler-deferred-emission", "!result.Committed", "result.Committed"},
		{"native-handler-deferred-emission", "got != 0", "got != 1"},
		{"native-handler-additive-emission", `"rule.emitted", "handler.succeeded"`, `"handler.succeeded", "rule.emitted"`},
		{"native-handler-rule-template-emission", "payload.score >= 80.0", "payload.score >= 40.0"},
		{"native-handler-rule-template-emission", `got != "high"`, `got != "low"`},
		{"native-handler-explicit-payload", `payload["legacy_entity"]`, `payload["other"]`},
		{"native-handler-escalation-empty", `payload["score"]`, `payload["other"]`},
		{"native-handler-escalation-fields", `"score_below_threshold"`, `"unrelated"`},
		{"native-handler-undeclared-payload", `"on_complete"`, `"unrelated"`},
		{"native-handler-undeclared-payload", "runtimeengine.ErrEmitPayloadContractViolation", "context.Canceled"},
	} {
		row := nativeMissingHeaderRecipe(t, probe.family)
		changed := strings.Replace(row.After, probe.before, probe.after, 1)
		if changed == row.After || nativeHandlerEmissionWorkload(t, row.Before) == nativeHandlerEmissionWorkload(t, changed) {
			t.Fatalf("lost original contract admitted: %s/%s", probe.family, probe.before)
		}
	}
}

func TestNativeDeclarativeEmissionRetiresSharedRawAndParsedSourceConstructors(t *testing.T) {
	path := filepath.Join("..", "..", "..", "internal", "runtime", "pipeline", "handler_engine_transaction_test.go")
	source, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		switch function.Name.Name {
		case "seedHandlerEngineExistingEntity", "newDeclarativeEmitContractCoordinator", "newDeclarativeEmitContractCoordinatorWithBundle", "declarativeEmitContractTestBundleWithEntry":
			t.Fatalf("retired shared fixture still live: %s", function.Name.Name)
		}
	}
	row := nativeMissingHeaderRecipe(t, "native-handler-declarative-source")
	if formattedNativeReadNode(projectionShapeFunction(t, row.After).Body) != formattedNativeReadNode(projectionShapeFunction(t, `func source(t *testing.T) *runtimecontracts.WorkflowContractBundle {t.Helper();return declarativeEmitContractSourceForTest(t,"custom.emitted:\n  label: text\n")}`).Body) {
		t.Fatal("declarative source producer stopped consuming admitted authored files")
	}
}

func TestNativeHandlerSharedSourceProducerIsImmutableAndPreservesAuthoredBytes(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-handler-immutable-source")
	before := projectionShapeFunction(t, row.Before)
	after := projectionShapeFunction(t, row.After)
	if len(before.Type.Params.List) != 2 || len(before.Body.List) != 4 {
		t.Fatal("unknown source predecessor")
	}
	if formattedNativeReadNode(before.Body.List[2]) != formattedNativeReadNode(projectionShapeStatement(t, `if len(fields)>0 {module.bundle.RootEntities["test_entity"]=runtimecontracts.EntityContract{Fields:fields[0]}}`)) {
		t.Fatal("unknown removed source override")
	}
	before.Type.Params.List = before.Type.Params.List[:1]
	before.Body.List = append(before.Body.List[:2], before.Body.List[3:]...)
	if formattedNativeReadNode(before) != formattedNativeReadNode(after) {
		t.Fatal("shared source bytes or remaining source behavior changed")
	}
	path := filepath.Join("..", "..", "..", "internal", "runtime", "pipeline")
	files, err := filepath.Glob(filepath.Join(path, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	for _, path := range files {
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || formattedNativeReadNode(call.Fun) != "handlerEngineProjectNodeModule" {
				return true
			}
			calls++
			if len(call.Args) != 1 || call.Ellipsis.IsValid() {
				t.Fatal("shared source caller still injects parsed fields")
			}
			return true
		})
	}
	if calls != 3 {
		t.Fatalf("shared source direct caller count=%d, want3", calls)
	}
	bridge := selectedCausalObservationBody(t, "internal/runtime/pipeline/declarative_default_node_test.go", "VerifyRetainedNodeContractHandlerUsesRuntimeEnginePathForTest")
	owner := selectedCausalObservationBody(t, "internal/runtime/pipeline/workflow_handler_native_fixture_test.go", "nativeHandlerEngineExistingEntityForTest")
	if strings.Count(bridge, "nativeHandlerEngineExistingEntityForTest(t, open, testPipelineRunID)") != 1 ||
		strings.Count(owner, "nativeHandlerEngineExistingEntityWithModuleForTest(t, open, handlerEngineProjectNodeModule(t), entityID, nil)") != 1 {
		t.Fatal("retained engine witness lost its one indirect canonical source consumer")
	}
}
