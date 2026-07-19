package events

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type eventConstructorCallsite struct {
	Path  string
	Scope string
}

type runtimeConstructorCallsite struct {
	Path        string
	Scope       string
	Constructor string
}

var productionProjectionEventAllowlist = map[eventConstructorCallsite]int{}

var productionRouteProbeEventAllowlist = map[eventConstructorCallsite]int{
	{Path: "internal/runtime/bus/eventbus_routing.go", Scope: "EventBus.resolveRoutedSubscribers"}: 1,
}

var productionRuntimeConstructorAllowlist = map[runtimeConstructorCallsite]int{
	{Path: "internal/apiv1/operator_decision_cards.go", Scope: "newMailboxRuntimeControlEvent", Constructor: "NewRunScopedRuntimeControlEvent"}:                                  1,
	{Path: "internal/runtime/agentcontrol/control.go", Scope: "NewDirectiveEvent", Constructor: "NewRunScopedDiagnosticDirectEvent"}:                                             1,
	{Path: "internal/runtime/budget.go", Scope: "BudgetTracker.evaluateScope", Constructor: "NewCausalRuntimeDiagnosticEvent"}:                                                   1,
	{Path: "internal/runtime/budget.go", Scope: "BudgetTracker.evaluateScope", Constructor: "NewStandaloneRuntimeDiagnosticEvent"}:                                               1,
	{Path: "internal/runtime/event_construction.go", Scope: "newStandaloneRuntimePlatformControlEvent", Constructor: "NewStandaloneRuntimeControlEvent"}:                         1,
	{Path: "internal/runtime/event_construction.go", Scope: "newStandaloneRuntimePlatformDiagnosticEvent", Constructor: "NewStandaloneRuntimeDiagnosticEvent"}:                   1,
	{Path: "internal/runtime/inbound.go", Scope: "projectInboundPublication", Constructor: "NewRunScopedDiagnosticDirectEvent"}:                                                  1,
	{Path: "internal/runtime/ingress/controller.go", Scope: "Controller.publishTransitionEvent", Constructor: "NewCausalRuntimeControlEvent"}:                                    1,
	{Path: "internal/runtime/ingress/controller.go", Scope: "Controller.publishTransitionEvent", Constructor: "NewStandaloneRuntimeControlEvent"}:                                1,
	{Path: "internal/runtime/llm/session_events.go", Scope: "newAgentStartedRuntimeDiagnostic", Constructor: "NewCausalRuntimeDiagnosticEvent"}:                                  1,
	{Path: "internal/runtime/llm/session_events.go", Scope: "newAgentStartedRuntimeDiagnostic", Constructor: "NewRunScopedRuntimeDiagnosticEvent"}:                               1,
	{Path: "internal/runtime/llm/session_events.go", Scope: "newAgentStartedRuntimeDiagnostic", Constructor: "NewStandaloneRuntimeDiagnosticEvent"}:                              1,
	{Path: "internal/runtime/manager/event_construction.go", Scope: "newPlatformCausalRuntimeControlEvent", Constructor: "NewCausalRuntimeControlEvent"}:                         1,
	{Path: "internal/runtime/manager/event_construction.go", Scope: "newPlatformCausalRuntimeDiagnosticEvent", Constructor: "NewCausalRuntimeDiagnosticEvent"}:                   1,
	{Path: "internal/runtime/manager/event_construction.go", Scope: "newPlatformStandaloneRuntimeControlEvent", Constructor: "NewStandaloneRuntimeControlEvent"}:                 1,
	{Path: "internal/runtime/manager/event_construction.go", Scope: "newPlatformStandaloneRuntimeDiagnosticEvent", Constructor: "NewStandaloneRuntimeDiagnosticEvent"}:           1,
	{Path: "internal/runtime/pipeline/coordinator.go", Scope: "newPipelineRuntimeDiagnostic", Constructor: "NewCausalRuntimeDiagnosticEvent"}:                                    1,
	{Path: "internal/runtime/pipeline/node_system_runner.go", Scope: "systemNodeRunner.emitDeadLetter", Constructor: "NewCausalRuntimeDiagnosticEvent"}:                          1,
	{Path: "internal/runtime/pipeline/workflow_gate_lifecycle.go", Scope: "PipelineCoordinator.publishWorkflowGateSuperseded", Constructor: "NewRunScopedRuntimeControlEvent"}:   1,
	{Path: "internal/runtime/pipeline/workflow_gate_terminal.go", Scope: "WorkflowInstanceStore.supersedeWorkflowInstanceGates", Constructor: "NewRunScopedRuntimeControlEvent"}: 1,
	{Path: "internal/runtime/pipeline/workflow_timer_owner.go", Scope: "WorkflowTimerLifecycle.Fire", Constructor: "NewRunScopedRuntimeControlEvent"}:                            1,
	{Path: "internal/runtime/runstalled/monitor.go", Scope: "Monitor.eventForSnapshot", Constructor: "NewRunScopedRuntimeDiagnosticEvent"}:                                       1,
	{Path: "internal/runtime/runtime.go", Scope: "scheduledEvent", Constructor: "NewRunScopedRuntimeControlEvent"}:                                                               1,
	{Path: "internal/runtime/runtime.go", Scope: "Runtime.publishBootCompleted", Constructor: "NewStandaloneRuntimeControlEvent"}:                                                1,
	{Path: "internal/store/eventfixture/event.go", Scope: "DiagnosticDirectForRun", Constructor: "NewCausalDiagnosticDirectEvent"}:                                               1,
	{Path: "internal/store/eventfixture/event.go", Scope: "DiagnosticDirectForRun", Constructor: "NewRunScopedDiagnosticDirectEvent"}:                                            1,
	{Path: "internal/store/eventfixture/event.go", Scope: "DiagnosticDirectForRun", Constructor: "NewStandaloneDiagnosticDirectEvent"}:                                           1,
	{Path: "internal/store/human_task_cards.go", Scope: "expireHumanTaskCards", Constructor: "NewRunScopedRuntimeControlEvent"}:                                                  1,
	{Path: "internal/store/runtime_log_persistence.go", Scope: "runtimeLogEvent", Constructor: "NewCausalDiagnosticDirectEvent"}:                                                 1,
	{Path: "internal/store/runtime_log_persistence.go", Scope: "runtimeLogEvent", Constructor: "NewRunScopedDiagnosticDirectEvent"}:                                              1,
	{Path: "internal/store/runtime_log_persistence.go", Scope: "runtimeLogEvent", Constructor: "NewStandaloneDiagnosticDirectEvent"}:                                             1,
}

func TestProductionEventConstructionUsesPublicAPI(t *testing.T) {
	repoRoot := repositoryRoot(t)
	projectionCallCounts := map[eventConstructorCallsite]int{}
	routeProbeCallCounts := map[eventConstructorCallsite]int{}
	runtimeCallCounts := map[runtimeConstructorCallsite]int{}
	for _, dir := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, dir)
		if _, err := os.Stat(root); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("stat %s: %v", root, err)
		}
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if path == filepath.Join(repoRoot, "internal", "events") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			checkProductionEventConstructionFile(t, repoRoot, path, projectionCallCounts, routeProbeCallCounts, runtimeCallCounts)
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	assertExactConstructorAllowlist(t, "NewProjectionEvent", productionProjectionEventAllowlist, projectionCallCounts)
	assertExactConstructorAllowlist(t, "NewRouteProbeEvent", productionRouteProbeEventAllowlist, routeProbeCallCounts)
	assertExactRuntimeConstructorAllowlist(t, runtimeCallCounts)
}

func TestTestEventFixturesUseFixtureBuilders(t *testing.T) {
	repoRoot := repositoryRoot(t)
	for _, dir := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, dir)
		if _, err := os.Stat(root); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("stat %s: %v", root, err)
		}
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if path == filepath.Join(repoRoot, "internal", "events") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, "_test.go") {
				return nil
			}
			checkTestEventFixtureFile(t, repoRoot, path)
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
}

func TestRemovedEventConstructionAliasesStayDeleted(t *testing.T) {
	repoRoot := repositoryRoot(t)
	removed := map[string]struct{}{
		"NodeProducer": {}, "AgentProducer": {}, "PlatformProducer": {}, "ExternalProducer": {},
		"EmptyEvent": {}, "NewProjectionEvent": {}, "NewRouteProbeEvent": {},
		"NewRuntimeControlEvent": {}, "NewRuntimeDiagnosticEvent": {}, "NewDiagnosticDirectEvent": {},
	}
	removedTypes := map[string]struct{}{
		"RuntimeEventInput": {}, "DiagnosticDirectEventInput": {},
	}
	root := filepath.Join(repoRoot, "internal", "events")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			switch value := declaration.(type) {
			case *ast.FuncDecl:
				if _, forbidden := removed[value.Name.Name]; forbidden {
					t.Fatalf("%s:%d reintroduces removed event construction alias %s", slashPath(repoRoot, path), fset.Position(value.Pos()).Line, value.Name.Name)
				}
			case *ast.GenDecl:
				for _, spec := range value.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					if _, forbidden := removedTypes[typeSpec.Name.Name]; forbidden {
						t.Fatalf("%s:%d reintroduces removed ambiguous runtime input %s", slashPath(repoRoot, path), fset.Position(typeSpec.Pos()).Line, typeSpec.Name.Name)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk internal/events: %v", err)
	}
}

func assertExactConstructorAllowlist(t *testing.T, constructor string, allowlist map[eventConstructorCallsite]int, counts map[eventConstructorCallsite]int) {
	t.Helper()
	for site, want := range allowlist {
		if got := counts[site]; got != want {
			t.Fatalf("%s in %s has %d %s calls, want %d; keep construction use exact or remove the allowlist entry", site.Scope, site.Path, got, constructor, want)
		}
	}
	for site, got := range counts {
		if _, ok := allowlist[site]; !ok {
			t.Fatalf("%s in %s has %d unallowlisted %s calls; use a semantic producer constructor or explicitly justify the projection/query/sentinel boundary", site.Scope, site.Path, got, constructor)
		}
	}
}

func assertExactRuntimeConstructorAllowlist(t *testing.T, counts map[runtimeConstructorCallsite]int) {
	t.Helper()
	for site, want := range productionRuntimeConstructorAllowlist {
		if got := counts[site]; got != want {
			t.Fatalf("%s in %s has %d %s calls, want %d; classify every runtime constructor exactly", site.Scope, site.Path, got, site.Constructor, want)
		}
	}
	for site, got := range counts {
		if _, ok := productionRuntimeConstructorAllowlist[site]; !ok {
			t.Fatalf("%s in %s has %d unclassified %s calls; add an exact causal, run-scoped, or standalone census entry", site.Scope, site.Path, got, site.Constructor)
		}
	}
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func checkProductionEventConstructionFile(t *testing.T, repoRoot, path string, projectionCallCounts, routeProbeCallCounts map[eventConstructorCallsite]int, runtimeCallCounts map[runtimeConstructorCallsite]int) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	relativePath := slashPath(repoRoot, path)
	if importsPath(file, "github.com/division-sh/swarm/internal/events/eventtest") {
		t.Fatalf("%s imports internal/events/eventtest from production code; test fixture builders are test-only", relativePath)
	}
	eventAliases, dotImported := eventImportAliases(file)
	if len(eventAliases) == 0 {
		return
	}
	if dotImported {
		t.Fatalf("%s dot-imports internal/events; use a named import so the production construction guard can classify constructor calls", relativePath)
	}
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok || !isEventsEventType(lit.Type, eventAliases) {
			return true
		}
		t.Fatalf("%s:%d constructs events.Event directly; use internal/events constructors or a named projection/query API", relativePath, fset.Position(lit.Pos()).Line)
		return false
	})
	for _, decl := range file.Decls {
		if _, ok := decl.(*ast.FuncDecl); ok {
			continue
		}
		ast.Inspect(decl, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || (!isNewProjectionEventCall(call, eventAliases) && !isNewRouteProbeEventCall(call, eventAliases)) {
				return true
			}
			t.Fatalf("%s:%d calls a projection/probe event constructor outside a named function scope; use a semantic constructor or move it behind an exact allowlisted function with proof", relativePath, fset.Position(call.Pos()).Line)
			return false
		})
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		scope := productionFunctionScope(fn)
		eventVars := map[string]struct{}{}
		recordEventFieldVars(fn.Type.Params, eventAliases, eventVars)
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.ValueSpec:
				if isEventsEventType(node.Type, eventAliases) {
					for _, name := range node.Names {
						eventVars[name.Name] = struct{}{}
					}
				}
			case *ast.AssignStmt:
				recordAssignedEventVars(node, eventAliases, eventVars)
				for _, lhs := range node.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok && isCoreEventField(sel.Sel.Name) {
						if ident, ok := sel.X.(*ast.Ident); ok {
							if _, eventVar := eventVars[ident.Name]; eventVar {
								t.Fatalf("%s:%d assigns %s.%s directly; construct the event with the final field values", relativePath, fset.Position(sel.Pos()).Line, ident.Name, sel.Sel.Name)
							}
						}
					}
				}
			case *ast.CallExpr:
				if constructor, ok := runtimeConstructorCallName(node, eventAliases); ok {
					site := runtimeConstructorCallsite{Path: relativePath, Scope: scope, Constructor: constructor}
					if _, classified := productionRuntimeConstructorAllowlist[site]; !classified {
						t.Fatalf("%s:%d calls %s from unclassified production scope %s", relativePath, fset.Position(node.Pos()).Line, constructor, scope)
					}
					runtimeCallCounts[site]++
				}
				if isNewProjectionEventCall(node, eventAliases) {
					site := eventConstructorCallsite{Path: relativePath, Scope: scope}
					if _, ok := productionProjectionEventAllowlist[site]; !ok {
						t.Fatalf("%s:%d calls NewProjectionEvent outside the projection allowlist; use a semantic event constructor or add an exact file/function/count entry with proof", relativePath, fset.Position(node.Pos()).Line)
					}
					projectionCallCounts[site]++
				}
				if isNewRouteProbeEventCall(node, eventAliases) {
					site := eventConstructorCallsite{Path: relativePath, Scope: scope}
					if _, ok := productionRouteProbeEventAllowlist[site]; !ok {
						t.Fatalf("%s:%d calls NewRouteProbeEvent outside the route-probe allowlist; use a non-event query type or add an exact file/function/count entry with proof", relativePath, fset.Position(node.Pos()).Line)
					}
					routeProbeCallCounts[site]++
				}
				sel, ok := node.Fun.(*ast.SelectorExpr)
				if !ok || !isEventProjectionMethod(sel.Sel.Name) || !eventProjectionReceiver(sel.X, eventAliases, eventVars) {
					return true
				}
				t.Fatalf("%s:%d calls %s on an event; construct or project the event with final field values instead", relativePath, fset.Position(sel.Pos()).Line, sel.Sel.Name)
			}
			return true
		})
	}
}

func checkTestEventFixtureFile(t *testing.T, repoRoot, path string) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	relativePath := slashPath(repoRoot, path)
	eventAliases, dotImported := eventImportAliases(file)
	if dotImported {
		t.Fatalf("%s dot-imports internal/events; use a named import so the test fixture guard can classify constructor calls", relativePath)
	}
	eventtestAliases := importAliases(file, "github.com/division-sh/swarm/internal/events/eventtest")
	packageAliases := allImportAliases(file)
	persistedProjectionVars := testPersistedProjectionVars(file, eventtestAliases)
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if isPublishSurfaceCall(call) {
			for _, arg := range call.Args {
				if exprContainsEventtestPersistedProjection(arg, eventtestAliases, persistedProjectionVars) {
					t.Fatalf("%s:%d passes eventtest.PersistedProjection to a publish/runtime producer API; use a runtime-intent fixture such as eventtest.RootIngress", relativePath, fset.Position(call.Pos()).Line)
				}
			}
		}
		if isNewProjectionEventCall(call, eventAliases) {
			t.Fatalf("%s:%d calls events.NewProjectionEvent directly in a test; use internal/events/eventtest fixture builders or add a narrow internal/events unit-test allowlist", relativePath, fset.Position(call.Pos()).Line)
		}
		if isNewRouteProbeEventCall(call, eventAliases) {
			t.Fatalf("%s:%d calls events.NewRouteProbeEvent directly in a test; use internal/events/eventtest.RouteProbe or add a narrow internal/events unit-test allowlist", relativePath, fset.Position(call.Pos()).Line)
		}
		if constructor, ok := testEventConstructorCallName(call, eventAliases); ok {
			t.Fatalf("%s:%d calls events.%s directly in a test; use the matching internal/events/eventtest fixture builder outside internal/events constructor unit tests", relativePath, fset.Position(call.Pos()).Line, constructor)
		}
		if isEventtestProjectionCall(call, eventtestAliases) {
			t.Fatalf("%s:%d calls eventtest.Projection in a test; choose eventtest.RootIngress, eventtest.ChildWithLineage, eventtest.Replay, runtime diagnostic/control helpers, or eventtest.PersistedProjection for persisted/readback fixtures", relativePath, fset.Position(call.Pos()).Line)
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !isEventProjectionMethod(sel.Sel.Name) {
			return true
		}
		if isPackageIdent(sel.X, eventtestAliases) {
			t.Fatalf("%s:%d calls eventtest.%s in a test; pass the final EventEnvelope/route context to the typed fixture constructor", relativePath, fset.Position(sel.Pos()).Line, sel.Sel.Name)
		}
		if isPackageIdent(sel.X, packageAliases) {
			return true
		}
		t.Fatalf("%s:%d calls %s directly in a test; route fixture event patching through internal/events/eventtest", relativePath, fset.Position(sel.Pos()).Line, sel.Sel.Name)
		return false
	})
}

func testPersistedProjectionVars(file *ast.File, eventtestAliases map[string]struct{}) map[string]struct{} {
	vars := map[string]struct{}{}
	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.AssignStmt:
			for idx, rhs := range node.Rhs {
				if !exprContainsEventtestPersistedProjection(rhs, eventtestAliases, nil) || idx >= len(node.Lhs) {
					continue
				}
				if ident, ok := node.Lhs[idx].(*ast.Ident); ok && ident.Name != "_" {
					vars[ident.Name] = struct{}{}
				}
			}
		case *ast.ValueSpec:
			for idx, value := range node.Values {
				if !exprContainsEventtestPersistedProjection(value, eventtestAliases, nil) || idx >= len(node.Names) {
					continue
				}
				if name := node.Names[idx].Name; name != "_" {
					vars[name] = struct{}{}
				}
			}
		}
		return true
	})
	return vars
}

func eventImportAliases(file *ast.File) (map[string]struct{}, bool) {
	aliases := importAliases(file, "github.com/division-sh/swarm/internal/events")
	dotImported := false
	for _, imp := range file.Imports {
		if strings.Trim(imp.Path.Value, `"`) != "github.com/division-sh/swarm/internal/events" {
			continue
		}
		switch {
		case imp.Name != nil && imp.Name.Name == ".":
			dotImported = true
		}
	}
	return aliases, dotImported
}

func importAliases(file *ast.File, importPath string) map[string]struct{} {
	aliases := map[string]struct{}{}
	for _, imp := range file.Imports {
		if strings.Trim(imp.Path.Value, `"`) != importPath {
			continue
		}
		switch {
		case imp.Name == nil:
			aliases[filepath.Base(importPath)] = struct{}{}
		case imp.Name.Name == ".":
			aliases[filepath.Base(importPath)] = struct{}{}
		case imp.Name.Name != "_":
			aliases[imp.Name.Name] = struct{}{}
		}
	}
	return aliases
}

func allImportAliases(file *ast.File) map[string]struct{} {
	aliases := map[string]struct{}{}
	for _, imp := range file.Imports {
		if imp.Name != nil {
			if imp.Name.Name != "." && imp.Name.Name != "_" {
				aliases[imp.Name.Name] = struct{}{}
			}
			continue
		}
		importPath := strings.Trim(imp.Path.Value, `"`)
		aliases[filepath.Base(importPath)] = struct{}{}
	}
	return aliases
}

func importsPath(file *ast.File, importPath string) bool {
	for _, imp := range file.Imports {
		if strings.Trim(imp.Path.Value, `"`) == importPath {
			return true
		}
	}
	return false
}

func isNewProjectionEventCall(call *ast.CallExpr, eventAliases map[string]struct{}) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "NewProjectionEvent" && isEventsPackageIdent(selector.X, eventAliases)
}

func isNewRouteProbeEventCall(call *ast.CallExpr, eventAliases map[string]struct{}) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "NewRouteProbe" && isEventsPackageIdent(selector.X, eventAliases)
}

func runtimeConstructorCallName(call *ast.CallExpr, eventAliases map[string]struct{}) (string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !isEventsPackageIdent(selector.X, eventAliases) {
		return "", false
	}
	switch selector.Sel.Name {
	case "NewCausalRuntimeControlEvent",
		"NewRunScopedRuntimeControlEvent",
		"NewStandaloneRuntimeControlEvent",
		"NewCausalRuntimeDiagnosticEvent",
		"NewRunScopedRuntimeDiagnosticEvent",
		"NewStandaloneRuntimeDiagnosticEvent",
		"NewCausalDiagnosticDirectEvent",
		"NewRunScopedDiagnosticDirectEvent",
		"NewStandaloneDiagnosticDirectEvent":
		return selector.Sel.Name, true
	default:
		return "", false
	}
}

func testEventConstructorCallName(call *ast.CallExpr, eventAliases map[string]struct{}) (string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !isEventsPackageIdent(selector.X, eventAliases) {
		return "", false
	}
	switch selector.Sel.Name {
	case "NewRootIngressEvent",
		"NewCausalRuntimeControlEvent",
		"NewRunScopedRuntimeControlEvent",
		"NewStandaloneRuntimeControlEvent",
		"NewCausalRuntimeDiagnosticEvent",
		"NewRunScopedRuntimeDiagnosticEvent",
		"NewStandaloneRuntimeDiagnosticEvent",
		"NewCausalDiagnosticDirectEvent",
		"NewRunScopedDiagnosticDirectEvent",
		"NewStandaloneDiagnosticDirectEvent",
		"NewChildEvent",
		"NewChildEventWithLineage",
		"NewReplayEvent":
		return selector.Sel.Name, true
	default:
		return "", false
	}
}

func isEventtestProjectionCall(call *ast.CallExpr, eventtestAliases map[string]struct{}) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "Projection" && isPackageIdent(selector.X, eventtestAliases)
}

func isEventtestPersistedProjectionCall(call *ast.CallExpr, eventtestAliases map[string]struct{}) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "PersistedProjection" && isPackageIdent(selector.X, eventtestAliases)
}

func isPublishSurfaceCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	switch selector.Sel.Name {
	case "Publish", "PublishDirect", "PublishInMutation", "DecisionEventPublish":
		return true
	default:
		return false
	}
}

func exprContainsEventtestPersistedProjection(expr ast.Expr, eventtestAliases map[string]struct{}, persistedProjectionVars map[string]struct{}) bool {
	found := false
	ast.Inspect(expr, func(n ast.Node) bool {
		if found {
			return false
		}
		switch node := n.(type) {
		case *ast.CallExpr:
			if isEventtestPersistedProjectionCall(node, eventtestAliases) {
				found = true
				return false
			}
		case *ast.Ident:
			if _, ok := persistedProjectionVars[node.Name]; ok {
				found = true
				return false
			}
		}
		return true
	})
	return found
}

func productionFunctionScope(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return fn.Name.Name
	}
	return receiverTypeName(fn.Recv.List[0].Type) + "." + fn.Name.Name
}

func receiverTypeName(expr ast.Expr) string {
	switch typ := expr.(type) {
	case *ast.StarExpr:
		return receiverTypeName(typ.X)
	case *ast.Ident:
		return typ.Name
	case *ast.SelectorExpr:
		return typ.Sel.Name
	case *ast.IndexExpr:
		return receiverTypeName(typ.X)
	case *ast.IndexListExpr:
		return receiverTypeName(typ.X)
	default:
		return "unknown"
	}
}

func recordEventFieldVars(fields *ast.FieldList, eventAliases map[string]struct{}, out map[string]struct{}) {
	if fields == nil {
		return
	}
	for _, field := range fields.List {
		if !isEventsEventType(field.Type, eventAliases) {
			continue
		}
		for _, name := range field.Names {
			out[name.Name] = struct{}{}
		}
	}
}

func recordAssignedEventVars(assign *ast.AssignStmt, eventAliases map[string]struct{}, out map[string]struct{}) {
	for idx, rhs := range assign.Rhs {
		if !rhsProducesEventsEvent(rhs, eventAliases) {
			continue
		}
		if idx >= len(assign.Lhs) {
			continue
		}
		if ident, ok := assign.Lhs[idx].(*ast.Ident); ok && ident.Name != "_" {
			out[ident.Name] = struct{}{}
		}
	}
}

func rhsProducesEventsEvent(expr ast.Expr, eventAliases map[string]struct{}) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	switch fun := call.Fun.(type) {
	case *ast.SelectorExpr:
		if isEventsPackageIdent(fun.X, eventAliases) {
			name := fun.Sel.Name
			return name == "EmptyEvent" || name == "NewRootIngressEvent" || name == "NewCausalRuntimeControlEvent" ||
				name == "NewRunScopedRuntimeControlEvent" || name == "NewStandaloneRuntimeControlEvent" ||
				name == "NewCausalRuntimeDiagnosticEvent" || name == "NewRunScopedRuntimeDiagnosticEvent" || name == "NewStandaloneRuntimeDiagnosticEvent" ||
				name == "NewCausalDiagnosticDirectEvent" || name == "NewRunScopedDiagnosticDirectEvent" || name == "NewStandaloneDiagnosticDirectEvent" ||
				name == "NewChildEvent" || name == "NewChildEventWithLineage" ||
				name == "NewReplayEvent" || name == "NewProjectionEvent" || name == "NewRouteProbeEvent"
		}
	case *ast.Ident:
		return strings.HasPrefix(fun.Name, "build") || strings.HasPrefix(fun.Name, "selected")
	}
	return false
}

func isEventsEventType(expr ast.Expr, eventAliases map[string]struct{}) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "Event" && isEventsPackageIdent(selector.X, eventAliases)
}

func isEventsPackageIdent(expr ast.Expr, eventAliases map[string]struct{}) bool {
	return isPackageIdent(expr, eventAliases)
}

func isPackageIdent(expr ast.Expr, aliases map[string]struct{}) bool {
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	_, ok = aliases[ident.Name]
	return ok
}

func isCoreEventField(name string) bool {
	switch name {
	case "ID", "Type", "SourceAgent", "TaskID", "Payload", "ChainDepth", "RunID", "ParentEventID", "Envelope", "CreatedAt":
		return true
	default:
		return false
	}
}

func isEventProjectionMethod(name string) bool {
	switch name {
	case "WithExecutionMode", "WithRunID", "WithParentEventID", "WithTaskID", "WithLineage", "WithEnvelope", "WithDeliveryContext",
		"WithEntityID", "WithFlowInstance", "WithSourceRoute", "WithTargetRoute",
		"WithTargetSet", "WithoutTargetRoute", "WithDeliveryTarget":
		return true
	default:
		return false
	}
}

func eventProjectionReceiver(expr ast.Expr, eventAliases map[string]struct{}, eventVars map[string]struct{}) bool {
	switch receiver := expr.(type) {
	case *ast.Ident:
		_, ok := eventVars[receiver.Name]
		return ok
	case *ast.SelectorExpr:
		return receiver.Sel.Name == "Event"
	case *ast.CallExpr:
		return rhsProducesEventsEvent(receiver, eventAliases)
	default:
		return false
	}
}

func slashPath(repoRoot, path string) string {
	rel, err := filepath.Rel(repoRoot, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}
