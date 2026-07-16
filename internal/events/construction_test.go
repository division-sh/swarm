package events

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

type eventConstructorCallsite struct {
	Path  string
	Scope string
}

var productionProjectionEventAllowlist = map[eventConstructorCallsite]int{
	{Path: "internal/store/event_persistence_identity.go", Scope: "eventFromPersistedIdentity"}: 1,
}

var productionRouteProbeEventAllowlist = map[eventConstructorCallsite]int{
	{Path: "internal/runtime/bus/eventbus_routing.go", Scope: "EventBus.resolveRoutedRecipients"}:  1,
	{Path: "internal/runtime/bus/eventbus_routing.go", Scope: "EventBus.resolveRoutedSubscribers"}: 1,
	{Path: "internal/runtime/bus/sweeper.go", Scope: "EventBus.SweepUndispatched"}:                 1,
}

func TestNewChildEventWithLineageOwnsRuntimeFields(t *testing.T) {
	parent := NewRootIngressEvent(
		"parent-event",
		EventType("root.started"),
		ExternalProducer("test-root"),
		"task-1",
		json.RawMessage(`{"ok":true}`),
		0,
		"run-1",
		"",
		EventEnvelope{},
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("test", 3600)),
	)

	child := NewChildEvent(
		"child-event",
		EventType("child.done"),
		AgentProducer("test-child"),
		"",
		json.RawMessage(`{"done":true}`),
		0,
		parent,
		EventEnvelope{EntityID: "entity-1", FlowInstance: "flow/root"},
		parent.CreatedAt().Add(time.Second),
	)

	if child.RunID() != "run-1" {
		t.Fatalf("RunID = %q, want %q", child.RunID(), "run-1")
	}
	if child.ParentEventID() != "parent-event" {
		t.Fatalf("ParentEventID = %q, want %q", child.ParentEventID(), "parent-event")
	}
	if child.TaskID() != "task-1" {
		t.Fatalf("TaskID = %q, want %q", child.TaskID(), "task-1")
	}
	if child.EntityID() != "entity-1" || child.FlowInstance() != "flow/root" {
		t.Fatalf("context = entity %q flow %q, want entity-1 flow/root", child.EntityID(), child.FlowInstance())
	}
}

func TestLineageConstructorsPreserveMockExecutionMode(t *testing.T) {
	parent := NewRootIngressEvent(
		"parent-event",
		EventType("root.started"),
		ExternalProducer("test-root"),
		"task-1",
		nil,
		0,
		"run-1",
		"",
		EventEnvelope{},
		time.Now().UTC(),
	).WithExecutionMode(executionmode.Mock)

	lineage := LineageFromEvent(parent)
	if lineage.ExecutionMode != executionmode.Mock {
		t.Fatalf("lineage execution mode = %q, want mock", lineage.ExecutionMode)
	}
	child := NewChildEvent("child-event", EventType("child.done"), AgentProducer("test-child"), "", nil, 0, parent, EventEnvelope{}, time.Now().UTC())
	if child.ExecutionMode() != executionmode.Mock {
		t.Fatalf("child execution mode = %q, want mock", child.ExecutionMode())
	}
	replay := NewReplayEvent("replay-event", EventType("child.done"), AgentProducer("test-child"), "", nil, 0, lineage, EventEnvelope{}, time.Now().UTC())
	if replay.ExecutionMode() != executionmode.Mock {
		t.Fatalf("replay execution mode = %q, want mock", replay.ExecutionMode())
	}
}

func TestNewRootIngressEventPreservesExplicitCheckpointLineage(t *testing.T) {
	evt := NewRootIngressEvent(
		"evt-1",
		EventType("operator.checkpoint"),
		ExternalProducer("operator"),
		"",
		json.RawMessage(`{"checkpoint":true}`),
		0,
		"run-1",
		"source-1",
		EventEnvelope{},
		time.Time{},
	)

	if evt.RunID() != "run-1" {
		t.Fatalf("RunID = %q, want run-1", evt.RunID())
	}
	if evt.ParentEventID() != "source-1" {
		t.Fatalf("ParentEventID = %q, want source-1", evt.ParentEventID())
	}
}

func TestNewRuntimeDiagnosticEventCopiesPayload(t *testing.T) {
	payload := json.RawMessage(`{"level":"warn"}`)
	evt := NewRuntimeDiagnosticEvent(
		"diag-1",
		EventType("platform.diagnostic"),
		PlatformProducer("runtime"),
		"",
		payload,
		0,
		"",
		"",
		EventEnvelope{},
		time.Time{},
	)
	payload[10] = 'e'

	if string(evt.Payload()) != `{"level":"warn"}` {
		t.Fatalf("Payload = %s, want original payload copy", evt.Payload())
	}
}

func TestProductionEventConstructionUsesPublicAPI(t *testing.T) {
	repoRoot := repositoryRoot(t)
	projectionCallCounts := map[eventConstructorCallsite]int{}
	routeProbeCallCounts := map[eventConstructorCallsite]int{}
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
			checkProductionEventConstructionFile(t, repoRoot, path, projectionCallCounts, routeProbeCallCounts)
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	assertExactConstructorAllowlist(t, "NewProjectionEvent", productionProjectionEventAllowlist, projectionCallCounts)
	assertExactConstructorAllowlist(t, "NewRouteProbeEvent", productionRouteProbeEventAllowlist, routeProbeCallCounts)
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

func repositoryRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func checkProductionEventConstructionFile(t *testing.T, repoRoot, path string, projectionCallCounts, routeProbeCallCounts map[eventConstructorCallsite]int) {
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
	return ok && selector.Sel.Name == "NewRouteProbeEvent" && isEventsPackageIdent(selector.X, eventAliases)
}

func testEventConstructorCallName(call *ast.CallExpr, eventAliases map[string]struct{}) (string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !isEventsPackageIdent(selector.X, eventAliases) {
		return "", false
	}
	switch selector.Sel.Name {
	case "NewRootIngressEvent",
		"NewRuntimeControlEvent",
		"NewRuntimeDiagnosticEvent",
		"NewDiagnosticDirectEvent",
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
			return name == "EmptyEvent" || name == "NewRootIngressEvent" || name == "NewRuntimeControlEvent" ||
				name == "NewRuntimeDiagnosticEvent" || name == "NewDiagnosticDirectEvent" || name == "NewChildEvent" || name == "NewChildEventWithLineage" ||
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
