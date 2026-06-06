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
)

type eventConstructorCallsite struct {
	Path  string
	Scope string
}

var productionProjectionEventAllowlist = map[eventConstructorCallsite]int{
	{Path: "internal/store/events.go", Scope: "PostgresStore.listEventsMissingPipelineReceiptSpec"}:                                1,
	{Path: "internal/store/events.go", Scope: "PostgresStore.listEventsMissingPipelineReceiptForRunSpec"}:                          1,
	{Path: "internal/store/pending_delivery_read_surface.go", Scope: "scanPendingAgentDeliveryRecords"}:                            1,
	{Path: "internal/store/sqlite_runtime_delivery_replay.go", Scope: "SQLiteRuntimeStore.ListPendingSubscribedEvents"}:            1,
	{Path: "internal/store/sqlite_runtime_delivery_replay.go", Scope: "SQLiteRuntimeStore.listSQLiteEventsMissingPipelineReceipt"}: 1,
	{Path: "internal/store/sqlite_runtime_delivery_replay.go", Scope: "SQLiteRuntimeStore.listSQLitePendingAgentDeliveryRecords"}:  1,
	{Path: "internal/runtime/bus/eventbus_routing.go", Scope: "EventBus.deliverToRecipientsWithRoutes"}:                            1,
	{Path: "internal/runtime/bus/outbox.go", Scope: "clonePostCommitEvent"}:                                                        1,
	{Path: "internal/runtime/core/pinrouting/pinrouting.go", Scope: "Resolve"}:                                                     1,
	{Path: "internal/runtime/correlation/context.go", Scope: "CorrelateEvent"}:                                                     1,
	{Path: "internal/runtime/manager/receipts.go", Scope: "AgentManager.processEventDetailed"}:                                     1,
	{Path: "internal/runtime/pipeline/engine_adapter.go", Scope: "cloneEvent"}:                                                     1,
	{Path: "internal/runtime/pipeline/node_declarative.go", Scope: "ensureHandlerEntityID"}:                                        2,
	{Path: "internal/runtime/pipeline/select_entity.go", Scope: "PipelineCoordinator.createdHandlerEntityForDeclaredKey"}:          1,
	{Path: "internal/runtime/pipeline/select_entity.go", Scope: "PipelineCoordinator.selectedHandlerEntityFromInstance"}:           1,
	{Path: "internal/runtime/pipeline/workflow_handler_preview.go", Scope: "PreviewContractHandlerExecution"}:                      1,
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
		"",
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
		"",
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

func TestNewRootIngressEventPreservesExplicitCheckpointLineage(t *testing.T) {
	evt := NewRootIngressEvent(
		"evt-1",
		EventType("operator.checkpoint"),
		"operator",
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
		"",
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
	eventAliases, dotImported := eventImportAliases(file)
	if len(eventAliases) == 0 {
		return
	}
	relativePath := slashPath(repoRoot, path)
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

func eventImportAliases(file *ast.File) (map[string]struct{}, bool) {
	aliases := map[string]struct{}{}
	dotImported := false
	for _, imp := range file.Imports {
		if strings.Trim(imp.Path.Value, `"`) != "github.com/division-sh/swarm/internal/events" {
			continue
		}
		switch {
		case imp.Name == nil:
			aliases["events"] = struct{}{}
		case imp.Name.Name == ".":
			dotImported = true
			aliases["events"] = struct{}{}
		case imp.Name.Name != "_":
			aliases[imp.Name.Name] = struct{}{}
		}
	}
	return aliases, dotImported
}

func isNewProjectionEventCall(call *ast.CallExpr, eventAliases map[string]struct{}) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "NewProjectionEvent" && isEventsPackageIdent(selector.X, eventAliases)
}

func isNewRouteProbeEventCall(call *ast.CallExpr, eventAliases map[string]struct{}) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "NewRouteProbeEvent" && isEventsPackageIdent(selector.X, eventAliases)
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
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	_, ok = eventAliases[ident.Name]
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
	case "WithRunID", "WithParentEventID", "WithTaskID", "WithLineage", "WithEnvelope",
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
