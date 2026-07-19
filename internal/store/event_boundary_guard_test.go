package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

type eventBoundaryCallsite struct {
	path  string
	scope string
	name  string
}

var admittedEventCallsites = map[eventBoundaryCallsite]int{
	{path: "internal/runtime/bus/eventbus_publish.go", scope: "admitEventForPublish", name: "AdmitForPublish"}:                                      1,
	{path: "internal/runtime/bus/eventbus_publish.go", scope: "EventBus.publishPersistedRecipients", name: "RevalidatePersistedEvent"}:              1,
	{path: "internal/runtime/bus/eventbus_publish.go", scope: "EventBus.PrepareSelectedForkPublish", name: "AdmitForPersistence"}:                   1,
	{path: "internal/runtime/manager/runtime.go", scope: "AgentManager.SendDirective", name: "AdmitForPersistence"}:                                 1,
	{path: "internal/store/eventfixture/event.go", scope: "Insert", name: "AdmitForPersistence"}:                                                    1,
	{path: "internal/store/inbound_publication.go", scope: "sqlInboundPublicationMutation.FinalizeInboundPublication", name: "AdmitForPersistence"}: 1,
	{path: "internal/store/run_fork_delivery_event_replay.go", scope: "projectRunForkReplayEvent", name: "AdmitForPersistence"}:                     1,
	{path: "internal/store/runtime_log_persistence.go", scope: "PostgresStore.PersistRuntimeLog", name: "AdmitForPersistence"}:                      1,
	{path: "internal/store/runtime_log_persistence.go", scope: "SQLiteRuntimeStore.PersistRuntimeLog", name: "AdmitForPersistence"}:                 1,
	{path: "internal/store/storetest/event.go", scope: "InsertCanonicalEventRecord", name: "AdmitForPersistence"}:                                   1,
	{path: "internal/store/storetest/event.go", scope: "commitSemanticEventWithInitialFacts", name: "AdmitForPublish"}:                              1,
}

var eventRecordImportFiles = map[string]struct{}{
	"internal/store/event_persistence_identity.go":                    {},
	"internal/store/eventfixture/event.go":                            {},
	"internal/store/events.go":                                        {},
	"internal/store/run_fork_selected_contract_execution_mutation.go": {},
	"internal/store/sqlite_runtime.go":                                {},
	"internal/store/storetest/event.go":                               {},
}

var eventRecordSQLFiles = map[string]struct{}{
	"internal/store/internal/eventrecord/postgres/adapter.go": {},
	"internal/store/internal/eventrecord/sqlite/adapter.go":   {},
}

var directEventSQLTestFixtures = map[string]int{
	"internal/cliapp/raw_sql_boundary_test.go":               1,
	"internal/runtime/pipeline/coordinator_recovery_test.go": 1,
	"internal/store/event_schema_contract_test.go":           2,
	"internal/store/runtime_mutation_guard_test.go":          3,
}

var eventInsertSQL = regexp.MustCompile(`(?is)\bINSERT\s+INTO\s+events\b`)
var completeEventReadSQL = regexp.MustCompile(`(?is)\bevent_class\b.*\bFROM\s+events\b`)

func TestEventAdmittedPersistenceBoundaryGuard(t *testing.T) {
	repoRoot := eventBoundaryRepositoryRoot(t)
	gotAdmission := map[eventBoundaryCallsite]int{}
	for _, rootName := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, rootName)
		if _, err := os.Stat(root); os.IsNotExist(err) {
			continue
		} else if err != nil {
			t.Fatalf("stat %s: %v", root, err)
		}
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			if strings.HasPrefix(relative, "internal/events/") {
				return nil
			}
			checkEventBoundaryFile(t, path, relative, gotAdmission)
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	for site, want := range admittedEventCallsites {
		if got := gotAdmission[site]; got != want {
			t.Fatalf("%s in %s has %d %s calls, want %d; admission callsites are a closed persistence boundary", site.scope, site.path, got, site.name, want)
		}
	}
	for site, got := range gotAdmission {
		if _, ok := admittedEventCallsites[site]; !ok {
			t.Fatalf("%s in %s has %d unclassified %s calls; add a closed named operation and update the exact boundary census", site.scope, site.path, got, site.name)
		}
	}
}

func TestEventFixtureWritersUseSemanticOwners(t *testing.T) {
	repoRoot := eventBoundaryRepositoryRoot(t)
	got := map[string]int{}
	for _, rootName := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, rootName)
		if _, err := os.Stat(root); os.IsNotExist(err) {
			continue
		} else if err != nil {
			t.Fatalf("stat %s: %v", root, err)
		}
		if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
				return nil
			}
			relative, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			relative = filepath.ToSlash(relative)
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				raw, err := strconv.Unquote(literal.Value)
				if err == nil {
					if count := len(eventInsertSQL.FindAllString(raw, -1)); count > 0 {
						got[relative] += count
					}
				}
				return true
			})
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	for path, want := range directEventSQLTestFixtures {
		if count := got[path]; count != want {
			t.Fatalf("%s contains %d direct event inserts, want %d exact corruption/guard fixtures", path, count, want)
		}
	}
	for path, count := range got {
		if _, ok := directEventSQLTestFixtures[path]; !ok {
			t.Fatalf("%s contains %d unclassified direct event inserts; use class-specific semantic fixtures", path, count)
		}
	}
}

func checkEventBoundaryFile(t *testing.T, path, relative string, gotAdmission map[eventBoundaryCallsite]int) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", relative, err)
	}
	eventAliases := eventBoundaryImportAliases(file, "github.com/division-sh/swarm/internal/events")
	for _, imported := range file.Imports {
		importPath := strings.Trim(imported.Path.Value, `"`)
		if !strings.HasPrefix(importPath, "github.com/division-sh/swarm/internal/store/internal/eventrecord") {
			continue
		}
		if strings.HasPrefix(relative, "internal/store/internal/eventrecord/") {
			continue
		}
		if _, ok := eventRecordImportFiles[relative]; !ok {
			t.Fatalf("%s imports private event records outside the closed store/fixture owner set", relative)
		}
	}

	ast.Inspect(file, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.BasicLit:
			if value.Kind != token.STRING {
				return true
			}
			raw, err := strconv.Unquote(value.Value)
			if err != nil {
				return true
			}
			if eventInsertSQL.MatchString(raw) || completeEventReadSQL.MatchString(raw) {
				if _, ok := eventRecordSQLFiles[relative]; !ok {
					t.Fatalf("%s:%d owns event-record SQL outside a private backend adapter", relative, fset.Position(value.Pos()).Line)
				}
			}
		case *ast.CompositeLit:
			if len(value.Elts) > 0 && eventBoundaryTypeIs(value.Type, eventAliases, "AdmittedEvent") {
				t.Fatalf("%s:%d populates opaque events.AdmittedEvent outside internal/events", relative, fset.Position(value.Pos()).Line)
			}
		case *ast.CallExpr:
			selector, ok := value.Fun.(*ast.SelectorExpr)
			if !ok || !eventBoundaryPackageIdent(selector.X, eventAliases) {
				return true
			}
			switch selector.Sel.Name {
			case "AdmitForPersistence", "AdmitForPublish", "RevalidatePersistedEvent":
				scope := eventBoundaryEnclosingScope(file, value.Pos())
				gotAdmission[eventBoundaryCallsite{path: relative, scope: scope, name: selector.Sel.Name}]++
			case "RestoreAdmittedEvent":
				if relative != "internal/store/internal/eventrecord/record.go" {
					t.Fatalf("%s:%d restores durable events outside the canonical record decoder", relative, fset.Position(value.Pos()).Line)
				}
			}
		}
		return true
	})

	if relative == "internal/store/eventfixture/event.go" || relative == "internal/store/storetest/event.go" {
		return
	}
	for _, declaration := range file.Decls {
		switch value := declaration.(type) {
		case *ast.FuncDecl:
			if strings.HasPrefix(relative, "internal/store/") && eventBoundaryPersistenceVerb(value.Name.Name) && eventBoundaryFieldsContain(value.Type.Params, eventAliases, "Event") {
				t.Fatalf("%s:%d persistence function %s accepts raw events.Event", relative, fset.Position(value.Pos()).Line, value.Name.Name)
			}
		case *ast.GenDecl:
			for _, spec := range value.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}
				iface, ok := typeSpec.Type.(*ast.InterfaceType)
				if !ok {
					continue
				}
				for _, method := range iface.Methods.List {
					fn, ok := method.Type.(*ast.FuncType)
					if !ok || !eventBoundaryFieldsContain(fn.Params, eventAliases, "Event") {
						continue
					}
					for _, name := range method.Names {
						if strings.HasPrefix(relative, "internal/store/") && eventBoundaryPersistenceVerb(name.Name) {
							t.Fatalf("%s:%d persistence interface method %s accepts raw events.Event", relative, fset.Position(method.Pos()).Line, name.Name)
						}
					}
				}
			}
		}
	}
}

func eventBoundaryRepositoryRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func eventBoundaryImportAliases(file *ast.File, importPath string) map[string]struct{} {
	aliases := map[string]struct{}{}
	for _, imported := range file.Imports {
		if strings.Trim(imported.Path.Value, `"`) != importPath {
			continue
		}
		if imported.Name == nil {
			aliases[filepath.Base(importPath)] = struct{}{}
		} else if imported.Name.Name != "_" && imported.Name.Name != "." {
			aliases[imported.Name.Name] = struct{}{}
		}
	}
	return aliases
}

func eventBoundaryTypeIs(expr ast.Expr, aliases map[string]struct{}, name string) bool {
	switch value := expr.(type) {
	case *ast.SelectorExpr:
		return value.Sel.Name == name && eventBoundaryPackageIdent(value.X, aliases)
	case *ast.StarExpr:
		return eventBoundaryTypeIs(value.X, aliases, name)
	case *ast.ArrayType:
		return eventBoundaryTypeIs(value.Elt, aliases, name)
	case *ast.Ellipsis:
		return eventBoundaryTypeIs(value.Elt, aliases, name)
	default:
		return false
	}
}

func eventBoundaryPackageIdent(expr ast.Expr, aliases map[string]struct{}) bool {
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	_, ok = aliases[ident.Name]
	return ok
}

func eventBoundaryFieldsContain(fields *ast.FieldList, aliases map[string]struct{}, name string) bool {
	if fields == nil {
		return false
	}
	for _, field := range fields.List {
		if eventBoundaryTypeIs(field.Type, aliases, name) {
			return true
		}
	}
	return false
}

func eventBoundaryPersistenceVerb(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	for _, prefix := range []string{"append", "commit", "insert", "persist", "save", "write"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func eventBoundaryEnclosingScope(file *ast.File, pos token.Pos) string {
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok || fn.Body == nil || pos < fn.Body.Pos() || pos > fn.Body.End() {
			continue
		}
		if fn.Recv == nil || len(fn.Recv.List) == 0 {
			return fn.Name.Name
		}
		return eventBoundaryReceiverName(fn.Recv.List[0].Type) + "." + fn.Name.Name
	}
	return "package"
}

func eventBoundaryReceiverName(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.StarExpr:
		return eventBoundaryReceiverName(value.X)
	case *ast.Ident:
		return value.Name
	case *ast.IndexExpr:
		return eventBoundaryReceiverName(value.X)
	case *ast.IndexListExpr:
		return eventBoundaryReceiverName(value.X)
	default:
		return "unknown"
	}
}
