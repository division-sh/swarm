package runtimepersistence

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
)

type eventBoundaryCallsite struct {
	path  string
	scope string
	name  string
}

var admittedEventCallsites = map[eventBoundaryCallsite]int{
	{path: "internal/store/internal/backend/eventpersistence/lifecycle_diagnostic.go", scope: "persistLifecycleDiagnosticTx", name: "AdmitForPersistence"}:                        1,
	{path: "internal/runtime/bus/outbox.go", scope: "engineDispatcher.DispatchPostCommit", name: "RevalidatePersistedEvent"}:                                                      1,
	{path: "internal/runtime/bus/eventbus_publish.go", scope: "EventBus.admitPublicationEventFacts", name: "AdmitForPersistence"}:                                                 1,
	{path: "internal/runtime/bus/eventbus_publish.go", scope: "admitEventForPublish", name: "AdmitForPublish"}:                                                                    1,
	{path: "internal/runtime/bus/eventbus_publish.go", scope: "EventBus.publishClaimedPipeline", name: "RevalidatePersistedEvent"}:                                                1,
	{path: "internal/runtime/bus/eventbus_publish.go", scope: "EventBus.PrepareSelectedForkPublish", name: "AdmitForPersistence"}:                                                 2,
	{path: "internal/runtime/bus/eventbus_publish.go", scope: "EventBus.prepareClosedPublication", name: "AdmitForPersistence"}:                                                   1,
	{path: "internal/runtime/bus/eventbus_publish.go", scope: "reuseDurableSubscribedEventRouteFacts", name: "AdmitForPersistence"}:                                               1,
	{path: "internal/runtime/manager/runtime.go", scope: "AgentManager.SendDirective", name: "AdmitForPersistence"}:                                                               1,
	{path: "internal/store/eventfixture/event.go", scope: "Insert", name: "AdmitForPersistence"}:                                                                                  1,
	{path: "internal/store/internal/backend/eventpersistence/inbound_publication.go", scope: "commitInboundPublicationTx", name: "AdmitForPersistence"}:                           1,
	{path: "internal/store/internal/backend/runforkpersistence/run_fork_delivery_event_replay.go", scope: "projectRunForkReplayEvent", name: "AdmitForPersistence"}:               1,
	{path: "internal/store/internal/backend/runforkpersistence/run_fork_delivery_event_replay.go", scope: "admitRunForkReplayEventTargetProjection", name: "AdmitForPersistence"}: 1,
	{path: "internal/store/internal/backend/eventpersistence/runtime_log_persistence.go", scope: "admitRuntimeLogRecord", name: "AdmitForPersistence"}:                            1,
	// Read-only exact diagnostic receipt comparison uses the selected store's payload admission owner.
	{path: "internal/store/internal/backend/eventpersistence/runtime_log_persistence.go", scope: "EventPostgresOwner.admitRuntimeLogRecord", name: "AdmitForPersistence"}: 1,
	{path: "internal/store/internal/backend/eventpersistence/runtime_log_persistence.go", scope: "EventSQLiteOwner.admitRuntimeLogRecord", name: "AdmitForPersistence"}:   1,
	{path: "internal/store/storetest/event.go", scope: "InsertCanonicalEventRecord", name: "AdmitForPersistence"}:                                                         1,
	{path: "internal/store/storetest/event.go", scope: "commitSemanticEventWithInitialFacts", name: "AdmitForPublish"}:                                                    1,
}

var eventRecordImportFiles = map[string]struct{}{
	// Barrier outcomes load canonical complete records through LoadAdmittedMany.
	"internal/store/internal/backend/pipelinepersistence/fan_out_barrier_owner.go": {},
	// Sealed-group readback uses LoadAdmitted before comparing exact member integrity.
	"internal/store/internal/backend/pipelinepersistence/publication_group.go": {},
	// Operator hydration uses canonical admitted records and canonical delivery snapshots.
	"internal/store/internal/operatorsurface/operator_event_batch.go":                {},
	"internal/store/internal/operatorsurface/operator_observability_read_surface.go": {},
	"internal/store/internal/operatorsurface/sqlite_runtime_observability.go":        {},
	// The materialization dependency owner decodes complete publication records; it does not reconstruct event identity.
	"internal/store/internal/backend/delivery/receiver_materialization.go": {},
	// Historical inherited ordinals are revalidated by the complete-record decoder before the shared fan-out owner.
	"internal/store/internal/backend/runforkpersistence/run_fork_inherited_fan_out_history.go":            {},
	"internal/store/eventfixture/event.go":                                                                {},
	"internal/store/internal/backend/delivery/lifecycle.go":                                               {},
	"internal/store/internal/backend/eventpersistence/event_persistence_identity.go":                      {},
	"internal/store/internal/backend/eventpersistence/owner.go":                                           {}, // Owns the named canonical SingleEventReader handle, not another decoder.
	"internal/store/internal/backend/eventpersistence/runtime_log_persistence.go":                         {}, // Exact named diagnostic replay validation.
	"internal/store/internal/backend/eventpersistence/event_reference_integrity.go":                       {},
	"internal/store/internal/backend/eventpersistence/events.go":                                          {},
	"internal/store/internal/backend/eventpersistence/sqlite_events.go":                                   {},
	"internal/store/internal/operatorsurface/helpers.go":                                                  {},
	"internal/store/internal/operatorsurface/pending_delivery_read_surface.go":                            {},
	"internal/store/internal/backend/pipelinepersistence/helpers.go":                                      {},
	"internal/store/internal/backend/runforkpersistence/run_fork_delivery_event_replay.go":                {},
	"internal/store/internal/backend/runforkpersistence/run_fork_activity_lineage.go":                     {},
	"internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_execution_mutation.go": {},
	// Fixed-revision input facts consume the same complete Record decoder;
	// this owner does not gain event SQL or admission-constructor authority.
	"internal/store/internal/backend/runforkpersistence/run_fork_input_publication.go": {},
	// selectedContractWorkflowSourceModes decodes complete records before checking
	// exact source-run ownership; it does not reconstruct identity with raw SQL.
	"internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_materialization_owner.go": {},
	"internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_discard_owner.go":         {},
	"internal/store/internal/backend/runlifecycle/standalone_runtime.go":                                     {},
	"internal/store/storetest/event.go": {},
}

var eventRecordSQLFiles = map[string]struct{}{
	"internal/store/internal/backend/eventrecord/postgres/adapter.go": {},
	"internal/store/internal/backend/eventrecord/sqlite/adapter.go":   {},
}

var eventPayloadBytesSQLFiles = map[string]struct{}{
	"internal/store/internal/backend/delivery/dead_letters.go":        {},
	"internal/store/internal/backend/eventrecord/postgres/adapter.go": {},
	"internal/store/internal/backend/eventrecord/sqlite/adapter.go":   {},
	"internal/store/internal/backend/runforkrevision/projection.go":   {},
}

var directEventSQLTestFixtures = map[string]int{
	// Isolated aggregate-query schema admits NULL/foreign rows deliberately;
	// these are not executable event fixtures or an event publication path.
	"internal/store/internal/backend/pipelinepersistence/pipeline_run_summary_materialization_test.go": 4,
	// Canonically reminted, never-executed requests must fail real fork activation.
	"internal/runtime/cataloge2e/selected_fork_activity_lineage_test.go":                         1,
	"internal/cliapp/raw_sql_boundary_test.go":                                                   1,
	"internal/store/internal/runtimepersistence/event_schema_contract_test.go":                   2,
	"internal/store/internal/runtimepersistence/run_fork_revision_selected_store_parity_test.go": 1,
}

var eventInsertSQL = regexp.MustCompile(`(?is)\bINSERT\s+INTO\s+events\b`)
var completeEventReadSQL = regexp.MustCompile(`(?is)\bevent_class\b.*\bFROM\s+events\b`)
var eventPayloadBytesSQL = regexp.MustCompile(`(?is)\bpayload_bytes\b`)
var eventPayloadColumnSQL = regexp.MustCompile(`(?i)\bpayload(?:_bytes)?\b`)

func TestActivityResultExistenceProjectionCannotOwnEventPayload(t *testing.T) {
	recordType := reflect.TypeOf(runtimeactivityresult.Record{})
	if recordType.NumField() != 2 || recordType.Field(0).Name != "EventID" || recordType.Field(1).Name != "EventType" {
		t.Fatalf("activity-result record fields = %#v; existence projection must expose only event identity and type", recordType)
	}

	path := filepath.Join(eventBoundaryRepositoryRoot(t), "internal/store/internal/backend/activityresult/reader.go")
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse activity-result reader: %v", err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		raw, err := strconv.Unquote(literal.Value)
		if err == nil && eventPayloadColumnSQL.MatchString(raw) {
			t.Errorf("activity-result existence projection contains payload SQL authority: %q", raw)
		}
		return true
	})
}

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
	markers := loadEventObservationMarkers(t)
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
			observations, err := classifyEventObservationMarkers(relative, file, markers)
			if err != nil {
				return err
			}
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				if observations[literal.Pos()] {
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

type eventObservationMarker struct {
	Path    string `json:"path"`
	Scope   string `json:"scope"`
	Literal string `json:"literal"`
}

func loadEventObservationMarkers(t *testing.T) []eventObservationMarker {
	t.Helper()
	raw, err := os.ReadFile("testdata/event_observation_markers.json")
	if err != nil {
		t.Fatal(err)
	}
	var markers []eventObservationMarker
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&markers); err != nil {
		t.Fatal(err)
	}
	if len(markers) != 8 {
		t.Fatalf("observation marker census = %d, want 8 exact non-writer controls", len(markers))
	}
	seen := map[eventObservationMarker]bool{}
	for _, marker := range markers {
		if seen[marker] || marker.Scope == "" || !eventInsertSQL.MatchString(marker.Literal) {
			t.Fatalf("invalid or duplicate observation marker: %+v", marker)
		}
		if _, err := os.Stat(filepath.Join(eventBoundaryRepositoryRoot(t), marker.Path)); err != nil {
			t.Fatal(err)
		}
		seen[marker] = true
	}
	return markers
}

// These exact literals observe native writes or test the observer's matcher;
// they confer no permission on other SQL in the same file or function.
func classifyEventObservationMarkers(path string, file *ast.File, markers []eventObservationMarker) (map[token.Pos]bool, error) {
	positions := map[token.Pos]bool{}
	for _, marker := range markers {
		if marker.Path != path {
			continue
		}
		count := 0
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			raw, err := strconv.Unquote(literal.Value)
			if err != nil || raw != marker.Literal {
				return true
			}
			scope := eventBoundaryEnclosingScope(file, literal.Pos())
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.CONST {
					continue
				}
				for _, spec := range gen.Specs {
					value := spec.(*ast.ValueSpec)
					if len(value.Names) == 1 && value.Pos() <= literal.Pos() && literal.End() <= value.End() {
						scope = "const:" + value.Names[0].Name
					}
				}
			}
			if scope == marker.Scope {
				positions[literal.Pos()] = true
				count++
			}
			return true
		})
		if count != 1 {
			return nil, fmt.Errorf("%s %s observation %q occurs %d times, want exactly 1", path, marker.Scope, marker.Literal, count)
		}
	}
	return positions, nil
}

func TestHostileEventObservationMarkerClassificationIsExact(t *testing.T) {
	for _, marker := range loadEventObservationMarkers(t) {
		t.Run(marker.Scope+"/"+marker.Literal, func(t *testing.T) {
			declaration := func(scope, literal string) string {
				if strings.HasPrefix(scope, "const:") {
					return fmt.Sprintf("const %s = %q\n", strings.TrimPrefix(scope, "const:"), literal)
				}
				if receiver, method, ok := strings.Cut(scope, "."); ok {
					return fmt.Sprintf("type %s struct{}\nfunc (*%s) %s() { _ = %q }\n", receiver, receiver, method, literal)
				}
				return fmt.Sprintf("func %s() { _ = %q }\n", scope, literal)
			}
			exact := declaration(marker.Scope, marker.Literal)
			for _, tc := range []struct {
				name, path, source     string
				approved, unclassified int
				wantErr                bool
			}{
				{"exact", marker.Path, exact, 1, 0, false},
				{"wrong-path", marker.Path + ".sibling", exact, 0, 1, false},
				{"wrong-scope", marker.Path, declaration(marker.Scope+"Sibling", marker.Literal), 0, 0, true},
				{"missing", marker.Path, "", 0, 0, true},
				{"duplicate", marker.Path, exact + exact, 0, 0, true},
				{"altered-literal", marker.Path, declaration(marker.Scope, marker.Literal+" "), 0, 0, true},
				{"sibling-write", marker.Path, exact + declaration("Sibling", marker.Literal), 1, 1, false},
				{"same-scope-extra-write", marker.Path, exact + declaration(marker.Scope, marker.Literal+" "), 1, 1, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					file, err := parser.ParseFile(token.NewFileSet(), "fixture_test.go", "package fixture\n"+tc.source, 0)
					if err != nil {
						t.Fatal(err)
					}
					positions, err := classifyEventObservationMarkers(tc.path, file, []eventObservationMarker{marker})
					if (err != nil) != tc.wantErr {
						t.Fatalf("classification error = %v", err)
					}
					if tc.wantErr {
						return
					}
					unclassified := 0
					ast.Inspect(file, func(node ast.Node) bool {
						literal, ok := node.(*ast.BasicLit)
						if !ok || literal.Kind != token.STRING || positions[literal.Pos()] {
							return true
						}
						raw, _ := strconv.Unquote(literal.Value)
						unclassified += len(eventInsertSQL.FindAllString(raw, -1))
						return true
					})
					if len(positions) != tc.approved || unclassified != tc.unclassified {
						t.Fatalf("approved/unclassified = %d/%d, want %d/%d", len(positions), unclassified, tc.approved, tc.unclassified)
					}
				})
			}
		})
	}
}

func TestHostileEventRecordConsumerClassificationDoesNotGrantSQL(t *testing.T) {
	for _, path := range []string{
		"internal/store/internal/backend/pipelinepersistence/fan_out_barrier_owner.go",
		"internal/store/internal/backend/pipelinepersistence/publication_group.go",
		"internal/store/internal/operatorsurface/operator_event_batch.go",
		"internal/store/internal/operatorsurface/operator_observability_read_surface.go",
		"internal/store/internal/operatorsurface/sqlite_runtime_observability.go",
	} {
		if _, ok := eventRecordImportFiles[path]; !ok {
			t.Errorf("missing canonical consumer %s", path)
		}
		if _, ok := eventRecordImportFiles[strings.TrimSuffix(path, ".go")+"_sibling.go"]; ok {
			t.Errorf("sibling consumer authorized: %s", path)
		}
		if _, ok := eventRecordSQLFiles[path]; ok {
			t.Errorf("consumer gained event SQL: %s", path)
		}
		if _, ok := eventPayloadBytesSQLFiles[path]; ok {
			t.Errorf("consumer gained payload SQL: %s", path)
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
		if !strings.HasPrefix(importPath, "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord") {
			continue
		}
		if strings.HasPrefix(relative, "internal/store/internal/backend/eventrecord/") {
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
				if _, ok := eventRecordSQLFiles[relative]; !ok && !(relative == "internal/store/internal/backend/runforkrevision/projection.go" && eventBoundaryEnclosingScope(file, value.Pos()) == "canonicalProjectionSpec" && !eventInsertSQL.MatchString(raw)) {
					t.Fatalf("%s:%d owns event-record SQL outside a private backend adapter", relative, fset.Position(value.Pos()).Line)
				}
			}
			if eventPayloadBytesSQL.MatchString(raw) {
				if _, ok := eventPayloadBytesSQLFiles[relative]; !ok {
					t.Fatalf("%s:%d consumes authoritative event payload bytes outside the closed owner set", relative, fset.Position(value.Pos()).Line)
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
				if relative != "internal/store/internal/backend/eventrecord/record.go" {
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
	return repoRootForRuntimeWriterGuard(t)
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
