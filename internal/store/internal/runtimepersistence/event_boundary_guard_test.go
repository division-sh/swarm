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

	"github.com/division-sh/swarm/internal/checkoutsource"
	runtimeactivityresult "github.com/division-sh/swarm/internal/runtime/activityresult"
)

type eventBoundaryCallsite struct {
	path  string
	scope string
	name  string
}

var eventRecordImportFiles = map[string]struct{}{
	"internal/store/eventfixture/unrevisioned.go":                             {},
	"internal/store/internal/runtimepersistence/test_staged_event_fixture.go": {},
	// Snapshot inversion fixtures stage validated records through their native attempt.
	"internal/store/internal/backend/pipelinepersistence/test_snapshot_fixture.go": {},
	// Barrier outcomes load canonical complete records through LoadAdmittedMany.
	"internal/store/internal/backend/pipelinepersistence/fan_out_barrier_owner.go": {},
	// Sealed-group readback uses LoadAdmitted before comparing exact member integrity.
	"internal/store/internal/backend/pipelinepersistence/publication_group.go": {},
	// Clock replay reads accepted occurrences through the canonical event record owner.
	"internal/store/internal/backend/pipelinepersistence/generic_schedule_occurrence_commit.go": {},
	// Fixed-session activity recovery reads retained requests/results through the
	// complete native decoder; it gains no event SQL or construction allowance.
	"internal/store/internal/startupownership/activity_recovery_native.go": {},
	// Operator hydration uses canonical admitted records and canonical delivery snapshots.
	"internal/store/internal/operatorsurface/operator_event_batch.go":                {},
	"internal/store/internal/operatorsurface/operator_observability_read_surface.go": {},
	"internal/store/internal/operatorsurface/sqlite_runtime_observability.go":        {},
	// #2720 Gate E 6221291258: read-only exact turn/publication attribution
	// consumes the canonical record decoder, never event SQL or admission.
	"internal/store/internal/operatorsurface/run_turn_attribution.go": {},
	// The materialization dependency owner decodes complete publication records; it does not reconstruct event identity.
	"internal/store/internal/backend/delivery/receiver_materialization.go": {},
	// Historical inherited ordinals are revalidated by the complete-record decoder before the shared fan-out owner.
	"internal/store/internal/backend/runforkpersistence/run_fork_inherited_fan_out_history.go": {},
	// Continuing fixed-cut outcomes consume canonical complete records; this is
	// readback, not another event decoder or publication admission owner.
	// Selected publication evidence consumes the same decoder before exact
	// lineage binding; the immutable revision projection is its source owner.
	"internal/store/internal/backend/runforkpersistence/run_fork_selected_fan_out_evidence.go": {},
	"internal/store/internal/backend/runforkrevision/projection.go":                            {},
	"internal/store/internal/backend/runforkpersistence/run_fork_continued_readback.go":        {},
	"internal/store/eventfixture/event.go":                                                     {},
	"internal/store/internal/backend/delivery/lifecycle.go":                                    {},
	"internal/store/internal/backend/eventpersistence/event_persistence_identity.go":           {},
	"internal/store/internal/backend/eventpersistence/owner.go":                                {}, // Owns the named canonical SingleEventReader handle, not another decoder.
	"internal/store/internal/backend/eventpersistence/runtime_log_persistence.go":              {}, // Exact named diagnostic replay validation.
	"internal/store/internal/backend/eventpersistence/event_reference_integrity.go":            {},
	"internal/store/internal/backend/eventpersistence/events.go":                               {},
	"internal/store/internal/backend/eventpersistence/sqlite_events.go":                        {},
	"internal/store/internal/operatorsurface/helpers.go":                                       {},
	"internal/store/internal/operatorsurface/pending_delivery_read_surface.go":                 {},
	"internal/store/internal/backend/pipelinepersistence/helpers.go":                           {},
	"internal/store/internal/backend/runforkpersistence/run_fork_delivery_event_replay.go":     {},
	"internal/store/internal/backend/runforkpersistence/run_fork_activity_lineage.go":          {},
	// Continued activity admission reads the canonical complete publication before
	// checking fixed-cut input and selected lineage; it does not write event SQL.
	"internal/store/internal/backend/runforkpersistence/run_fork_activity_source_identity.go":             {},
	"internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_execution_mutation.go": {},
	// Fixed-revision input facts consume the same complete Record decoder;
	// this owner does not gain event SQL or admission-constructor authority.
	"internal/store/internal/backend/runforkpersistence/run_fork_input_publication.go": {},
	// Current selected-input replay and timer-root settlement read canonical
	// admitted records; neither is an event SQL writer or alternate decoder.
	"internal/store/internal/backend/runforkpersistence/selected_input_recovery.go": {},
	"internal/store/internal/backend/runforkpersistence/selected_timer_lineage.go":  {},
	// selectedContractWorkflowSourceModes decodes complete records before checking
	// exact source-run ownership; it does not reconstruct identity with raw SQL.
	"internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_materialization_owner.go": {},
	"internal/store/internal/backend/runforkpersistence/run_fork_selected_contract_discard_owner.go":         {},
	"internal/store/internal/backend/runlifecycle/standalone_runtime.go":                                     {},
	"internal/store/storetest/event.go": {},
	// Named fixture orchestration consumes canonical record encoding only.
	"internal/store/internal/runtimepersistence/test_event_support.go": {},
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

var eventInsertSQL = regexp.MustCompile(`(?is)\bINSERT\s+INTO\s+events\b`)

var eventPayloadColumnSQL = regexp.MustCompile(`(?i)\bpayload(?:_bytes)?\b`)

const unrevisionedEventFixturePath = "internal/store/internal/backend/eventrecord/staged/fixture.go"

var unrevisionedEventFixtureSQL = map[string]string{
	"postgres": "INSERT INTO " + `events (
		event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload, payload_bytes,
		payload_schema_bundle_hash, payload_schema_flow_id, payload_schema_event_key,
		payload_schema_digest, payload_schema_class,
		execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
		routing_source_kind, routing_source_authority, source_route, target_route, target_set,
		route_settlement, operator_reference_event_id, inherited_fan_out_origin, run_owner_key
	) VALUES (
		$1, $2::uuid, NULLIF($3,'')::uuid, $4, NULLIF($5,''), NULLIF($6,'')::uuid, NULLIF($7,''), $8, $9::jsonb, $10::bytea,
		$11, NULLIF($12,''), $13, $14, $15,
		$16, $17, $18, $19, NULLIF($20,'')::uuid, $21,
		$22, NULLIF($23,''), $24::jsonb, $25::jsonb, $26::jsonb,
		$27::jsonb, NULLIF($28,'')::uuid, NULLIF($29,'')::jsonb,
		COALESCE('run:' || CAST(NULLIF($3,'')::uuid AS TEXT), '')
	) ON CONFLICT (event_id) DO NOTHING`,
	"sqlite": "INSERT INTO " + `events (
		event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload, payload_bytes,
		payload_schema_bundle_hash, payload_schema_flow_id, payload_schema_event_key,
		payload_schema_digest, payload_schema_class,
		execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
		routing_source_kind, routing_source_authority, source_route, target_route, target_set,
		route_settlement, operator_reference_event_id, inherited_fan_out_origin, run_owner_key
	) VALUES (?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), COALESCE('run:' || CAST(NULLIF(?, '') AS TEXT), ''))
	ON CONFLICT(event_id) DO NOTHING`,
}

func classifiedUnrevisionedEventFixtureSQL(path, scope, query string) string {
	if path != unrevisionedEventFixturePath || scope != "insertRecord" {
		return ""
	}
	for name, allowed := range unrevisionedEventFixtureSQL {
		if strings.Join(strings.Fields(query), " ") == strings.Join(strings.Fields(allowed), " ") {
			return name
		}
	}
	return ""
}

var unrevisionedEventFixtureConsumers = map[eventBoundaryCallsite]int{
	{path: "internal/store/internal/runtimepersistence/test_staged_event_fixture.go", scope: "InsertStagedChildEventForTest", name: "StagedChild"}: 1,
}

func classifiedUnrevisionedEventFixtureConsumer(path, scope, name string) bool {
	_, ok := unrevisionedEventFixtureConsumers[eventBoundaryCallsite{path: path, scope: scope, name: name}]
	return ok
}

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

func TestUnrevisionedEventFixtureSQLAllowanceIsExact(t *testing.T) {
	for name, allowed := range unrevisionedEventFixtureSQL {
		t.Run(name, func(t *testing.T) {
			if got := classifiedUnrevisionedEventFixtureSQL(unrevisionedEventFixturePath, "insertRecord", allowed); got != name {
				t.Fatalf("fixture SQL classification = %q, want %q", got, name)
			}
			for _, hostile := range []struct{ path, scope, query string }{
				{"internal/store/internal/backend/eventrecord/staged/sibling.go", "insertRecord", allowed},
				{unrevisionedEventFixturePath, "siblingFixture", allowed},
				{unrevisionedEventFixturePath, "Insert", allowed},
				{unrevisionedEventFixturePath, "insertRecord", allowed + " RETURNING event_id"},
				{unrevisionedEventFixturePath, "insertRecord", strings.ReplaceAll(allowed, "run_owner_key", "other_owner_key")},
				{unrevisionedEventFixturePath, "insertRecord", strings.ReplaceAll(allowed, "'run:'", "'event:'")},
			} {
				if got := classifiedUnrevisionedEventFixtureSQL(hostile.path, hostile.scope, hostile.query); got != "" {
					t.Fatalf("hostile fixture SQL classified as %q: %#v", got, hostile)
				}
			}
		})
	}
}

func TestUnrevisionedEventFixtureHasOnlyStoretestConsumers(t *testing.T) {
	repoRoot := eventBoundaryRepositoryRoot(t)
	found := map[eventBoundaryCallsite]int{}
	for _, rootName := range []string{"internal", "cmd"} {
		root := filepath.Join(repoRoot, rootName)
		if err := checkoutsource.WalkDir(repoRoot, root, func(path string, entry os.DirEntry, walkErr error) error {
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
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return err
			}
			for _, imported := range file.Imports {
				if strings.Trim(imported.Path.Value, `"`) == "github.com/division-sh/swarm/internal/store/storetest" && !checkoutsource.IsTestFixturePackage(filepath.Dir(relative)) {
					t.Errorf("%s imports test-only storetest fixtures from production code", relative)
				}
			}
			aliases := eventBoundaryImportAliases(file, "github.com/division-sh/swarm/internal/store/eventfixture")
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || !eventBoundaryPackageIdent(selector.X, aliases) || selector.Sel.Name != "StagedChild" {
					return true
				}
				scope := eventBoundaryEnclosingScope(file, call.Pos())
				site := eventBoundaryCallsite{path: relative, scope: scope, name: selector.Sel.Name}
				found[site]++
				if !classifiedUnrevisionedEventFixtureConsumer(relative, scope, selector.Sel.Name) {
					t.Errorf("%s.%s in %s calls a test-only raw event fixture outside its closed consumer", scope, selector.Sel.Name, relative)
				}
				return true
			})
			return nil
		}); err != nil {
			t.Fatalf("walk %s: %v", rootName, err)
		}
	}
	for site, want := range unrevisionedEventFixtureConsumers {
		if found[site] != want {
			t.Errorf("%s.%s in %s has %d raw fixture calls, want %d", site.scope, site.name, site.path, found[site], want)
		}
	}
}

func TestUnrevisionedEventFixtureConsumerAllowanceIsExact(t *testing.T) {
	for site := range unrevisionedEventFixtureConsumers {
		if !classifiedUnrevisionedEventFixtureConsumer(site.path, site.scope, site.name) {
			t.Fatalf("approved fixture consumer rejected: %#v", site)
		}
		for _, hostile := range []eventBoundaryCallsite{
			{path: "internal/store/storetest/sibling.go", scope: site.scope, name: site.name},
			{path: site.path, scope: "siblingFixture", name: site.name},
			{path: site.path, scope: site.scope, name: "InsertOtherFixture"},
		} {
			if classifiedUnrevisionedEventFixtureConsumer(hostile.path, hostile.scope, hostile.name) {
				t.Fatalf("hostile fixture consumer admitted: %#v", hostile)
			}
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
		"internal/store/internal/startupownership/activity_recovery_native.go",
		"internal/store/internal/backend/pipelinepersistence/fan_out_barrier_owner.go",
		"internal/store/internal/backend/pipelinepersistence/publication_group.go",
		"internal/store/internal/backend/pipelinepersistence/generic_schedule_occurrence_commit.go",
		"internal/store/internal/operatorsurface/operator_event_batch.go",
		"internal/store/internal/operatorsurface/operator_observability_read_surface.go",
		"internal/store/internal/operatorsurface/sqlite_runtime_observability.go",
		"internal/store/internal/operatorsurface/run_turn_attribution.go",
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

func eventBoundaryPackageIdent(expr ast.Expr, aliases map[string]struct{}) bool {
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	_, ok = aliases[ident.Name]
	return ok
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
