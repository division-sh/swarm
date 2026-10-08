package runtimepersistence

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func TestRunLifecycleOwnershipBoundaryGuard(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	allowedWrites := map[string]bool{
		"internal/store/internal/backend/runlifecycle/counterprojection/event_count.go": true,
		"internal/store/internal/backend/runlifecycle/run_lifecycle_candidates.go":      true,
		"internal/store/internal/backend/runlifecycle/run_lifecycle_mutation.go":        true,
		"internal/store/internal/backend/runlifecycle/run_lifecycle_state.go":           true,
		"internal/store/internal/backend/runlifecycle/test_snapshot_fault.go":           true,
		"internal/testutil/runlifecyclefixture/fixture.go":                              true,
	}
	allowedCandidateColumns := map[string]bool{
		"internal/store/internal/backend/runlifecycle/run_lifecycle_candidates.go": true,
		"internal/store/internal/backend/runlifecycle/run_lifecycle_state.go":      true,
		"internal/testutil/runlifecyclefixture/fixture.go":                         true,
	}
	runWrite := regexp.MustCompile(`(?is)\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+runs\b`)
	var violations []string
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		source := string(raw)
		if relative == "internal/store/internal/runtimepersistence/test_mailbox_fixture.go" {
			source, err = maskNamedMailboxSourceFixtureWrite(source)
			if err != nil {
				violations = append(violations, relative+": "+err.Error())
			}
		}
		if runWrite.MatchString(source) && !allowedWrites[relative] {
			violations = append(violations, relative+": writes runs outside the private lifecycle adapters")
		}
		if (strings.Contains(source, "completion_due_at") || strings.Contains(source, "completion_revision")) && !allowedCandidateColumns[relative] {
			violations = append(violations, relative+": accesses private completion candidate columns")
		}
		for _, legacy := range []string{
			"ConvergeNormalRunCompletion",
			"ConvergeStandaloneRuntimePlatformRun",
			"MarkRunTerminal(",
			"RunLifecyclePersistence",
			"internal/store/runlifecycle",
			"status IN ('running', 'paused')",
		} {
			if strings.Contains(source, legacy) {
				violations = append(violations, relative+": retains legacy lifecycle owner "+legacy)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("run lifecycle ownership violations:\n%s", strings.Join(violations, "\n"))
	}
}

func TestRunLifecycleCompletionReadsOnlyOwnerSummaries(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	adjacentOwnerFacts := []string{
		"agent_sessions",
		"decision_cards",
		"human_task_continuations",
		"proposed_effect_continuations",
		"stage_gates",
		"runtime_external_effect_operations",
		"runtime_external_effect_attempts",
		"runtime_effect_budget_reservations",
		"entity_state",
		"flow_instances",
	}
	var violations []string
	for _, name := range []string{
		"run_lifecycle_candidates.go",
		"run_lifecycle_obligations.go",
		"sqlite_run_completion.go",
	} {
		path := filepath.Join(root, "internal/store/internal/backend/runlifecycle", name)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		source := string(raw)
		for _, fact := range adjacentOwnerFacts {
			if strings.Contains(source, fact) {
				relative, _ := filepath.Rel(root, path)
				violations = append(violations, filepath.ToSlash(relative)+": interprets "+fact)
			}
		}
	}
	sort.Strings(violations)
	if len(violations) != 0 {
		t.Fatalf("run lifecycle completion bypasses canonical owner summaries:\n%s", strings.Join(violations, "\n"))
	}
}

func TestSemanticRunFixturesUseLifecycleOwner(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	runWrite := regexp.MustCompile(`(?is)\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+runs\b`)
	var violations []string
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == "internal/store/internal/runtimepersistence/run_lifecycle_ownership_guard_test.go" ||
			relative == "internal/runtime/pipeline/run_lifecycle_test_adapter_test.go" {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		hostile, err := classifyOperatorLineageHostileRunLiterals(relative, file)
		if err != nil {
			violations = append(violations, relative+": "+err.Error())
		}
		minimalHistory, err := classifyReceiverHistoryMinimalRunLiterals(relative, file)
		if err != nil {
			violations = append(violations, relative+": "+err.Error())
		}
		minimalProjection, err := classifyBackendMinimalRunLiterals(relative, file)
		if err != nil {
			violations = append(violations, relative+": "+err.Error())
		}
		counterOracles := classifyCounterOracleRunLiterals(relative, file)
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil || !runWrite.MatchString(value) {
				return true
			}
			if hostile[literal.Pos()] || minimalHistory[literal.Pos()] || minimalProjection[literal.Pos()] || counterOracles[literal.Pos()] || allowedSemanticRunFixtureLiteral(relative, value) {
				return true
			}
			violations = append(violations, relative+": "+compactSQLForLifecycleGuard(value))
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("semantic run fixtures bypass the lifecycle owner:\n%s", strings.Join(violations, "\n"))
	}
}

// These literals assert counter-owner behavior; none can execute a run mutation.
func classifyCounterOracleRunLiterals(path string, file *ast.File) map[token.Pos]bool {
	approved := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		mockOracles := map[string]bool{}
		if path == "internal/store/internal/backend/mutationprotocol/event_counts_test.go" && fn.Name.Name == "TestEventCountDeltasBatchOrderAndForeignReadRefusal" {
			mockOracles[`UPDATE runs SET event_count = event_count \+`] = true
		}
		if path == "internal/store/internal/backend/runlifecycle/run_admission_test.go" && fn.Name.Name == "TestRunAdmissionCanonicalSourceRevisionReloads" {
			mockOracles["UPDATE runs SET bundle_hash = $2 WHERE run_id = $1::uuid AND status IN ('running', 'paused')"] = true
			mockOracles["UPDATE runs SET bundle_hash = ? WHERE run_id = ? AND status IN ('running', 'paused')"] = true
		}
		if len(mockOracles) != 0 {
			ast.Inspect(fn, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || len(call.Args) != 1 {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "ExpectExec" {
					return true
				}
				receiver, ok := selector.X.(*ast.Ident)
				if !ok || receiver.Name != "mock" {
					return true
				}
				literal, ok := call.Args[0].(*ast.BasicLit)
				if ok {
					value, err := strconv.Unquote(literal.Value)
					if err == nil && mockOracles[value] {
						approved[literal.Pos()] = true
					}
				}
				return true
			})
		}
		if path == "internal/store/internal/backend/runlifecycle/counterprojection/owner_guard_test.go" && fn.Name.Name == "TestRunCounterRetirementGuardRejectsRecountAndUnknownZero" {
			nonExecuting := true
			ast.Inspect(fn, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || (selector.Sel.Name != "MatchString" && selector.Sel.Name != "Fatal" && selector.Sel.Name != "Fatalf") {
					nonExecuting = false
				}
				return true
			})
			if !nonExecuting {
				continue
			}
			ast.Inspect(fn, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err == nil && (value == "UPDATE runs SET event_count = (SELECT COUNT(*) FROM events)" || value == "UPDATE runs SET event_count = event_count + $1") {
					approved[literal.Pos()] = true
				}
				return true
			})
		}
	}
	return approved
}

func TestCounterRunOracleClassificationDoesNotPermitWriters(t *testing.T) {
	const oraclePath = "internal/store/internal/backend/runlifecycle/counterprojection/owner_guard_test.go"
	const source = "package fixture\nfunc TestRunCounterRetirementGuardRejectsRecountAndUnknownZero() { retiredCounterSQL.MatchString(`UPDATE runs SET event_count = (SELECT COUNT(*) FROM events)`) }"
	for _, tc := range []struct {
		name, path, source string
		want               int
	}{
		{"oracle", oraclePath, source, 1},
		{"other-path", "internal/runtime/other_test.go", source, 0},
		{"executing", oraclePath, strings.Replace(source, "retiredCounterSQL.MatchString", "db.Exec", 1), 0},
		{"extra-writer", oraclePath, strings.Replace(source, " { ", " { db.Exec(`DELETE FROM runs`); ", 1), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(classifyCounterOracleRunLiterals(tc.path, file)); got != tc.want {
				t.Fatalf("classified %d, want %d", got, tc.want)
			}
		})
	}
}

func classifyBackendMinimalRunLiterals(path string, file *ast.File) (map[token.Pos]bool, error) {
	if path == "internal/store/internal/backend/pipelinepersistence/a2_collection_projection_test.go" {
		return classifyA2CollectionMinimalRunLiterals(file)
	}
	type fixtureShape struct {
		schema string
		writes []string
	}
	shapes := map[string]fixtureShape{
		"internal/store/internal/backend/mutationprotocol/event_counts_test.go": {
			"CREATE TABLE runs (run_id UUID PRIMARY KEY, event_count INTEGER NOT NULL DEFAULT 0 CHECK (event_count >= 0))", []string{"INSERT INTO runs (run_id) VALUES ($1)"}},
		"internal/store/internal/backend/agentpersistence/selected_grant_point_test.go": {
			"CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL)", []string{"INSERT INTO runs VALUES ($1,$2)"}},
		"internal/store/internal/backend/delivery/selected_execution_fence_test.go": {
			"CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL)", []string{"INSERT INTO runs VALUES ($1,$2)", "UPDATE runs SET bundle_hash=$1"}},
		"internal/store/internal/backend/delivery/selected_successor_handoff_test.go": {
			"CREATE TABLE runs (run_id TEXT PRIMARY KEY,bundle_hash TEXT NOT NULL)", []string{"INSERT INTO runs VALUES ($1,$2)"}},
		"internal/store/internal/backend/pipelinepersistence/fan_out_deployment_test.go": {
			"CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL, status TEXT NOT NULL)", []string{"INSERT INTO runs VALUES ($1,$2,'running')"}},
		"internal/store/internal/backend/pipelinepersistence/fan_out_selected_successor_test.go": {
			"CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL, status TEXT NOT NULL)", []string{"INSERT INTO runs VALUES ($1,$2,'running')"}},
		"internal/store/internal/backend/runforkpersistence/deployment_revision_point_test.go": {
			"CREATE TABLE runs (run_id TEXT PRIMARY KEY, origin_kind TEXT NOT NULL)", []string{"INSERT INTO runs VALUES($1,'deployment')"}},
		"internal/store/internal/backend/runforkpersistence/historical_snapshot_race_test.go": {
			"CREATE TABLE runs (run_id TEXT PRIMARY KEY, origin_kind TEXT NOT NULL)", []string{"INSERT INTO runs VALUES($1,'event')"}},
		"internal/store/internal/backend/runforkpersistence/lifecycle_diagnostic_point_test.go": {
			"CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL)", []string{"INSERT INTO runs VALUES ($1,$2)"}},
		"internal/store/internal/backend/runforkpersistence/selected_finite_feed_operation_test.go": {
			"CREATE TABLE runs (run_id TEXT PRIMARY KEY,status TEXT NOT NULL)", []string{"INSERT INTO runs VALUES ($1,'paused')"}},
		"internal/store/internal/backend/runforkpersistence/run_fork_branch_divergence_test.go": {
			"CREATE TABLE runs (run_id ID_TYPE PRIMARY KEY, bundle_hash TEXT NOT NULL, origin_kind TEXT, status TEXT, forked_from_run_id ID_TYPE, forked_from_point_kind TEXT, forked_from_revision BIGINT, forked_from_event_id ID_TYPE)",
			[]string{
				"INSERT INTO runs (run_id,bundle_hash,origin_kind,status) VALUES ($1,$2,'deployment',$3)",
				"INSERT INTO runs (run_id,bundle_hash,origin_kind,status,forked_from_run_id,forked_from_point_kind,forked_from_revision,forked_from_event_id) VALUES ($1,$2,'fork_materialization','running',$3,$4,$5,$6)",
				"UPDATE runs SET forked_from_revision=forked_from_revision+1 WHERE run_id=$1",
				"UPDATE runs SET forked_from_revision=forked_from_revision-1 WHERE run_id=$1",
			},
		},
	}
	shape, ok := shapes[path]
	if !ok {
		return nil, nil
	}
	schemaFound := false
	approved := map[token.Pos]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}
		compact := compactSQLForLifecycleGuard(value)
		if compact == shape.schema {
			schemaFound = true
		}
		for _, write := range shape.writes {
			if compact == write {
				approved[literal.Pos()] = true
			}
		}
		return true
	})
	if !schemaFound {
		return nil, fmt.Errorf("minimal backend runs schema is missing")
	}
	return approved, nil
}

func classifyA2CollectionMinimalRunLiterals(file *ast.File) (map[token.Pos]bool, error) {
	schemas := map[string]int{
		"CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL, forked_from_run_id TEXT)": 0,
		"CREATE TABLE runs (run_id UUID PRIMARY KEY, bundle_hash TEXT NOT NULL, forked_from_run_id UUID)": 0,
	}
	approved := map[token.Pos]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}
		value = compactSQLForLifecycleGuard(value)
		if _, known := schemas[value]; known {
			schemas[value]++
		}
		switch value {
		case "INSERT INTO runs (run_id,bundle_hash) VALUES ($1,$2)",
			"INSERT INTO runs (run_id,bundle_hash,forked_from_run_id) VALUES ($1,$4,NULL),($2,$4,$1),($3,$4,NULL)":
			approved[literal.Pos()] = true
		}
		return true
	})
	for schema, count := range schemas {
		if count != 1 {
			return nil, fmt.Errorf("collection fixture requires exactly one %q, got %d", schema, count)
		}
	}
	return approved, nil
}

func TestBranchDivergenceMinimalRunFixtureClassificationIsExact(t *testing.T) {
	const path = "internal/store/internal/backend/runforkpersistence/run_fork_branch_divergence_test.go"
	raw, err := os.ReadFile(filepath.Join(repoRootForRuntimeWriterGuard(t), path))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	runWrite := regexp.MustCompile(`(?is)\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+runs\b`)
	for _, tc := range []struct {
		name, path, source string
		want               bool
	}{
		{"exact-native-projection", path, source, true},
		{"other-owner", "internal/runtime/other_test.go", source, false},
		{"missing-bundle-schema", path, strings.Replace(source, "bundle_hash TEXT NOT NULL, ", "", 1), false},
		{"expanded-schema", path, strings.Replace(source, "status TEXT,", "status TEXT, event_count BIGINT,", 1), false},
		{"missing-source-bundle", path, strings.Replace(source, "(run_id,bundle_hash,origin_kind,status)", "(run_id,origin_kind,status)", 1), false},
		{"lifecycle-write", path, source + "\nvar unsupported = " + strconv.Quote("UPDATE runs SET status='completed' WHERE run_id=$1"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "fixture_test.go", tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			approved, err := classifyBackendMinimalRunLiterals(tc.path, file)
			allowed := err == nil
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, unquoteErr := strconv.Unquote(literal.Value)
				if unquoteErr == nil && runWrite.MatchString(value) && !approved[literal.Pos()] {
					allowed = false
				}
				return true
			})
			if allowed != tc.want {
				t.Fatalf("allowed=%t want=%t err=%v", allowed, tc.want, err)
			}
		})
	}
}

func TestA2CollectionMinimalRunFixtureClassificationIsExact(t *testing.T) {
	const path = "internal/store/internal/backend/pipelinepersistence/a2_collection_projection_test.go"
	const schema = "CREATE TABLE runs (run_id TEXT PRIMARY KEY, bundle_hash TEXT NOT NULL, forked_from_run_id TEXT)"
	const postgresSchema = "CREATE TABLE runs (run_id UUID PRIMARY KEY, bundle_hash TEXT NOT NULL, forked_from_run_id UUID)"
	const insert = "INSERT INTO runs (run_id,bundle_hash) VALUES ($1,$2)"
	const lineage = "INSERT INTO runs (run_id,bundle_hash,forked_from_run_id) VALUES ($1,$4,NULL),($2,$4,$1),($3,$4,NULL)"
	const extra = "UPDATE runs SET status='running' WHERE run_id=$1"
	source := "package fixture\nfunc fixture() { use(" + strconv.Quote(schema) + "); use(" + strconv.Quote(postgresSchema) + "); use(" + strconv.Quote(insert) + "); use(" + strconv.Quote(lineage) + ") }"
	runWrite := regexp.MustCompile(`(?is)\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+runs\b`)
	for _, tc := range []struct {
		name, path, source string
		want               bool
	}{
		{"exact", path, source, true},
		{"wrong-path", "internal/runtime/other_test.go", source, false},
		{"missing-schema", path, strings.Replace(source, strconv.Quote(schema), `"SELECT 1"`, 1), false},
		{"semantic-schema", path, strings.Replace(source, "forked_from_run_id TEXT)", "forked_from_run_id TEXT, status TEXT)", 1), false},
		{"semantic-postgres-schema", path, strings.Replace(source, "forked_from_run_id UUID)", "forked_from_run_id UUID, status TEXT)", 1), false},
		{"missing-bundle", path, strings.Replace(source, insert, "INSERT "+"INTO runs (run_id) VALUES ($1)", 1), false},
		{"additional-lifecycle-write", path, strings.Replace(source, "use(", "use("+strconv.Quote(extra)+"); use(", 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "fixture_test.go", tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			approved, err := classifyBackendMinimalRunLiterals(tc.path, file)
			allowed := err == nil
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err == nil && runWrite.MatchString(value) && !approved[literal.Pos()] {
					allowed = false
				}
				return true
			})
			if allowed != tc.want {
				t.Fatalf("allowed=%v want=%v err=%v", allowed, tc.want, err)
			}
		})
	}
}

func classifyReceiverHistoryMinimalRunLiterals(path string, file *ast.File) (map[token.Pos]bool, error) {
	if path != "internal/store/internal/backend/runforkpersistence/receiver_config_history_test.go" {
		return nil, nil
	}
	// This native revision-capture test shadows runs with a transaction-local
	// one-column TEMP table. It cannot create a semantic runtime run. Require
	// that exact schema and insert in the same named test, not a file exemption.
	const scope = "TestRuntimeHeaderHistoricalCaptureAndReadinessBothStores"
	const schema = "CREATE TEMP TABLE runs (run_id TEXT PRIMARY KEY)"
	const insert = "INSERT INTO runs VALUES ($1)"
	schemas, inserts := 0, 0
	approved := map[token.Pos]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING || eventBoundaryEnclosingScope(file, literal.Pos()) != scope {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}
		switch compactSQLForLifecycleGuard(value) {
		case schema:
			schemas++
		case insert:
			inserts++
			approved[literal.Pos()] = true
		}
		return true
	})
	if schemas != 1 || inserts != 1 {
		return nil, fmt.Errorf("receiver history minimal schema requires exactly one TEMP runs declaration and insert, got %d/%d", schemas, inserts)
	}
	return approved, nil
}

func TestReceiverHistoryMinimalRunFixtureClassificationIsExact(t *testing.T) {
	const path = "internal/store/internal/backend/runforkpersistence/receiver_config_history_test.go"
	const scope = "TestRuntimeHeaderHistoricalCaptureAndReadinessBothStores"
	const schema = "CREATE TEMP TABLE runs (run_id TEXT PRIMARY KEY)"
	const insert = "INSERT INTO runs VALUES ($1)"
	const extra = "UPDATE runs SET status='running' WHERE run_id=$1"
	source := "package fixture\nfunc " + scope + "() { use(" + strconv.Quote(schema) + "); use(" + strconv.Quote(insert) + ") }\n"
	runWrite := regexp.MustCompile(`(?is)\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+runs\b`)
	for _, tc := range []struct {
		name, path, source string
		want               bool
	}{
		{"exact", path, source, true},
		{"wrong-path", "internal/runtime/other_test.go", source, false},
		{"wrong-function", path, strings.ReplaceAll(source, scope, "Other"), false},
		{"missing-schema", path, strings.Replace(source, strconv.Quote(schema), `"SELECT 1"`, 1), false},
		{"non-temporary-schema", path, strings.Replace(source, "CREATE TEMP TABLE", "CREATE TABLE", 1), false},
		{"semantic-schema", path, strings.Replace(source, "run_id TEXT PRIMARY KEY)", "run_id TEXT PRIMARY KEY, status TEXT)", 1), false},
		{"duplicate-schema", path, strings.Replace(source, "use(", "use("+strconv.Quote(schema)+"); use(", 1), false},
		{"duplicate-insert", path, strings.Replace(source, "use(", "use("+strconv.Quote(insert)+"); use(", 1), false},
		{"changed-insert", path, strings.Replace(source, insert, "INSERT INTO runs (run_id,bundle_hash) VALUES ($1,$2)", 1), false},
		{"same-function-extra", path, strings.Replace(source, "use(", "use("+strconv.Quote(extra)+"); use(", 1), false},
		{"sibling-function", path, source + "func other() { use(" + strconv.Quote(insert) + ") }", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "fixture_test.go", tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			approved, err := classifyReceiverHistoryMinimalRunLiterals(tc.path, file)
			allowed := err == nil
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err == nil && runWrite.MatchString(value) && !approved[literal.Pos()] {
					allowed = false
				}
				return true
			})
			if allowed != tc.want {
				t.Fatalf("allowed=%v want=%v err=%v", allowed, tc.want, err)
			}
		})
	}
}

func classifyOperatorLineageHostileRunLiterals(path string, file *ast.File) (map[token.Pos]bool, error) {
	if path != "internal/store/internal/runtimepersistence/operator_event_snapshot_lineage_test.go" {
		return nil, nil
	}
	// This test corrupts an already lawfully created fork while its read is pinned,
	// then restores it. Neither literal is a semantic run constructor/transition.
	exact := map[string]int{
		"UPDATE runs SET forked_from_run_id=$3,forked_from_event_id=$4 WHERE run_id=$1 AND forked_from_run_id=$2": 0,
		"UPDATE runs SET forked_from_run_id=$1,forked_from_event_id=$3 WHERE run_id=$2":                           0,
	}
	approved := map[token.Pos]bool{}
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING || eventBoundaryEnclosingScope(file, literal.Pos()) != "TestOperatorEventSnapshotInheritedLineageInterleavingBothStores" {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}
		value = compactSQLForLifecycleGuard(value)
		if _, ok := exact[value]; ok {
			exact[value]++
			approved[literal.Pos()] = true
		}
		return true
	})
	for query, count := range exact {
		if count != 1 {
			return nil, fmt.Errorf("lineage hostile fixture requires exactly one %q, got %d", query, count)
		}
	}
	return approved, nil
}

func TestOperatorLineageHostileRunFixtureClassificationIsExact(t *testing.T) {
	const path = "internal/store/internal/runtimepersistence/operator_event_snapshot_lineage_test.go"
	const scope = "TestOperatorEventSnapshotInheritedLineageInterleavingBothStores"
	const corrupt = "UPDATE runs SET forked_from_run_id=$3,forked_from_event_id=$4 WHERE run_id=$1 AND forked_from_run_id=$2"
	const restore = "UPDATE runs SET forked_from_run_id=$1,forked_from_event_id=$3 WHERE run_id=$2"
	const extra = "UPDATE runs SET status='paused' WHERE run_id=$1"
	source := "package fixture\nfunc " + scope + "() { use(" + strconv.Quote(corrupt) + "); use(" + strconv.Quote(restore) + ") }\n"
	runWrite := regexp.MustCompile(`(?is)\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+runs\b`)
	for _, tc := range []struct {
		name, path, source string
		want               bool
	}{
		{"exact", path, source, true},
		{"wrong-path", "internal/runtime/other_test.go", source, false},
		{"wrong-function", path, strings.ReplaceAll(source, scope, "Other"), false},
		{"missing-restore", path, strings.Replace(source, strconv.Quote(restore), `"SELECT 1"`, 1), false},
		{"missing-CAS", path, strings.Replace(source, " AND forked_from_run_id=$2", "", 1), false},
		{"duplicate-corruption", path, strings.Replace(source, "use(", "use("+strconv.Quote(corrupt)+"); use(", 1), false},
		{"same-function-extra", path, strings.Replace(source, "use(", "use("+strconv.Quote(extra)+"); use(", 1), false},
		{"sibling-function", path, source + "func other() { use(" + strconv.Quote(corrupt) + ") }", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "fixture_test.go", tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			approved, err := classifyOperatorLineageHostileRunLiterals(tc.path, file)
			allowed := err == nil
			ast.Inspect(file, func(node ast.Node) bool {
				literal, ok := node.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					return true
				}
				value, err := strconv.Unquote(literal.Value)
				if err == nil && runWrite.MatchString(value) && !approved[literal.Pos()] {
					allowed = false
				}
				return true
			})
			if allowed != tc.want {
				t.Fatalf("allowed=%t want=%t err=%v", allowed, tc.want, err)
			}
		})
	}
}

func allowedSemanticRunFixtureLiteral(relative string, value string) bool {
	compact := compactSQLForLifecycleGuard(value)
	switch relative {
	case "internal/runtime/cataloge2e/selected_fork_activity_lineage_test.go":
		// Deliberate persisted-source corruption and restoration at final fork
		// validation, not a semantic run constructor or lifecycle transition.
		return compact == "UPDATE runs SET bundle_hash=$1 WHERE run_id=$2"
	case "internal/store/internal/runtimepersistence/schema_compatibility_bootstrap_test.go":
		for _, legacyRunID := range []string{
			"00000000-0000-0000-0000-000000002055",
			"00000000-0000-0000-0000-000000002057",
		} {
			if strings.Contains(compact, legacyRunID) {
				return true
			}
		}
	case "internal/cliapp/api_consumption_boundary_test.go":
		return compact == "UPDATE runs"
	case "internal/store/internal/runtimepersistence/run_terminal_delivery_lock_order_test.go":
		return compact == "UPDATE RUNS"
	case "internal/store/internal/runtimepersistence/run_lifecycle_candidate_parity_test.go":
		return compact == "UPDATE runs SET completion_revision = 1, completion_due_at = ? WHERE run_id = ?" ||
			compact == "UPDATE runs SET completion_revision = 1, completion_due_at = $1 WHERE run_id = $2::uuid"
	case "internal/store/internal/schemastore/fork_point_schema_test.go":
		// This test exercises physical CHECK and FK constraints on the real schema.
		return compact == "INSERT INTO runs (run_id,bundle_hash,origin_kind) VALUES ($1,$2,'scenario_setup')" ||
			compact == "INSERT INTO runs (run_id,bundle_hash,origin_kind,forked_from_run_id,forked_from_point_kind,forked_from_revision,forked_from_event_id) VALUES ($1,$2,'fork_materialization',$3,$4,$5,$6)"
	}
	return false
}

// The source-corruption fixture moved into the selected transaction owner.
// Exempt only its exact CAS literal, never another write in the same file.
func maskNamedMailboxSourceFixtureWrite(source string) (string, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "test_mailbox_fixture.go", source, 0)
	if err != nil {
		return source, err
	}
	masked := []byte(source)
	count := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "SwapMailboxRunSourceForTest" {
			continue
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err == nil && value == "UPDATE runs SET bundle_hash=$1 WHERE run_id=$2 AND bundle_hash=$3" {
				count++
				for i := set.Position(literal.Pos()).Offset; i < set.Position(literal.End()).Offset; i++ {
					masked[i] = ' '
				}
			}
			return true
		})
	}
	if count != 1 {
		return source, fmt.Errorf("expected exactly one named source-fixture CAS, got %d", count)
	}
	return string(masked), nil
}

func TestRunLifecycleFixtureGuardRejectsSiblingWrites(t *testing.T) {
	const source = "package fixture\nfunc SwapMailboxRunSourceForTest() { use(`UPDATE runs SET bundle_hash=$1 WHERE run_id=$2 AND bundle_hash=$3`) }\n"
	runWrite := regexp.MustCompile(`(?is)\b(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+runs\b`)
	for _, tc := range []struct {
		name, input string
		allowed     bool
	}{
		{"exact", source, true},
		{"sibling_function", source + "func other() { use(`UPDATE runs SET bundle_hash=$1 WHERE run_id=$2`) }", false},
		{"same_function_extra", strings.Replace(source, "use(", "use(`DELETE FROM runs`); use(", 1), false},
		{"duplicate_exact", strings.Replace(source, "use(", "use(`UPDATE runs SET bundle_hash=$1 WHERE run_id=$2 AND bundle_hash=$3`); use(", 1), false},
		{"wrong_function", strings.ReplaceAll(source, "SwapMailboxRunSourceForTest", "unapproved"), false},
		{"missing_CAS", strings.ReplaceAll(source, " AND bundle_hash=$3", ""), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			masked, err := maskNamedMailboxSourceFixtureWrite(tc.input)
			if got := err == nil && !runWrite.MatchString(masked); got != tc.allowed {
				t.Fatalf("allowed=%v want=%v err=%v", got, tc.allowed, err)
			}
		})
	}
	if allowedSemanticRunFixtureLiteral("internal/serveapp/mailbox_source_admission_test.go", "UPDATE runs SET bundle_hash=$1 WHERE run_id=$2") {
		t.Fatal("retired raw fixture writer still authorized")
	}
}

func compactSQLForLifecycleGuard(value string) string {
	return strings.Join(strings.Fields(value), " ")
}
