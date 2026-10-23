package runtimepersistence

import (
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

	"gopkg.in/yaml.v3"
)

var runInsertPattern = regexp.MustCompile(`(?is)INSERT\s+INTO\s+runs\s*\(([^)]*)\)`)

func TestRepositoryRunCreationHasCanonicalOwnersOnly(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	wantOwners := map[string]bool{
		"internal/store/internal/backend/runlifecycle/run_lifecycle_mutation.go": false,
		"internal/store/internal/backend/runlifecycle/test_snapshot_fault.go":    false,
	}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel := repositoryRelativePath(t, root, path)
		if rel == "internal/store/storetest/event.go" {
			return nil
		}
		source := readRepositoryFile(t, path)
		if !runInsertPattern.MatchString(source) {
			return nil
		}
		if _, ok := wantOwners[rel]; !ok {
			t.Errorf("%s can create runs outside the canonical run lifecycle owner", rel)
			return nil
		}
		wantOwners[rel] = true
		return nil
	})
	if err != nil {
		t.Fatalf("walk production run writers: %v", err)
	}
	for owner, found := range wantOwners {
		if !found {
			t.Errorf("canonical run lifecycle owner %s no longer contains its run insert", owner)
		}
	}
}

func runFixtureIdentityViolations(path string, file *ast.File) ([]string, error) {
	minimal, err := classifyBackendMinimalRunLiterals(path, file)
	if err != nil {
		return nil, err
	}
	historical, err := classifyReceiverHistoryMinimalRunLiterals(path, file)
	if err != nil {
		return nil, err
	}
	oracles := classifyRunFixtureIdentityOracleLiterals(path, file)
	var violations []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING || minimal[literal.Pos()] || historical[literal.Pos()] || oracles[literal.Pos()] {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			return true
		}
		for _, match := range runInsertPattern.FindAllStringSubmatch(value, -1) {
			columns := normalizedSQLColumns(match[1])
			if !columns["bundle_hash"] {
				violations = append(violations, "run fixture insert lacks explicit bundle_hash")
			}
			if columns["bundle_source"] {
				violations = append(violations, "retired bundle_source identity")
			}
		}
		return true
	})
	return violations, nil
}

// Guard counterexamples describe SQL without executing it. The exemption is
// declaration-local, and disappears if the declaration acquires a SQL caller.
func classifyRunFixtureIdentityOracleLiterals(path string, file *ast.File) map[token.Pos]bool {
	approved := map[token.Pos]bool{}
	if path != "internal/store/internal/runtimepersistence/run_lifecycle_ownership_guard_test.go" &&
		path != "internal/store/internal/runtimepersistence/run_bundle_identity_repository_test.go" {
		return approved
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		switch fn.Name.Name {
		case "TestRepositoryRunFixtureIdentityGuardRejectsNewWriters":
			if path != "internal/store/internal/runtimepersistence/run_bundle_identity_repository_test.go" {
				continue
			}
		case "classifyBackendMinimalRunLiterals", "classifyA2CollectionMinimalRunLiterals",
			"TestA2CollectionMinimalRunFixtureClassificationIsExact", "classifyReceiverHistoryMinimalRunLiterals",
			"TestReceiverHistoryMinimalRunFixtureClassificationIsExact", "allowedSemanticRunFixtureLiteral":
			if path != "internal/store/internal/runtimepersistence/run_lifecycle_ownership_guard_test.go" {
				continue
			}
		default:
			continue
		}
		literals := map[token.Pos]bool{}
		nonExecuting := true
		ast.Inspect(fn, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok {
				switch selector.Sel.Name {
				case "Exec", "ExecContext", "Query", "QueryContext", "QueryRow", "QueryRowContext",
					"Prepare", "PrepareContext", "Begin", "BeginTx", "WithSQL":
					nonExecuting = false
				}
			}
			if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
				literals[literal.Pos()] = true
			}
			return true
		})
		if nonExecuting {
			for pos := range literals {
				approved[pos] = true
			}
		}
	}
	return approved
}

func TestRepositoryRunFixtureIdentityGuardRejectsNewWriters(t *testing.T) {
	const oraclePath = "internal/store/internal/runtimepersistence/run_lifecycle_ownership_guard_test.go"
	const source = "package fixture\nfunc allowedSemanticRunFixtureLiteral() { use(`INSERT INTO runs (run_id) VALUES ($1)`) }"
	const projectionPath = "internal/store/internal/backend/mutationprotocol/event_counts_test.go"
	const projection = "package fixture\nvar ddl = `CREATE TABLE runs (run_id UUID PRIMARY KEY, event_count INTEGER NOT NULL DEFAULT 0 CHECK (event_count >= 0))`\nvar insert = `INSERT INTO runs (run_id) VALUES ($1)`"
	for _, tc := range []struct {
		name, path, source string
		want               int
	}{
		{"oracle", oraclePath, source, 0},
		{"wrong-owner", "internal/runtime/other_test.go", source, 1},
		{"sql-in-oracle", oraclePath, strings.Replace(source, "use(", "db.Exec(", 1), 1},
		{"additional-writer", oraclePath, source + "\nfunc other() { db.Exec(`INSERT INTO runs (run_id) VALUES ($1)`) }", 1},
		{"minimal-projection", projectionPath, projection, 0},
		{"extra-projection-writer", projectionPath, projection + "\nvar extra = `INSERT INTO runs (run_id, status) VALUES ($1, 'running')`", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			violations, err := runFixtureIdentityViolations(tc.path, file)
			if err != nil || len(violations) != tc.want {
				t.Fatalf("violations=%v, want %d, err=%v", violations, tc.want, err)
			}
		})
	}

	const historyPath = "internal/store/internal/backend/runforkpersistence/receiver_config_history_test.go"
	const historyScope = "TestRuntimeHeaderHistoricalCaptureAndReadinessBothStores"
	const historySchema = "CREATE TEMP TABLE runs (run_id TEXT PRIMARY KEY, origin_kind TEXT, trigger_event_id TEXT, trigger_event_type TEXT, origin_service_id TEXT, origin_generation BIGINT, forked_from_run_id TEXT, forked_from_point_kind TEXT, forked_from_revision BIGINT, forked_from_event_id TEXT)"
	const historyInsert = "INSERT INTO runs (run_id,origin_kind) VALUES ($1,'deployment')"
	historySource := "package fixture\nfunc " + historyScope + "() { db.Exec(" + strconv.Quote(historySchema) + "); db.Exec(" + strconv.Quote(historyInsert) + ") }\n"
	for _, tc := range []struct {
		name, path, source string
		want               bool
	}{
		{"historical-stub-exact", historyPath, historySource, true},
		{"historical-stub-wrong-owner", "internal/runtime/other_test.go", historySource, false},
		{"historical-stub-wrong-scope", historyPath, strings.ReplaceAll(historySource, historyScope, "Other"), false},
		{"historical-stub-nontemporary", historyPath, strings.ReplaceAll(historySource, "CREATE TEMP TABLE", "CREATE TABLE"), false},
		{"historical-stub-semantic-column", historyPath, strings.ReplaceAll(historySource, "forked_from_event_id TEXT)", "forked_from_event_id TEXT, status TEXT)"), false},
		{"historical-stub-foreign-origin", historyPath, strings.ReplaceAll(historySource, "'deployment'", "'fork_materialization'"), false},
		{"historical-stub-sibling-writer", historyPath, historySource + "func other() { db.Exec(" + strconv.Quote(historyInsert) + ") }", false},
		{"historical-stub-extra-writer", historyPath, strings.Replace(historySource, "db.Exec(", "db.Exec("+strconv.Quote(historyInsert)+"); db.Exec(", 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			violations, err := runFixtureIdentityViolations(tc.path, file)
			if allowed := err == nil && len(violations) == 0; allowed != tc.want {
				t.Fatalf("allowed=%t want=%t violations=%v err=%v", allowed, tc.want, violations, err)
			}
		})
	}
}

func TestRepositoryContainsNoLegacyBundleIdentityInterpreter(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	prohibited := []string{
		"bundle_" + "ref",
		"Bundle" + "Ref",
		"Bundle" + "Fingerprint",
		"Bundle" + "SourceFact",
		"bundle_" + "fingerprint",
		"bundle_" + "source",
		"SourceArtifact" + "Legacy",
		"bundle-" + "fingerprint",
		"legacy_bundle_" + "fingerprint",
		"UNSUPPORTED_BUNDLE_" + "REF",
	}
	var files []string
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk production sources: %v", err)
	}
	files = append(files, filepath.Join(root, "platform-spec.yaml"), filepath.Join(root, "openrpc.json"))
	sort.Strings(files)
	for _, path := range files {
		source := readRepositoryFile(t, path)
		for _, retired := range prohibited {
			if strings.Contains(source, retired) {
				t.Errorf("%s retains retired bundle identity interpreter %q", repositoryRelativePath(t, root, path), retired)
			}
		}
	}
}

func TestPlatformSpecSourceArtifactRecoveryRejectsLegacySourceArtifactStates(t *testing.T) {
	root := repositoryRootForBundleIdentityTest(t)
	var document map[string]any
	if err := yaml.Unmarshal([]byte(readRepositoryFile(t, filepath.Join(root, "platform-spec.yaml"))), &document); err != nil {
		t.Fatalf("parse platform spec: %v", err)
	}
	sourceModel, ok := document["filesystem_source_model"].(map[string]any)
	if !ok {
		t.Fatal("platform spec is missing filesystem_source_model")
	}
	durableLifecycle, ok := sourceModel["durable_lifecycle"].(map[string]any)
	if !ok {
		t.Fatal("platform spec is missing filesystem_source_model.durable_lifecycle")
	}
	var legacyPaths []string
	collectLegacySourceArtifactStatePaths(durableLifecycle, "filesystem_source_model.durable_lifecycle", &legacyPaths)
	if len(legacyPaths) != 0 {
		t.Fatalf("resume/recovery retains legacy bundle source states: %s", strings.Join(legacyPaths, ", "))
	}
}

const (
	operationMutation      = "mutation"
	operationRetained      = "retained capability"
	operationAdmittedChild = "already-admitted child"
	operationPureRead      = "pure read"
)

func collectLegacySourceArtifactStatePaths(value any, path string, out *[]string) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			childPath := path + "." + key
			if strings.Contains(strings.ToLower(key), "legacy") {
				*out = append(*out, childPath)
			}
			collectLegacySourceArtifactStatePaths(child, childPath, out)
		}
	case []any:
		for index, child := range typed {
			collectLegacySourceArtifactStatePaths(child, path+"["+strconv.Itoa(index)+"]", out)
		}
	case string:
		if strings.Contains(strings.ToLower(typed), "legacy") {
			*out = append(*out, path)
		}
	}
}

func repositoryRootForBundleIdentityTest(t *testing.T) string {
	t.Helper()
	return repoRootForRuntimeWriterGuard(t)
}

func repositoryRelativePath(t *testing.T, root, path string) string {
	t.Helper()
	rel, err := filepath.Rel(root, path)
	if err != nil {
		t.Fatalf("relative path for %s: %v", path, err)
	}
	return filepath.ToSlash(rel)
}

func readRepositoryFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func normalizedSQLColumns(raw string) map[string]bool {
	columns := map[string]bool{}
	for _, column := range strings.Split(raw, ",") {
		columns[strings.ToLower(strings.TrimSpace(column))] = true
	}
	return columns
}
