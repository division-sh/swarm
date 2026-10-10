package cliapp

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

type rawSQLBoundaryClassification string

const (
	rawSQLConstructionBoundary      rawSQLBoundaryClassification = "construction_boundary"
	rawSQLRuntimeUnitOfWorkBoundary rawSQLBoundaryClassification = "runtime_unit_of_work_boundary"
	rawSQLOptionalProductBoundary   rawSQLBoundaryClassification = "optional_product_boundary"
	rawSQLWorkspaceProcessBoundary  rawSQLBoundaryClassification = "workspace_process_boundary"
	rawSQLTestSupportBoundary       rawSQLBoundaryClassification = "test_support_boundary"
)

type rawSQLBoundaryEntry struct {
	Classification rawSQLBoundaryClassification
	Issue          int
	SpecRef        string
	Reason         string
}

func TestSelectedRawSQLBoundaryInventoryIsClassified(t *testing.T) {
	root := repoRootForRawSQLBoundaryGuard(t)
	matches, err := collectRawSQLBoundaryMatches(root)
	if err != nil {
		t.Fatalf("collect raw SQL boundary matches: %v", err)
	}
	if len(matches) == 0 {
		t.Fatal("expected production raw SQL/TX boundary matches")
	}
	failures := classifyRawSQLBoundaryMatches(matches, selectedRawSQLBoundaryLedger())
	if len(failures) > 0 {
		t.Fatalf("unclassified or stale raw SQL/TX producer seams:\n%s", strings.Join(failures, "\n"))
	}
}

func TestSelectedRawSQLBoundaryRejectsUnclassifiedProducerFixture(t *testing.T) {
	matches, err := rawSQLBoundaryMatchesFromSources(map[string]string{
		"internal/runtime/unclassified_sql_producer.go": `package runtime

import (
	"context"
	"database/sql"
)

func unclassifiedProducer(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "INSERT INTO events(execution_mode, event_id) VALUES ('live', ?)", "evt")
	return err
}
`,
	})
	if err != nil {
		t.Fatal(err)
	}
	failures := classifyRawSQLBoundaryMatches(matches, selectedRawSQLBoundaryLedger())
	if len(failures) == 0 {
		t.Fatal("expected unclassified raw SQL producer fixture to fail")
	}
	if !strings.Contains(strings.Join(failures, "\n"), "internal/runtime/unclassified_sql_producer.go") {
		t.Fatalf("expected failure to name fixture path, got:\n%s", strings.Join(failures, "\n"))
	}
}

func TestSelectedRawSQLBoundaryRejectsUnclassifiedConcreteStoreFixture(t *testing.T) {
	matches, err := rawSQLBoundaryMatchesFromSources(map[string]string{
		"internal/runtime/unclassified_concrete_store_producer.go": `package runtime

import (
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store"
)

func unclassifiedConcreteStoreProducer(pg *store.PostgresStore) *pipeline.PipelineCoordinator {
	return pipeline.NewPipelineCoordinator(nil, pg.DB)
}
`,
	})
	if err != nil {
		t.Fatal(err)
	}
	failures := classifyRawSQLBoundaryMatches(matches, selectedRawSQLBoundaryLedger())
	if len(failures) == 0 {
		t.Fatal("expected unclassified concrete store producer fixture to fail")
	}
	if !strings.Contains(strings.Join(failures, "\n"), "internal/runtime/unclassified_concrete_store_producer.go") {
		t.Fatalf("expected failure to name fixture path, got:\n%s", strings.Join(failures, "\n"))
	}
}

func selectedRawSQLBoundaryLedger() map[string]rawSQLBoundaryEntry {
	return map[string]rawSQLBoundaryEntry{
		"internal/testutil/runtimepipelinefixture/context.go": {
			Classification: rawSQLTestSupportBoundary,
			Issue:          2148,
			SpecRef:        "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.runtime_execution_persistence_authority",
			Reason:         "test-only fixture exposes transaction-bound execution for hostile atomicity proof; production runtime has no raw SQL context capability",
		},
		"internal/testutil/postgres.go": {
			Classification: rawSQLTestSupportBoundary,
			Issue:          1943,
			Reason:         "testutil is the thin testing adapter over the canonical testpostgres lifecycle owner",
		},
		"internal/testutil/runlifecyclefixture/fixture.go": {
			Classification: rawSQLTestSupportBoundary,
			Issue:          2111,
			SpecRef:        "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.run_lifecycle_authority",
			Reason:         "test-only hostile readback fixtures deliberately materialize persisted run states that valid lifecycle construction forbids",
		},
		"internal/testpostgres/connection.go": {
			Classification: rawSQLTestSupportBoundary,
			Issue:          1943,
			Reason:         "testpostgres owns the typed Postgres DSN and connector boundary used by test lifecycle consumers",
		},
		"internal/testpostgres/capacity.go": {
			Classification: rawSQLTestSupportBoundary,
			Issue:          1702,
			Reason:         "shared test-server admission reads max_connections before resource writes; it exposes no product SQL authority",
		},
		"internal/testpostgres/capacity_probe.go": {
			Classification: rawSQLTestSupportBoundary,
			Issue:          1702,
			Reason:         "private capacity fixtures independently read database names and metadata through the canonical test connection for no-mutation proof",
		},
		"internal/testpostgres/manager.go": {
			Classification: rawSQLTestSupportBoundary,
			Issue:          1943,
			Reason:         "testpostgres owns server-scoped template, sandbox, lease, reconciliation, and cleanup SQL",
		},
		"internal/testpostgres/service_registry.go": {
			Classification: rawSQLTestSupportBoundary,
			Issue:          1943,
			Reason:         "runner-owned service verification reads canonical Postgres settings through the typed test connection",
		},
	}
}

func TestSelectedRawSQLBoundaryRejectsRetiredSessionProviderProducers(t *testing.T) {
	for _, path := range []string{"internal/sessionprovider/capture.go", "internal/sessionprovider/publication.go", "internal/sessionprovider/claim_recovery.go", "internal/sessionprovider/session_state_unix.go"} {
		t.Run(path, func(t *testing.T) {
			matches, err := rawSQLBoundaryMatchesFromSources(map[string]string{path: `package sessionprovider
import ("context"; "database/sql")
func forbidden(ctx context.Context, db *sql.DB) error {
 _, err := db.ExecContext(ctx, "DELETE FROM whatsapp_incoming_capture")
 return err
}
`})
			if err != nil {
				t.Fatal(err)
			}
			failures := classifyRawSQLBoundaryMatches(matches, selectedRawSQLBoundaryLedger())
			if len(failures) == 0 || !strings.Contains(strings.Join(failures, "\n"), path) {
				t.Fatalf("retired public SQL producer regained an allowance: %v", failures)
			}
		})
	}
}

func repoRootForRawSQLBoundaryGuard(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func collectRawSQLBoundaryMatches(root string) (map[string][]string, error) {
	sources := map[string]string{}
	err := checkoutsource.WalkDir(root, root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".swarm", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, "internal/store/") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		sources[rel] = string(raw)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rawSQLBoundaryMatchesFromSources(sources)
}

func rawSQLBoundaryMatchesFromSources(sources map[string]string) (map[string][]string, error) {
	literalPatterns := []string{
		`"database/sql"`,
		"*sql.DB",
		"*sql.Tx",
		"*store.PostgresStore",
		"*store.SQLiteRuntimeStore",
		"QueryContext(",
		"QueryRowContext(",
		"ExecContext(",
		"BeginTx(",
		"PipelineSQLTxFromContext",
		"RunInPipelineTransaction",
		"RunEventTransaction",
		"RunRuntimeMutation",
		"RunPipelineMutation",
	}
	regexPatterns := map[string]*regexp.Regexp{
		".DB": regexp.MustCompile(`\.DB\b`),
	}
	out := map[string][]string{}
	for path, src := range sources {
		code, err := rawSQLBoundaryExecutableSource(path, src)
		if err != nil {
			return nil, err
		}
		for _, pattern := range literalPatterns {
			if strings.Contains(code, pattern) {
				out[path] = append(out[path], pattern)
			}
		}
		for label, pattern := range regexPatterns {
			if pattern.MatchString(code) {
				out[path] = append(out[path], label)
			}
		}
		if len(out[path]) > 0 {
			sort.Strings(out[path])
		}
	}
	return out, nil
}

// Import paths are dependency evidence. Other literals and comments describe
// data (including codemod input), not live SQL types, calls or capabilities.
func rawSQLBoundaryExecutableSource(path, source string) (string, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, source, parser.ParseComments|parser.AllErrors)
	if err != nil {
		return "", fmt.Errorf("parse owned raw-boundary source %s: %w", path, err)
	}
	code := []byte(source)
	clear := func(start, end token.Pos) {
		for i := set.Position(start).Offset; i < set.Position(end).Offset; i++ {
			if code[i] != '\n' {
				code[i] = ' '
			}
		}
	}
	imports := map[*ast.BasicLit]bool{}
	cgo := false
	for _, spec := range file.Imports {
		imports[spec.Path] = true
		cgo = cgo || spec.Path.Value == `"C"`
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if literal, ok := node.(*ast.BasicLit); ok && !imports[literal] && (literal.Kind == token.STRING || literal.Kind == token.CHAR) {
			clear(literal.Pos(), literal.End())
		}
		return true
	})
	for _, group := range file.Comments {
		compilerEvidence := cgo
		for _, comment := range group.List {
			compilerEvidence = compilerEvidence || strings.HasPrefix(comment.Text, "//go:")
		}
		if compilerEvidence {
			continue
		}
		clear(group.Pos(), group.End())
	}
	return string(code), nil
}

func TestSelectedRawSQLBoundaryDistinguishesCodemodDataFromLiveAuthority(t *testing.T) {
	for _, path := range []string{
		"tools/fixture-codemod/conformance_columns.go",
		"tools/fixture-codemod/conformance_mutation_projection.go",
		"tools/fixture-codemod/conformance_native_setup.go",
		"tools/fixture-codemod/main.go",
		"tools/fixture-codemod/notify_execution_owner.go",
	} {
		t.Run(path, func(t *testing.T) {
			inert := "package fixture\n// *sql.Tx ExecContext( .DB\nconst input = `\"database/sql\" *sql.DB *store.PostgresStore .DB QueryContext(`\n"
			matches, err := rawSQLBoundaryMatchesFromSources(map[string]string{path: inert})
			if err != nil || len(matches) != 0 {
				t.Fatalf("inert source described live authority: %v %v", matches, err)
			}
			live := `package fixture
import ("context"; alias "database/sql")
func execute(ctx context.Context, db *alias.DB)error{
 _,err:=db.ExecContext(ctx,"DELETE FROM events")
 return err
}`
			matches, err = rawSQLBoundaryMatchesFromSources(map[string]string{path: live})
			if err != nil {
				t.Fatal(err)
			}
			patterns := strings.Join(matches[path], ",")
			if !strings.Contains(patterns, `"database/sql"`) || !strings.Contains(patterns, "ExecContext(") {
				t.Fatalf("aliased dependency or live SQL call lost: %v", matches)
			}
			if failures := classifyRawSQLBoundaryMatches(matches, selectedRawSQLBoundaryLedger()); len(failures) == 0 {
				t.Fatal("real SQL in codemod path escaped the unchanged ledger")
			}
		})
	}
}

func TestSelectedRawSQLBoundaryMalformedOwnedSourceFailsClosed(t *testing.T) {
	path := "internal/runtime/broken.go"
	matches, err := rawSQLBoundaryMatchesFromSources(map[string]string{
		path:                        "package runtime\nfunc broken(",
		"internal/runtime/other.go": "package runtime\nimport \"database/sql\"\nvar db *sql.DB",
	})
	if err == nil || !strings.Contains(err.Error(), path) || matches != nil {
		t.Fatalf("malformed source returned partial evidence: %v %v", matches, err)
	}
}

func TestSelectedRawSQLBoundaryPreservesCompilerOwnedCommentEvidence(t *testing.T) {
	for name, source := range map[string]string{
		"linkname": "package fixture\nimport _ \"unsafe\"\n//go:linkname borrowed example.RunRuntimeMutation\nfunc borrowed()\n",
		"cgo":      "package fixture\n/* void RunRuntimeMutation(void) {} */\nimport \"C\"\n",
	} {
		t.Run(name, func(t *testing.T) {
			path := "internal/runtime/unknown_" + name + ".go"
			matches, err := rawSQLBoundaryMatchesFromSources(map[string]string{path: source})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(strings.Join(matches[path], ","), "RunRuntimeMutation") {
				t.Fatalf("compiler evidence treated as inert data: %v", matches)
			}
			if failures := classifyRawSQLBoundaryMatches(matches, selectedRawSQLBoundaryLedger()); len(failures) == 0 {
				t.Fatal("compiler-owned raw evidence escaped the unchanged ledger")
			}
		})
	}
}

func classifyRawSQLBoundaryMatches(matches map[string][]string, ledger map[string]rawSQLBoundaryEntry) []string {
	var failures []string
	for path, patterns := range matches {
		entry, ok := ledger[path]
		if !ok {
			failures = append(failures, path+" matched raw SQL/TX patterns "+strings.Join(patterns, ", ")+" but is not classified")
			continue
		}
		if entry.Classification == "" {
			failures = append(failures, path+" classification is empty")
		}
		if entry.Issue == 0 && strings.TrimSpace(entry.SpecRef) == "" {
			failures = append(failures, path+" classification is missing tracker issue or governing spec ref")
		}
		if strings.TrimSpace(entry.Reason) == "" {
			failures = append(failures, path+" classification reason is empty")
		}
	}
	for path := range ledger {
		if _, ok := matches[path]; !ok {
			failures = append(failures, path+" is classified but no longer contains raw SQL/TX boundary patterns")
		}
	}
	sort.Strings(failures)
	return failures
}
