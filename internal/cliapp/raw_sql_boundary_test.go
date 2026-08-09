package cliapp

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
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
	matches := rawSQLBoundaryMatchesFromSources(map[string]string{
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
	failures := classifyRawSQLBoundaryMatches(matches, selectedRawSQLBoundaryLedger())
	if len(failures) == 0 {
		t.Fatal("expected unclassified raw SQL producer fixture to fail")
	}
	if !strings.Contains(strings.Join(failures, "\n"), "internal/runtime/unclassified_sql_producer.go") {
		t.Fatalf("expected failure to name fixture path, got:\n%s", strings.Join(failures, "\n"))
	}
}

func TestSelectedRawSQLBoundaryRejectsUnclassifiedConcreteStoreFixture(t *testing.T) {
	matches := rawSQLBoundaryMatchesFromSources(map[string]string{
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
		"internal/serveapp/main.go": {
			Classification: rawSQLConstructionBoundary,
			Issue:          1783,
			Reason:         "backend selection, selected-store construction, workspace lifecycle construction, and DB close plumbing are allowed construction/process boundaries",
		},
		"internal/serveapp/store_roles.go": {
			Classification: rawSQLConstructionBoundary,
			Issue:          1783,
			Reason:         "compile-time selected store role assertions are construction/model proof, not producer-side concrete store capability authority",
		},
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
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".swarm", "node_modules", "vendor":
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
	return rawSQLBoundaryMatchesFromSources(sources), nil
}

func rawSQLBoundaryMatchesFromSources(sources map[string]string) map[string][]string {
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
		for _, pattern := range literalPatterns {
			if strings.Contains(src, pattern) {
				out[path] = append(out[path], pattern)
			}
		}
		for label, pattern := range regexPatterns {
			if pattern.MatchString(src) {
				out[path] = append(out[path], label)
			}
		}
		if len(out[path]) > 0 {
			sort.Strings(out[path])
		}
	}
	return out
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
