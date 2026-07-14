package main

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
	rawSQLConstructionBoundary        rawSQLBoundaryClassification = "construction_boundary"
	rawSQLDashboardDigestReadBoundary rawSQLBoundaryClassification = "dashboard_digest_read_boundary"
	rawSQLRuntimeUnitOfWorkBoundary   rawSQLBoundaryClassification = "runtime_unit_of_work_boundary"
	rawSQLOptionalProductBoundary     rawSQLBoundaryClassification = "optional_product_boundary"
	rawSQLWorkspaceProcessBoundary    rawSQLBoundaryClassification = "workspace_process_boundary"
	rawSQLTestSupportBoundary         rawSQLBoundaryClassification = "test_support_boundary"
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
	_, err := db.ExecContext(ctx, "INSERT INTO events(event_id) VALUES (?)", "evt")
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
		"cmd/swarm/main.go": {
			Classification: rawSQLConstructionBoundary,
			Issue:          1783,
			Reason:         "backend selection, selected-store construction, workspace lifecycle construction, and DB close plumbing are allowed construction/process boundaries",
		},
		"cmd/swarm/store_facade.go": {
			Classification: rawSQLConstructionBoundary,
			Issue:          1783,
			Reason:         "selected facade may expose named construction-time SQL exceptions such as workspace DB and Postgres-only RuntimeSQLDB",
		},
		"cmd/swarm/store_roles.go": {
			Classification: rawSQLConstructionBoundary,
			Issue:          1783,
			Reason:         "compile-time selected store role assertions are construction/model proof, not producer-side concrete store capability authority",
		},
		"internal/dashboard/server/agents_sql.go": {
			Classification: rawSQLDashboardDigestReadBoundary,
			Issue:          1783,
			Reason:         "dashboard agent reader is an explicit derived SQL read-model exception pending selected read-owner migration",
		},
		"internal/dashboard/server/conversations_sql.go": {
			Classification: rawSQLDashboardDigestReadBoundary,
			Issue:          1783,
			Reason:         "dashboard conversation reader is an explicit derived SQL read-model exception pending selected read-owner migration",
		},
		"internal/dashboard/server/observability_sql.go": {
			Classification: rawSQLDashboardDigestReadBoundary,
			Issue:          1783,
			Reason:         "dashboard observability reader is an explicit derived SQL read-model exception pending selected read-owner migration",
		},
		"internal/digest/source.go": {
			Classification: rawSQLDashboardDigestReadBoundary,
			Issue:          1783,
			Reason:         "digest source is an explicit SQL read-model exception pending selected read-owner migration",
		},
		"internal/runtime/bus/eventbus.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "event bus observes pipeline SQL transaction context as part of the existing runtime unit-of-work boundary",
		},
		"internal/runtime/bus/outbox.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "outbox write path participates in the selected runtime event/pipeline transaction boundary",
		},
		"internal/runtime/bus/store.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "bus store interface names raw event transaction primitives that remain confined to runtime unit-of-work ownership",
		},
		"internal/runtime/dbtx.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "runtime DB/TX helpers are explicit runtime unit-of-work boundary primitives, not selected capability authority",
		},
		"internal/runtime/deadletters/record.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "dead-letter persistence is an explicit runtime SQL owner used from event/runtime transaction boundaries",
		},
		"internal/runtime/mutationlog/mutationlog.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "mutation log persistence is an explicit runtime SQL owner used from mutation transaction boundaries",
		},
		"internal/runtime/authoractivity/mutation.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          2010,
			SpecRef:        "platform-spec.yaml#platform_tables.author_activity_story",
			Reason:         "author activity mutation is the canonical ordered story transaction owner for registered runtime producers",
		},
		"internal/runtime/authoractivity/read.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          2010,
			SpecRef:        "platform-spec.yaml#platform_tables.author_activity_story",
			Reason:         "author activity read is the canonical backend-neutral story occurrence reader",
		},
		"internal/runtime/manager/flow_activation.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1962,
			SpecRef:        "platform-spec.yaml#tool_model.provider_trigger_adapters.pack_manifest_engine.provider_delivery_transaction",
			Reason:         "flow activation observes the ambient selected-store transaction only to persist route authority in-unit and defer agent process startup until commit",
		},
		"internal/runtime/pipeline/activity_engine.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "pipeline activity engine reads through the existing pipeline SQL owner boundary",
		},
		"internal/runtime/pipeline/activity_journal.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "pipeline activity journal writes through the existing pipeline transaction owner",
		},
		"internal/runtime/pipeline/author_activity.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          2010,
			SpecRef:        "platform-spec.yaml#platform_tables.author_activity_story",
			Reason:         "pipeline author activity bridges registered pipeline producers into the canonical ordered story owner",
		},
		"internal/runtime/pipeline/coordinator.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "pipeline coordinator owns pipeline SQL dependency injection, not selected capability inference",
		},
		"internal/runtime/pipeline/engine_adapter.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "pipeline engine adapter uses selected mutation ownership and diagnostic SQL reads under the pipeline owner boundary",
		},
		"internal/runtime/pipeline/node_background.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "background workflow node carries the pipeline SQL dependency for receipt/delivery unit-of-work ownership",
		},
		"internal/runtime/pipeline/node_system_runner.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "system node runner delivery receipt/settlement writes are explicit pipeline unit-of-work primitives",
		},
		"internal/runtime/pipeline/run_fork_revision_context.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          2049,
			SpecRef:        "platform-spec.yaml#run_model.fork.fixed_event_revision_and_workset",
			Reason:         "pipeline transactions collect changed fork-reader families and publish them only at the existing unit-of-work commit boundary",
		},
		"internal/runtime/pipeline/runtime_interfaces.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "pipeline runtime interface names the selected pipeline mutation owner boundary",
		},
		"internal/runtime/runforkexecution/activation_gate.go": {
			Classification: rawSQLOptionalProductBoundary,
			SpecRef:        "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.optional_public_mutating_backend_support.run_fork",
			Reason:         "selected-contract run.fork activation is a spec-classified optional Postgres-backed product seam until promoted behind selected owners",
		},
		"internal/runtime/runforkexecution/execution.go": {
			Classification: rawSQLOptionalProductBoundary,
			SpecRef:        "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.optional_public_mutating_backend_support.run_fork",
			Reason:         "selected-contract run.fork execution constructs a fork-local runtime pipeline from the Postgres store DB; this is an explicit optional product split, not backend-neutral selected capability authority",
		},
		"internal/runtime/runforkexecution/agent_runtime_materialization.go": {
			Classification: rawSQLOptionalProductBoundary,
			SpecRef:        "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.optional_public_mutating_backend_support.run_fork",
			Reason:         "selected-contract fork agent runtime binds the Postgres-backed workflow instance owner used by fork-local lifecycle activation; this remains inside the explicit optional run.fork product seam",
		},
		"internal/runtime/runforkexecution/runtime_container.go": {
			Classification: rawSQLOptionalProductBoundary,
			SpecRef:        "platform-spec.yaml#engine.runtime_core_persistence_store_contracts.optional_public_mutating_backend_support.run_fork",
			Reason:         "selected-contract fork-local runtime container uses the Postgres store DB for optional run.fork logging/pipeline support and remains a spec-classified optional product split",
		},
		"internal/runtime/runforkrevision/revision.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          2049,
			SpecRef:        "platform-spec.yaml#run_model.fork.fixed_event_revision_and_workset",
			Reason:         "the bounded eleven-family revision registry is the canonical fixed-event persistence owner and runs inside existing PostgreSQL transactions",
		},
		"internal/runtime/pipeline/runtime_support.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "pipeline runtime SQL/TX helpers are explicit unit-of-work boundary primitives",
		},
		"internal/runtime/pipeline/system_node_receipt_store.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "system-node receipt store is the explicit pipeline receipt/delivery SQL owner",
		},
		"internal/runtime/pipeline/workflow_entity_type_repair.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "workflow entity-type repair is an explicit pipeline repair SQL owner",
		},
		"internal/runtime/pipeline/workflow_instance_store.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "workflow instance store is the explicit pipeline instance SQL owner for Postgres/default query forms",
		},
		"internal/runtime/pipeline/workflow_instance_store_sqlite.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "workflow instance SQLite implementation is the explicit backend-local pipeline instance SQL owner",
		},
		"internal/runtime/pipeline/workflow_join_lifecycle.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1848,
			Reason:         "join stage arm, expected-zero intent, and timeout lifecycle consume the selected workflow instance RunPipelineMutation owner as one unit of work",
		},
		"internal/runtime/pipeline/workflow_gate_decision.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1986,
			Reason:         "gate decision routing consumes the selected workflow mutation owner atomically with activation state",
		},
		"internal/runtime/pipeline/workflow_gate_fence.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1986,
			Reason:         "gate activation and card fencing are explicit selected-store transaction owners",
		},
		"internal/runtime/pipeline/workflow_gate_lifecycle.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1986,
			Reason:         "gate stage entry and exit require the selected pipeline transaction owner so activation and card state cannot commit separately",
		},
		"internal/runtime/pipeline/workflow_nodes.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "workflow node construction carries pipeline SQL dependency to explicit node/unit-of-work owners",
		},
		"internal/runtime/pipeline/workflow_state_persistence.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1848,
			Reason:         "workflow state, timer, and join lifecycle persistence share the selected workflow instance RunPipelineMutation owner",
		},
		"internal/runtime/pipeline/workflow_timer_lifecycle.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1846,
			Reason:         "stage timer fire handling uses the selected workflow instance RunPipelineMutation owner to keep fired state and timed transition application in one unit of work",
		},
		"internal/runtime/pipeline/workflow_transitions.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "workflow transition receipts are an explicit pipeline SQL read/write owner",
		},
		"internal/runtime/runtime.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1783,
			Reason:         "runtime dependency struct carries the named Postgres-only RuntimeSQLDB exception and must not be used as SQLite capability authority",
		},
		"internal/runtime/standing_targets.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1951,
			Reason:         "standing activation consumes the selected WorkflowInstanceStore RunPipelineMutation owner for atomic run, instance, entity, route, and timer creation",
		},
		"internal/apiv1/operator_decision_cards.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1986,
			Reason:         "typed decision-card API mutations enter the selected workflow unit-of-work owner",
		},
		"internal/apiv1/operator_read.go": {
			Classification: rawSQLRuntimeUnitOfWorkBoundary,
			Issue:          1986,
			Reason:         "operator dependency wiring names the selected workflow mutation capability",
		},
		"internal/runtime/workspace/host_manager.go": {
			Classification: rawSQLWorkspaceProcessBoundary,
			Issue:          1783,
			Reason:         "host workspace lifecycle persistence is an allowed workspace/process SQL boundary, not runtime selected-store authority",
		},
		"internal/runtime/workspace/manager.go": {
			Classification: rawSQLWorkspaceProcessBoundary,
			Issue:          1783,
			Reason:         "Docker workspace lifecycle persistence is an allowed workspace/process SQL boundary, not runtime selected-store authority",
		},
		"internal/testutil/postgres.go": {
			Classification: rawSQLTestSupportBoundary,
			Issue:          1943,
			Reason:         "testutil is the thin testing adapter over the canonical testpostgres lifecycle owner",
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
