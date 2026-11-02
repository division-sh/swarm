package pipelinepersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	runstore "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	artifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type StatementProcessFixtureForTest func(testing.TB, string, bool, schemastore.SchemaBootstrapRequest) context.Context

func statementRunSchemaRequest(t testing.TB, snapshot bool) schemastore.SchemaBootstrapRequest {
	t.Helper()
	spec, err := contracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "..", "..", "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	plans, err := schemastore.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	var selected []schemastore.SchemaTableDDL
	for _, plan := range plans {
		switch plan.TableName {
		case "runtime_store_metadata", "runtime_process_generation", "runs", "run_event_counts", "run_completion_evidence", "source_artifacts", "source_root_admissions", "author_activity_order", "author_activity_occurrences",
			"run_fork_revision_heads", "run_fork_revisions", "run_fork_fact_revisions":
			selected = append(selected, plan)
		case "events", "entity_state":
			if snapshot {
				selected = append(selected, plan)
			}
		}
	}
	return schemastore.SchemaBootstrapRequest{
		PlatformPlans: selected,
		Origin:        schemastore.RuntimeStoreOrigin{SwarmVersion: "native-statement-run-proof", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()},
	}
}

func requireStatementRun(t testing.TB, ctx context.Context, owner runlifecycle.OperationOwner, artifact interface {
	EnsureRootedSource(context.Context, sourceartifact.RootedSource) (sourceartifact.RootedEnsureResult, error)
}, ids ...string) {
	t.Helper()
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(uuid.NewString(), sourceartifactfixture.BundleHash))
	ctx = correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.Fact())
	rooted, _ := sourceartifactfixture.Rooted(t, "fixture", sourceartifactfixture.Artifact())
	ctx = correlation.WithRootAdmission(ctx, rooted.Admission())
	if _, err := artifact.EnsureRootedSource(ctx, rooted); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, err := owner.CreateRun(ctx, runlifecycle.CreateRequest{
			RunID: id, Source: sourceartifactfixture.Fact(), RootAdmission: rooted.Admission(), Origin: runlifecycle.ScenarioSetupRunOrigin(), StartedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func materializeSQLiteStatementRuns(t testing.TB, db *sql.DB, location string, snapshot bool, prepare StatementProcessFixtureForTest, ids ...string) (runlifecycle.OperationOwner, context.Context) {
	t.Helper()
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := schemastore.NewSQLiteWithBackend(backend, "")
	if err != nil {
		t.Fatal(err)
	}
	request := statementRunSchemaRequest(t, snapshot)
	if err := schema.BootstrapSchema(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	ctx := prepare(t, location, false, request)
	owner, err := runstore.NewSQLite(backend, schema.RequireCurrent, runhandoff.NewCandidateCoordinator(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := artifactstore.NewSQLite(backend, schema.RequireCurrent, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	requireStatementRun(t, ctx, owner, artifacts, ids...)
	return owner, ctx
}

func materializePostgresStatementRuns(t testing.TB, db *sql.DB, location string, snapshot bool, prepare StatementProcessFixtureForTest, ids ...string) (runlifecycle.OperationOwner, context.Context) {
	t.Helper()
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := schemastore.NewPostgres(backend)
	if err != nil {
		t.Fatal(err)
	}
	request := statementRunSchemaRequest(t, snapshot)
	if err := schema.BootstrapSchema(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	ctx := prepare(t, location, true, request)
	owner, err := runstore.NewPostgres(backend, schema.RequireCurrent, runhandoff.NewCandidateCoordinator())
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := artifactstore.NewPostgres(backend, schema.RequireCurrent)
	if err != nil {
		t.Fatal(err)
	}
	requireStatementRun(t, ctx, owner, artifacts, ids...)
	return owner, ctx
}
