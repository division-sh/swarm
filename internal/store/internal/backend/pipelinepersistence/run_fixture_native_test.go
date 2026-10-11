package pipelinepersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
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

// These statement/decoder units retain their independent relational oracles.
// Run preconditions use real lifecycle writers and their canonical schema and
// journals; this fragment does not claim full runtime-store admission.
func bootstrapStatementRunSchema(t testing.TB, schema schemastore.SchemaBootstrapper, snapshot bool) {
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
		case "runtime_store_metadata", "runs", "source_artifacts", "author_activity_order", "author_activity_occurrences",
			"run_fork_revision_heads", "run_fork_revisions", "run_fork_fact_revisions":
			selected = append(selected, plan)
		case "events", "entity_state", "standing_services", "standing_service_generations":
			if snapshot {
				selected = append(selected, plan)
			}
		}
	}
	want := 8
	if snapshot {
		want += 4
	}
	if len(selected) != want {
		t.Fatalf("statement run fixture plans=%d, want %d", len(selected), want)
	}
	if err := schema.BootstrapSchema(context.Background(), schemastore.SchemaBootstrapRequest{
		PlatformPlans: selected,
		Origin:        schemastore.RuntimeStoreOrigin{SwarmVersion: "native-statement-run-proof", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
}

func requireStatementRun(t testing.TB, owner runlifecycle.OperationOwner, artifact interface {
	EnsureSourceArtifact(context.Context, *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error)
}, ids ...string) {
	t.Helper()
	ctx := authoractivity.WithScope(context.Background(), authoractivity.BundleScope(uuid.NewString(), sourceartifactfixture.BundleHash))
	if _, err := artifact.EnsureSourceArtifact(ctx, sourceartifactfixture.Artifact()); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if _, err := owner.CreateRun(ctx, runlifecycle.CreateRequest{
			RunID: id, Source: sourceartifactfixture.Fact(), Origin: runlifecycle.ScenarioSetupRunOrigin(), StartedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func requireSQLiteStatementRuns(t testing.TB, db *sql.DB, ids ...string) runlifecycle.OperationOwner {
	t.Helper()
	return materializeSQLiteStatementRuns(t, db, false, ids...)
}

func materializeSQLiteStatementRuns(t testing.TB, db *sql.DB, snapshot bool, ids ...string) runlifecycle.OperationOwner {
	t.Helper()
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := schemastore.NewSQLiteWithBackend(backend, "")
	if err != nil {
		t.Fatal(err)
	}
	bootstrapStatementRunSchema(t, schema, snapshot)
	owner, err := runstore.NewSQLite(backend, schema.RequireCurrent, runhandoff.NewCandidateCoordinator(), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := artifactstore.NewSQLite(backend, schema.RequireCurrent, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	requireStatementRun(t, owner, artifacts, ids...)
	return owner
}

func requirePostgresStatementRuns(t testing.TB, db *sql.DB, ids ...string) runlifecycle.OperationOwner {
	t.Helper()
	return materializePostgresStatementRuns(t, db, false, ids...)
}

func materializePostgresStatementRuns(t testing.TB, db *sql.DB, snapshot bool, ids ...string) runlifecycle.OperationOwner {
	t.Helper()
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	schema, err := schemastore.NewPostgres(backend)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapStatementRunSchema(t, schema, snapshot)
	owner, err := runstore.NewPostgres(backend, schema.RequireCurrent, runhandoff.NewCandidateCoordinator())
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := artifactstore.NewPostgres(backend, schema.RequireCurrent)
	if err != nil {
		t.Fatal(err)
	}
	requireStatementRun(t, owner, artifacts, ids...)
	return owner
}
