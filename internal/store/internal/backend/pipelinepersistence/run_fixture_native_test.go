package pipelinepersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
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
		case "runtime_store_metadata", "runtime_process_generation", "runs", "run_event_counts", "source_artifacts", "author_activity_order", "author_activity_occurrences",
			"run_fork_revision_heads", "run_fork_revisions", "run_fork_fact_revisions":
			selected = append(selected, plan)
		case "events", "entity_state", "standing_services", "standing_service_generations":
			if snapshot {
				selected = append(selected, plan)
			}
		}
	}
	want := 10
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

func requireStatementRun(t testing.TB, ctx context.Context, owner runlifecycle.OperationOwner, artifact interface {
	EnsureSourceArtifact(context.Context, *sourceartifact.AdmittedSourceArtifact) (sourceartifact.EnsureResult, error)
}, ids ...string) {
	t.Helper()
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

func statementOriginalContext(t testing.TB, sqlite bool, acquire func(context.Context, func(context.Context, *sql.Tx) error) (bool, error)) context.Context {
	t.Helper()
	identity := processbinding.LeaseIdentity{LeaseID: uuid.NewString(), OwnerID: "native-statement-fixture", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: processbinding.PostgresLeaseBackend}
	if sqlite {
		identity.Backend = processbinding.SQLiteLeaseBackend
	}
	var original processbinding.LeaseStamp
	committed, err := acquire(context.Background(), func(ctx context.Context, tx *sql.Tx) error {
		var err error
		original, err = generationauthority.AdvanceProcessGeneration(ctx, tx, identity, sqlite)
		return err
	})
	if err != nil || !committed {
		t.Fatalf("acquire original statement process: committed=%v err=%v", committed, err)
	}
	ctx, err := generationauthority.MutationContext(context.Background(), original, false)
	if err != nil {
		t.Fatal(err)
	}
	return authoractivity.WithScope(ctx, authoractivity.BundleScope(identity.RuntimeInstanceID, sourceartifactfixture.BundleHash))
}

func materializeSQLiteStatementRuns(t testing.TB, db *sql.DB, snapshot bool, ids ...string) (runlifecycle.OperationOwner, context.Context) {
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
	ctx := statementOriginalContext(t, true, backend.RunProcessAcquisitionOutcome)
	requireStatementRun(t, ctx, owner, artifacts, ids...)
	return owner, ctx
}

func materializePostgresStatementRuns(t testing.TB, db *sql.DB, snapshot bool, ids ...string) (runlifecycle.OperationOwner, context.Context) {
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
	lease, acquired, err := postgresbackend.AcquireAdvisoryLockLease(context.Background(), backend, "swarm:runtime:shared-store-owner")
	if err != nil || !acquired {
		t.Fatalf("acquire retained statement session: acquired=%v err=%v", acquired, err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ctx := statementOriginalContext(t, false, lease.RunProcessAcquisitionOutcome)
	requireStatementRun(t, ctx, owner, artifacts, ids...)
	return owner, ctx
}
