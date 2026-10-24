package runforkpersistence

import (
	"context"
	"database/sql"

	"net/url"
	"path/filepath"

	"runtime"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	artifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/division-sh/swarm/internal/yamlsource"
)

type timerHistoryNativeOwner interface {
	runForkWorkflowTimerMaterializationOwner
	runForkArrivalJoinMaterializationOwner
}

type timerHistoryNativeScheduleOwner interface {
	SetNowFnForTest(func() time.Time)
	AdmitGenericScheduleOutcome(context.Context, genericschedule.AdmissionCommand) (genericschedule.AdmissionCommit, error)
	CancelGenericScheduleOutcome(context.Context, genericschedule.CancelCommand) (genericschedule.CancelCommit, error)
	LoadGenericScheduleActivation(context.Context, string) (genericschedule.Activation, bool, error)
}

type timerHistoryNativeFixture struct {
	requireCurrent func() error
	db             *sql.DB
	postgres       *postgresbackend.Backend
	sqlite         *sqlitebackend.Backend
	pipeline       timerHistoryNativeOwner
	generic        timerHistoryNativeScheduleOwner
	runs           interface {
		CreateRun(context.Context, runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error)
	}
}

func openTimerHistoryNativeFixture(t *testing.T, dialect string) *timerHistoryNativeFixture {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	path := contracts.DefaultPlatformSpecFile(filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..")))
	source, err := yamlsource.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := contracts.AdmitPlatformSpecValue(source.Document(path).Root())
	if err != nil {
		t.Fatal(err)
	}
	plans, err := platformschema.GeneratePlatformTableDDLs(spec)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := schemastore.SchemaBootstrapRequest{PlatformPlans: plans, Origin: schemastore.RuntimeStoreOrigin{SwarmVersion: "timer-history-native-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC()}}
	setup := authoractivity.WithScope(t.Context(), authoractivity.BundleScope("00000000-0000-4000-8000-000000000001", sourceartifactfixture.BundleHash))
	f := &timerHistoryNativeFixture{}
	if dialect == "postgres" {
		_, f.db, _ = testutil.StartPostgres(t)
		f.postgres, err = postgresbackend.New(f.db)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := schemastore.NewPostgres(f.postgres)
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.BootstrapSchema(t.Context(), bootstrap); err != nil {
			t.Fatal(err)
		}
		f.requireCurrent = schema.RequireCurrent
		artifacts, err := artifactstore.NewPostgres(f.postgres, schema.RequireCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := artifacts.EnsureSourceArtifact(setup, sourceartifactfixture.Artifact()); err != nil {
			t.Fatal(err)
		}
		runs, err := storerunlifecycle.NewPostgres(f.postgres, schema.RequireCurrent, runhandoff.NewCandidateCoordinator())
		if err != nil {
			t.Fatal(err)
		}
		f.runs = runs
		f.pipeline = &pipelinepersistence.PipelinePostgresOwner{RunLifecyclePostgresOwner: runs}
		f.generic, err = storegenericschedule.NewPostgres(f.postgres, schema.RequireCurrent, runs)
		if err != nil {
			t.Fatal(err)
		}
	} else {
		dbPath := filepath.Join(t.TempDir(), "timer-history.db")
		dsn := url.URL{Scheme: "file", Opaque: dbPath}
		query := dsn.Query()
		query.Add("_pragma", "foreign_keys(ON)")
		dsn.RawQuery = query.Encode()
		f.db, err = sql.Open("sqlite", dsn.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.db.Close() })
		f.sqlite, err = sqlitebackend.New(f.db)
		if err != nil {
			t.Fatal(err)
		}
		schema, err := schemastore.NewSQLiteWithBackend(f.sqlite, dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.BootstrapSchema(t.Context(), bootstrap); err != nil {
			t.Fatal(err)
		}
		f.requireCurrent = schema.RequireCurrent
		artifacts, err := artifactstore.NewSQLite(f.sqlite, schema.RequireCurrent, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := artifacts.EnsureSourceArtifact(setup, sourceartifactfixture.Artifact()); err != nil {
			t.Fatal(err)
		}
		runs, err := storerunlifecycle.NewSQLite(f.sqlite, schema.RequireCurrent, runhandoff.NewCandidateCoordinator(), nil)
		if err != nil {
			t.Fatal(err)
		}
		f.runs = runs
		f.pipeline = &pipelinepersistence.PipelineSQLiteOwner{RunLifecycleSQLiteOwner: runs}
		f.generic, err = storegenericschedule.NewSQLite(f.sqlite, schema.RequireCurrent, runs)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *timerHistoryNativeFixture) runContext(t *testing.T, runID string, at time.Time) context.Context {
	t.Helper()
	fact := sourceartifactfixture.Fact()
	ctx := authoractivity.WithScope(correlation.WithRunID(correlation.WithSourceArtifactFact(t.Context(), fact), runID), authoractivity.BundleScope("00000000-0000-4000-8000-000000000001", fact.BundleHash()))
	disposition, err := f.runs.CreateRun(ctx, runtimerunlifecycle.CreateRequest{RunID: runID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(), Source: fact, StartedAt: runtimerunlifecycle.CanonicalTimestamp(at)})
	if err != nil || disposition != runtimerunlifecycle.MutationApplied {
		t.Fatalf("canonical native run creation: disposition=%s err=%v", disposition, err)
	}
	return ctx
}

func (f *timerHistoryNativeFixture) run(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error)) mutationprotocol.Result[runfork.RunForkReplayResumeAdmission] {
	if f.postgres != nil {
		return mutationprotocol.RunPostgres(ctx, f.postgres, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
	}
	return mutationprotocol.RunSQLite(ctx, f.sqlite, "native timer history readback proof", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
}
