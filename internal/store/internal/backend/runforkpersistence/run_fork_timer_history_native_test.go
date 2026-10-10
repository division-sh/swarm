package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
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
	db       *sql.DB
	postgres *postgresbackend.Backend
	sqlite   *sqlitebackend.Backend
	pipeline timerHistoryNativeOwner
	generic  timerHistoryNativeScheduleOwner
}

// Native persistence/readback of an admitted typed-plan fixture, not historical
// source capture, selected join serving, or a public API proof. Ordinary history
// is terminal and inert; the active mixed-family matrix remains mocked.
func TestRunForkTimerHistoryNativeTerminalOrdinaryAndArrivalBothStores(t *testing.T) {
	for _, dialect := range []string{"sqlite", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			plan, bornAt, _, _ := mixedTimerHistoryFixture(t)
			terminal := workflowTimerHistoryTerminal(t, "fired", plan.WorkflowTimers[0].ActivationID)
			terminal.RunID, terminal.EntityID = plan.WorkflowTimers[0].RunID, plan.WorkflowTimers[0].EntityID
			terminal.Route, terminal.RoutingSource = plan.WorkflowTimers[0].Route, plan.WorkflowTimers[0].RoutingSource
			plan.WorkflowTimers = []pipeline.WorkflowTimerActivationPersistenceRecord{terminal.PersistenceRecord()}
			f := openTimerHistoryNativeFixture(t, dialect)
			sourceCtx := f.runContext(t, plan.SourceRunID, plan.JoinSchedules[0].AdmittedAt)
			ctx := f.runContext(t, workflowTimerProjectionChildRun, bornAt)
			for i, source := range plan.JoinSchedules {
				f.generic.SetNowFnForTest(func() time.Time { return source.AdmittedAt })
				commit, err := f.generic.AdmitGenericScheduleOutcome(sourceCtx, source.Command)
				if err != nil || !commit.Acknowledged || commit.Result.Outcome != genericschedule.AdmissionCreated {
					t.Fatalf("source arrival admission: %+v err=%v", commit, err)
				}
				actual := commit.Result.Activation
				if source.Status == genericschedule.StatusCancelled {
					cancelled, err := f.generic.CancelGenericScheduleOutcome(sourceCtx, genericschedule.CancelCommand{ActivationID: actual.ID, Cause: source.CancelCause, CancelledAt: source.CancelledAt})
					if err != nil || !cancelled.Acknowledged || cancelled.Result.Outcome != genericschedule.CancelChanged {
						t.Fatalf("source arrival cancellation: %+v err=%v", cancelled, err)
					}
					actual = cancelled.Result.Activation
				}
				source.ID = actual.ID
				if !reflect.DeepEqual(source.Canonical(), actual.Canonical()) {
					t.Fatal("native source admission changed the typed fixture beyond its minted identity")
				}
				plan.JoinSchedules[i] = actual.Canonical()
			}
			inventory, err := runForkTimerRecordInventory(plan.SourceRunID, plan.WorkflowTimers, plan.JoinSchedules)
			if err != nil {
				t.Fatal(err)
			}
			inventory.Complete, inventory.Point = true, plan.ForkPoint
			plan.ReplayResumeAdmission = runForkReplayResumeAdmission(runForkAdmissionEvidence{RelevantTimer: true, TimerHistory: inventory})
			before, err := json.Marshal(plan)
			if err != nil {
				t.Fatal(err)
			}
			refusal := errors.New("native missing arrival inventory rollback")
			missing := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error) {
				removed, err := materializeRunForkWorkflowTimers(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, f.pipeline, bornAt)
				if err != nil || len(removed) != 0 {
					return plan.ReplayResumeAdmission, errors.New("terminal history constructed ordinary work")
				}
				ordinary, err := f.pipeline.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, workflowTimerProjectionChildRun)
				if err != nil || len(ordinary) != 0 {
					return plan.ReplayResumeAdmission, errors.New("terminal ordinary inventory was not empty")
				}
				got, err := requireMaterializedRunForkTimerHistory(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, f.pipeline, f.pipeline, bornAt, plan.ReplayResumeAdmission)
				if err == nil || !reflect.DeepEqual(got, plan.ReplayResumeAdmission) {
					return got, errors.New("empty ordinary inventory discharged missing arrival history")
				}
				return got, refusal
			})
			if missing.Acknowledged() || !errors.Is(missing.Err(), refusal) {
				t.Fatalf("missing inventory outcome: acknowledged=%t err=%v", missing.Acknowledged(), missing.Err())
			}
			var certificate runfork.RunForkReplayResumeAdmission
			var retained []genericschedule.Activation
			for pass, construct := range []bool{true, false, true, false} {
				result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error) {
					if construct {
						if _, err := materializeRunForkWorkflowTimers(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, f.pipeline, bornAt); err != nil {
							return plan.ReplayResumeAdmission, err
						}
						if err := materializeRunForkArrivalJoinSchedules(ctx, attempt, plan, workflowTimerProjectionChildRun, bornAt, f.pipeline); err != nil {
							return plan.ReplayResumeAdmission, err
						}
					}
					ordinary, err := f.pipeline.ReadRunForkWorkflowTimerInventoryTx(ctx, attempt, workflowTimerProjectionChildRun)
					if err != nil || len(ordinary) != 0 {
						return plan.ReplayResumeAdmission, errors.New("terminal ordinary history rearmed")
					}
					rows, err := f.pipeline.ReadRunForkArrivalJoinScheduleInventoryTx(ctx, attempt, workflowTimerProjectionChildRun)
					if err != nil || len(rows) != len(plan.JoinSchedules) || (pass != 0 && !reflect.DeepEqual(rows, retained)) {
						return plan.ReplayResumeAdmission, errors.New("arrival inventory missing or changed on retry")
					}
					cut, err := requireMaterializedRunForkTimerHistory(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, f.pipeline, f.pipeline, bornAt, plan.ReplayResumeAdmission)
					if err != nil {
						return cut, err
					}
					continuing, err := requireContinuingRunForkTimerHistory(ctx, attempt, plan, workflowTimerProjectionChildRun, workflowTimerMaterializerSelection{}, f.pipeline, f.pipeline, bornAt, plan.ReplayResumeAdmission)
					if err != nil || !reflect.DeepEqual(cut, continuing) {
						return continuing, errors.New("cut and continuing immutable certificates differ")
					}
					retained = rows
					return cut, nil
				})
				got, acknowledged := result.Value()
				if err := result.Err(); err != nil || !acknowledged || len(got.UnsupportedBlockers) != 0 || got.StateOnlyExecutionReady || !got.ReplayResumeFactsPresent || (pass != 0 && !reflect.DeepEqual(got, certificate)) {
					t.Fatalf("native readback pass=%d acknowledged=%t admission=%+v err=%v", pass, acknowledged, got, err)
				}
				assertWorkflowTimerAppliedDisposition(t, got, runfork.RunForkReplayResumeDispositionReconstruct)
				certificate = got
			}
			for _, source := range plan.JoinSchedules {
				actual, found, err := f.generic.LoadGenericScheduleActivation(sourceCtx, source.ID)
				if err != nil || !found || !reflect.DeepEqual(actual.Canonical(), source.Canonical()) {
					t.Fatalf("child materialization or readback changed source: found=%t err=%v", found, err)
				}
			}
			after, err := json.Marshal(plan)
			if err != nil || string(before) != string(after) {
				t.Fatalf("native child operations changed admitted source plan: %v", err)
			}
		})
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
		f.pipeline = &pipelinepersistence.PipelinePostgresOwner{RunLifecyclePostgresOwner: &storerunlifecycle.RunLifecyclePostgresOwner{}}
		f.generic, err = storegenericschedule.NewPostgres(f.postgres, schema.RequireCurrent, &storerunlifecycle.RunLifecyclePostgresOwner{})
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
		f.pipeline = &pipelinepersistence.PipelineSQLiteOwner{RunLifecycleSQLiteOwner: &storerunlifecycle.RunLifecycleSQLiteOwner{}}
		f.generic, err = storegenericschedule.NewSQLite(f.sqlite, schema.RequireCurrent, &storerunlifecycle.RunLifecycleSQLiteOwner{})
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
	fixture := runlifecyclefixture.Fixture{RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin(), Source: fact, Artifact: sourceartifactfixture.Artifact(), BundleHash: fact.BundleHash(), StartedAt: runtimerunlifecycle.CanonicalTimestamp(at)}
	if f.postgres != nil {
		runlifecyclefixture.RequirePostgres(t, ctx, f.db, fixture)
	} else {
		runlifecyclefixture.RequireSQLite(t, ctx, f.db, fixture)
	}
	return ctx
}

func (f *timerHistoryNativeFixture) run(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runfork.RunForkReplayResumeAdmission, error)) mutationprotocol.Result[runfork.RunForkReplayResumeAdmission] {
	if f.postgres != nil {
		return mutationprotocol.RunPostgres(ctx, f.postgres, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
	}
	return mutationprotocol.RunSQLite(ctx, f.sqlite, "native timer history readback proof", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
}
