package genericschedule_test

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/forkpoint"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
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
	"github.com/google/uuid"
)

type forkJoinNativePipelineOwner interface {
	MaterializeRunForkArrivalJoinScheduleTx(context.Context, *mutationprotocol.Attempt, storegenericschedule.ForkJoinRequest) (runtimegenericschedule.Activation, error)
	RequireRunForkArrivalJoinScheduleTx(context.Context, *mutationprotocol.Attempt, storegenericschedule.ForkJoinRequest) (runtimegenericschedule.Activation, error)
}

type forkJoinNativeScheduleOwner interface {
	AdmitGenericScheduleOutcome(context.Context, runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.AdmissionCommit, error)
	CancelGenericScheduleOutcome(context.Context, runtimegenericschedule.CancelCommand) (runtimegenericschedule.CancelCommit, error)
	LoadGenericScheduleActivation(context.Context, string) (runtimegenericschedule.Activation, bool, error)
	ListActiveGenericScheduleActivations(context.Context) ([]runtimegenericschedule.Activation, error)
}

type forkJoinNativeFixture struct {
	db       *sql.DB
	postgres *postgresbackend.Backend
	sqlite   *sqlitebackend.Backend
	generic  forkJoinNativeScheduleOwner
	pipeline forkJoinNativePipelineOwner
}

// Persistence-only proof: the positive typed cut is command provenance, not a
// planner snapshot. No executor, scheduler, publication, or current grant is installed.
func TestForkJoinNativeRestoreReadbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openForkJoinNativeFixture(t, backend)
			for _, flow := range []string{".", "orders"} {
				for _, shape := range []struct {
					name      string
					timeout   bool
					cancelled bool
				}{
					{name: "active_completion"},
					{name: "cancelled_completion", cancelled: true},
					{name: "active_timeout", timeout: true},
					{name: "cancelled_timeout", timeout: true, cancelled: true},
				} {
					t.Run(flow+"/"+shape.name, func(t *testing.T) {
						ctx, request := forkJoinNativeRequest(t, f, flow, shape.timeout, shape.cancelled)
						sourceBefore := request.Source.Canonical()
						forkJoinNativeRequireMissing(t, f, ctx, request)
						created := forkJoinNativeRestore(t, f, ctx, request)
						expected, err := request.Expected(created.ID)
						if err != nil || !reflect.DeepEqual(created.Canonical(), expected.Canonical()) || !created.InitialDueAt.Before(created.AdmittedAt) {
							t.Fatalf("native restore lost the captured overdue due, mode, disposition, or origin: got=%+v want=%+v err=%v", created, expected, err)
						}
						reused := forkJoinNativeRestore(t, f, ctx, request)
						if !reflect.DeepEqual(created.Canonical(), reused.Canonical()) {
							t.Fatal("exact native restoration minted or changed the child row")
						}
						read := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
							return f.pipeline.RequireRunForkArrivalJoinScheduleTx(ctx, attempt, request)
						})
						actual := forkJoinNativeAcknowledged(t, read)
						if !reflect.DeepEqual(created.Canonical(), actual.Canonical()) {
							t.Fatal("transaction-bound readback changed the exact child projection")
						}
						forkJoinNativeRequireWrongProvenance(t, f, ctx, request, created)
						forkJoinNativeRequireRollback(t, f, ctx, request)
						sourceAfter, found, err := f.generic.LoadGenericScheduleActivation(correlation.WithRunID(ctx, request.Source.Command.RunID), request.Source.ID)
						if err != nil || !found || !reflect.DeepEqual(sourceBefore, sourceAfter.Canonical()) || !reflect.DeepEqual(sourceBefore, request.Source.Canonical()) {
							t.Fatalf("native child operations changed source history: found=%t source=%+v err=%v", found, sourceAfter, err)
						}
					})
				}
			}
		})
	}
}

func openForkJoinNativeFixture(t *testing.T, dialect string) *forkJoinNativeFixture {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve native join fixture source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", ".."))
	path := contracts.DefaultPlatformSpecFile(root)
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
	bootstrap := schemastore.SchemaBootstrapRequest{PlatformPlans: plans, Origin: schemastore.RuntimeStoreOrigin{
		SwarmVersion: "fork-join-native-test", PlatformVersion: spec.Platform.Version, CreatedAt: time.Now().UTC(),
	}}
	f := &forkJoinNativeFixture{}
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
		f.generic, err = storegenericschedule.NewPostgres(f.postgres, schema.RequireCurrent)
		f.pipeline = &pipelinepersistence.PipelinePostgresOwner{RunLifecyclePostgresOwner: &storerunlifecycle.RunLifecyclePostgresOwner{}}
	} else {
		dbPath := filepath.Join(t.TempDir(), "fork-join.db")
		dsn := url.URL{Scheme: "file", Opaque: dbPath}
		query := dsn.Query()
		query.Add("_pragma", "foreign_keys(ON)")
		query.Add("_pragma", "busy_timeout(50)")
		dsn.RawQuery = query.Encode()
		f.db, err = sql.Open("sqlite", dsn.String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.db.Close() })
		f.db.SetMaxOpenConns(4)
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
		f.generic, err = storegenericschedule.NewSQLite(f.sqlite, schema.RequireCurrent)
		f.pipeline = &pipelinepersistence.PipelineSQLiteOwner{RunLifecycleSQLiteOwner: &storerunlifecycle.RunLifecycleSQLiteOwner{}}
	}
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func forkJoinNativeRunContext(t *testing.T, f *forkJoinNativeFixture, runID string) context.Context {
	t.Helper()
	fact := sourceartifactfixture.Fact()
	ctx := correlation.WithRunID(correlation.WithSourceArtifactFact(t.Context(), fact), runID)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope("00000000-0000-4000-8000-000000000001", fact.BundleHash()))
	fixture := runlifecyclefixture.Fixture{
		RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin(), Source: fact,
		Artifact: sourceartifactfixture.Artifact(), BundleHash: fact.BundleHash(), StartedAt: runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC().Add(-2 * time.Hour)),
	}
	if f.postgres != nil {
		runlifecyclefixture.RequirePostgres(t, ctx, f.db, fixture)
	} else {
		runlifecyclefixture.RequireSQLite(t, ctx, f.db, fixture)
	}
	return ctx
}

func forkJoinNativeRequest(t *testing.T, f *forkJoinNativeFixture, flow string, timeout, cancelled bool) (context.Context, storegenericschedule.ForkJoinRequest) {
	t.Helper()
	sourceRun, childRun := uuid.NewString(), uuid.NewString()
	sourceCtx := forkJoinNativeRunContext(t, f, sourceRun)
	childCtx := forkJoinNativeRunContext(t, f, childRun)
	arm := runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC().Add(-2 * time.Hour))
	entityID := sourceRun
	route := flowidentity.StoredRoute(".", sourceRun, sourceRun)
	node := identitytest.RootNode(t, "join-node")
	if flow != "." {
		entityID = uuid.NewString()
		route = flowidentity.StoredRoute(flow, "order-1", flow+"/order-1")
		node = identitytest.FlowNode(t, flow, "join-node")
	}
	owner, err := flowidentity.NewRunScopedFlowInstance(sourceRun, route)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := workflowlifecycle.NewInitialEntry(route, identity.NormalizeEntityID(entityID), "awaiting", executionmode.Live, arm)
	if err != nil {
		t.Fatal(err)
	}
	entry, enters, err := effect.StageEntry(owner)
	if err != nil || !enters {
		t.Fatalf("source initial entry: enters=%t err=%v", enters, err)
	}
	ref, err := timeridentity.NewJoinRef(node, "item.completed", "awaiting", "shared")
	if err != nil {
		t.Fatal(err)
	}
	ref, err = ref.BindStageEntry(entry, attemptgeneration.Generation{})
	if err != nil {
		t.Fatal(err)
	}
	members, fireAt := []string{}, time.Time{}
	if timeout {
		members, fireAt = []string{"member-a"}, arm.Add(time.Hour)
	}
	join, err := joinruntime.NewActivation(ref, members, nil, arm, fireAt)
	if err != nil {
		t.Fatal(err)
	}
	if !timeout {
		join.Close(joinruntime.CloseReasonComplete, true, false)
		join, err = join.WithTimerHandle(join.TimerHandle(), arm)
		if err != nil {
			t.Fatal(err)
		}
	}
	command, err := runtimegenericschedule.WorkflowJoinAdmission(join, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := f.generic.AdmitGenericScheduleOutcome(sourceCtx, command)
	if err != nil || !admission.Acknowledged || admission.Result.Outcome != runtimegenericschedule.AdmissionCreated {
		t.Fatalf("real source schedule admission: result=%+v err=%v", admission, err)
	}
	actual := admission.Result.Activation
	if cancelled {
		cause := "join_stage_exit"
		if timeout {
			cause = "join_closed"
		}
		at := runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC())
		if at.Before(actual.AdmittedAt) {
			at = actual.AdmittedAt
		}
		cancellation, err := f.generic.CancelGenericScheduleOutcome(sourceCtx, runtimegenericschedule.CancelCommand{ActivationID: actual.ID, Cause: cause, CancelledAt: at})
		if err != nil || !cancellation.Acknowledged || cancellation.Result.Outcome != runtimegenericschedule.CancelChanged {
			t.Fatalf("real source schedule cancellation: result=%+v err=%v", cancellation, err)
		}
		actual = cancellation.Result.Activation
	}
	loaded, found, err := f.generic.LoadGenericScheduleActivation(sourceCtx, actual.ID)
	if err != nil || !found || !reflect.DeepEqual(actual.Canonical(), loaded.Canonical()) {
		t.Fatalf("real source readback: found=%t err=%v", found, err)
	}
	projection, err := runfork.ProjectEntityOwnership(sourceRun, childRun, entityID, route.InstancePath)
	if err != nil {
		t.Fatal(err)
	}
	childRoute, err := runfork.ProjectExecutionRoute(sourceRun, childRun, flow, route)
	if err != nil {
		t.Fatal(err)
	}
	childEntry := entry
	childEntry.RunID, childEntry.OriginRunID, childEntry.EntityID = childRun, sourceRun, projection.Fork.EntityID
	childEntry.InstanceID, childEntry.InstancePath = childRoute.InstanceID, childRoute.InstancePath
	childRef, err := ref.Declaration().BindStageEntry(childEntry, ref.Generation())
	if err != nil {
		t.Fatal(err)
	}
	childJoin, err := join.WithForkReference(childRef)
	if err != nil {
		t.Fatal(err)
	}
	child, err := runtimegenericschedule.WorkflowJoinAdmission(childJoin, actual.Command.ExecutionMode)
	if err != nil {
		t.Fatal(err)
	}
	born := runtimerunlifecycle.CanonicalTimestamp(time.Now().UTC())
	if born.Before(actual.AdmittedAt) {
		born = actual.AdmittedAt
	}
	if born.Before(actual.CancelledAt) {
		born = actual.CancelledAt
	}
	request := storegenericschedule.ForkJoinRequest{Source: loaded, Child: child, PointKind: forkpoint.DeploymentRevision, PointRevision: 7, BornAt: born}
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	return childCtx, request
}

func (f *forkJoinNativeFixture) run(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error)) mutationprotocol.Result[runtimegenericschedule.Activation] {
	if f.postgres != nil {
		return mutationprotocol.RunPostgres(ctx, f.postgres, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
	}
	return mutationprotocol.RunSQLite(ctx, f.sqlite, "native fork join proof", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
}

func forkJoinNativeAcknowledged(t *testing.T, result mutationprotocol.Result[runtimegenericschedule.Activation]) runtimegenericschedule.Activation {
	t.Helper()
	value, acknowledged := result.Value()
	if err := result.Err(); err != nil || !acknowledged {
		t.Fatalf("native mutation was not cleanly acknowledged: acknowledged=%t err=%v", acknowledged, err)
	}
	return value
}

func forkJoinNativeRestore(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, request storegenericschedule.ForkJoinRequest) runtimegenericschedule.Activation {
	t.Helper()
	result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		created, err := f.pipeline.MaterializeRunForkArrivalJoinScheduleTx(ctx, attempt, request)
		if err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		read, err := f.pipeline.RequireRunForkArrivalJoinScheduleTx(ctx, attempt, request)
		if err == nil && !reflect.DeepEqual(created.Canonical(), read.Canonical()) {
			return runtimegenericschedule.Activation{}, errors.New("same-attempt child readback changed")
		}
		return read, err
	})
	return forkJoinNativeAcknowledged(t, result)
}

func forkJoinNativeRequireMissing(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, request storegenericschedule.ForkJoinRequest) {
	t.Helper()
	before, err := f.generic.ListActiveGenericScheduleActivations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		return f.pipeline.RequireRunForkArrivalJoinScheduleTx(ctx, attempt, request)
	})
	if result.Err() == nil || result.Acknowledged() {
		t.Fatal("read-only requirement minted or acknowledged a missing child schedule")
	}
	after, err := f.generic.ListActiveGenericScheduleActivations(ctx)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("missing-child requirement changed the native active inventory: err=%v", err)
	}
}

func forkJoinNativeRequireWrongProvenance(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, request storegenericschedule.ForkJoinRequest, original runtimegenericschedule.Activation) {
	t.Helper()
	wrong := request
	wrong.PointRevision++
	if err := wrong.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, restore := range []bool{false, true} {
		result := f.run(ctx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
			if restore {
				return f.pipeline.MaterializeRunForkArrivalJoinScheduleTx(ctx, attempt, wrong)
			}
			return f.pipeline.RequireRunForkArrivalJoinScheduleTx(ctx, attempt, wrong)
		})
		if result.Err() == nil || result.Acknowledged() {
			t.Fatal("same scoped command accepted different captured cut provenance")
		}
	}
	foreignCtx := correlation.WithRunID(ctx, request.Source.Command.RunID)
	result := f.run(foreignCtx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		return f.pipeline.RequireRunForkArrivalJoinScheduleTx(ctx, attempt, request)
	})
	if result.Err() == nil || result.Acknowledged() {
		t.Fatal("foreign run context acknowledged child schedule readback")
	}
	actual, found, err := f.generic.LoadGenericScheduleActivation(ctx, original.ID)
	if err != nil || !found || !reflect.DeepEqual(original.Canonical(), actual.Canonical()) {
		t.Fatalf("refused provenance/context repaired or changed the exact child: found=%t err=%v", found, err)
	}
}

func forkJoinNativeRequireRollback(t *testing.T, f *forkJoinNativeFixture, ctx context.Context, request storegenericschedule.ForkJoinRequest) {
	t.Helper()
	rollbackRun := uuid.NewString()
	rollbackCtx := forkJoinNativeRunContext(t, f, rollbackRun)
	handle, ref, valid := timeridentity.ParseJoinHandle(request.Child.Payload.Interface().(map[string]any))
	if !valid {
		t.Fatal("rollback fixture lost its child arrival handle")
	}
	entry := ref.StageEntry()
	projection, err := runfork.ProjectEntityOwnership(request.Child.RunID, rollbackRun, entry.EntityID, entry.InstancePath)
	if err != nil {
		t.Fatal(err)
	}
	route, err := runfork.ProjectExecutionRoute(request.Child.RunID, rollbackRun, entry.FlowScope, flowidentity.StoredRoute(entry.FlowScope, entry.InstanceID, entry.InstancePath))
	if err != nil {
		t.Fatal(err)
	}
	entry.RunID, entry.EntityID, entry.InstanceID, entry.InstancePath = rollbackRun, projection.Fork.EntityID, route.InstanceID, route.InstancePath
	ref, err = ref.Declaration().BindStageEntry(entry, ref.Generation())
	if err != nil {
		t.Fatal(err)
	}
	if handle.Kind() == timeridentity.TimerHandleJoinComplete {
		handle, err = timeridentity.JoinCompleteHandle(ref)
	} else {
		handle, err = timeridentity.JoinTimeoutHandle(ref)
	}
	if err != nil {
		t.Fatal(err)
	}
	rollback := request
	rollback.Child.RunID, rollback.Child.EntityID = rollbackRun, entry.EntityID
	rollback.Child.RoutingSource, err = runfork.ProjectProducerOwnership(request.Child.RunID, rollbackRun, request.Child.RoutingSource)
	if err != nil {
		t.Fatal(err)
	}
	rollback.Child.FlowInstance = request.Child.FlowInstance
	rollback.Child.Payload, err = canonicaljson.FromGo(handle.PayloadMetadata())
	if err != nil {
		t.Fatal(err)
	}
	rollback.Child.TaskID, rollback.Child.ScheduleKey, rollback.Child.EventType = handle.TaskID(), handle.TaskID(), handle.EventType()
	if err := rollback.Validate(); err != nil {
		t.Fatal(err)
	}
	abort := errors.New("abort after exact child restoration")
	var childID string
	result := f.run(rollbackCtx, func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimegenericschedule.Activation, error) {
		created, err := f.pipeline.MaterializeRunForkArrivalJoinScheduleTx(ctx, attempt, rollback)
		if err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		childID = created.ID
		if _, err := f.pipeline.RequireRunForkArrivalJoinScheduleTx(ctx, attempt, rollback); err != nil {
			return runtimegenericschedule.Activation{}, err
		}
		return runtimegenericschedule.Activation{}, abort
	})
	if !errors.Is(result.Err(), abort) || result.Acknowledged() || childID == "" {
		t.Fatalf("rollback did not follow real restoration/readback: child=%q acknowledged=%t err=%v", childID, result.Acknowledged(), result.Err())
	}
	if _, found, err := f.generic.LoadGenericScheduleActivation(rollbackCtx, childID); err != nil || found {
		t.Fatalf("rolled-back child row survived: found=%t err=%v", found, err)
	}
	forkJoinNativeRequireMissing(t, f, rollbackCtx, rollback)
}
