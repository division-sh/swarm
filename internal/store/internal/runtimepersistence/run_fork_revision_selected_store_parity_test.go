package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type runForkRevisionMatrixFixture struct {
	runID        string
	eventID      string
	entityID     string
	mutationID   string
	deliveryID   string
	receiptID    string
	deadLetterID string
	timerID      string
	sessionID    string
	turnID       string
	auditID      string
	replyID      string
	surfaceID    string
	operationID  string
	attemptID    string
	authorityID  string
	at           time.Time
}

type runForkRevisionMatrixFact struct {
	Family  runforkrevision.Family
	Key     string
	Fact    any
	Present bool
}

func TestRunForkRevisionThirteenFamilySelectedStoreParity(t *testing.T) {
	fixture := runForkRevisionMatrixFixture{
		runID:        "00000000-0000-0000-0000-000000002272",
		eventID:      "00000000-0000-0000-0000-000000002273",
		entityID:     "00000000-0000-0000-0000-000000002274",
		mutationID:   "00000000-0000-0000-0000-000000002275",
		deliveryID:   "00000000-0000-0000-0000-000000002276",
		receiptID:    "00000000-0000-0000-0000-000000002277",
		deadLetterID: "00000000-0000-0000-0000-000000002278",
		timerID:      "00000000-0000-0000-0000-000000002279",
		sessionID:    "00000000-0000-0000-0000-000000002280",
		turnID:       "00000000-0000-0000-0000-000000002281",
		auditID:      "00000000-0000-0000-0000-000000002282",
		replyID:      "revision-matrix-reply",
		surfaceID:    "00000000-0000-0000-0000-000000002283",
		operationID:  "00000000-0000-0000-0000-000000002284",
		attemptID:    "00000000-0000-0000-0000-000000002285",
		authorityID:  "00000000-0000-0000-0000-000000002286",
		at:           time.Date(2026, 8, 25, 19, 0, 0, 123000000, time.UTC),
	}

	results := make(map[string][]runForkRevisionMatrixFact, 2)
	t.Run("sqlite", func(t *testing.T) {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		db := store.backend.ConstructionHandle()
		results["sqlite"] = proveRunForkRevisionThirteenFamilyMatrix(t, db, false, fixture)
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		results["postgres"] = proveRunForkRevisionThirteenFamilyMatrix(t, db, true, fixture)
	})
	if !reflect.DeepEqual(results["sqlite"], results["postgres"]) {
		t.Fatalf("selected-store canonical revision facts differ:\nsqlite=%#v\npostgres=%#v", results["sqlite"], results["postgres"])
	}
}

func TestRunForkPlannerSelectedStoreParity(t *testing.T) {
	fixture := newRunForkRevisionMatrixFixture()
	type plannerProof struct {
		Explicit runfork.RunForkPlan
		Latest   runfork.RunForkPlan
	}
	plans := make(map[string]plannerProof, 2)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var db *sql.DB
			var selected interface {
				PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
			}
			if backend == "sqlite" {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				db = store.backend.ConstructionHandle()
				selected = store
			} else {
				_, db, _ = testutil.StartPostgres(t)
				selected = newPostgresStoreWithBackend(mustPostgresBackend(db))
			}
			requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
				Origin: semanticScenarioSetupRunOriginForTest(), RunID: fixture.runID, StartedAt: fixture.at,
			})
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin planner parity revision: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			seedRunForkRevisionMatrixEvent(t, ctx, tx, fixture.runID, fixture.eventID, fixture.at, backend == "postgres")
			effects, err := runforkrevision.ForRun(fixture.runID, runforkrevision.AllFamilies()...)
			if err != nil {
				t.Fatalf("declare planner parity revision: %v", err)
			}
			if _, err := finalizeRunForkRevisionMatrix(ctx, tx, backend == "postgres", effects); err != nil {
				t.Fatalf("finalize planner parity revision: %v", err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit planner parity revision: %v", err)
			}
			explicit, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: fixture.runID, At: fixture.eventID})
			if err != nil {
				t.Fatalf("plan selected-store fork at explicit event: %v", err)
			}
			latest, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: fixture.runID})
			if err != nil {
				t.Fatalf("plan selected-store fork at latest event: %v", err)
			}
			explicitPoint, latestPoint := explicit.ForkPoint, latest.ForkPoint
			if explicitPoint.Input != fixture.eventID || latestPoint.Input != "" {
				t.Fatalf("fork point selector echo = explicit:%q latest:%q", explicitPoint.Input, latestPoint.Input)
			}
			explicitPoint.Input, latestPoint.Input = "", ""
			if latestPoint != explicitPoint {
				t.Fatalf("latest fork point = %#v, want explicit latest point %#v", latest.ForkPoint, explicit.ForkPoint)
			}
			plans[backend] = plannerProof{Explicit: explicit, Latest: latest}
		})
	}
	if !reflect.DeepEqual(plans["sqlite"], plans["postgres"]) {
		t.Fatalf("selected-store fork plans differ:\nsqlite=%#v\npostgres=%#v", plans["sqlite"], plans["postgres"])
	}
}

type runForkSelectedLifecycleStore interface {
	PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)
	ActivateRunFork(context.Context, runfork.RunForkActivateRequest) (runfork.RunForkActivation, error)
}

type runForkSelectedLifecycleProof struct {
	SourceInitialStatus  string
	MaterializedStatus   string
	MaterializedEntities int
	EntityState          string
	EntityName           string
	SourceFinalStatus    string
	ForkFinalStatus      string
	SourceFrozen         bool
}

func TestRunForkSelectedStoreLifecycleParity(t *testing.T) {
	for _, sourceState := range []runtimerunlifecycle.State{
		runtimerunlifecycle.StateRunning,
		runtimerunlifecycle.StatePaused,
	} {
		t.Run(string(sourceState), func(t *testing.T) {
			proofs := make(map[string]runForkSelectedLifecycleProof, 2)
			for _, backend := range []string{"sqlite", "postgres"} {
				t.Run(backend, func(t *testing.T) {
					if backend == "sqlite" {
						store := newBootstrappedSQLiteRuntimeStoreForTest(t)
						proofs[backend] = proveRunForkSelectedStoreLifecycle(t, store, store, store.backend.ConstructionHandle(), false, sourceState)
						return
					}
					_, db, _ := testutil.StartPostgres(t)
					store := newPostgresStoreWithBackend(mustPostgresBackend(db))
					proofs[backend] = proveRunForkSelectedStoreLifecycle(t, store, store, db, true, sourceState)
				})
			}
			if !reflect.DeepEqual(proofs["sqlite"], proofs["postgres"]) {
				t.Fatalf("selected-store fork lifecycle differs:\nsqlite=%#v\npostgres=%#v", proofs["sqlite"], proofs["postgres"])
			}
		})
	}
}

func proveRunForkSelectedStoreLifecycle(t *testing.T, selected runForkSelectedLifecycleStore, store any, db *sql.DB, postgres bool, sourceState runtimerunlifecycle.State) runForkSelectedLifecycleProof {
	t.Helper()
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	at := time.Date(2026, 8, 25, 20, 0, 0, 123000000, time.UTC)
	requireRunFixtureForTest(t, ctx, store, semanticRunFixture{
		Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, State: sourceState, StartedAt: at.Add(-time.Minute),
		BundleHash: authorActivityTestBundleHash,
	})
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	mustExecRunForkRevisionMatrix(t, ctx, tx, `
		INSERT INTO events (
			event_class,event_id,run_id,event_name,entity_id,scope,payload,payload_bytes,execution_mode,
			chain_depth,produced_by,produced_by_type,created_at,routing_source_kind,source_route,target_route,target_set,route_settlement
		) VALUES ('selected_fork_replay',$1,$2,'fork.ready',$3,'entity',$4,$5,'live',0,'sqlite-parity','platform',$6,'absent',$7,$7,$8,$9)
	`, eventID, runID, entityID, `{"name":"Snapshot Entity"}`, []byte(`{"name":"Snapshot Entity"}`), at, `{}`, `[]`, `{"write_class":"historical_run_fork_replay","arm":"delivery"}`)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `
		INSERT INTO entity_mutations (
			mutation_id,run_id,entity_id,domain,path,old_value,new_value,caused_by_event,writer_type,writer_id,handler_step,created_at
		) VALUES
			($1,$2,$3,'lifecycle_state','','null','"ready"',$4,'platform','sqlite-parity','seed',$5),
			($6,$2,$3,'authored_field','name','null','"Snapshot Entity"',$4,'platform','sqlite-parity','seed',$5)
	`, uuid.NewString(), runID, entityID, eventID, at, uuid.NewString())
	mustExecRunForkRevisionMatrix(t, ctx, tx, `
		INSERT INTO entity_state (
			run_id,entity_id,flow_instance,entity_type,name,current_state,gates,fields,bookkeeping,accumulator,
			revision,entered_state_at,created_at,updated_at
		) VALUES ($1,$2,'flow-a/1','fork_entity','Snapshot Entity','ready','{}','{"name":"Snapshot Entity"}','{}','{}',1,$3,$3,$3)
	`, runID, entityID, at)
	effects, err := runforkrevision.ForRun(runID, runforkrevision.AllFamilies()...)
	if err != nil {
		t.Fatal(err)
	}
	if postgres {
		if _, err := runforkrevision.FinalizePostgres(ctx, tx, effects); err != nil {
			t.Fatalf("finalize PostgreSQL fork source revision: %v", err)
		}
	} else if _, err := runforkrevision.FinalizeSQLite(ctx, tx, effects); err != nil {
		t.Fatalf("finalize SQLite fork source revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := selected.MaterializeRunFork(cancelledCtx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: eventID}); err == nil {
		t.Fatal("pre-cancelled fork materialization succeeded")
	}
	var cancelledForks int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1 AND forked_from_event_id = $2`, runID, eventID).Scan(&cancelledForks); err != nil {
		t.Fatalf("count cancelled fork materializations: %v", err)
	}
	if cancelledForks != 0 {
		t.Fatalf("pre-cancelled fork materialization created %d runs", cancelledForks)
	}

	materialized, err := selected.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("materialize selected-store run fork: %v", err)
	}
	if materialized.ForkRunStatus != runfork.RunForkMaterializedStatus || materialized.MaterializedEntityCount != 1 || !materialized.ExecutionReady {
		t.Fatalf("selected-store materialization = %#v", materialized)
	}
	repeated, err := selected.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("repeat exact fork materialization: %v", err)
	}
	if repeated.ForkRunID != materialized.ForkRunID || repeated.ForkPoint != materialized.ForkPoint || repeated.MaterializedEntityCount != materialized.MaterializedEntityCount {
		t.Fatalf("repeat materialization = %#v, want exact replay of %#v", repeated, materialized)
	}
	var status, sourceRunID, sourceEventID, currentState, name string
	if err := db.QueryRowContext(ctx, `
		SELECT r.status, r.forked_from_run_id, r.forked_from_event_id, e.current_state, e.name
		FROM runs r JOIN entity_state e ON e.run_id = r.run_id
		WHERE r.run_id = $1 AND e.entity_id = $2
	`, materialized.ForkRunID, entityID).Scan(&status, &sourceRunID, &sourceEventID, &currentState, &name); err != nil {
		t.Fatalf("read materialized selected-store fork: %v", err)
	}
	if status != runfork.RunForkMaterializedStatus || sourceRunID != runID || sourceEventID != eventID || currentState != "ready" || name != "Snapshot Entity" {
		t.Fatalf("materialized selected-store fork readback = %q %q %q %q %q", status, sourceRunID, sourceEventID, currentState, name)
	}
	if _, err := selected.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID}); err == nil {
		t.Fatal("fork activation without source-freeze confirmation succeeded")
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(sourceState) {
		t.Fatalf("source status after rejected activation = %q, want %q", status, sourceState)
	}
	cancelledCtx, cancel = context.WithCancel(ctx)
	cancel()
	if _, err := selected.ActivateRunFork(cancelledCtx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID, ConfirmSourceFreeze: true}); err == nil {
		t.Fatal("pre-cancelled fork activation succeeded")
	}
	var forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1`, runID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != string(sourceState) || forkStatus != runfork.RunForkMaterializedStatus {
		t.Fatalf("pre-cancelled activation changed source/fork state to %q/%q", status, forkStatus)
	}
	activation, err := selected.ActivateRunFork(ctx, runfork.RunForkActivateRequest{
		ForkRunID: materialized.ForkRunID, ConfirmSourceFreeze: true,
	})
	if err != nil {
		t.Fatalf("activate selected-store run fork: %v", err)
	}
	if !activation.Activated || !activation.SourceFrozen || activation.ForkRunStatus != runfork.RunForkActivatedStatus {
		t.Fatalf("selected-store activation = %#v", activation)
	}
	var continuedAs string
	if err := db.QueryRowContext(ctx, `SELECT status, continued_as_run_id FROM runs WHERE run_id = $1`, runID).Scan(&status, &continuedAs); err != nil {
		t.Fatal(err)
	}
	if status != runfork.RunForkSourceFrozenStatus || continuedAs != materialized.ForkRunID {
		t.Fatalf("selected-store source freeze readback = %q continued_as=%q", status, continuedAs)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1`, materialized.ForkRunID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != runfork.RunForkActivatedStatus {
		t.Fatalf("selected-store fork status after activation = %q", status)
	}
	return runForkSelectedLifecycleProof{
		SourceInitialStatus: string(sourceState), MaterializedStatus: materialized.ForkRunStatus,
		MaterializedEntities: materialized.MaterializedEntityCount, EntityState: currentState, EntityName: name,
		SourceFinalStatus: runfork.RunForkSourceFrozenStatus, ForkFinalStatus: status, SourceFrozen: activation.SourceFrozen,
	}
}

func TestGoldenRuntimeRunsRemainForkPlannablePostgres(t *testing.T) {
	for _, workload := range []struct {
		name             string
		extraEvents      int
		extraDeadLetters int
	}{
		{name: "golden_workload"},
		{name: "jobflow_damaged_run", extraEvents: 100, extraDeadLetters: 15},
	} {
		t.Run(workload.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			ctx := testAuthorActivityContext()
			fixture := newRunForkRevisionMatrixFixture()
			selected := newPostgresStoreWithBackend(mustPostgresBackend(db))
			requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
				Origin: semanticScenarioSetupRunOriginForTest(), RunID: fixture.runID, StartedAt: fixture.at,
			})
			seedTestAgentRow(t, ctx, db, true, testAgentIdentity(t, "revision-matrix-agent", ""), "active")

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin representative workload revision: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			seedRunForkRevisionMatrixFacts(t, ctx, tx, fixture, true, true)
			selectedEventID := fixture.eventID
			for i := 0; i < workload.extraEvents; i++ {
				selectedEventID = uuid.NewString()
				seedRunForkRevisionMatrixEvent(t, ctx, tx, fixture.runID, selectedEventID, fixture.at.Add(time.Duration(i+1)*time.Microsecond), true)
			}
			for i := 0; i < workload.extraDeadLetters; i++ {
				mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO dead_letters (dead_letter_id,original_event_id,original_event,original_payload,flow_instance,failure,created_at) VALUES ($1,$2,'matrix.event',$3,'matrix-flow',$4,$5)`, uuid.NewString(), fixture.eventID, `{}`, `{"class":"matrix"}`, fixture.at.Add(time.Duration(i+1)*time.Microsecond))
			}
			effects, err := runforkrevision.ForRun(fixture.runID, runforkrevision.AllFamilies()...)
			if err != nil {
				t.Fatalf("declare representative workload effects: %v", err)
			}
			results, err := runforkrevision.FinalizePostgres(ctx, tx, effects)
			if err != nil {
				t.Fatalf("finalize representative workload revision: %v", err)
			}
			if got := results[fixture.runID]; !got.Changed || got.Revision != 1 {
				t.Fatalf("representative workload revision = %#v, want changed revision 1", got)
			}
			if err := tx.Commit(); err != nil {
				t.Fatalf("commit representative workload revision: %v", err)
			}

			plan, err := selected.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: fixture.runID, At: selectedEventID})
			if err != nil {
				t.Fatalf("representative workload is not fork-plannable: %v", err)
			}
			if plan.ForkPoint.EventID != selectedEventID || plan.ForkPoint.Revision != 1 {
				t.Fatalf("representative workload fork point = %#v, want event %s at revision 1", plan.ForkPoint, selectedEventID)
			}
		})
	}
}

func newRunForkRevisionMatrixFixture() runForkRevisionMatrixFixture {
	return runForkRevisionMatrixFixture{
		runID: uuid.NewString(), eventID: uuid.NewString(), entityID: uuid.NewString(), mutationID: uuid.NewString(),
		deliveryID: uuid.NewString(), receiptID: uuid.NewString(), deadLetterID: uuid.NewString(), timerID: uuid.NewString(),
		sessionID: uuid.NewString(), turnID: uuid.NewString(), auditID: uuid.NewString(), replyID: "revision-matrix-" + uuid.NewString(),
		surfaceID: uuid.NewString(), operationID: uuid.NewString(), attemptID: uuid.NewString(), authorityID: uuid.NewString(),
		at: time.Date(2026, 8, 25, 19, 0, 0, 123000000, time.UTC),
	}
}

func proveRunForkRevisionThirteenFamilyMatrix(t *testing.T, db *sql.DB, postgres bool, fixture runForkRevisionMatrixFixture) []runForkRevisionMatrixFact {
	t.Helper()
	ctx := testAuthorActivityContext()
	var selected any = NewSQLiteRuntimeStoreForTest(db)
	if postgres {
		selected = newPostgresStoreWithBackend(mustPostgresBackend(db))
	}
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
		Origin: semanticScenarioSetupRunOriginForTest(), RunID: fixture.runID, StartedAt: fixture.at,
	})
	identity := testAgentIdentity(t, "revision-matrix-agent", "")
	seedTestAgentRow(t, ctx, db, postgres, identity, "active")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin thirteen-family transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	seedRunForkRevisionMatrixFacts(t, ctx, tx, fixture, true, postgres)
	effects, err := runforkrevision.ForRun(fixture.runID, runforkrevision.AllFamilies()...)
	if err != nil {
		t.Fatalf("declare thirteen-family effects: %v", err)
	}
	result, err := finalizeRunForkRevisionMatrix(ctx, tx, postgres, effects)
	if err != nil {
		t.Fatalf("finalize thirteen-family revision: %v", err)
	}
	if got := result[fixture.runID]; !got.Changed || got.Revision != 1 {
		t.Fatalf("initial thirteen-family result = %#v, want changed revision 1", got)
	}
	if err := validateRunForkRevisionMatrix(ctx, tx, postgres, fixture.runID); err != nil {
		t.Fatalf("validate initial thirteen-family revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit thirteen-family revision: %v", err)
	}

	noChangeTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin no-change transaction: %v", err)
	}
	defer func() { _ = noChangeTx.Rollback() }()
	noChange, err := finalizeRunForkRevisionMatrix(ctx, noChangeTx, postgres, effects)
	if err != nil {
		t.Fatalf("finalize exact no-change: %v", err)
	}
	if got := noChange[fixture.runID]; got.Changed || got.Revision != 1 {
		t.Fatalf("exact no-change result = %#v, want unchanged revision 1", got)
	}
	if err := noChangeTx.Commit(); err != nil {
		t.Fatalf("commit exact no-change: %v", err)
	}
	initial := loadRunForkRevisionMatrixFacts(t, ctx, db, fixture.runID, 1)
	assertRunForkRevisionMatrixShape(t, initial, true)

	proveRunForkRevisionCorruptionFailsClosed(t, ctx, db, postgres, fixture)

	deleteTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin thirteen-family deletion: %v", err)
	}
	defer func() { _ = deleteTx.Rollback() }()
	deleteRunForkRevisionMatrixFacts(t, ctx, deleteTx, fixture)
	deleted, err := finalizeRunForkRevisionMatrix(ctx, deleteTx, postgres, effects)
	if err != nil {
		t.Fatalf("finalize thirteen-family deletion: %v", err)
	}
	if got := deleted[fixture.runID]; !got.Changed || got.Revision != 2 {
		t.Fatalf("thirteen-family deletion result = %#v, want changed revision 2", got)
	}
	if err := validateRunForkRevisionMatrix(ctx, deleteTx, postgres, fixture.runID); err != nil {
		t.Fatalf("validate thirteen-family deletion: %v", err)
	}
	if err := deleteTx.Commit(); err != nil {
		t.Fatalf("commit thirteen-family deletion: %v", err)
	}
	assertRunForkRevisionMatrixShape(t, loadRunForkRevisionMatrixFacts(t, ctx, db, fixture.runID, 2), false)

	rollbackRunID := "00000000-0000-0000-0000-000000002287"
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
		Origin: semanticScenarioSetupRunOriginForTest(), RunID: rollbackRunID, StartedAt: fixture.at,
	})
	rollbackTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin rollback proof: %v", err)
	}
	rollbackEventID := "00000000-0000-0000-0000-000000002288"
	seedRunForkRevisionMatrixEvent(t, ctx, rollbackTx, rollbackRunID, rollbackEventID, fixture.at, postgres)
	rollbackEffects, err := runforkrevision.ForRun(rollbackRunID, runforkrevision.FamilyEvents)
	if err != nil {
		t.Fatalf("declare rollback effects: %v", err)
	}
	if _, err := finalizeRunForkRevisionMatrix(ctx, rollbackTx, postgres, rollbackEffects); err != nil {
		t.Fatalf("finalize rollback proof: %v", err)
	}
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("rollback domain and revision facts: %v", err)
	}
	for _, table := range []string{"events", "run_fork_revision_heads", "run_fork_revisions", "run_fork_fact_revisions"} {
		var count int
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE run_id=$1", table), rollbackRunID).Scan(&count); err != nil {
			t.Fatalf("count rolled-back %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("rolled-back %s rows = %d, want 0", table, count)
		}
	}
	proveRunForkRevisionMultiRunFinalization(t, ctx, db, postgres, selected, fixture.at)
	return initial
}

func proveRunForkRevisionCorruptionFailsClosed(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, fixture runForkRevisionMatrixFixture) {
	t.Helper()
	for _, proof := range []struct {
		name   string
		mutate func(*sql.Tx) error
	}{
		{
			name: "unrevisioned_current_projection",
			mutate: func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE entity_state SET name='Unrevisioned Matrix Entity' WHERE run_id=$1`, fixture.runID)
				return err
			},
		},
		{
			name: "corrupt_latest_revision_fact",
			mutate: func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND revision=1 AND family='entity_metadata'`, `{"corrupt":true}`, fixture.runID)
				return err
			},
		},
	} {
		t.Run(proof.name, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin corruption transaction: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			if err := proof.mutate(tx); err != nil {
				t.Fatalf("apply corruption: %v", err)
			}
			err = validateRunForkRevisionMatrix(ctx, tx, postgres, fixture.runID)
			if err == nil || !strings.Contains(err.Error(), "unsupported unrevisioned entity_metadata facts") {
				t.Fatalf("corruption validation error = %v, want fail-closed entity_metadata mismatch", err)
			}
		})
	}
}

func proveRunForkRevisionMultiRunFinalization(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, selected any, at time.Time) {
	t.Helper()
	runIDs := []string{
		"00000000-0000-0000-0000-000000002290",
		"00000000-0000-0000-0000-000000002289",
	}
	for _, runID := range runIDs {
		requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
			Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: at,
		})
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin multi-run revision transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	effects := runforkrevision.NewEffects()
	for index, runID := range runIDs {
		seedRunForkRevisionMatrixEvent(t, ctx, tx, runID, fmt.Sprintf("00000000-0000-0000-0000-00000000229%d", index+1), at.Add(time.Duration(index)*time.Second), postgres)
		if err := effects.Add(runID, runforkrevision.FamilyEvents); err != nil {
			t.Fatalf("declare multi-run revision effect: %v", err)
		}
	}
	results, err := finalizeRunForkRevisionMatrix(ctx, tx, postgres, effects)
	if err != nil {
		t.Fatalf("finalize multi-run revision: %v", err)
	}
	for _, runID := range runIDs {
		if got := results[runID]; !got.Changed || got.Revision != 1 {
			t.Fatalf("multi-run result for %s = %#v, want changed revision 1", runID, got)
		}
		if err := validateRunForkRevisionMatrix(ctx, tx, postgres, runID); err != nil {
			t.Fatalf("validate multi-run revision for %s: %v", runID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit multi-run revision: %v", err)
	}
}

func finalizeRunForkRevisionMatrix(ctx context.Context, tx *sql.Tx, postgres bool, effects *runforkrevision.Effects) (map[string]runforkrevision.Result, error) {
	if postgres {
		return runforkrevision.FinalizePostgres(ctx, tx, effects)
	}
	return runforkrevision.FinalizeSQLite(ctx, tx, effects)
}

func validateRunForkRevisionMatrix(ctx context.Context, tx *sql.Tx, postgres bool, runID string) error {
	if postgres {
		return runforkrevision.ValidateCompletePostgres(ctx, tx, runID)
	}
	return runforkrevision.ValidateCompleteSQLite(ctx, tx, runID)
}

func seedRunForkRevisionMatrixFacts(t *testing.T, ctx context.Context, tx *sql.Tx, f runForkRevisionMatrixFixture, includeEventDelivery, postgres bool) {
	t.Helper()
	if includeEventDelivery {
		target := events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "matrix-flow", FlowInstance: "matrix-flow/one"})
		route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("matrix-node")), Target: target}
		routeIdentity, err := route.Identity()
		if err != nil {
			t.Fatalf("construct revision matrix delivery route: %v", err)
		}
		targetJSON, err := json.Marshal(target)
		if err != nil {
			t.Fatalf("encode revision matrix delivery target: %v", err)
		}
		seedRunForkRevisionMatrixEvent(t, ctx, tx, f.runID, f.eventID, f.at, postgres)
		producer, err := events.NewRootRoutingSource(f.entityID)
		if err != nil {
			t.Fatalf("construct revision matrix fan-out producer: %v", err)
		}
		capsuleJSON, err := json.Marshal(fanoutobligation.Capsule{
			NodeKey:         "root.matrix-node",
			ExecutionFlowID: "root",
			Route:           runtimeflowidentity.StoredRoute("root", "root", "root"),
			HandlerEventKey: "matrix.event",
			ProducerSource:  producer,
			Lineage: events.EventLineage{
				RunID:         f.runID,
				ParentEventID: f.eventID,
				ExecutionMode: executionmode.Live,
			},
		})
		if err != nil {
			t.Fatalf("encode revision matrix fan-out capsule: %v", err)
		}
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO event_deliveries (delivery_id,run_id,event_id,route_identity,subscriber_type,subscriber_id,agent_name_owner,agent_name_source,agent_route_presence,agent_flow_scope_key,agent_flow_instance_id,agent_flow_instance_path,delivery_target_route,delivery_context,delivery_payload_projection,connect_execution_claim,execution_authority_kind,authority_bundle_hash,execution_authority_id,execution_authority_generation,status,retry_count,max_retries,next_eligible_at,claim_version,created_at,updated_at) VALUES ($1,$2,$3,$4,'node',$5,'','','','','','',$6,$7,$7,$7,'normal_runtime',$8,'revision-matrix',1,'pending',0,3,$9,0,$9,$9)`, f.deliveryID, f.runID, f.eventID, events.EncodeDeliveryRouteIdentity(routeIdentity), route.Recipient.ID(), string(targetJSON), `{}`, "bundle-v2:sha256:"+strings.Repeat("1", 64), f.at)
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO fan_out_intents (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,source_kind,source_event_id,source_field,cardinality,cursor,status,next_chunk_size,capsule,created_at,updated_at) VALUES ($1,$2,'root','handler_rule','handlers["items.ready"].rules[0]',$3,'sha256:revision-matrix','event_payload_field',$4,'items',1,0,'open',4,$5,$6,$6)`, f.runID, f.deliveryID, "bundle-v2:sha256:"+strings.Repeat("1", 64), f.eventID, string(capsuleJSON), f.at)
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,slug,name,current_state,created_at,updated_at) VALUES ($1,$2,'matrix-flow','matrix-type','matrix-slug','Matrix Entity','ready',$3,$3)`, f.runID, f.entityID, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO entity_mutations (mutation_id,run_id,entity_id,domain,path,new_value,caused_by_event,writer_type,writer_id,created_at) VALUES ($1,$2,$3,'authored_field','name',$4,$5,'platform','revision-matrix',$6)`, f.mutationID, f.runID, f.entityID, `"Matrix Entity"`, f.eventID, f.at)
	if includeEventDelivery {
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO committed_replay_scopes (event_id,run_id,scope,created_at,updated_at) VALUES ($1,$2,'direct',$3,$3)`, f.eventID, f.runID, f.at)
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO event_receipts (receipt_id,event_id,subscriber_type,subscriber_id,outcome,side_effects,processed_at) VALUES ($1,$2,'platform','pipeline','success',$3,$4)`, f.receiptID, f.eventID, `{}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO dead_letters (dead_letter_id,original_event_id,original_event,original_payload,flow_instance,failure,created_at) VALUES ($1,$2,'matrix.event',$3,'matrix-flow',$4,$5)`, f.deadLetterID, f.eventID, `{}`, `{"class":"matrix"}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO timers (timer_id,timer_name,schedule_scope,schedule_key,immutable_hash,run_id,fire_event,fire_payload,routing_source,execution_mode,fire_at,initial_fire_at,recurring,owner_node,owner_kind,due_basis_kind,due_basis_absolute,task_type,status,created_at) VALUES ($1,'matrix-timer','run','matrix-key','matrix-hash',$2,'matrix.fire',$3,$4,'live',$5,$5,FALSE,'matrix-node','system','absolute',$5,'timer','active',$6)`, f.timerID, f.runID, `{}`, `{"kind":"root"}`, f.at.Add(time.Hour), f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO agent_sessions (session_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,memory_enabled,memory_source,conversation,turn_count,runtime_state,status,created_at,updated_at) VALUES ($1,$2,'revision-matrix-agent','store-test-fixture','runtime_created','root','','','',TRUE,'authored',$3,0,$4,'active',$5,$5)`, f.sessionID, f.runID, `[]`, `{}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO managed_agent_capability_surfaces (surface_id,integrity_hash,authority_kind,authority_id,execution_kind,execution_authority_id,run_id,actor_id,provider,transport,surface,created_at) VALUES ($1,'revision-matrix-surface','startup_probe',$2,'normal_agent','revision-matrix',$3,'revision-matrix-agent','mock','in_process',$4,$5)`, f.surfaceID, f.authorityID, f.runID, `{}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO runtime_external_effect_operations (operation_id,effect_kind,effect_class,execution_mode,bundle_hash,authority_kind,authority_id,generation,startup_authority_id,capability_plan_fingerprint,authority_evidence,lineage,request_fingerprint,state,created_at,updated_at,completed_at) VALUES ($1,'provider_turn','read_only','mock','matrix-bundle','startup_probe','revision-matrix',1,$2,'matrix-plan',$3,$3,'matrix-request','settled',$4,$4,$4)`, f.operationID, f.authorityID, `{}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO runtime_external_effect_attempts (attempt_id,operation_id,attempt_ordinal,adapter,transport,execution_mode,generation,execution_owner,lease_expires_at,fence_generation,usage_target_kind,usage_target_id,capability_surface_id,state,evidence,authorized_at,launched_at,response_observed_at,completed_at,updated_at) VALUES ($1,$2,1,'mock','in_process','mock',1,'revision-matrix',$3,1,'agent_turn',$4,$5,'settled',$6,$7,$7,$7,$7,$7)`, f.attemptID, f.operationID, f.at.Add(time.Hour), f.turnID, f.surfaceID, `{}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO agent_turns (turn_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,session_id,flow_instance,memory_enabled,memory_source,capability_surface_id,tool_calls,emitted_events,turn_blocks,parse_ok,latency_ms,retry_count,agent_frame_bytes,completion_attempt_id,execution_mode,created_at) VALUES ($1,$2,'revision-matrix-agent','store-test-fixture','runtime_created','root','','',$3,'',TRUE,'authored',$4,$5,$5,$5,TRUE,1,0,$6,$7,'mock',$8)`, f.turnID, f.runID, f.sessionID, f.surfaceID, `[]`, []byte("matrix-frame"), f.attemptID, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO agent_conversation_audits (session_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,memory_enabled,memory_source,conversation,turn_count,runtime_state,status,created_at,updated_at) VALUES ($1,$2,'revision-matrix-audit','store-test-fixture','runtime_created','root','','','',FALSE,'platform_default',$3,0,$4,'active',$5,$5)`, f.auditID, f.runID, `[]`, `{}`, f.at)
	if includeEventDelivery {
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO reply_contexts (reply_context_id,run_id,request_event_id,requester_flow_id,request_output_pin,reply_input_pin,provider_flow_id,provider_input_pin,provider_output_pin,origin_route,request_correlation_id,state,created_at,updated_at) VALUES ($1,$2,$3,'requester','out','reply','provider','in','out',$4,'matrix-correlation','open',$5,$5)`, f.replyID, f.runID, f.eventID, `{}`, f.at)
	}
}

func seedRunForkRevisionMatrixEvent(t *testing.T, ctx context.Context, tx *sql.Tx, runID, eventID string, at time.Time, postgres bool) {
	t.Helper()
	dialect := authoractivityfixture.DialectSQLite
	if postgres {
		dialect = authoractivityfixture.DialectPostgres
	}
	event := eventtest.ExistingRunRootIngress(
		eventID,
		events.EventType("matrix.event"),
		"revision-matrix",
		"",
		json.RawMessage(`{"matrix":true,"items":["matrix"]}`),
		0,
		runID,
		events.EventEnvelope{Scope: events.EventScopeGlobal},
		at,
	)
	if err := eventfixture.Insert(ctx, tx, dialect, event); err != nil {
		t.Fatalf("seed canonical run-fork revision event: %v", err)
	}
}

func deleteRunForkRevisionMatrixFacts(t *testing.T, ctx context.Context, tx *sql.Tx, f runForkRevisionMatrixFixture) {
	t.Helper()
	for _, statement := range []struct {
		query string
		arg   string
	}{
		{`DELETE FROM fan_out_outcomes WHERE run_id=$1`, f.runID},
		{`DELETE FROM fan_out_intents WHERE run_id=$1`, f.runID},
		{`DELETE FROM reply_contexts WHERE run_id=$1`, f.runID},
		{`DELETE FROM dead_letters WHERE original_event_id=$1`, f.eventID},
		{`DELETE FROM event_receipts WHERE event_id=$1`, f.eventID},
		{`DELETE FROM committed_replay_scopes WHERE run_id=$1`, f.runID},
		{`DELETE FROM event_deliveries WHERE run_id=$1`, f.runID},
		{`DELETE FROM timers WHERE run_id=$1`, f.runID},
		{`DELETE FROM agent_turns WHERE run_id=$1`, f.runID},
		{`DELETE FROM agent_conversation_audits WHERE run_id=$1`, f.runID},
		{`DELETE FROM agent_sessions WHERE run_id=$1`, f.runID},
		{`DELETE FROM entity_mutations WHERE run_id=$1`, f.runID},
		{`DELETE FROM entity_state WHERE run_id=$1`, f.runID},
		{`DELETE FROM events WHERE run_id=$1`, f.runID},
	} {
		mustExecRunForkRevisionMatrix(t, ctx, tx, statement.query, statement.arg)
	}
}

func mustExecRunForkRevisionMatrix(t *testing.T, ctx context.Context, tx *sql.Tx, query string, args ...any) {
	t.Helper()
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("execute revision matrix statement %q: %v", strings.Join(strings.Fields(query), " "), err)
	}
}

func loadRunForkRevisionMatrixFacts(t *testing.T, ctx context.Context, db *sql.DB, runID string, revision int64) []runForkRevisionMatrixFact {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT family,fact_key,fact,present FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2 ORDER BY family,fact_key`, runID, revision)
	if err != nil {
		t.Fatalf("load revision matrix facts: %v", err)
	}
	defer rows.Close()
	var facts []runForkRevisionMatrixFact
	for rows.Next() {
		var family runforkrevision.Family
		var key string
		var encoded []byte
		var present bool
		if err := rows.Scan(&family, &key, &encoded, &present); err != nil {
			t.Fatalf("scan revision matrix fact: %v", err)
		}
		var fact any
		if err := json.Unmarshal(encoded, &fact); err != nil {
			t.Fatalf("decode %s/%s revision matrix fact: %v", family, key, err)
		}
		facts = append(facts, runForkRevisionMatrixFact{Family: family, Key: key, Fact: fact, Present: present})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read revision matrix facts: %v", err)
	}
	return facts
}

func assertRunForkRevisionMatrixShape(t *testing.T, facts []runForkRevisionMatrixFact, present bool) {
	t.Helper()
	if len(facts) != len(runforkrevision.AllFamilies()) {
		t.Fatalf("revision matrix fact count = %d, want %d: %#v", len(facts), len(runforkrevision.AllFamilies()), facts)
	}
	got := make([]runforkrevision.Family, 0, len(facts))
	for _, fact := range facts {
		if fact.Present != present {
			t.Fatalf("%s/%s present = %v, want %v", fact.Family, fact.Key, fact.Present, present)
		}
		got = append(got, fact.Family)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	want := runforkrevision.AllFamilies()
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("revision matrix families = %q, want exact closed registry %q", got, want)
	}
}
