package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type standingDispositionParityFixture struct {
	backend  string
	db       *sql.DB
	selected workflowTestSelectedStore
	workflow *runtimepipeline.PipelineCoordinator
	hash     string
}

func TestStandingRestartDispositionSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			for _, runID := range []string{"", "not-a-run-id"} {
				if _, err := fixture.workflow.StandingRunRestartDisposition(ctx, runID); err == nil || !strings.Contains(err.Error(), "standing restart run_id") {
					t.Fatalf("invalid %s run identity %q error = %v", backend, runID, err)
				}
			}
			assertStandingDisposition(t, ctx, fixture, uuid.NewString(), runtimepipeline.StandingRestartOrdinary)

			active := fixture.create(t, ctx, "active")
			assertStandingDisposition(t, ctx, fixture, active.RunID, runtimepipeline.StandingRestartActiveIntrinsic)

			suspended := fixture.create(t, ctx, "suspended")
			if _, err := fixture.workflow.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: suspended.ServiceID, Actor: "test"}); err != nil {
				t.Fatalf("suspend standing service: %v", err)
			}
			assertStandingDisposition(t, ctx, fixture, suspended.RunID, runtimepipeline.StandingRestartSuspended)

			orphaned := fixture.create(t, ctx, "orphaned")
			if _, err := fixture.workflow.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: orphaned.ServiceID, Actor: "test"}); err != nil {
				t.Fatalf("suspend orphan candidate: %v", err)
			}
			fixture.setDesiredState(t, orphaned.ServiceID, false, "orphaned", "suspended")
			assertStandingDisposition(t, ctx, fixture, orphaned.RunID, runtimepipeline.StandingRestartOrphaned)

			for _, terminal := range []runtimerunlifecycle.State{
				runtimerunlifecycle.StateCompleted,
				runtimerunlifecycle.StateFailed,
				runtimerunlifecycle.StateCancelled,
				runtimerunlifecycle.StateForked,
			} {
				declared := fixture.create(t, ctx, "terminal-declared-"+string(terminal))
				fixture.terminalize(t, ctx, declared.RunID, terminal)
				assertStandingDisposition(t, ctx, fixture, declared.RunID, runtimepipeline.StandingRestartTerminalDeclared)

				removed := fixture.create(t, ctx, "terminal-orphaned-"+string(terminal))
				fixture.terminalize(t, ctx, removed.RunID, terminal)
				fixture.setDesiredState(t, removed.ServiceID, false, "orphaned", "none")
				assertStandingDisposition(t, ctx, fixture, removed.RunID, runtimepipeline.StandingRestartTerminalOrphaned)
			}

			invalid := fixture.create(t, ctx, "invalid")
			fixture.setDesiredState(t, invalid.ServiceID, true, "suspended", "suspended")
			assertStandingDisposition(t, ctx, fixture, invalid.RunID, runtimepipeline.StandingRestartInvalidCurrent)

			invalidOrphan := fixture.create(t, ctx, "invalid-orphan")
			fixture.setDesiredState(t, invalidOrphan.ServiceID, false, "orphaned", "none")
			disposition := assertStandingDisposition(t, ctx, fixture, invalidOrphan.RunID, runtimepipeline.StandingRestartInvalidCurrent)
			if disposition.Remediation != runtimepipeline.StandingRestartRestoreThenReset {
				t.Fatalf("invalid orphan remediation = %s, want restore_then_reset", disposition.Remediation)
			}

			predecessor := fixture.create(t, ctx, "historical")
			reset, err := fixture.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: predecessor.ServiceID, Actor: "test"})
			if err != nil {
				t.Fatalf("reset standing service: %v", err)
			}
			assertStandingDisposition(t, ctx, fixture, predecessor.RunID, runtimepipeline.StandingRestartOrdinary)
			assertStandingDisposition(t, ctx, fixture, reset.RunID, runtimepipeline.StandingRestartActiveIntrinsic)
		})
	}
}

func TestStandingReconciliationNormalizesRunPauseForActiveDeclarationParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			candidate := fixture.candidate("operator-paused")
			created, err := fixture.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatalf("create standing service: %v", err)
			}
			controller, ok := fixture.selected.(interface {
				PauseRunControl(context.Context, runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error)
			})
			if !ok {
				t.Fatalf("selected store %T does not own public run pause", fixture.selected)
			}
			if _, err := controller.PauseRunControl(ctx, runtimeruncontrol.TransitionRequest{
				RunID: created.RunID, Reason: "operator_pause_before_restart", ControlledBy: "operator", Now: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("pause standing run: %v", err)
			}

			reconciled, err := fixture.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatalf("reconcile operator-paused standing service: %v", err)
			}
			if reconciled.RunID != created.RunID || reconciled.Generation != created.Generation ||
				reconciled.RestartDisposition.Kind != runtimepipeline.StandingRestartActiveIntrinsic ||
				reconciled.RestartDisposition.RunState != string(runtimerunlifecycle.StateRunning) {
				t.Fatalf("operator-paused reconciliation = %#v", reconciled)
			}
			query := `SELECT status, reason, controlled_by FROM runs JOIN run_control_state USING (run_id) WHERE run_id=?`
			args := []any{created.RunID}
			if backend == "postgres" {
				query = `SELECT status, reason, controlled_by FROM runs JOIN run_control_state USING (run_id) WHERE run_id=$1::uuid`
			}
			var status, reason, controlledBy string
			if err := fixture.db.QueryRowContext(ctx, query, args...).Scan(&status, &reason, &controlledBy); err != nil {
				t.Fatalf("read operator-paused standing state: %v", err)
			}
			if status != string(runtimerunlifecycle.StateRunning) || reason != "standing_reconcile" || controlledBy != "runtime" {
				t.Fatalf("active standing reconciliation did not normalize run pause: status=%s reason=%s controlled_by=%s", status, reason, controlledBy)
			}
		})
	}
}

func TestTerminalStandingMembersDoNotAbortCompleteSetParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			terminalCandidate := fixture.candidate("terminal-member")
			healthyCandidate := fixture.candidate("healthy-member")
			terminal, err := fixture.workflow.ReconcileStandingService(ctx, terminalCandidate)
			if err != nil {
				t.Fatalf("create terminal member: %v", err)
			}
			fixture.terminalize(t, ctx, terminal.RunID, runtimerunlifecycle.StateCancelled)

			results, err := fixture.workflow.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{terminalCandidate, healthyCandidate})
			if err != nil || len(results) != 2 {
				t.Fatalf("reconcile terminal plus healthy set = %#v err=%v", results, err)
			}
			byService := standingResultsByService(results)
			if got := byService[terminalCandidate.ServiceID]; got.RunID != terminal.RunID || got.Generation != terminal.Generation || got.RestartDisposition.Kind != runtimepipeline.StandingRestartTerminalDeclared {
				t.Fatalf("terminal member result = %#v", got)
			}
			if healthy := byService[healthyCandidate.ServiceID]; !healthy.RestartDisposition.Executable() {
				t.Fatalf("healthy sibling result = %#v", healthy)
			}

			renamedCandidate := fixture.candidate("renamed-new")
			results, err = fixture.workflow.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{healthyCandidate, renamedCandidate})
			if err != nil || len(results) != 3 {
				t.Fatalf("remove terminal and create renamed sibling = %#v err=%v", results, err)
			}
			byService = standingResultsByService(results)
			removed := byService[terminalCandidate.ServiceID]
			if removed.RunID != terminal.RunID || removed.Generation != terminal.Generation || removed.RestartDisposition.Kind != runtimepipeline.StandingRestartTerminalOrphaned {
				t.Fatalf("terminal removed member = %#v", removed)
			}
			if !byService[healthyCandidate.ServiceID].RestartDisposition.Executable() || !byService[renamedCandidate.ServiceID].RestartDisposition.Executable() {
				t.Fatalf("healthy/new siblings were not executable: %#v", results)
			}

			results, err = fixture.workflow.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{terminalCandidate, healthyCandidate, renamedCandidate})
			if err != nil || len(results) != 3 {
				t.Fatalf("restore terminal declaration = %#v err=%v", results, err)
			}
			restored := standingResultsByService(results)[terminalCandidate.ServiceID]
			if restored.RunID != terminal.RunID || restored.Generation != terminal.Generation || restored.EffectiveState != "active" ||
				restored.RestartDisposition.Kind != runtimepipeline.StandingRestartTerminalDeclared {
				t.Fatalf("restored terminal member = %#v", restored)
			}
			reset, err := fixture.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: terminalCandidate.ServiceID, Actor: "test"})
			if err != nil {
				t.Fatalf("reset restored terminal member: %v", err)
			}
			if reset.Generation != terminal.Generation+1 || reset.RunID == terminal.RunID || !reset.RestartDisposition.Executable() {
				t.Fatalf("explicit terminal reset = %#v", reset)
			}
		})
	}
}

func TestInvalidStandingMemberDoesNotAbortOrMutateCompleteSetParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			invalidCandidate := fixture.candidate("invalid-member")
			healthyCandidate := fixture.candidate("invalid-member-healthy-sibling")
			invalid, err := fixture.workflow.ReconcileStandingService(ctx, invalidCandidate)
			if err != nil {
				t.Fatalf("create invalid member: %v", err)
			}
			fixture.setDesiredState(t, invalid.ServiceID, true, "suspended", "suspended")

			results, err := fixture.workflow.ReconcileStandingServiceSet(ctx, []runtimepipeline.StandingServiceCandidate{healthyCandidate})
			if err != nil || len(results) != 2 {
				t.Fatalf("reconcile invalid plus healthy set = %#v err=%v", results, err)
			}
			byService := standingResultsByService(results)
			quarantined := byService[invalid.ServiceID]
			if quarantined.RunID != invalid.RunID || quarantined.Generation != invalid.Generation || quarantined.RestartDisposition.Kind != runtimepipeline.StandingRestartInvalidCurrent {
				t.Fatalf("invalid member result = %#v", quarantined)
			}
			if !byService[healthyCandidate.ServiceID].RestartDisposition.Executable() {
				t.Fatalf("healthy sibling was not executable: %#v", results)
			}
			disposition := assertStandingDisposition(t, ctx, fixture, invalid.RunID, runtimepipeline.StandingRestartInvalidCurrent)
			if !disposition.DeclarationPresent || disposition.EffectiveState != "suspended" || disposition.OperatorOverride != "suspended" || disposition.RunState != "running" {
				t.Fatalf("invalid member was mutated during complete-set reconciliation: %#v", disposition)
			}
		})
	}
}

func TestStandingRestartDispositionRejectsBrokenGenerationRelationParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			created := fixture.create(t, ctx, "broken-generation-relation")
			query := `UPDATE standing_service_generations SET retired_at=? WHERE service_id=? AND generation=?`
			args := []any{time.Now().UTC(), created.ServiceID, created.Generation}
			if backend == "postgres" {
				query = `UPDATE standing_service_generations SET retired_at=$1 WHERE service_id=$2::uuid AND generation=$3`
			}
			if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
				t.Fatalf("break %s standing generation relation: %v", backend, err)
			}
			if _, err := fixture.workflow.StandingRunRestartDisposition(ctx, created.RunID); err == nil || !strings.Contains(err.Error(), "exact active generation relations") {
				t.Fatalf("broken %s standing generation relation error = %v", backend, err)
			}
		})
	}
}

func TestStandingRestartDispositionRejectsBrokenServiceIdentityParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			created := fixture.create(t, ctx, "broken-service-identity")
			query := `UPDATE standing_services SET package_key=? WHERE service_id=?`
			args := []any{"corrupt-package", created.ServiceID}
			if backend == "postgres" {
				query = `UPDATE standing_services SET package_key=$1 WHERE service_id=$2::uuid`
			}
			if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
				t.Fatalf("break %s standing service identity: %v", backend, err)
			}
			if _, err := fixture.workflow.StandingRunRestartDisposition(ctx, created.RunID); err == nil || !strings.Contains(err.Error(), "does not match package/flow owner") {
				t.Fatalf("broken %s standing service identity error = %v", backend, err)
			}
		})
	}
}

func standingResultsByService(results []runtimepipeline.StandingServiceReconciliation) map[string]runtimepipeline.StandingServiceReconciliation {
	byService := make(map[string]runtimepipeline.StandingServiceReconciliation, len(results))
	for _, result := range results {
		byService[result.ServiceID] = result
	}
	return byService
}

func openStandingDispositionParityFixture(t *testing.T, backend string) standingDispositionParityFixture {
	t.Helper()
	fixture := standingDispositionParityFixture{backend: backend, hash: "bundle-v1:sha256:" + strings.Repeat("9", 64)}
	if backend == "sqlite" {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		fixture.db, fixture.selected = store.backend.ConstructionHandle(), store
		fixture.workflow = newSQLiteWorkflowTestCoordinator(t, fixture.db, store)
	} else {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		store := admitTestPostgresStore(t, db)
		fixture.db, fixture.selected = db, store
		fixture.workflow = newPostgresWorkflowTestCoordinator(t, db, store)
	}
	seedStoreTestPersistedBundle(t, fixture.db, fixture.hash)
	return fixture
}

func (f standingDispositionParityFixture) create(t *testing.T, ctx context.Context, name string) runtimepipeline.StandingServiceReconciliation {
	t.Helper()
	candidate := f.candidate(name)
	created, err := f.workflow.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatalf("create %s standing service: %v", name, err)
	}
	return created
}

func (f standingDispositionParityFixture) candidate(name string) runtimepipeline.StandingServiceCandidate {
	return runtimepipeline.StandingServiceCandidate{
		ServiceID:  runtimeflowidentity.StandingServiceID("restart-disposition", f.backend+"-"+name),
		PackageKey: "restart-disposition", FlowID: f.backend + "-" + name,
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestPersistedBundleSourceFact(f.hash),
	}
}

func (f standingDispositionParityFixture) setDesiredState(t *testing.T, serviceID string, declared bool, effective, override string) {
	t.Helper()
	actor, at := any(nil), any(nil)
	if override == "suspended" {
		actor, at = "test", time.Now().UTC()
	}
	query := `UPDATE standing_services SET declaration_present=?, effective_state=?, operator_override=?, override_actor=?, override_reason=NULL, override_at=? WHERE service_id=?`
	args := []any{declared, effective, override, actor, at, serviceID}
	if f.backend == "postgres" {
		query = `UPDATE standing_services SET declaration_present=$1, effective_state=$2, operator_override=$3, override_actor=$4, override_reason=NULL, override_at=$5 WHERE service_id=$6::uuid`
	}
	if _, err := f.db.Exec(query, args...); err != nil {
		t.Fatalf("set standing desired state %t/%s/%s: %v", declared, effective, override, err)
	}
}

func (f standingDispositionParityFixture) terminalize(t *testing.T, ctx context.Context, runID string, state runtimerunlifecycle.State) {
	t.Helper()
	if state == runtimerunlifecycle.StateForked {
		childRunID := uuid.NewString()
		if err := ensureEphemeralRunForTest(ctx, f.selected, childRunID, time.Now().UTC()); err != nil {
			t.Fatalf("create standing fork continuation %s: %v", childRunID, err)
		}
		if _, _, err := forkRunForTest(ctx, f.selected, runtimerunlifecycle.ForkSourceRequest{
			RunID: runID, ContinuedAsRunID: childRunID, EndedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("fork standing run %s: %v", runID, err)
		}
		return
	}
	if state == runtimerunlifecycle.StateCompleted {
		owner, ok := f.selected.(runLifecycleTerminalTestStore)
		if !ok {
			t.Fatalf("selected store %T does not own run completion", f.selected)
		}
		if err := materializeCompletedRunEntityForTest(ctx, f.selected, runID); err != nil {
			t.Fatalf("materialize completed standing entity for %s: %v", runID, err)
		}
		if disposition, err := owner.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil {
			t.Fatalf("request standing completion candidate for %s: %v", runID, err)
		} else if disposition != runtimerunlifecycle.CandidateRequested {
			t.Fatalf("standing completion candidate disposition for %s = %s, want requested", runID, disposition)
		}
		result, err := executeRunCompletionCandidateForRun(
			ctx,
			owner,
			f.hash,
			runID,
			runtimerunlifecycle.NewTerminalCatalog(nil, map[string][]string{semanticRunFixtureFlow: {"completed"}}),
		)
		if err != nil {
			t.Fatalf("complete standing run %s: %v", runID, err)
		}
		if result.Outcome != runtimerunlifecycle.OutcomeTerminallyEligible && result.Outcome != runtimerunlifecycle.OutcomeExactNoop {
			t.Fatalf("standing completion outcome for %s = %s, want terminally eligible", runID, result.Outcome)
		}
		return
	}
	var failure *runtimefailures.Envelope
	if state == runtimerunlifecycle.StateFailed {
		envelope, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(runtimefailures.ClassInternalFailure, "standing_disposition_test_failure", "standing-test", "terminalize", nil))
		if !ok {
			t.Fatal("construct standing disposition test failure")
		}
		failure = &envelope
	}
	if _, err := markRunTerminalStatusForTest(ctx, f.selected, runID, string(state), failure, time.Now().UTC()); err != nil {
		t.Fatalf("terminalize standing run %s as %s: %v", runID, state, err)
	}
}

func assertStandingDisposition(t *testing.T, ctx context.Context, fixture standingDispositionParityFixture, runID string, want runtimepipeline.StandingRestartDispositionKind) runtimepipeline.StandingRestartDisposition {
	t.Helper()
	disposition, err := fixture.workflow.StandingRunRestartDisposition(ctx, runID)
	if err != nil {
		t.Fatalf("read %s standing disposition for %s: %v", fixture.backend, runID, err)
	}
	if disposition.Kind != want {
		t.Fatalf("%s standing disposition for %s = %s, want %s (%s)", fixture.backend, runID, disposition.Kind, want, fmt.Sprint(disposition))
	}
	return disposition
}
