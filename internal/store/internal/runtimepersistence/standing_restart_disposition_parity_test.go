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
			query := `UPDATE standing_services SET flow_path=? WHERE service_id=?`
			args := []any{"corrupt-flow", created.ServiceID}
			if backend == "postgres" {
				query = `UPDATE standing_services SET flow_path=$1 WHERE service_id=$2::uuid`
			}
			if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
				t.Fatalf("break %s standing service identity: %v", backend, err)
			}
			if _, err := fixture.workflow.StandingRunRestartDisposition(ctx, created.RunID); err == nil || !strings.Contains(err.Error(), "does not match flow_path owner") {
				t.Fatalf("broken %s standing service identity error = %v", backend, err)
			}
		})
	}
}

func TestTerminalDeclaredStandingResetUsesLatestDeclarationSourceParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			candidate := fixture.candidate("terminal-declared-source-revision")
			created, err := fixture.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatalf("create standing service: %v", err)
			}
			fixture.terminalize(t, ctx, created.RunID, runtimerunlifecycle.StateCancelled)

			revised := fixture.reviseCandidateSource(t, candidate, "a")
			if _, found, err := fixture.workflow.LoadReconciledStandingService(ctx, revised); err != nil || found {
				t.Fatalf("load stale terminal declaration: found=%t err=%v", found, err)
			}
			reconciled, err := fixture.workflow.ReconcileStandingService(ctx, revised)
			if err != nil {
				t.Fatalf("reconcile terminal source revision: %v", err)
			}
			if reconciled.RunID != created.RunID || reconciled.Generation != created.Generation || reconciled.BundleHash != revised.Source.BundleHash() {
				t.Fatalf("terminal source reconciliation = %#v", reconciled)
			}
			fixture.assertRunSource(t, ctx, created.RunID, candidate.Source.BundleHash())
			beforeRevision, beforeJournal := fixture.standingRevisionAndJournalCount(t, ctx, created.ServiceID)
			if loaded, found, err := fixture.workflow.LoadReconciledStandingService(ctx, revised); err != nil || !found || loaded.BundleHash != revised.Source.BundleHash() {
				t.Fatalf("load reconciled terminal declaration = %#v found=%t err=%v", loaded, found, err)
			}
			if _, err := fixture.workflow.ReconcileStandingService(ctx, revised); err != nil {
				t.Fatalf("repeat unchanged terminal reconciliation: %v", err)
			}
			afterRevision, afterJournal := fixture.standingRevisionAndJournalCount(t, ctx, created.ServiceID)
			if afterRevision != beforeRevision || afterJournal != beforeJournal {
				t.Fatalf("unchanged terminal source churned revision/journal: before=%d/%d after=%d/%d", beforeRevision, beforeJournal, afterRevision, afterJournal)
			}

			reset, err := fixture.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "test"})
			if err != nil {
				t.Fatalf("reset revised terminal service: %v", err)
			}
			fixture.assertRunSource(t, ctx, reset.RunID, revised.Source.BundleHash())
		})
	}
}

func TestTerminalOrphanStandingRestoreAndResetUsesLatestDeclarationSourceParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			candidate := fixture.candidate("terminal-orphan-source-revision")
			created, err := fixture.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatalf("create standing service: %v", err)
			}
			fixture.terminalize(t, ctx, created.RunID, runtimerunlifecycle.StateCancelled)
			fixture.setDesiredState(t, created.ServiceID, false, "orphaned", "none")
			revised := fixture.reviseCandidateSource(t, candidate, "b")

			restored, err := fixture.workflow.ReconcileStandingService(ctx, revised)
			if err != nil {
				t.Fatalf("restore terminal orphan under revised source: %v", err)
			}
			if restored.RestartDisposition.Kind != runtimepipeline.StandingRestartTerminalDeclared || restored.BundleHash != revised.Source.BundleHash() {
				t.Fatalf("restored terminal source = %#v", restored)
			}
			fixture.assertRunSource(t, ctx, created.RunID, candidate.Source.BundleHash())
			reset, err := fixture.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "test"})
			if err != nil {
				t.Fatalf("reset restored terminal service: %v", err)
			}
			fixture.assertRunSource(t, ctx, reset.RunID, revised.Source.BundleHash())
		})
	}
}

func TestInvalidStandingResetUsesLatestDeclarationSourceParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			candidate := fixture.candidate("invalid-declared-source-revision")
			created, err := fixture.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatalf("create standing service: %v", err)
			}
			fixture.setDesiredState(t, created.ServiceID, true, "suspended", "suspended")
			revised := fixture.reviseCandidateSource(t, candidate, "c")

			quarantined, err := fixture.workflow.ReconcileStandingService(ctx, revised)
			if err != nil {
				t.Fatalf("reconcile invalid service source revision: %v", err)
			}
			if quarantined.RestartDisposition.Kind != runtimepipeline.StandingRestartInvalidCurrent || quarantined.BundleHash != revised.Source.BundleHash() {
				t.Fatalf("invalid source reconciliation = %#v", quarantined)
			}
			reset, err := fixture.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "test"})
			if err != nil {
				t.Fatalf("reset invalid service: %v", err)
			}
			if reset.RestartDisposition.Kind != runtimepipeline.StandingRestartSuspended {
				t.Fatalf("invalid suspended reset disposition = %#v", reset.RestartDisposition)
			}
			fixture.assertRunSource(t, ctx, reset.RunID, revised.Source.BundleHash())
		})
	}
}

func TestInvalidOrphanStandingRestoreThenResetParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			candidate := fixture.candidate("invalid-orphan-restore-reset")
			created, err := fixture.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatalf("create standing service: %v", err)
			}
			fixture.setDesiredState(t, created.ServiceID, false, "orphaned", "none")
			assertStandingDisposition(t, ctx, fixture, created.RunID, runtimepipeline.StandingRestartInvalidCurrent)
			revised := fixture.reviseCandidateSource(t, candidate, "d")

			restored, err := fixture.workflow.ReconcileStandingService(ctx, revised)
			if err != nil {
				t.Fatalf("restore invalid orphan declaration: %v", err)
			}
			if !restored.RestartDisposition.Executable() || restored.BundleHash != revised.Source.BundleHash() {
				t.Fatalf("restored invalid orphan = %#v", restored)
			}
			if loaded, found, err := fixture.workflow.LoadReconciledStandingService(ctx, revised); err != nil || !found || !loaded.RestartDisposition.Executable() {
				t.Fatalf("load restored invalid orphan = %#v found=%t err=%v", loaded, found, err)
			}
			reset, err := fixture.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "test"})
			if err != nil {
				t.Fatalf("reset restored invalid orphan: %v", err)
			}
			fixture.assertRunSource(t, ctx, reset.RunID, revised.Source.BundleHash())
		})
	}
}

func TestInvalidOrphanStandingSameSourceRestoreThenResetParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		for _, override := range []string{"none", "suspended"} {
			override := override
			t.Run(backend+"/"+override, func(t *testing.T) {
				fixture := openStandingDispositionParityFixture(t, backend)
				ctx := testAuthorActivityRuntimeContext()
				candidate := fixture.candidate("invalid-orphan-same-source-" + override)
				created, err := fixture.workflow.ReconcileStandingService(ctx, candidate)
				if err != nil {
					t.Fatalf("create standing service: %v", err)
				}
				fixture.setDesiredState(t, created.ServiceID, false, "orphaned", override)
				assertStandingDisposition(t, ctx, fixture, created.RunID, runtimepipeline.StandingRestartInvalidCurrent)

				restored, err := fixture.workflow.ReconcileStandingService(ctx, candidate)
				if err != nil {
					t.Fatalf("restore same-source invalid orphan declaration: %v", err)
				}
				wantRestored := runtimepipeline.StandingRestartActiveIntrinsic
				wantReset := runtimepipeline.StandingRestartActiveIntrinsic
				if override == "suspended" {
					wantRestored = runtimepipeline.StandingRestartInvalidCurrent
					wantReset = runtimepipeline.StandingRestartSuspended
				}
				if restored.RestartDisposition.Kind != wantRestored || !restored.RestartDisposition.DeclarationPresent || restored.BundleHash != candidate.Source.BundleHash() {
					t.Fatalf("restored same-source invalid orphan = %#v, want disposition %s", restored, wantRestored)
				}
				if loaded, found, err := fixture.workflow.LoadReconciledStandingService(ctx, candidate); err != nil || !found || loaded.RestartDisposition.Kind != wantRestored {
					t.Fatalf("load restored same-source invalid orphan = %#v found=%t err=%v", loaded, found, err)
				}

				reset, err := fixture.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "test"})
				if err != nil {
					t.Fatalf("reset restored same-source invalid orphan: %v", err)
				}
				if reset.RestartDisposition.Kind != wantReset {
					t.Fatalf("same-source invalid orphan reset = %#v, want disposition %s", reset, wantReset)
				}
				fixture.assertRunSource(t, ctx, reset.RunID, candidate.Source.BundleHash())
			})
		}
	}
}

func TestSuspendedStandingResetInstallsSuccessorBeforePauseParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openStandingDispositionParityFixture(t, backend)
			ctx := testAuthorActivityRuntimeContext()
			created := fixture.create(t, ctx, "direct-suspended-reset")
			if _, err := fixture.workflow.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "test"}); err != nil {
				t.Fatalf("suspend standing service: %v", err)
			}
			nextRunID := runtimeflowidentity.StandingGenerationRunID(created.ServiceID, created.Generation+1)
			fixture.installResetPauseFailure(t, ctx, nextRunID)
			if _, err := fixture.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "test"}); err == nil || !strings.Contains(err.Error(), "injected standing reset pause failure") {
				t.Fatalf("injected suspended reset error = %v", err)
			}
			fixture.assertResetRolledBack(t, ctx, created, nextRunID)
			fixture.removeResetPauseFailure(t, ctx)

			reset, err := fixture.workflow.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: created.ServiceID, Actor: "test"})
			if err != nil {
				t.Fatalf("direct suspended reset: %v", err)
			}
			if reset.RunID != nextRunID || reset.Generation != created.Generation+1 || reset.EffectiveState != "suspended" || reset.RestartDisposition.Kind != runtimepipeline.StandingRestartSuspended {
				t.Fatalf("direct suspended reset = %#v", reset)
			}
			fixture.assertGenerationOwner(t, ctx, created.ServiceID, created.Generation, reset.Generation)
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
	artifact := storeTestSourceArtifact("standing-restart-disposition-" + backend)
	fixture := standingDispositionParityFixture{backend: backend, hash: artifact.BundleHash()}
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
	seedStoreTestPersistedArtifact(t, fixture.db, artifact)
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
	flowPath := "restart-disposition/" + f.backend + "-" + name
	return runtimepipeline.StandingServiceCandidate{
		ServiceID: runtimeflowidentity.StandingServiceID(flowPath), FlowPath: flowPath,
		InstanceID: uuid.NewString(), EntityID: uuid.NewString(),
		Source: mustStoreTestSourceArtifactFact(f.hash),
	}
}

func (f standingDispositionParityFixture) reviseCandidateSource(t *testing.T, candidate runtimepipeline.StandingServiceCandidate, digit string) runtimepipeline.StandingServiceCandidate {
	t.Helper()
	artifact := storeTestSourceArtifact("standing-restart-revision-" + f.backend + "-" + digit)
	seedStoreTestPersistedArtifact(t, f.db, artifact)
	candidate.Source = mustStoreTestSourceArtifactFact(artifact.BundleHash())
	return candidate
}

func (f standingDispositionParityFixture) assertRunSource(t *testing.T, ctx context.Context, runID, wantHash string) {
	t.Helper()
	query := `SELECT bundle_hash FROM runs WHERE run_id=?`
	args := []any{runID}
	if f.backend == "postgres" {
		query = `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`
	}
	var hash string
	if err := f.db.QueryRowContext(ctx, query, args...).Scan(&hash); err != nil {
		t.Fatalf("read %s run source for %s: %v", f.backend, runID, err)
	}
	if hash != wantHash {
		t.Fatalf("%s run source for %s = %s, want %s", f.backend, runID, hash, wantHash)
	}
}

func (f standingDispositionParityFixture) standingRevisionAndJournalCount(t *testing.T, ctx context.Context, serviceID string) (int64, int64) {
	t.Helper()
	query := `SELECT revision_sequence, (SELECT COUNT(*) FROM standing_service_journal WHERE service_id=?) FROM standing_services WHERE service_id=?`
	args := []any{serviceID, serviceID}
	if f.backend == "postgres" {
		query = `SELECT revision_sequence, (SELECT COUNT(*) FROM standing_service_journal WHERE service_id=$1::uuid) FROM standing_services WHERE service_id=$1::uuid`
		args = []any{serviceID}
	}
	var revision, journal int64
	if err := f.db.QueryRowContext(ctx, query, args...).Scan(&revision, &journal); err != nil {
		t.Fatalf("read %s standing revision/journal: %v", f.backend, err)
	}
	return revision, journal
}

func (f standingDispositionParityFixture) installResetPauseFailure(t *testing.T, ctx context.Context, runID string) {
	t.Helper()
	if f.backend == "sqlite" {
		_, err := f.db.ExecContext(ctx, fmt.Sprintf(`CREATE TRIGGER fail_standing_reset_pause BEFORE INSERT ON run_control_state WHEN NEW.run_id = '%s' BEGIN SELECT RAISE(ABORT, 'injected standing reset pause failure'); END`, runID))
		if err != nil {
			t.Fatalf("install sqlite reset pause failure: %v", err)
		}
		return
	}
	if _, err := f.db.ExecContext(ctx, `CREATE FUNCTION fail_standing_reset_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected standing reset pause failure'; END $$`); err != nil {
		t.Fatalf("install postgres reset pause failure function: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, fmt.Sprintf(`CREATE TRIGGER fail_standing_reset_pause BEFORE INSERT ON run_control_state FOR EACH ROW WHEN (NEW.run_id = '%s'::uuid) EXECUTE FUNCTION fail_standing_reset_pause()`, runID)); err != nil {
		t.Fatalf("install postgres reset pause failure trigger: %v", err)
	}
}

func (f standingDispositionParityFixture) removeResetPauseFailure(t *testing.T, ctx context.Context) {
	t.Helper()
	if f.backend == "sqlite" {
		if _, err := f.db.ExecContext(ctx, `DROP TRIGGER fail_standing_reset_pause`); err != nil {
			t.Fatalf("drop sqlite reset pause failure: %v", err)
		}
		return
	}
	if _, err := f.db.ExecContext(ctx, `DROP TRIGGER fail_standing_reset_pause ON run_control_state`); err != nil {
		t.Fatalf("drop postgres reset pause failure trigger: %v", err)
	}
	if _, err := f.db.ExecContext(ctx, `DROP FUNCTION fail_standing_reset_pause()`); err != nil {
		t.Fatalf("drop postgres reset pause failure function: %v", err)
	}
}

func (f standingDispositionParityFixture) assertResetRolledBack(t *testing.T, ctx context.Context, current runtimepipeline.StandingServiceReconciliation, nextRunID string) {
	t.Helper()
	query := `SELECT current_generation, current_run_id FROM standing_services WHERE service_id=?`
	args := []any{current.ServiceID}
	if f.backend == "postgres" {
		query = `SELECT current_generation, current_run_id::text FROM standing_services WHERE service_id=$1::uuid`
	}
	var generation int64
	var runID string
	if err := f.db.QueryRowContext(ctx, query, args...).Scan(&generation, &runID); err != nil {
		t.Fatalf("read reset rollback standing owner: %v", err)
	}
	if generation != current.Generation || runID != current.RunID {
		t.Fatalf("reset rollback current owner = generation:%d run:%s, want %d/%s", generation, runID, current.Generation, current.RunID)
	}
	query = `SELECT COUNT(*) FROM runs WHERE run_id=?`
	if f.backend == "postgres" {
		query = `SELECT COUNT(*) FROM runs WHERE run_id=$1::uuid`
	}
	var count int64
	if err := f.db.QueryRowContext(ctx, query, nextRunID).Scan(&count); err != nil {
		t.Fatalf("read reset rollback successor: %v", err)
	}
	if count != 0 {
		t.Fatalf("reset rollback retained %d successor runs", count)
	}
}

func (f standingDispositionParityFixture) assertGenerationOwner(t *testing.T, ctx context.Context, serviceID string, retiredGeneration, currentGeneration int64) {
	t.Helper()
	query := `SELECT COUNT(*) FROM standing_service_generations WHERE service_id=? AND generation=? AND retired_at IS NOT NULL`
	args := []any{serviceID, retiredGeneration}
	if f.backend == "postgres" {
		query = `SELECT COUNT(*) FROM standing_service_generations WHERE service_id=$1::uuid AND generation=$2 AND retired_at IS NOT NULL`
	}
	var retired int64
	if err := f.db.QueryRowContext(ctx, query, args...).Scan(&retired); err != nil {
		t.Fatalf("read retired generation owner: %v", err)
	}
	query = `SELECT COUNT(*) FROM standing_service_generations WHERE service_id=? AND generation=? AND retired_at IS NULL`
	args = []any{serviceID, currentGeneration}
	if f.backend == "postgres" {
		query = `SELECT COUNT(*) FROM standing_service_generations WHERE service_id=$1::uuid AND generation=$2 AND retired_at IS NULL`
	}
	var current int64
	if err := f.db.QueryRowContext(ctx, query, args...).Scan(&current); err != nil {
		t.Fatalf("read current generation owner: %v", err)
	}
	if retired != 1 || current != 1 {
		t.Fatalf("generation ownership = retired:%d current:%d, want 1/1", retired, current)
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
