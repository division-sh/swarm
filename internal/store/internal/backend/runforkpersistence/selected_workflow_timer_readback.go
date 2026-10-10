package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type runForkWorkflowTimerReadbackPort struct {
	postgres         bool
	snapshot         runForkLifecycleSnapshotLoader
	plan             func(context.Context, *sql.Tx, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
	timers           runForkWorkflowTimerMaterializationOwner
	arrivalSchedules runForkArrivalJoinMaterializationOwner
}

// Successor issuance consumes the permanent activation fact, never "paused"
// as proof that the predecessor did not execute. Readback creates no work.
func requireSelectedSuccessorWorkflowTimers(ctx context.Context, attempt *mutationprotocol.Attempt, req runfork.SelectedContractRuntimeExecutionIssueRequest, port runForkWorkflowTimerReadbackPort) error {
	if port.snapshot == nil || port.plan == nil || port.timers == nil || port.arrivalSchedules == nil {
		return fmt.Errorf("selected timer continuation requires its canonical readback owners")
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := port.snapshot(ctx, tx, req.Admission.ForkRunID)
		if err != nil {
			return err
		}
		binding, err := loadRunForkSelectedContractBinding(ctx, tx, req.Admission.ForkRunID)
		if err != nil {
			return err
		}
		operation, err := selectedForkOperationForRecoveryTx(ctx, tx, binding, snapshot.BundleHash, port.postgres)
		if err != nil {
			return err
		}
		if binding.SourceRunID != req.Admission.SourceRunID || binding.ForkPoint != req.Admission.ForkPoint || snapshot.BundleHash != req.DeclarationPlan.BundleHash {
			return fmt.Errorf("selected timer continuation differs from its permanent child binding")
		}
		// The binding retains cut identity; the permanent operation retains its
		// full resolved selector and event evidence. Do not reconstruct one from
		// the other during successor admission.
		point := *operation.Request.ResolvedPoint
		plan, err := port.plan(ctx, tx, runfork.RunForkPlanRequest{
			SourceRunID: binding.SourceRunID, AtStart: point.Kind == runfork.RunForkPointRunStart, ResolvedPoint: &point,
		})
		if err != nil {
			return err
		}
		admission := runfork.RunForkSelectedContractReplayResumeAdmission(plan)
		ctx = correlation.WithRunID(ctx, snapshot.RunID)
		bornAt := runlifecycle.CanonicalTimestamp(snapshot.StartedAt)
		switch {
		case operation.Status == runfork.ForkOperationMaterialized && snapshot.State == runlifecycle.StatePaused:
			_, err = requireMaterializedRunForkTimerHistory(ctx, attempt, plan, snapshot.RunID,
				req.InheritedWorkflowTimers, port.timers, port.arrivalSchedules, bornAt, admission)
		case operation.Status == runfork.ForkOperationActivated && snapshot.State == runlifecycle.StateRunning:
			_, err = requireContinuingRunForkTimerHistory(ctx, attempt, plan, snapshot.RunID,
				req.InheritedWorkflowTimers, port.timers, port.arrivalSchedules, bornAt, admission)
		default:
			err = fmt.Errorf("selected timer continuation has no executable permanent operation/state pair")
		}
		return err
	})
}

func postgresRunForkWorkflowTimerReadbackPort(s *RunForkPostgresOwner) runForkWorkflowTimerReadbackPort {
	return runForkWorkflowTimerReadbackPort{
		postgres: true,
		snapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runlifecycle.Snapshot, error) {
			return s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, true)
		},
		plan: func(ctx context.Context, tx *sql.Tx, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
			return planRunForkSnapshot(ctx, tx, req, runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint)
		},
		timers: s.PipelinePostgresOwner, arrivalSchedules: s.PipelinePostgresOwner,
	}
}

func sqliteRunForkWorkflowTimerReadbackPort(s *RunForkSQLiteOwner) runForkWorkflowTimerReadbackPort {
	return runForkWorkflowTimerReadbackPort{
		snapshot: func(ctx context.Context, tx *sql.Tx, runID string) (runlifecycle.Snapshot, error) {
			return s.RunLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, runID)
		},
		plan: func(ctx context.Context, tx *sql.Tx, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
			return planRunForkSnapshot(ctx, tx, req, runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint)
		},
		timers: s.PipelineSQLiteOwner, arrivalSchedules: s.PipelineSQLiteOwner,
	}
}
