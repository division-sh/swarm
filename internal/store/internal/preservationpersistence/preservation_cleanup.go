package preservationpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storeworkflowtimer "github.com/division-sh/swarm/internal/store/internal/backend/workflowtimer"
	"github.com/lib/pq"
)

type deliveryOwner interface {
	ActiveRunDeliverySnapshotsTx(context.Context, *sql.Tx, string) ([]runtimedelivery.Snapshot, error)
	TerminalizeRunDeliveriesTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, *privaterunforkrevision.Effects, string, string) ([]runtimedelivery.Terminalization, error)
}

type pipelineOwner interface {
	TerminalizeRunTx(context.Context, *sql.Tx, *privaterunforkrevision.Effects, string, runtimepipelineobligation.Disposition, time.Time) (int, error)
}

type lifecycleOwner interface {
	MarkTerminalTx(context.Context, *sql.Tx, runtimeauthoractivity.Mutation, *privaterunforkrevision.Effects, runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error)
	UpsertQuiescedRunControlTx(context.Context, *sql.Tx, string, string, string, time.Time) error
}

type revisionOwner interface {
	FinalizeRunForkRevisionTx(context.Context, *sql.Tx, *privaterunforkrevision.Effects) error
}

type PreservationPostgresOwner struct {
	backend        *postgresbackend.Backend
	requireCurrent func() error
	delivery       deliveryOwner
	pipeline       pipelineOwner
	lifecycle      lifecycleOwner
	runFork        revisionOwner
}

func NewPostgres(backend *postgresbackend.Backend, requireCurrent func() error, delivery deliveryOwner, pipeline pipelineOwner, lifecycle lifecycleOwner, runFork revisionOwner) (*PreservationPostgresOwner, error) {
	if backend == nil || !backend.Valid() || requireCurrent == nil || delivery == nil || pipeline == nil || lifecycle == nil || runFork == nil {
		return nil, errors.New("preservation cleanup PostgreSQL owner dependencies are required")
	}
	return &PreservationPostgresOwner{backend: backend, requireCurrent: requireCurrent, delivery: delivery, pipeline: pipeline, lifecycle: lifecycle, runFork: runFork}, nil
}

type preservationCleanupRunTarget struct {
	RunID        string
	Status       string
	BundleSource runbundle.AvailabilitySource
}

type preservationCleanupSessionTarget struct {
	SessionID string
	RunID     string
	AgentID   string
	Status    string
}

func (s *PreservationPostgresOwner) ApplyUnavailableBundleStartupPreservationCleanup(ctx context.Context, req preservationcleanup.Request) (preservationcleanup.Result, error) {
	return s.applyPreservationCleanup(ctx, req, preservationcleanup.UnavailableBundleStartupOperationName, preservationcleanup.UnavailableBundleStartupControlledBy)
}

func (s *PreservationPostgresOwner) ApplyBundleForceDeletePreservationCleanup(ctx context.Context, req preservationcleanup.Request) (preservationcleanup.Result, error) {
	return s.applyPreservationCleanup(ctx, req, preservationcleanup.BundleForceDeleteOperationName, preservationcleanup.BundleForceDeleteControlledBy)
}

func (s *PreservationPostgresOwner) applyPreservationCleanup(ctx context.Context, req preservationcleanup.Request, defaultOperationName, defaultControlledBy string) (preservationcleanup.Result, error) {
	if s == nil || s.backend == nil {
		return preservationcleanup.Result{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrent(); err != nil {
		return preservationcleanup.Result{}, err
	}
	now := req.RequestedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	operationName := strings.TrimSpace(req.OperationName)
	if operationName == "" {
		operationName = strings.TrimSpace(defaultOperationName)
	}
	controlledBy := strings.TrimSpace(req.ControlledBy)
	if controlledBy == "" {
		controlledBy = strings.TrimSpace(defaultControlledBy)
		if controlledBy == "" {
			controlledBy = operationName
		}
	}
	targets, err := preservationcleanup.NormalizeTargets(req.Targets)
	if err != nil {
		return preservationcleanup.Result{}, err
	}
	out := preservationcleanup.Result{
		OperationName: operationName,
		AppliedAt:     now,
		ControlledBy:  controlledBy,
	}
	if len(targets) == 0 {
		return out, nil
	}
	targetByRun := make(map[string]preservationcleanup.RunTarget, len(targets))
	runIDs := make([]string, 0, len(targets))
	for _, target := range targets {
		targetByRun[target.RunID] = target
		runIDs = append(runIDs, target.RunID)
	}

	tx, err := s.backend.BeginTx(ctx, nil)
	if err != nil {
		return preservationcleanup.Result{}, fmt.Errorf("begin preservation cleanup tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
	if err != nil {
		return preservationcleanup.Result{}, err
	}
	effects := privaterunforkrevision.NewEffects()

	runs, err := lockUnavailableBundlePreservationRunsTx(ctx, tx, runIDs)
	if err != nil {
		return preservationcleanup.Result{}, err
	}
	activeRunIDs := make([]string, 0, len(runs))
	for _, run := range runs {
		target, ok := targetByRun[run.RunID]
		if !ok {
			return preservationcleanup.Result{}, fmt.Errorf("preservation cleanup locked unexpected run %s", run.RunID)
		}
		if run.BundleSource != target.BundleSource {
			return preservationcleanup.Result{}, fmt.Errorf("preservation cleanup source changed for run %s: got %s want %s", run.RunID, run.BundleSource, target.BundleSource)
		}
		activeRunIDs = append(activeRunIDs, run.RunID)
		out.Runs = append(out.Runs, preservationcleanup.RunResult{
			RunID:          run.RunID,
			BundleSource:   run.BundleSource,
			PreviousStatus: run.Status,
			Status:         preservationcleanup.RunStatusCancelled,
			ReasonCode:     target.ReasonCode,
			Changed:        activeRunQuiescenceRunStatusActive(run.Status),
		})
	}
	if len(activeRunIDs) == 0 {
		if err := tx.Commit(); err != nil {
			return preservationcleanup.Result{}, fmt.Errorf("commit empty preservation cleanup tx: %w", err)
		}
		committed = true
		return out, nil
	}

	deliveries := []runtimedelivery.Snapshot{}
	for _, runID := range activeRunIDs {
		snapshots, err := s.delivery.ActiveRunDeliverySnapshotsTx(ctx, tx, runID)
		if err != nil {
			return preservationcleanup.Result{}, err
		}
		deliveries = append(deliveries, snapshots...)
	}
	for _, delivery := range deliveries {
		target := targetByRun[delivery.RunID]
		out.Deliveries = append(out.Deliveries, preservationcleanup.DeliveryResult{
			DeliveryID:      delivery.DeliveryID,
			RunID:           delivery.RunID,
			EventID:         delivery.EventID,
			SubscriberType:  string(delivery.SubscriberClass),
			SubscriberID:    delivery.SubscriberID,
			PreviousStatus:  string(delivery.Status),
			Status:          preservationcleanup.DeliveryOutcomeDeadLetter,
			ReasonCode:      target.ReasonCode,
			PreviousReason:  delivery.ReasonCode,
			ActiveSessionID: delivery.ActiveSessionID,
			Changed:         true,
		})
	}
	sessions, err := lockUnavailableBundlePreservationSessionsTx(ctx, tx, activeRunIDs)
	if err != nil {
		return preservationcleanup.Result{}, err
	}
	for _, session := range sessions {
		target := targetByRun[session.RunID]
		out.Sessions = append(out.Sessions, preservationcleanup.SessionResult{
			SessionID:      session.SessionID,
			RunID:          session.RunID,
			AgentID:        session.AgentID,
			PreviousStatus: session.Status,
			Status:         "terminated",
			ReasonCode:     target.ReasonCode,
			Changed:        session.Status != "terminated",
		})
	}
	for _, runID := range activeRunIDs {
		target := targetByRun[runID]
		if _, err := s.delivery.TerminalizeRunDeliveriesTx(ctx, tx, story, effects, runID, target.ReasonCode); err != nil {
			return preservationcleanup.Result{}, err
		}
		terminalized, err := s.pipeline.TerminalizeRunTx(ctx, tx, effects, runID, runtimepipelineobligation.DeadLetter(target.ReasonCode, nil), now)
		if err != nil {
			return preservationcleanup.Result{}, err
		}
		out.PipelineReceiptCount += terminalized
	}
	for _, session := range sessions {
		target := targetByRun[session.RunID]
		if err := terminateUnavailableBundlePreservationSessionTx(ctx, tx, effects, session.RunID, session.SessionID, target.ReasonCode, now); err != nil {
			return preservationcleanup.Result{}, err
		}
	}
	for _, runID := range activeRunIDs {
		target := targetByRun[runID]
		generic, err := storegenericschedule.CancelRunsTx(ctx, tx, true, effects, []string{runID}, target.ReasonCode, now)
		if err != nil {
			return preservationcleanup.Result{}, fmt.Errorf("cancel preservation generic schedules: %w", err)
		}
		workflow, err := storeworkflowtimer.CancelRunsTx(ctx, tx, true, effects, []string{runID})
		if err != nil {
			return preservationcleanup.Result{}, fmt.Errorf("cancel preservation workflow timers: %w", err)
		}
		for _, timer := range append(generic, workflow...) {
			out.Timers = append(out.Timers, preservationcleanup.TimerResult{
				Family: timer.Family, TimerID: timer.ActivationID, RunID: timer.RunID, TimerName: timer.TaskID,
				PreviousStatus: "active", Status: preservationcleanup.TimerStatusCancelled,
				ReasonCode: target.ReasonCode, Changed: true,
			})
		}
	}
	for _, run := range runs {
		target := targetByRun[run.RunID]
		if _, _, err := s.lifecycle.MarkTerminalTx(ctx, tx, story, effects, runtimerunlifecycle.TerminalRequest{
			RunID: run.RunID, State: runtimerunlifecycle.StateCancelled, EndedAt: now,
		}); err != nil {
			return preservationcleanup.Result{}, err
		}
		if err := s.lifecycle.UpsertQuiescedRunControlTx(ctx, tx, run.RunID, target.ReasonCode, controlledBy, now); err != nil {
			return preservationcleanup.Result{}, err
		}
	}
	if err := story.Finalize(ctx); err != nil {
		return preservationcleanup.Result{}, err
	}
	if err := s.runFork.FinalizeRunForkRevisionTx(ctx, tx, effects); err != nil {
		return preservationcleanup.Result{}, fmt.Errorf("finalize preservation cleanup revisions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return preservationcleanup.Result{}, fmt.Errorf("commit preservation cleanup tx: %w", err)
	}
	committed = true
	return out, nil
}

func lockUnavailableBundlePreservationRunsTx(ctx context.Context, tx *sql.Tx, runIDs []string) ([]preservationCleanupRunTarget, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT run_id::text, COALESCE(status, ''), COALESCE(bundle_source, '')
		FROM runs
		WHERE run_id = ANY($1::uuid[])
		  AND lower(COALESCE(status, '')) IN ('running', 'paused')
		ORDER BY run_id::text
		FOR UPDATE
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("lock unavailable bundle preservation runs: %w", err)
	}
	defer rows.Close()
	var out []preservationCleanupRunTarget
	for rows.Next() {
		var item preservationCleanupRunTarget
		var rawSource string
		if err := rows.Scan(&item.RunID, &item.Status, &rawSource); err != nil {
			return nil, fmt.Errorf("scan unavailable bundle preservation run: %w", err)
		}
		item.RunID = strings.TrimSpace(item.RunID)
		item.Status = strings.TrimSpace(item.Status)
		item.BundleSource, err = runbundle.DecodeAvailabilitySource(rawSource)
		if err != nil {
			return nil, fmt.Errorf("decode unavailable bundle preservation run %s source: %w", item.RunID, err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read unavailable bundle preservation runs: %w", err)
	}
	return out, nil
}

func lockUnavailableBundlePreservationSessionsTx(ctx context.Context, tx *sql.Tx, runIDs []string) ([]preservationCleanupSessionTarget, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT session_id::text, run_id::text, COALESCE(agent_id, ''), COALESCE(status, '')
		FROM agent_sessions
		WHERE run_id = ANY($1::uuid[])
		  AND status IN ('active', 'suspended')
		ORDER BY run_id::text, agent_id, session_id::text
		FOR UPDATE
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("lock unavailable bundle preservation sessions: %w", err)
	}
	defer rows.Close()
	var out []preservationCleanupSessionTarget
	for rows.Next() {
		var item preservationCleanupSessionTarget
		if err := rows.Scan(&item.SessionID, &item.RunID, &item.AgentID, &item.Status); err != nil {
			return nil, fmt.Errorf("scan unavailable bundle preservation session: %w", err)
		}
		item.SessionID = strings.TrimSpace(item.SessionID)
		item.RunID = strings.TrimSpace(item.RunID)
		item.AgentID = strings.TrimSpace(item.AgentID)
		item.Status = strings.TrimSpace(item.Status)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read unavailable bundle preservation sessions: %w", err)
	}
	return out, nil
}

func terminateUnavailableBundlePreservationSessionTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, runID, sessionID, reasonCode string, at time.Time) error {
	result, err := tx.ExecContext(ctx, `
		UPDATE agent_sessions
		SET
			status = 'terminated',
			termination_reason = $2,
			termination_detail = $3,
			successor_session_id = NULL,
			terminated_at = COALESCE(terminated_at, $4),
			lease_holder = NULL,
			lease_expires_at = NULL,
			updated_at = $4
		WHERE session_id = $1::uuid
		  AND status IN ('active', 'suspended')
	`, sessionID, preservationcleanup.SessionTerminationReasonOrphaned, reasonCode, at.UTC())
	if err != nil {
		return fmt.Errorf("terminate unavailable bundle preservation session %s: %w", sessionID, err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed > 0 {
		return effects.Add(runID, privaterunforkrevision.FamilyAgentSessions)
	}
	return nil
}

func activeRunQuiescenceRunStatusActive(status string) bool {
	state, err := runtimerunlifecycle.ParseState(status)
	return err == nil && state.Active()
}
