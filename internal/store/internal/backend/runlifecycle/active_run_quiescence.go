package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	runtimetimercancellation "github.com/division-sh/swarm/internal/runtime/timercancellation"
	"github.com/division-sh/swarm/internal/store/internal/adminpersistence"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/lib/pq"
)

func (s *RunLifecyclePostgresOwner) ApplyServeAbandonActiveRunQuiescence(ctx context.Context, at time.Time) (runtimerunquiescence.Result, error) {
	return s.ApplyActiveRunQuiescence(ctx, runtimerunquiescence.Request{
		OperationName: runtimerunquiescence.ServeAbandonOperationName,
		RequestedAt:   at,
		AllActiveRuns: true,
		ReasonCode:    runtimerunquiescence.ServeAbandonReasonCode,
		ControlledBy:  runtimerunquiescence.ServeAbandonControlledBy,
		DeliveryNote:  runtimerunquiescence.ServeAbandonDeliveryNote,
	})
}

func (s *RunLifecycleSQLiteOwner) ApplyServeAbandonActiveRunQuiescence(ctx context.Context, at time.Time) (runtimerunquiescence.Result, error) {
	return s.ApplyActiveRunQuiescence(ctx, runtimerunquiescence.Request{
		OperationName: runtimerunquiescence.ServeAbandonOperationName,
		RequestedAt:   at,
		AllActiveRuns: true,
		ReasonCode:    runtimerunquiescence.ServeAbandonReasonCode,
		ControlledBy:  runtimerunquiescence.ServeAbandonControlledBy,
		DeliveryNote:  runtimerunquiescence.ServeAbandonDeliveryNote,
	})
}

func (s *RunLifecyclePostgresOwner) ApplyActiveRunQuiescence(ctx context.Context, req runtimerunquiescence.Request) (runtimerunquiescence.Result, error) {
	return s.applyActiveRunQuiescence(ctx, req, nil)
}

func (s *RunLifecyclePostgresOwner) applyActiveRunQuiescence(ctx context.Context, req runtimerunquiescence.Request, reset *runtimedestructivereset.QuiescenceRequest) (runtimerunquiescence.Result, error) {
	if s == nil || s.backend == nil {
		return runtimerunquiescence.Result{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunquiescence.Result{}, err
	}
	now := req.RequestedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	out := runtimerunquiescence.Result{
		OperationName: strings.TrimSpace(req.OperationName),
		DryRun:        req.DryRun,
		AppliedAt:     now,
		ReasonCode:    strings.TrimSpace(req.ReasonCode),
		ControlledBy:  strings.TrimSpace(req.ControlledBy),
	}
	if out.OperationName == "" {
		return out, fmt.Errorf("active run quiescence operation_name is required")
	}
	if out.ReasonCode == "" {
		return out, fmt.Errorf("active run quiescence reason_code is required")
	}
	if out.ControlledBy == "" {
		return out, fmt.Errorf("active run quiescence controlled_by is required")
	}
	deliveryNote := strings.TrimSpace(req.DeliveryNote)
	if deliveryNote == "" {
		deliveryNote = out.ReasonCode
	}

	runIDs := normalizeQuiescenceRunIDs(req.RunIDs)
	if len(runIDs) == 0 && !req.AllActiveRuns && reset == nil {
		return out, nil
	}

	evidence := mutationprotocol.Story
	if req.DryRun {
		evidence = mutationprotocol.RevisionOnly
	}
	emptySelection := errors.New("active run quiescence selected no runs")
	var emptyResult runtimerunquiescence.Result
	result := mutationprotocol.RunPostgres(ctx, s.backend, evidence, mutationprotocol.Ordinary, nil, nil, func(sqlCtx context.Context, attempt *mutationprotocol.Attempt) (runtimerunquiescence.Result, error) {
		var value runtimerunquiescence.Result
		err := attempt.WithSQL(sqlCtx, func(sqlCtx context.Context, tx *sql.Tx) (err error) {
			value, err = s.applyActiveRunQuiescenceTx(sqlCtx, tx, attempt, req, reset, out, runIDs, now)
			return err
		})
		if err == nil && !req.DryRun && len(value.Runs) == 0 && reset == nil {
			emptyResult = value
			return value, emptySelection
		}
		return value, err
	})
	if result.Err() == emptySelection {
		return emptyResult, nil
	}
	value, committed := result.Value()
	if !committed {
		return runtimerunquiescence.Result{}, result.Err()
	}
	value.Acknowledged = true
	return value, result.Err()
}

func (s *RunLifecyclePostgresOwner) applyActiveRunQuiescenceTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, req runtimerunquiescence.Request, reset *runtimedestructivereset.QuiescenceRequest, out runtimerunquiescence.Result, runIDs []string, now time.Time) (runtimerunquiescence.Result, error) {
	var runs []runtimerunquiescence.QuiescedRun
	var err error
	if req.AllActiveRuns {
		runs, err = lockAllActiveQuiescenceRunsTx(ctx, tx)
	} else {
		runs, err = lockActiveRunQuiescenceRunsTx(ctx, tx, runIDs)
	}
	if err != nil {
		return runtimerunquiescence.Result{}, err
	}
	runIDs = quiescenceRunIDs(runs)
	if len(runIDs) == 0 && reset == nil {
		return out, nil
	}
	active := []runtimedelivery.Snapshot{}
	for _, runID := range runIDs {
		snapshots, err := s.delivery.ActiveRunDeliverySnapshotsTx(ctx, tx, runID)
		if err != nil {
			return runtimerunquiescence.Result{}, err
		}
		active = append(active, snapshots...)
	}
	for _, delivery := range active {
		out.Deliveries = append(out.Deliveries, runtimerunquiescence.QuiescedDelivery{
			DeliveryID:      delivery.DeliveryID,
			RunID:           delivery.RunID,
			EventID:         delivery.EventID,
			SubscriberType:  string(delivery.SubscriberClass),
			SubscriberID:    delivery.SubscriberID,
			PreviousStatus:  string(delivery.Status),
			Status:          "dead_letter",
			ReasonCode:      out.ReasonCode,
			PreviousReason:  delivery.ReasonCode,
			ActiveSessionID: delivery.ActiveSessionID,
			Changed:         true,
		})
	}
	for _, run := range runs {
		nextStatus := run.Status
		changed := false
		if activeRunQuiescenceRunStatusActive(run.Status) {
			nextStatus = "cancelled"
			changed = true
		}
		out.Runs = append(out.Runs, runtimerunquiescence.QuiescedRun{
			RunID:          run.RunID,
			BundleHash:     run.BundleHash,
			PreviousStatus: run.Status,
			Status:         nextStatus,
			ReasonCode:     out.ReasonCode,
			Changed:        changed,
		})
	}
	if req.DryRun {
		return out, nil
	}
	for _, runID := range runIDs {
		if _, err := s.delivery.TerminalizeRunDeliveriesTx(ctx, attempt, runID, out.ReasonCode); err != nil {
			return runtimerunquiescence.Result{}, err
		}
		terminalized, err := s.pipeline.TerminalizeRunTx(ctx, attempt, runID, runtimepipelineobligation.DeadLetter(out.ReasonCode, nil), now)
		if err != nil {
			return runtimerunquiescence.Result{}, err
		}
		out.PipelineReceiptCount += terminalized
	}
	out.SessionCount, err = terminateActiveRunSessionsTx(ctx, tx, attempt, runIDs, out.ReasonCode, now)
	if err != nil {
		return runtimerunquiescence.Result{}, err
	}
	out.TimerCancellations, err = cancelActiveRunTimerFamiliesTx(ctx, tx, attempt, true, runIDs, out.ReasonCode, now)
	if err != nil {
		return runtimerunquiescence.Result{}, err
	}
	out.TimerCount = len(out.TimerCancellations)
	for _, run := range runs {
		if !activeRunQuiescenceRunStatusActive(run.Status) {
			continue
		}
		if _, _, err := (postgresRunLifecycleMutation{store: s, tx: tx, attempt: attempt}).MarkTerminal(ctx, runtimerunlifecycle.TerminalRequest{
			RunID: run.RunID, State: runtimerunlifecycle.StateCancelled, EndedAt: now,
		}); err != nil {
			return runtimerunquiescence.Result{}, fmt.Errorf("mark active run quiescence run terminal: %w", err)
		}
		if err := upsertActiveRunQuiescenceRunControlTx(ctx, tx, run.RunID, out.ReasonCode, out.ControlledBy, now); err != nil {
			return runtimerunquiescence.Result{}, err
		}
	}
	if reset != nil {
		if err := adminpersistence.CommitResetQuiescenceTx(ctx, tx, *reset, destructiveResetQuiescenceResult(out), false); err != nil {
			return runtimerunquiescence.Result{}, err
		}
	}
	return out, nil
}

func (s *RunLifecycleSQLiteOwner) ApplyActiveRunQuiescence(ctx context.Context, req runtimerunquiescence.Request) (runtimerunquiescence.Result, error) {
	return s.applyActiveRunQuiescence(ctx, req, nil)
}

func (s *RunLifecycleSQLiteOwner) applyActiveRunQuiescence(ctx context.Context, req runtimerunquiescence.Request, reset *runtimedestructivereset.QuiescenceRequest) (runtimerunquiescence.Result, error) {
	if s == nil || s.backend == nil {
		return runtimerunquiescence.Result{}, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunquiescence.Result{}, err
	}
	now := req.RequestedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	out := runtimerunquiescence.Result{
		OperationName: strings.TrimSpace(req.OperationName),
		DryRun:        req.DryRun,
		AppliedAt:     now,
		ReasonCode:    strings.TrimSpace(req.ReasonCode),
		ControlledBy:  strings.TrimSpace(req.ControlledBy),
	}
	if out.OperationName == "" {
		return out, fmt.Errorf("active run quiescence operation_name is required")
	}
	if out.ReasonCode == "" {
		return out, fmt.Errorf("active run quiescence reason_code is required")
	}
	if out.ControlledBy == "" {
		return out, fmt.Errorf("active run quiescence controlled_by is required")
	}
	deliveryNote := strings.TrimSpace(req.DeliveryNote)
	if deliveryNote == "" {
		deliveryNote = out.ReasonCode
	}

	requestedRunIDs := normalizeQuiescenceRunIDs(req.RunIDs)
	if len(requestedRunIDs) == 0 && !req.AllActiveRuns && reset == nil {
		return out, nil
	}

	evidence := mutationprotocol.Story
	if req.DryRun {
		evidence = mutationprotocol.RevisionOnly
	}
	emptySelection := errors.New("active run quiescence selected no runs")
	var emptyResult runtimerunquiescence.Result
	result := mutationprotocol.RunSQLite(ctx, s.backend, "sqlite active run quiescence", evidence, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (runtimerunquiescence.Result, error) {
		attemptOut := out
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var runs []runtimerunquiescence.QuiescedRun
			var err error
			if req.AllActiveRuns {
				runs, err = sqliteLockAllActiveQuiescenceRunsTx(txctx, tx)
			} else {
				runs, err = sqliteLockActiveQuiescenceRunsTx(txctx, tx, requestedRunIDs)
			}
			if err != nil {
				return err
			}
			attemptRunIDs := quiescenceRunIDs(runs)
			if len(attemptRunIDs) == 0 && reset == nil {
				return nil
			}
			active := []runtimedelivery.Snapshot{}
			for _, runID := range attemptRunIDs {
				snapshots, err := s.delivery.ActiveRunDeliverySnapshotsTx(txctx, tx, runID)
				if err != nil {
					return err
				}
				active = append(active, snapshots...)
			}
			for _, delivery := range active {
				attemptOut.Deliveries = append(attemptOut.Deliveries, runtimerunquiescence.QuiescedDelivery{
					DeliveryID:      delivery.DeliveryID,
					RunID:           delivery.RunID,
					EventID:         delivery.EventID,
					SubscriberType:  string(delivery.SubscriberClass),
					SubscriberID:    delivery.SubscriberID,
					PreviousStatus:  string(delivery.Status),
					Status:          "dead_letter",
					ReasonCode:      attemptOut.ReasonCode,
					PreviousReason:  delivery.ReasonCode,
					ActiveSessionID: delivery.ActiveSessionID,
					Changed:         true,
				})
			}
			for _, run := range runs {
				nextStatus := run.Status
				changed := false
				if activeRunQuiescenceRunStatusActive(run.Status) {
					nextStatus = "cancelled"
					changed = true
				}
				attemptOut.Runs = append(attemptOut.Runs, runtimerunquiescence.QuiescedRun{
					RunID:          run.RunID,
					BundleHash:     run.BundleHash,
					PreviousStatus: run.Status,
					Status:         nextStatus,
					ReasonCode:     attemptOut.ReasonCode,
					Changed:        changed,
				})
			}
			if req.DryRun {
				return nil
			}
			for _, runID := range attemptRunIDs {
				if _, err := s.delivery.TerminalizeRunDeliveriesTx(txctx, attempt, runID, attemptOut.ReasonCode); err != nil {
					return err
				}
				terminalized, err := s.pipeline.TerminalizeRunTx(txctx, attempt, runID, runtimepipelineobligation.DeadLetter(attemptOut.ReasonCode, nil), now)
				if err != nil {
					return err
				}
				attemptOut.PipelineReceiptCount += terminalized
			}
			attemptOut.SessionCount, err = sqliteTerminateActiveRunSessionsTx(txctx, tx, attempt, attemptRunIDs, attemptOut.ReasonCode, now)
			if err != nil {
				return err
			}
			attemptOut.TimerCancellations, err = cancelActiveRunTimerFamiliesTx(txctx, tx, attempt, false, attemptRunIDs, attemptOut.ReasonCode, now)
			if err != nil {
				return err
			}
			attemptOut.TimerCount = len(attemptOut.TimerCancellations)
			for _, run := range runs {
				if !activeRunQuiescenceRunStatusActive(run.Status) {
					continue
				}
				if _, _, err := (sqliteRunLifecycleMutation{store: s, tx: tx, attempt: attempt}).MarkTerminal(txctx, runtimerunlifecycle.TerminalRequest{
					RunID: run.RunID, State: runtimerunlifecycle.StateCancelled, EndedAt: now,
				}); err != nil {
					return err
				}
				if err := sqliteUpsertActiveRunQuiescenceRunControlTx(txctx, tx, run.RunID, attemptOut.ReasonCode, attemptOut.ControlledBy, now); err != nil {
					return err
				}
			}
			if reset != nil {
				return adminpersistence.CommitResetQuiescenceTx(txctx, tx, *reset, destructiveResetQuiescenceResult(attemptOut), true)
			}
			return nil
		})
		if err == nil && !req.DryRun && len(attemptOut.Runs) == 0 && reset == nil {
			emptyResult = attemptOut
			return attemptOut, emptySelection
		}
		return attemptOut, err
	})
	if result.Err() == emptySelection {
		return emptyResult, nil
	}
	value, committed := result.Value()
	if !committed {
		return runtimerunquiescence.Result{}, result.Err()
	}
	value.Acknowledged = true
	return value, result.Err()
}

func normalizeQuiescenceRunIDs(runIDs []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(runIDs))
	for _, raw := range runIDs {
		id := nullUUIDString(raw)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func quiescenceRunIDs(runs []runtimerunquiescence.QuiescedRun) []string {
	out := make([]string, 0, len(runs))
	for _, run := range runs {
		if id := nullUUIDString(run.RunID); id != "" {
			out = append(out, id)
		}
	}
	return out
}

func activeRunQuiescenceRunStatusActive(status string) bool {
	state, err := runtimerunlifecycle.ParseState(status)
	return err == nil && state.Active()
}

func lockAllActiveQuiescenceRunsTx(ctx context.Context, tx *sql.Tx) ([]runtimerunquiescence.QuiescedRun, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT run_id::text, COALESCE(bundle_hash, ''), COALESCE(status, '')
		FROM runs
		WHERE status IN (`+runLifecycleActiveStateSQLValues+`)
		  AND NOT EXISTS (
			SELECT 1 FROM standing_services ss WHERE ss.current_run_id = runs.run_id
		  )
		ORDER BY run_id::text
		FOR UPDATE
	`)
	if err != nil {
		return nil, fmt.Errorf("lock all active quiescence runs: %w", err)
	}
	return scanActiveRunQuiescenceRuns(rows)
}

func lockActiveRunQuiescenceRunsTx(ctx context.Context, tx *sql.Tx, runIDs []string) ([]runtimerunquiescence.QuiescedRun, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT run_id::text, COALESCE(bundle_hash, ''), COALESCE(status, '')
		FROM runs
		WHERE run_id = ANY($1::uuid[])
		  AND lower(COALESCE(status, '')) IN ('running', 'paused')
		ORDER BY run_id::text
		FOR UPDATE
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("lock active quiescence runs: %w", err)
	}
	return scanActiveRunQuiescenceRuns(rows)
}

func scanActiveRunQuiescenceRuns(rows *sql.Rows) ([]runtimerunquiescence.QuiescedRun, error) {
	defer rows.Close()
	var out []runtimerunquiescence.QuiescedRun
	for rows.Next() {
		var run runtimerunquiescence.QuiescedRun
		if err := rows.Scan(&run.RunID, &run.BundleHash, &run.PreviousStatus); err != nil {
			return nil, fmt.Errorf("scan active quiescence run: %w", err)
		}
		run.RunID = strings.TrimSpace(run.RunID)
		run.BundleHash = strings.TrimSpace(run.BundleHash)
		run.PreviousStatus = strings.TrimSpace(run.PreviousStatus)
		run.Status = run.PreviousStatus
		out = append(out, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read active quiescence runs: %w", err)
	}
	return out, nil
}

func sqliteLockAllActiveQuiescenceRunsTx(ctx context.Context, tx *sql.Tx) ([]runtimerunquiescence.QuiescedRun, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT run_id, COALESCE(bundle_hash, ''), COALESCE(status, '')
		FROM runs
		WHERE lower(COALESCE(status, '')) IN ('running', 'paused')
		  AND NOT EXISTS (
			SELECT 1 FROM standing_services ss WHERE ss.current_run_id = runs.run_id
		  )
		ORDER BY run_id
	`)
	if err != nil {
		return nil, fmt.Errorf("lock sqlite all active quiescence runs: %w", err)
	}
	return scanActiveRunQuiescenceRuns(rows)
}

func sqliteLockActiveQuiescenceRunsTx(ctx context.Context, tx *sql.Tx, runIDs []string) ([]runtimerunquiescence.QuiescedRun, error) {
	if len(runIDs) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(runIDs))
	for _, runID := range runIDs {
		args = append(args, runID)
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT run_id, COALESCE(bundle_hash, ''), COALESCE(status, '')
		FROM runs
		WHERE run_id IN (`+sqlitePlaceholders(len(runIDs))+`)
		  AND lower(COALESCE(status, '')) IN ('running', 'paused')
		ORDER BY run_id
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("lock sqlite active quiescence runs: %w", err)
	}
	return scanActiveRunQuiescenceRuns(rows)
}

func terminateActiveRunSessionsTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, runIDs []string, reason string, at time.Time) (int, error) {
	rows, err := tx.QueryContext(ctx, `
		UPDATE agent_sessions
		SET status = 'terminated',
		    termination_reason = 'cancelled',
		    termination_detail = $2,
		    terminated_at = COALESCE(terminated_at, $3),
		    lease_holder = NULL,
		    lease_expires_at = NULL,
		    updated_at = $3
		WHERE run_id = ANY($1::uuid[])
		  AND status IN ('active', 'suspended')
		RETURNING run_id::text, session_id::text
	`, pq.Array(runIDs), reason, at.UTC())
	if err != nil {
		return 0, fmt.Errorf("terminate active run sessions: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var runID, sessionID string
		if err := rows.Scan(&runID, &sessionID); err != nil {
			return 0, err
		}
		if err := attempt.AddFact(runID, runforkrevision.FamilyAgentSessions, sessionID); err != nil {
			return 0, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func sqliteTerminateActiveRunSessionsTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, runIDs []string, reason string, at time.Time) (int, error) {
	args := make([]any, 0, len(runIDs)+3)
	args = append(args, reason, at.UTC(), at.UTC())
	for _, runID := range runIDs {
		args = append(args, runID)
	}
	rows, err := tx.QueryContext(ctx, `
		UPDATE agent_sessions
		SET status = 'terminated',
		    termination_reason = 'cancelled',
		    termination_detail = ?,
		    terminated_at = COALESCE(terminated_at, ?),
		    lease_holder = NULL,
		    lease_expires_at = NULL,
		    updated_at = ?
		WHERE run_id IN (`+sqlitePlaceholders(len(runIDs))+`)
		  AND status IN ('active', 'suspended')
		RETURNING run_id, session_id
	`, args...)
	if err != nil {
		return 0, fmt.Errorf("terminate sqlite active run sessions: %w", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var runID, sessionID string
		if err := rows.Scan(&runID, &sessionID); err != nil {
			return 0, err
		}
		if err := attempt.AddFact(runID, runforkrevision.FamilyAgentSessions, sessionID); err != nil {
			return 0, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *RunLifecycleSQLiteOwner) TerminateActiveSessionsTx(ctx context.Context, attempt *mutationprotocol.Attempt, runIDs []string, reason string, at time.Time) (int, error) {
	var count int
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		count, err = sqliteTerminateActiveRunSessionsTx(ctx, tx, attempt, runIDs, reason, at)
		return err
	})
	return count, err
}

func cancelActiveRunTimerFamiliesTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, postgres bool, runIDs []string, cause string, at time.Time) ([]runtimetimercancellation.Ref, error) {
	generic, err := storegenericschedule.CancelRunsTx(ctx, attempt, postgres, runIDs, cause, at)
	if err != nil {
		return nil, fmt.Errorf("cancel active generic schedules: %w", err)
	}
	workflow, err := cancelActiveRunWorkflowTimersTx(ctx, tx, attempt, postgres, runIDs)
	if err != nil {
		return nil, fmt.Errorf("cancel active workflow timers: %w", err)
	}
	return append(generic, workflow...), nil
}

func cancelActiveRunWorkflowTimersTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, postgres bool, runIDs []string) ([]runtimetimercancellation.Ref, error) {
	runIDs = normalizeQuiescenceRunIDs(runIDs)
	if len(runIDs) == 0 {
		return nil, nil
	}
	query := `SELECT CAST(timer_id AS TEXT), timer_name, fire_at, CAST(run_id AS TEXT)
		FROM timers WHERE run_id IN (` + sqlitePlaceholders(len(runIDs)) + `)
		AND task_type = 'workflow_timer' AND status = 'active' ORDER BY timer_id`
	args := make([]any, len(runIDs))
	for i, runID := range runIDs {
		args[i] = runID
	}
	if postgres {
		query = `SELECT timer_id::text, timer_name, fire_at, run_id::text
			FROM timers WHERE run_id = ANY($1::uuid[])
			AND task_type = 'workflow_timer' AND status = 'active' ORDER BY timer_id FOR UPDATE`
		args = []any{pq.Array(runIDs)}
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("lock active workflow timers: %w", err)
	}
	refs := make([]runtimetimercancellation.Ref, 0)
	for rows.Next() {
		var ref runtimetimercancellation.Ref
		var dueRaw any
		if err := rows.Scan(&ref.ActivationID, &ref.TaskID, &dueRaw, &ref.RunID); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan active workflow timer: %w", err)
		}
		var ok bool
		ref.DueAt, ok, err = sqliteTimeValue(dueRaw)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("decode active workflow timer due coordinate: %w", err)
		}
		if !ok {
			rows.Close()
			return nil, fmt.Errorf("active workflow timer %s has no due coordinate", ref.ActivationID)
		}
		ref.Family = runtimetimercancellation.FamilyWorkflowTimer
		if err := ref.Validate(); err != nil {
			rows.Close()
			return nil, err
		}
		refs = append(refs, ref.Canonical())
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, ref := range refs {
		update := `UPDATE timers SET status = 'cancelled' WHERE timer_id = ? AND task_type = 'workflow_timer' AND status = 'active'`
		if postgres {
			update = `UPDATE timers SET status = 'cancelled' WHERE timer_id = $1::uuid AND task_type = 'workflow_timer' AND status = 'active'`
		}
		result, err := tx.ExecContext(ctx, update, ref.ActivationID)
		if err != nil {
			return nil, fmt.Errorf("cancel workflow timer %s: %w", ref.ActivationID, err)
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return nil, fmt.Errorf("workflow timer %s cancellation changed %d rows: %w", ref.ActivationID, changed, err)
		}
		if err := attempt.AddFact(ref.RunID, runforkrevision.FamilyTimers, ref.ActivationID); err != nil {
			return nil, err
		}
	}
	return refs, nil
}

func upsertActiveRunQuiescenceRunControlTx(ctx context.Context, tx *sql.Tx, runID, reasonCode, controlledBy string, at time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES ($1::uuid, 'stopped', $2, $3, $4, NULL, $4)
		ON CONFLICT (run_id) DO UPDATE SET
			control_status = 'stopped',
			reason = $2,
			controlled_by = $3,
			updated_at = $4,
			paused_at = NULL,
			stopped_at = COALESCE(run_control_state.stopped_at, $4)
	`, runID, reasonCode, controlledBy, at.UTC()); err != nil {
		return fmt.Errorf("persist active run quiescence run control state: %w", err)
	}
	return nil
}

func (s *RunLifecyclePostgresOwner) UpsertQuiescedRunControlTx(ctx context.Context, tx *sql.Tx, runID, reasonCode, controlledBy string, at time.Time) error {
	return upsertActiveRunQuiescenceRunControlTx(ctx, tx, runID, reasonCode, controlledBy, at)
}

func sqliteUpsertActiveRunQuiescenceRunControlTx(ctx context.Context, tx *sql.Tx, runID, reasonCode, controlledBy string, at time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO run_control_state (run_id, control_status, reason, controlled_by, updated_at, paused_at, stopped_at)
		VALUES (?, 'stopped', ?, ?, ?, NULL, ?)
		ON CONFLICT(run_id) DO UPDATE SET
			control_status = 'stopped',
			reason = excluded.reason,
			controlled_by = excluded.controlled_by,
			updated_at = excluded.updated_at,
			paused_at = NULL,
			stopped_at = COALESCE(run_control_state.stopped_at, excluded.stopped_at)
	`, runID, reasonCode, controlledBy, at.UTC(), at.UTC()); err != nil {
		return fmt.Errorf("persist sqlite active run quiescence run control state: %w", err)
	}
	return nil
}

func (s *RunLifecyclePostgresOwner) ActiveRunDeliveryQuiesced(ctx context.Context, eventID string, route events.DeliveryRoute) (string, bool, error) {
	if s == nil || s.backend == nil {
		return "", false, fmt.Errorf("postgres store is required")
	}
	eventID = strings.TrimSpace(eventID)
	route = route.Normalized()
	routeIdentity, routeErr := route.Identity()
	if routeErr != nil {
		return "", false, fmt.Errorf("check postgres active run delivery quiescence route: %w", routeErr)
	}
	if eventID == "" {
		return "", false, nil
	}
	snapshots, err := s.delivery.DeliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return "", false, fmt.Errorf("check postgres active run delivery quiescence: %w", err)
	}
	return activeRunDeliveryQuiescenceReason(snapshots, routeIdentity)
}

func (s *RunLifecycleSQLiteOwner) ActiveRunDeliveryQuiesced(ctx context.Context, eventID string, route events.DeliveryRoute) (string, bool, error) {
	if s == nil || s.backend == nil {
		return "", false, fmt.Errorf("sqlite runtime store is required")
	}
	eventID = strings.TrimSpace(eventID)
	route = route.Normalized()
	routeIdentity, routeErr := route.Identity()
	if routeErr != nil {
		return "", false, fmt.Errorf("check sqlite active run delivery quiescence route: %w", routeErr)
	}
	if eventID == "" {
		return "", false, nil
	}
	snapshots, err := s.delivery.DeliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return "", false, fmt.Errorf("check sqlite active run delivery quiescence: %w", err)
	}
	return activeRunDeliveryQuiescenceReason(snapshots, routeIdentity)
}

func activeRunDeliveryQuiescenceReason(snapshots []runtimedelivery.Snapshot, routeIdentity events.DeliveryRouteIdentity) (string, bool, error) {
	reasons := map[string]struct{}{}
	for _, reason := range activeRunQuiescenceTerminalReasonCodes() {
		reasons[reason] = struct{}{}
	}
	matches := []string{}
	for _, snapshot := range snapshots {
		if snapshot.RouteIdentity != routeIdentity || !snapshot.Terminal() {
			continue
		}
		if _, ok := reasons[snapshot.ReasonCode]; ok {
			matches = append(matches, snapshot.ReasonCode)
		}
	}
	if len(matches) == 0 {
		return "", false, nil
	}
	sort.Strings(matches)
	return matches[0], true, nil
}

func (s *RunLifecycleSQLiteOwner) DestructiveResetDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("sqlite runtime store is required")
	}
	return s.deliveryQuiescedForReason(ctx, eventID, subscriberType, subscriberID, runtimedestructivereset.QuiescenceReasonCode)
}

func (s *RunLifecycleSQLiteOwner) ServeAbandonDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("sqlite runtime store is required")
	}
	return s.deliveryQuiescedForReason(ctx, eventID, subscriberType, subscriberID, runtimerunquiescence.ServeAbandonReasonCode)
}

func (s *RunLifecycleSQLiteOwner) deliveryQuiescedForReason(ctx context.Context, eventID, subscriberType, subscriberID, reasonCode string) (bool, error) {
	eventID = strings.TrimSpace(eventID)
	subscriberType = strings.TrimSpace(subscriberType)
	subscriberID = strings.TrimSpace(subscriberID)
	reasonCode = strings.TrimSpace(reasonCode)
	if eventID == "" || subscriberType == "" || subscriberID == "" || reasonCode == "" {
		return false, nil
	}
	snapshots, err := s.delivery.DeliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return false, fmt.Errorf("check sqlite delivery quiescence: %w", err)
	}
	for _, snapshot := range snapshots {
		if string(snapshot.SubscriberClass) == subscriberType && snapshot.SubscriberID == subscriberID && snapshot.Terminal() && snapshot.ReasonCode == reasonCode {
			return true, nil
		}
	}
	return false, nil
}

func sqlitePlaceholders(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.TrimRight(strings.Repeat("?,", count), ",")
}

func activeRunQuiescenceTerminalReasonCodes() []string {
	out := []string{
		runtimedestructivereset.QuiescenceReasonCode,
		runtimerunquiescence.ServeAbandonReasonCode,
	}
	return out
}
