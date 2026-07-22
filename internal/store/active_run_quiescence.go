package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimedestructivereset "github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

const activeRunQuiescencePipelineSubscriberID = "pipeline"

func (s *PostgresStore) ApplyServeAbandonActiveRunQuiescence(ctx context.Context, at time.Time) (runtimerunquiescence.Result, error) {
	return s.ApplyActiveRunQuiescence(ctx, runtimerunquiescence.Request{
		OperationName: runtimerunquiescence.ServeAbandonOperationName,
		RequestedAt:   at,
		AllActiveRuns: true,
		ReasonCode:    runtimerunquiescence.ServeAbandonReasonCode,
		ControlledBy:  runtimerunquiescence.ServeAbandonControlledBy,
		DeliveryNote:  runtimerunquiescence.ServeAbandonDeliveryNote,
	})
}

func (s *SQLiteRuntimeStore) ApplyServeAbandonActiveRunQuiescence(ctx context.Context, at time.Time) (runtimerunquiescence.Result, error) {
	return s.ApplyActiveRunQuiescence(ctx, runtimerunquiescence.Request{
		OperationName: runtimerunquiescence.ServeAbandonOperationName,
		RequestedAt:   at,
		AllActiveRuns: true,
		ReasonCode:    runtimerunquiescence.ServeAbandonReasonCode,
		ControlledBy:  runtimerunquiescence.ServeAbandonControlledBy,
		DeliveryNote:  runtimerunquiescence.ServeAbandonDeliveryNote,
	})
}

func (s *PostgresStore) ApplyActiveRunQuiescence(ctx context.Context, req runtimerunquiescence.Request) (runtimerunquiescence.Result, error) {
	if s == nil || s.DB == nil {
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
	if len(runIDs) == 0 && !req.AllActiveRuns {
		return out, nil
	}

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return runtimerunquiescence.Result{}, fmt.Errorf("begin active run quiescence tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	storyctx, err := runtimeauthoractivity.Begin(ctx, tx, runtimeauthoractivity.DialectPostgres)
	if err != nil {
		return runtimerunquiescence.Result{}, err
	}
	ctx = storyctx

	var runs []runtimerunquiescence.QuiescedRun
	if req.AllActiveRuns {
		runs, err = lockAllActiveQuiescenceRunsTx(ctx, tx)
	} else {
		runs, err = lockActiveRunQuiescenceRunsTx(ctx, tx, runIDs)
	}
	if err != nil {
		return runtimerunquiescence.Result{}, err
	}
	runIDs = quiescenceRunIDs(runs)
	if len(runIDs) == 0 {
		return out, nil
	}
	active := []runtimedelivery.Snapshot{}
	for _, runID := range runIDs {
		snapshots, err := s.activeRunDeliverySnapshotsTx(ctx, tx, runID)
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

	eventIDs := map[string]struct{}{}
	for _, runID := range runIDs {
		transitions, err := s.terminalizeRunDeliveriesTx(ctx, tx, runID, out.ReasonCode)
		if err != nil {
			return runtimerunquiescence.Result{}, err
		}
		for _, transition := range transitions {
			if transition.Current.EventID != "" {
				eventIDs[transition.Current.EventID] = struct{}{}
			}
		}
	}
	for eventID := range eventIDs {
		if err := upsertActiveRunQuiescencePipelineReceiptTx(ctx, tx, eventID, out.ReasonCode, deliveryNote, now); err != nil {
			return runtimerunquiescence.Result{}, err
		}
		out.PipelineReceiptCount++
	}
	out.SessionCount, err = terminateActiveRunSessionsTx(ctx, tx, runIDs, out.ReasonCode, now)
	if err != nil {
		return runtimerunquiescence.Result{}, err
	}
	out.TimerCount, err = cancelActiveRunTimersTx(ctx, tx, runIDs)
	if err != nil {
		return runtimerunquiescence.Result{}, err
	}
	for _, run := range runs {
		if !activeRunQuiescenceRunStatusActive(run.Status) {
			continue
		}
		if err := supersedeDecisionCardsForRun(ctx, tx, run.RunID, "run_quiesced", now, false, true); err != nil {
			return runtimerunquiescence.Result{}, err
		}
		opts := runLifecycleOptions()
		opts.BundleHash = run.BundleHash
		if _, err := storerunlifecycle.MarkTerminal(ctx, tx, run.RunID, "cancelled", nil, now, opts); err != nil {
			return runtimerunquiescence.Result{}, fmt.Errorf("mark active run quiescence run terminal: %w", err)
		}
		if err := upsertActiveRunQuiescenceRunControlTx(ctx, tx, run.RunID, out.ReasonCode, out.ControlledBy, now); err != nil {
			return runtimerunquiescence.Result{}, err
		}
	}
	changes := make([]runforkrevision.Change, 0, len(runIDs))
	for _, runID := range runIDs {
		changes = append(changes, runforkrevision.Change{RunID: runID, Families: []runforkrevision.Family{
			runforkrevision.FamilyEventDeliveries,
			runforkrevision.FamilyEventReceipts,
		}})
	}
	if len(active) > 0 {
		if _, err := runforkrevision.CaptureChanges(ctx, tx, changes...); err != nil {
			return runtimerunquiescence.Result{}, err
		}
	}
	if err := runtimeauthoractivity.Finalize(ctx); err != nil {
		return runtimerunquiescence.Result{}, err
	}
	if err := tx.Commit(); err != nil {
		return runtimerunquiescence.Result{}, fmt.Errorf("commit active run quiescence tx: %w", err)
	}
	committed = true
	return out, nil
}

func (s *SQLiteRuntimeStore) ApplyActiveRunQuiescence(ctx context.Context, req runtimerunquiescence.Request) (runtimerunquiescence.Result, error) {
	if s == nil || s.DB == nil {
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
	if len(requestedRunIDs) == 0 && !req.AllActiveRuns {
		return out, nil
	}

	baseOut := out
	if err := s.runAuthorActivityMutation(ctx, "sqlite active run quiescence", func(txctx context.Context, tx *sql.Tx) error {
		attemptOut := baseOut
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
		if len(attemptRunIDs) == 0 {
			out = attemptOut
			return nil
		}
		active := []runtimedelivery.Snapshot{}
		for _, runID := range attemptRunIDs {
			snapshots, err := s.activeRunDeliverySnapshotsTx(txctx, tx, runID)
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
			out = attemptOut
			return nil
		}

		eventIDs := map[string]struct{}{}
		for _, runID := range attemptRunIDs {
			transitions, err := s.terminalizeRunDeliveriesTx(txctx, tx, runID, attemptOut.ReasonCode)
			if err != nil {
				return err
			}
			for _, transition := range transitions {
				if transition.Current.EventID != "" {
					eventIDs[transition.Current.EventID] = struct{}{}
				}
			}
		}
		for eventID := range eventIDs {
			if err := sqliteUpsertActiveRunQuiescencePipelineReceiptTx(txctx, tx, eventID, attemptOut.ReasonCode, deliveryNote, now); err != nil {
				return err
			}
			attemptOut.PipelineReceiptCount++
		}
		attemptOut.SessionCount, err = sqliteTerminateActiveRunSessionsTx(txctx, tx, attemptRunIDs, attemptOut.ReasonCode, now)
		if err != nil {
			return err
		}
		attemptOut.TimerCount, err = sqliteCancelActiveRunTimersTx(txctx, tx, attemptRunIDs)
		if err != nil {
			return err
		}
		for _, run := range runs {
			if !activeRunQuiescenceRunStatusActive(run.Status) {
				continue
			}
			if _, err := s.sqliteMarkRunTerminalTx(txctx, tx, run.RunID, "cancelled", nil, now); err != nil {
				return err
			}
			if err := sqliteUpsertActiveRunQuiescenceRunControlTx(txctx, tx, run.RunID, attemptOut.ReasonCode, attemptOut.ControlledBy, now); err != nil {
				return err
			}
		}
		out = attemptOut
		return nil
	}); err != nil {
		return runtimerunquiescence.Result{}, err
	}
	return out, nil
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
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "running", "paused":
		return true
	default:
		return false
	}
}

func lockAllActiveQuiescenceRunsTx(ctx context.Context, tx *sql.Tx) ([]runtimerunquiescence.QuiescedRun, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT run_id::text, COALESCE(bundle_hash, ''), COALESCE(status, '')
		FROM runs
		WHERE lower(COALESCE(status, '')) IN ('running', 'paused')
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

func upsertActiveRunQuiescencePipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, reasonCode, note string, at time.Time) error {
	sideEffects, err := marshalPipelineReceiptSideEffects(newPipelineReceiptSideEffects("dead_letter", reasonCode))
	if err != nil {
		return fmt.Errorf("marshal active run quiescence pipeline receipt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id, 'platform', $2, e.entity_id, e.flow_instance,
			'dead_letter', $3, $4::jsonb, $5
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO NOTHING
	`, eventID, activeRunQuiescencePipelineSubscriberID, reasonCode, string(sideEffects), at.UTC()); err != nil {
		return fmt.Errorf("upsert active run quiescence pipeline receipt: %w", err)
	}
	return nil
}

func sqliteUpsertActiveRunQuiescencePipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, reasonCode, note string, at time.Time) error {
	sideEffects, err := marshalPipelineReceiptSideEffects(newPipelineReceiptSideEffects("dead_letter", reasonCode))
	if err != nil {
		return fmt.Errorf("marshal sqlite active run quiescence pipeline receipt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			?, e.event_id, 'platform', ?, e.entity_id, e.flow_instance,
			'dead_letter', ?, ?, ?
		FROM events e
		WHERE e.event_id = ?
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO NOTHING
	`, uuid.NewString(), activeRunQuiescencePipelineSubscriberID, reasonCode, string(sideEffects), at.UTC(), eventID); err != nil {
		return fmt.Errorf("upsert sqlite active run quiescence pipeline receipt: %w", err)
	}
	return nil
}

func terminateActiveRunSessionsTx(ctx context.Context, tx *sql.Tx, runIDs []string, reason string, at time.Time) (int, error) {
	result, err := tx.ExecContext(ctx, `
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
	`, pq.Array(runIDs), reason, at.UTC())
	if err != nil {
		return 0, fmt.Errorf("terminate active run sessions: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func cancelActiveRunTimersTx(ctx context.Context, tx *sql.Tx, runIDs []string) (int, error) {
	result, err := tx.ExecContext(ctx, `
		UPDATE timers
		SET status = 'cancelled'
		WHERE run_id = ANY($1::uuid[])
		  AND status = 'active'
	`, pq.Array(runIDs))
	if err != nil {
		return 0, fmt.Errorf("cancel active run timers: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func sqliteTerminateActiveRunSessionsTx(ctx context.Context, tx *sql.Tx, runIDs []string, reason string, at time.Time) (int, error) {
	args := make([]any, 0, len(runIDs)+3)
	args = append(args, reason, at.UTC(), at.UTC())
	for _, runID := range runIDs {
		args = append(args, runID)
	}
	result, err := tx.ExecContext(ctx, `
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
	`, args...)
	if err != nil {
		return 0, fmt.Errorf("terminate sqlite active run sessions: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(count), nil
}

func sqliteCancelActiveRunTimersTx(ctx context.Context, tx *sql.Tx, runIDs []string) (int, error) {
	args := make([]any, 0, len(runIDs))
	for _, runID := range runIDs {
		args = append(args, runID)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE timers
		SET status = 'cancelled'
		WHERE run_id IN (`+sqlitePlaceholders(len(runIDs))+`)
		  AND status = 'active'
	`, args...)
	if err != nil {
		return 0, fmt.Errorf("cancel sqlite active run timers: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(count), nil
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

func sqliteMarkActiveRunQuiescenceRunTerminalTx(ctx context.Context, tx *sql.Tx, runID string, at time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = 'cancelled',
		    failure = NULL,
		    ended_at = COALESCE(ended_at, ?)
		WHERE run_id = ?
		  AND (status IN ('running', 'paused') OR status = 'cancelled')
	`, at.UTC(), runID); err != nil {
		return fmt.Errorf("mark sqlite active run quiescence run terminal: %w", err)
	}
	return nil
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

func (s *PostgresStore) ActiveRunDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (string, bool, error) {
	if s == nil || s.DB == nil {
		return "", false, fmt.Errorf("postgres store is required")
	}
	eventID = strings.TrimSpace(eventID)
	subscriberType = strings.TrimSpace(subscriberType)
	subscriberID = strings.TrimSpace(subscriberID)
	if eventID == "" || subscriberType == "" || subscriberID == "" {
		return "", false, nil
	}
	snapshots, err := s.deliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return "", false, fmt.Errorf("check postgres active run delivery quiescence: %w", err)
	}
	return activeRunDeliveryQuiescenceReason(snapshots, subscriberType, subscriberID)
}

func (s *SQLiteRuntimeStore) ActiveRunDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (string, bool, error) {
	if s == nil || s.DB == nil {
		return "", false, fmt.Errorf("sqlite runtime store is required")
	}
	eventID = strings.TrimSpace(eventID)
	subscriberType = strings.TrimSpace(subscriberType)
	subscriberID = strings.TrimSpace(subscriberID)
	if eventID == "" || subscriberType == "" || subscriberID == "" {
		return "", false, nil
	}
	snapshots, err := s.deliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return "", false, fmt.Errorf("check sqlite active run delivery quiescence: %w", err)
	}
	return activeRunDeliveryQuiescenceReason(snapshots, subscriberType, subscriberID)
}

func activeRunDeliveryQuiescenceReason(snapshots []runtimedelivery.Snapshot, subscriberType, subscriberID string) (string, bool, error) {
	reasons := map[string]struct{}{}
	for _, reason := range activeRunQuiescenceTerminalReasonCodes() {
		reasons[reason] = struct{}{}
	}
	matches := []string{}
	for _, snapshot := range snapshots {
		if string(snapshot.SubscriberClass) != subscriberType || snapshot.SubscriberID != subscriberID || !snapshot.Terminal() {
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

func (s *SQLiteRuntimeStore) DestructiveResetDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (bool, error) {
	if s == nil || s.DB == nil {
		return false, fmt.Errorf("sqlite runtime store is required")
	}
	return s.deliveryQuiescedForReason(ctx, eventID, subscriberType, subscriberID, runtimedestructivereset.QuiescenceReasonCode)
}

func (s *SQLiteRuntimeStore) ServeAbandonDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (bool, error) {
	if s == nil || s.DB == nil {
		return false, fmt.Errorf("sqlite runtime store is required")
	}
	return s.deliveryQuiescedForReason(ctx, eventID, subscriberType, subscriberID, runtimerunquiescence.ServeAbandonReasonCode)
}

func (s *SQLiteRuntimeStore) deliveryQuiescedForReason(ctx context.Context, eventID, subscriberType, subscriberID, reasonCode string) (bool, error) {
	eventID = strings.TrimSpace(eventID)
	subscriberType = strings.TrimSpace(subscriberType)
	subscriberID = strings.TrimSpace(subscriberID)
	reasonCode = strings.TrimSpace(reasonCode)
	if eventID == "" || subscriberType == "" || subscriberID == "" || reasonCode == "" {
		return false, nil
	}
	snapshots, err := s.deliverySnapshotsForEvent(ctx, eventID)
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
	out = append(out, preservationcleanup.TerminalReasonCodes()...)
	return out
}
