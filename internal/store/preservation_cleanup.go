package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/preservationcleanup"
	"github.com/lib/pq"
)

type preservationCleanupRunTarget struct {
	RunID        string
	Status       string
	BundleSource string
}

type preservationCleanupSessionTarget struct {
	SessionID string
	RunID     string
	AgentID   string
	Status    string
}

type preservationCleanupTimerTarget struct {
	TimerID   string
	RunID     string
	TimerName string
	Status    string
}

func (s *PostgresStore) ApplyUnavailableBundleStartupPreservationCleanup(ctx context.Context, req preservationcleanup.Request) (preservationcleanup.Result, error) {
	return s.applyPreservationCleanup(ctx, req, preservationcleanup.UnavailableBundleStartupOperationName, preservationcleanup.UnavailableBundleStartupControlledBy)
}

func (s *PostgresStore) ApplyBundleForceDeletePreservationCleanup(ctx context.Context, req preservationcleanup.Request) (preservationcleanup.Result, error) {
	return s.applyPreservationCleanup(ctx, req, preservationcleanup.BundleForceDeleteOperationName, preservationcleanup.BundleForceDeleteControlledBy)
}

func (s *PostgresStore) applyPreservationCleanup(ctx context.Context, req preservationcleanup.Request, defaultOperationName, defaultControlledBy string) (preservationcleanup.Result, error) {
	if s == nil || s.DB == nil {
		return preservationcleanup.Result{}, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return preservationcleanup.Result{}, err
	}
	if err := requirePreservationCleanupCapabilities(caps); err != nil {
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

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return preservationcleanup.Result{}, fmt.Errorf("begin preservation cleanup tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

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

	deliveries, err := lockUnavailableBundlePreservationDeliveriesTx(ctx, tx, activeRunIDs)
	if err != nil {
		return preservationcleanup.Result{}, err
	}
	for _, delivery := range deliveries {
		target := targetByRun[delivery.RunID]
		out.Deliveries = append(out.Deliveries, preservationcleanup.DeliveryResult{
			DeliveryID:      delivery.DeliveryID,
			RunID:           delivery.RunID,
			EventID:         delivery.EventID,
			SubscriberType:  delivery.SubscriberType,
			SubscriberID:    delivery.SubscriberID,
			PreviousStatus:  delivery.Status,
			Status:          preservationcleanup.DeliveryOutcomeDeadLetter,
			ReasonCode:      target.ReasonCode,
			PreviousReason:  delivery.ReasonCode,
			ActiveSessionID: delivery.ActiveSessionID,
			Changed:         delivery.Status != preservationcleanup.DeliveryOutcomeDeadLetter || delivery.ReasonCode != target.ReasonCode,
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
	timers, err := lockUnavailableBundlePreservationTimersTx(ctx, tx, activeRunIDs)
	if err != nil {
		return preservationcleanup.Result{}, err
	}
	for _, timer := range timers {
		target := targetByRun[timer.RunID]
		out.Timers = append(out.Timers, preservationcleanup.TimerResult{
			TimerID:        timer.TimerID,
			RunID:          timer.RunID,
			TimerName:      timer.TimerName,
			PreviousStatus: timer.Status,
			Status:         preservationcleanup.TimerStatusCancelled,
			ReasonCode:     target.ReasonCode,
			Changed:        timer.Status != preservationcleanup.TimerStatusCancelled,
		})
	}

	eventReasons := map[string]string{}
	for _, delivery := range deliveries {
		target := targetByRun[delivery.RunID]
		if err := terminalizeActiveRunQuiescenceDeliveryTx(ctx, tx, delivery, target.ReasonCode, target.ReasonCode, now); err != nil {
			return preservationcleanup.Result{}, err
		}
		if delivery.EventID != "" {
			eventReasons[delivery.EventID] = target.ReasonCode
		}
	}
	for eventID, reason := range eventReasons {
		if err := upsertActiveRunQuiescencePipelineReceiptTx(ctx, tx, eventID, reason, reason, now); err != nil {
			return preservationcleanup.Result{}, err
		}
		out.PipelineReceiptCount++
	}
	for _, session := range sessions {
		target := targetByRun[session.RunID]
		if err := terminateUnavailableBundlePreservationSessionTx(ctx, tx, session.SessionID, target.ReasonCode, now); err != nil {
			return preservationcleanup.Result{}, err
		}
	}
	for _, timer := range timers {
		if err := cancelUnavailableBundlePreservationTimerTx(ctx, tx, timer.TimerID); err != nil {
			return preservationcleanup.Result{}, err
		}
	}
	for _, run := range runs {
		target := targetByRun[run.RunID]
		if err := markUnavailableBundlePreservationRunTx(ctx, tx, run.RunID, now); err != nil {
			return preservationcleanup.Result{}, err
		}
		if err := upsertActiveRunQuiescenceRunControlTx(ctx, tx, run.RunID, target.ReasonCode, controlledBy, now); err != nil {
			return preservationcleanup.Result{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return preservationcleanup.Result{}, fmt.Errorf("commit preservation cleanup tx: %w", err)
	}
	committed = true
	return out, nil
}

func requirePreservationCleanupCapabilities(caps StoreSchemaCapabilities) error {
	if !caps.Events.HasRuns || !caps.Events.RunTerminalFields || !caps.Events.RunBundleSource {
		return fmt.Errorf("unavailable bundle preservation cleanup requires runs terminal fields and bundle_source")
	}
	if caps.Events.Deliveries != SchemaFlavorCanonical || !caps.Events.DeliveryRunID {
		if caps.Events.Deliveries != SchemaFlavorCanonical {
			return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
		}
		return fmt.Errorf("unavailable bundle preservation cleanup requires canonical event_deliveries.run_id")
	}
	if caps.Events.Receipts != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical || !caps.Conversations.SessionRunID {
		if caps.Conversations.Sessions != SchemaFlavorCanonical {
			return unsupportedSchemaCapability("agent_sessions", caps.Conversations.Sessions)
		}
		return fmt.Errorf("unavailable bundle preservation cleanup requires canonical agent_sessions.run_id")
	}
	if caps.Schedules != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("timers/schedules", caps.Schedules)
	}
	return nil
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
		if err := rows.Scan(&item.RunID, &item.Status, &item.BundleSource); err != nil {
			return nil, fmt.Errorf("scan unavailable bundle preservation run: %w", err)
		}
		item.RunID = strings.TrimSpace(item.RunID)
		item.Status = strings.TrimSpace(item.Status)
		item.BundleSource = strings.TrimSpace(item.BundleSource)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read unavailable bundle preservation runs: %w", err)
	}
	return out, nil
}

func lockUnavailableBundlePreservationDeliveriesTx(ctx context.Context, tx *sql.Tx, runIDs []string) ([]activeRunQuiescenceDeliveryTarget, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			d.delivery_id::text,
			d.run_id::text,
			d.event_id::text,
			COALESCE(d.subscriber_type, ''),
			COALESCE(d.subscriber_id, ''),
			COALESCE(d.status, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.active_session_id::text, '')
		FROM event_deliveries d
		WHERE d.run_id = ANY($1::uuid[])
		  AND d.subscriber_type IN ('agent', 'node')
		  AND d.status IN ('pending', 'in_progress')
		ORDER BY d.run_id::text, d.event_id::text, d.subscriber_type, d.subscriber_id
		FOR UPDATE
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("lock unavailable bundle preservation deliveries: %w", err)
	}
	defer rows.Close()
	var out []activeRunQuiescenceDeliveryTarget
	for rows.Next() {
		var item activeRunQuiescenceDeliveryTarget
		if err := rows.Scan(&item.DeliveryID, &item.RunID, &item.EventID, &item.SubscriberType, &item.SubscriberID, &item.Status, &item.ReasonCode, &item.ActiveSessionID); err != nil {
			return nil, fmt.Errorf("scan unavailable bundle preservation delivery: %w", err)
		}
		item.DeliveryID = strings.TrimSpace(item.DeliveryID)
		item.RunID = strings.TrimSpace(item.RunID)
		item.EventID = strings.TrimSpace(item.EventID)
		item.SubscriberType = strings.TrimSpace(item.SubscriberType)
		item.SubscriberID = strings.TrimSpace(item.SubscriberID)
		item.Status = strings.TrimSpace(item.Status)
		item.ReasonCode = strings.TrimSpace(item.ReasonCode)
		item.ActiveSessionID = strings.TrimSpace(item.ActiveSessionID)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read unavailable bundle preservation deliveries: %w", err)
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

func lockUnavailableBundlePreservationTimersTx(ctx context.Context, tx *sql.Tx, runIDs []string) ([]preservationCleanupTimerTarget, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT timer_id::text, run_id::text, COALESCE(timer_name, ''), COALESCE(status, '')
		FROM timers
		WHERE run_id = ANY($1::uuid[])
		  AND status = 'active'
		ORDER BY run_id::text, timer_name, timer_id::text
		FOR UPDATE
	`, pq.Array(runIDs))
	if err != nil {
		return nil, fmt.Errorf("lock unavailable bundle preservation timers: %w", err)
	}
	defer rows.Close()
	var out []preservationCleanupTimerTarget
	for rows.Next() {
		var item preservationCleanupTimerTarget
		if err := rows.Scan(&item.TimerID, &item.RunID, &item.TimerName, &item.Status); err != nil {
			return nil, fmt.Errorf("scan unavailable bundle preservation timer: %w", err)
		}
		item.TimerID = strings.TrimSpace(item.TimerID)
		item.RunID = strings.TrimSpace(item.RunID)
		item.TimerName = strings.TrimSpace(item.TimerName)
		item.Status = strings.TrimSpace(item.Status)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read unavailable bundle preservation timers: %w", err)
	}
	return out, nil
}

func terminateUnavailableBundlePreservationSessionTx(ctx context.Context, tx *sql.Tx, sessionID, reasonCode string, at time.Time) error {
	if _, err := tx.ExecContext(ctx, `
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
	`, sessionID, preservationcleanup.SessionTerminationReasonOrphaned, reasonCode, at.UTC()); err != nil {
		return fmt.Errorf("terminate unavailable bundle preservation session %s: %w", sessionID, err)
	}
	return nil
}

func cancelUnavailableBundlePreservationTimerTx(ctx context.Context, tx *sql.Tx, timerID string) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE timers
		SET status = 'cancelled'
		WHERE timer_id = $1::uuid
		  AND status = 'active'
	`, timerID); err != nil {
		return fmt.Errorf("cancel unavailable bundle preservation timer %s: %w", timerID, err)
	}
	return nil
}

func markUnavailableBundlePreservationRunTx(ctx context.Context, tx *sql.Tx, runID string, at time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET
			status = 'cancelled',
			failure = NULL,
			ended_at = COALESCE(ended_at, $2)
		WHERE run_id = $1::uuid
		  AND lower(COALESCE(status, '')) IN ('running', 'paused')
	`, runID, at.UTC()); err != nil {
		return fmt.Errorf("mark unavailable bundle preservation run %s: %w", runID, err)
	}
	return nil
}
