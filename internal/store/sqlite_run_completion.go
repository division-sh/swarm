package store

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
)

type sqliteRunCompletionDBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *SQLiteRuntimeStore) LoadRunLifecycleSnapshot(ctx context.Context, runID string) (runtimebus.RunLifecycleSnapshot, error) {
	if s == nil || s.DB == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("sqlite runtime store is required")
	}
	snap, err := s.sqliteLoadRunLifecycleSnapshot(ctx, s.DB, runID)
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	return runtimebus.RunLifecycleSnapshot{
		RunID:       snap.RunID,
		Status:      snap.Status,
		EventCount:  snap.EventCount,
		EntityCount: snap.EntityCount,
		Failure:     runtimefailures.CloneEnvelope(snap.Failure),
		StartedAt:   snap.StartedAt,
		EndedAt:     snap.EndedAt,
	}, nil
}

func (s *SQLiteRuntimeStore) MarkRunTerminal(ctx context.Context, runID, status string, failure *runtimefailures.Envelope, endedAt time.Time) (runtimebus.RunLifecycleSnapshot, error) {
	if s == nil || s.DB == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	status, err := canonicalRunTerminalStatus(status)
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	if status == "completed" {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("completed run terminalization is owned by normal run completion convergence")
	}
	var snap storerunlifecycle.Snapshot
	err = s.runAuthorActivityMutation(ctx, "sqlite mark run terminal", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		snap, err = s.sqliteMarkRunTerminalTx(txctx, tx, runID, status, failure, endedAt)
		return err
	})
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	return runtimebus.RunLifecycleSnapshot{
		RunID:       snap.RunID,
		Status:      snap.Status,
		EventCount:  snap.EventCount,
		EntityCount: snap.EntityCount,
		Failure:     runtimefailures.CloneEnvelope(snap.Failure),
		StartedAt:   snap.StartedAt,
		EndedAt:     snap.EndedAt,
	}, nil
}

func (s *SQLiteRuntimeStore) ConvergeStandaloneRuntimePlatformRun(ctx context.Context, evt events.Event) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	eventID := sanitizeOptionalUUID(strings.TrimSpace(evt.ID()))
	if eventID == "" {
		return nil
	}
	return s.runAuthorActivityMutation(ctx, "sqlite standalone platform run convergence", func(txctx context.Context, tx *sql.Tx) error {
		return s.sqliteConvergeStandaloneRuntimePlatformRunByEventIDTx(txctx, tx, eventID)
	})
}

func (s *SQLiteRuntimeStore) sqliteConvergeStandaloneRuntimePlatformRunByEventIDTx(ctx context.Context, tx *sql.Tx, eventID string) error {
	rec, found, err := sqliteLoadStandaloneRuntimePlatformRunRecord(ctx, tx, eventID)
	if err != nil || !found || !isStandaloneRuntimePlatformRunRecord(rec) {
		return err
	}
	switch strings.TrimSpace(rec.RunStatus) {
	case "completed":
		return nil
	case "failed", "cancelled", "forked":
		return fmt.Errorf("standalone runtime platform run %s already terminal with status %s", rec.RunID, strings.TrimSpace(rec.RunStatus))
	}
	summary, err := sqliteDeliveryAdapter.SummarizeRun(ctx, tx, rec.RunID)
	if err != nil {
		return err
	}
	if !summary.Settled() {
		return nil
	}
	_, err = s.sqliteMarkRunTerminalTx(ctx, tx, rec.RunID, "completed", nil, s.now())
	return err
}

func (s *SQLiteRuntimeStore) ConvergeNormalRunCompletion(ctx context.Context, eventID string, workflowTerminalStates []string, flowTerminalStates map[string][]string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	eventID = sanitizeOptionalUUID(eventID)
	if eventID == "" {
		return nil
	}
	workflowTerminals := normalRunCompletionStateSet(workflowTerminalStates)
	flowTerminals := normalRunCompletionFlowStateSets(flowTerminalStates)
	if len(workflowTerminals) == 0 && len(flowTerminals) == 0 {
		return nil
	}
	if err := s.requireCurrentSchema(); err != nil {
		return err
	}
	return s.runAuthorActivityMutation(ctx, "sqlite normal run completion", func(txctx context.Context, tx *sql.Tx) error {
		candidate, found, err := sqliteNormalRunCompletionCandidateTx(txctx, tx, eventID)
		if err != nil || !found {
			return err
		}
		switch candidate.Status {
		case "completed":
			return nil
		case "running":
		default:
			return nil
		}
		if platformRec, platformFound, err := sqliteLoadStandaloneRuntimePlatformRunRecord(txctx, tx, eventID); err != nil {
			return err
		} else if platformFound && isStandaloneRuntimePlatformRunRecord(platformRec) {
			return nil
		}
		ready, err := s.sqliteNormalRunCompletionRunReadyTx(txctx, tx, candidate.RunID, workflowTerminals, flowTerminals)
		if err != nil || !ready {
			return err
		}
		_, err = s.sqliteMarkRunTerminalTx(txctx, tx, candidate.RunID, "completed", nil, s.now())
		return err
	})
}

func (s *SQLiteRuntimeStore) sqliteLoadRunLifecycleSnapshot(ctx context.Context, q execQueryer, runID string) (storerunlifecycle.Snapshot, error) {
	runID = nullUUIDString(runID)
	if s == nil || q == nil || runID == "" {
		return storerunlifecycle.Snapshot{}, fmt.Errorf("run_id is required")
	}
	var (
		snap       storerunlifecycle.Snapshot
		failureRaw sql.NullString
		startedAt  any
		endedAt    any
	)
	err := q.QueryRowContext(ctx, `
		SELECT
			run_id,
			LOWER(COALESCE(status, '')),
			COALESCE(bundle_hash, ''),
			COALESCE(event_count, 0),
			COALESCE((SELECT COUNT(DISTINCT es.entity_id) FROM entity_state es WHERE es.run_id = runs.run_id), 0),
			failure,
			started_at,
			ended_at
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(
		&snap.RunID,
		&snap.Status,
		&snap.BundleHash,
		&snap.EventCount,
		&snap.EntityCount,
		&failureRaw,
		&startedAt,
		&endedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return storerunlifecycle.Snapshot{}, fmt.Errorf("run %s not found", runID)
		}
		return storerunlifecycle.Snapshot{}, fmt.Errorf("load sqlite run snapshot: %w", err)
	}
	if at, ok, err := sqliteTimeValue(startedAt); err != nil {
		return storerunlifecycle.Snapshot{}, err
	} else if ok {
		snap.StartedAt = at
	}
	if at, ok, err := sqliteTimeValue(endedAt); err != nil {
		return storerunlifecycle.Snapshot{}, err
	} else if ok {
		snap.EndedAt = &at
	}
	snap.RunID = strings.TrimSpace(snap.RunID)
	snap.Status = strings.TrimSpace(strings.ToLower(snap.Status))
	snap.BundleHash = strings.TrimSpace(snap.BundleHash)
	if failureRaw.Valid && strings.TrimSpace(failureRaw.String) != "" {
		failure, err := runtimefailures.UnmarshalEnvelope([]byte(failureRaw.String))
		if err != nil {
			return storerunlifecycle.Snapshot{}, fmt.Errorf("load sqlite run snapshot failure: %w", err)
		}
		snap.Failure = &failure
	}
	if err := storerunlifecycle.ValidateStatusFailure(snap.Status, snap.Failure); err != nil {
		return storerunlifecycle.Snapshot{}, fmt.Errorf("load sqlite run snapshot: %w", err)
	}
	return snap, nil
}

func (s *SQLiteRuntimeStore) sqliteMarkRunTerminalTx(ctx context.Context, tx *sql.Tx, runID, status string, failure *runtimefailures.Envelope, endedAt time.Time) (storerunlifecycle.Snapshot, error) {
	if tx == nil {
		return storerunlifecycle.Snapshot{}, fmt.Errorf("sqlite run terminal tx is required")
	}
	runID = nullUUIDString(runID)
	if runID == "" {
		return storerunlifecycle.Snapshot{}, fmt.Errorf("run_id is required")
	}
	if err := runtimeauthoractivity.Require(ctx); err != nil {
		return storerunlifecycle.Snapshot{}, fmt.Errorf("mark sqlite run terminal: %w", err)
	}
	var err error
	status, err = canonicalRunTerminalStatus(status)
	if err != nil {
		return storerunlifecycle.Snapshot{}, err
	}
	if err := storerunlifecycle.ValidateStatusFailure(status, failure); err != nil {
		return storerunlifecycle.Snapshot{}, err
	}
	occurrenceScope, err := sqliteRunBundleScope(ctx, tx, runID)
	if err != nil {
		return storerunlifecycle.Snapshot{}, fmt.Errorf("mark sqlite run terminal: %w", err)
	}
	var failureJSON any
	if failure != nil {
		raw, err := runtimefailures.MarshalEnvelope(*failure)
		if err != nil {
			return storerunlifecycle.Snapshot{}, fmt.Errorf("mark sqlite run terminal failure: %w", err)
		}
		failureJSON = string(raw)
	}
	if endedAt.IsZero() {
		endedAt = s.now()
	}
	if err := sqliteSyncRunCounts(ctx, tx, runID); err != nil {
		return storerunlifecycle.Snapshot{}, err
	}
	if status == "completed" {
		summary, err := sqliteDeliveryAdapter.SummarizeRun(ctx, tx, runID)
		if err != nil {
			return storerunlifecycle.Snapshot{}, err
		}
		if !summary.Settled() {
			return storerunlifecycle.Snapshot{}, fmt.Errorf("run %s still has active deliveries", runID)
		}
	} else {
		if _, err := sqliteDeliveryAdapter.TerminalizeRun(ctx, tx, runID, "run_"+status); err != nil {
			return storerunlifecycle.Snapshot{}, err
		}
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE runs
		SET status = ?,
		    failure = ?,
		    ended_at = COALESCE(ended_at, ?)
		WHERE run_id = ?
		  AND (status IN ('running', 'paused') OR (status = ? AND failure IS ?))
	`, status, failureJSON, endedAt.UTC(), runID, status, failureJSON)
	if err != nil {
		return storerunlifecycle.Snapshot{}, fmt.Errorf("mark sqlite run terminal: %w", err)
	}
	if rows, err := result.RowsAffected(); err == nil && rows == 0 {
		current, loadErr := s.sqliteLoadRunLifecycleSnapshot(ctx, tx, runID)
		if loadErr != nil {
			return storerunlifecycle.Snapshot{}, loadErr
		}
		if current.Status != status {
			return storerunlifecycle.Snapshot{}, fmt.Errorf("run %s already terminal with status %s", runID, current.Status)
		}
		if !sameSQLiteRunFailure(current.Failure, failure) {
			return storerunlifecycle.Snapshot{}, fmt.Errorf("run %s already terminal with conflicting failure", runID)
		}
		if err := supersedeDecisionCardsForRun(ctx, tx, runID, "run_"+status, endedAt, false, false); err != nil {
			return storerunlifecycle.Snapshot{}, err
		}
		return current, nil
	}
	if err := supersedeDecisionCardsForRun(ctx, tx, runID, "run_"+status, endedAt, false, false); err != nil {
		return storerunlifecycle.Snapshot{}, err
	}
	snapshot, err := s.sqliteLoadRunLifecycleSnapshot(ctx, tx, runID)
	if err != nil {
		return storerunlifecycle.Snapshot{}, err
	}
	occurredAt := endedAt.UTC()
	if snapshot.EndedAt != nil {
		occurredAt = snapshot.EndedAt.UTC()
	}
	if err := runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindRunLifecycle, Transition: status,
		SourceOwner: "runs", SourceIdentity: runID + ":" + status, DedupKey: "run-terminal:" + runID + ":" + status,
		OccurredAt: occurredAt, RunID: runID, Scope: occurrenceScope, Failure: failure,
		Projection: runtimeauthoractivity.Projection{SubjectType: "run", SubjectID: runID},
	}); err != nil {
		return storerunlifecycle.Snapshot{}, err
	}
	return snapshot, nil
}

func sqliteRunBundleScope(ctx context.Context, q execQueryer, runID string) (runtimeauthoractivity.Scope, error) {
	var bundleHash string
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(bundle_hash, '') FROM runs WHERE run_id = ?`, runID).Scan(&bundleHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimeauthoractivity.Scope{}, fmt.Errorf("run %s not found", runID)
		}
		return runtimeauthoractivity.Scope{}, fmt.Errorf("load source-owned run bundle_hash: %w", err)
	}
	return runtimeauthoractivity.BundleScopeForSource(ctx, bundleHash)
}

func sameSQLiteRunFailure(left, right *runtimefailures.Envelope) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftRaw, leftErr := runtimefailures.MarshalEnvelope(*left)
	rightRaw, rightErr := runtimefailures.MarshalEnvelope(*right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

func sqliteSyncRunCounts(ctx context.Context, q execQueryer, runID string) error {
	runID = nullUUIDString(runID)
	if q == nil || runID == "" {
		return nil
	}
	_, err := q.ExecContext(ctx, `
		UPDATE runs
		SET
			event_count = (SELECT COUNT(*) FROM events WHERE run_id = ?),
			entity_count = (SELECT COUNT(DISTINCT entity_id) FROM entity_state WHERE run_id = ?)
		WHERE run_id = ?
	`, runID, runID, runID)
	if err != nil {
		return fmt.Errorf("sync sqlite run counters: %w", err)
	}
	return nil
}

func sqliteNormalRunCompletionCandidateTx(ctx context.Context, tx *sql.Tx, eventID string) (normalRunCompletionCandidate, bool, error) {
	var candidate normalRunCompletionCandidate
	err := tx.QueryRowContext(ctx, `
		SELECT
			r.run_id,
			LOWER(COALESCE(r.status, ''))
		FROM events e
		INNER JOIN runs r ON r.run_id = e.run_id
		WHERE e.event_id = ?
	`, eventID).Scan(&candidate.RunID, &candidate.Status)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return normalRunCompletionCandidate{}, false, nil
	case err != nil:
		return normalRunCompletionCandidate{}, false, fmt.Errorf("load sqlite normal run completion candidate: %w", err)
	default:
		candidate.RunID = strings.TrimSpace(candidate.RunID)
		candidate.Status = strings.TrimSpace(candidate.Status)
		return candidate, candidate.RunID != "", nil
	}
}

func (s *SQLiteRuntimeStore) sqliteNormalRunCompletionRunReadyTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	workflowTerminals map[string]struct{},
	flowTerminals map[string]map[string]struct{},
) (bool, error) {
	runID = nullUUIDString(runID)
	if runID == "" {
		return false, nil
	}
	checks := []func(context.Context, *sql.Tx, string) (bool, error){
		sqliteNormalRunCompletionPipelinesSettledTx,
		sqliteNormalRunCompletionDeliveriesSettledTx,
		sqliteNormalRunCompletionTimersSettledTx,
		s.sqliteNormalRunCompletionSessionLeasesSettledTx,
		sqliteNormalRunCompletionHumanTasksSettledTx,
		sqliteNormalRunCompletionProposedEffectsSettledTx,
		sqliteNormalRunCompletionGateObligationsSettledTx,
	}
	for _, check := range checks {
		ready, err := check(ctx, tx, runID)
		if err != nil || !ready {
			return ready, err
		}
	}
	return sqliteNormalRunCompletionEntitiesTerminalTx(ctx, tx, runID, workflowTerminals, flowTerminals)
}

func sqliteNormalRunCompletionProposedEffectsSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var unresolved bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM decision_cards c
			LEFT JOIN proposed_effect_continuations p ON p.card_id = c.card_id
			WHERE c.run_id = ?
			  AND c.anchor_kind = 'proposed_effect'
			  AND (
				p.card_id IS NULL
				OR p.run_id <> c.run_id
				OR (
					c.status = 'decided'
					AND (
						p.state NOT IN ('request_released', 'outcome_dispatched')
						OR p.decision_event_id IS NULL
						OR p.route_event_id IS NULL
						OR c.decision_event_id IS NULL
						OR c.decision_event_id <> p.decision_event_id
						OR p.route_event_id <> p.decision_event_id
						OR NOT EXISTS (
							SELECT 1 FROM events e
							WHERE e.run_id = c.run_id
							  AND (
								(p.verdict = 'approve' AND e.event_id = p.request_event_id AND e.event_name = 'platform.activity_requested')
								OR (p.verdict = 'revise' AND e.source_event_id = p.decision_event_id AND e.event_name = json_extract(p.effect, '$.revision_event'))
								OR (p.verdict = 'reject' AND e.source_event_id = p.decision_event_id AND e.event_name = json_extract(p.effect, '$.rejected_event'))
							  )
						)
					)
				)
				OR (c.status = 'superseded' AND p.state <> 'superseded')
				OR c.status NOT IN ('decided', 'superseded')
			  )
		)
	`, runID).Scan(&unresolved); err != nil {
		return false, fmt.Errorf("check sqlite normal run proposed-effect settlement: %w", err)
	}
	if unresolved {
		return false, nil
	}
	return normalRunCompletionProposedEffectOutcomeEventsPersistedTx(ctx, tx, runID, false)
}

func sqliteNormalRunCompletionHumanTasksSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var unresolved bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM decision_cards c
			LEFT JOIN human_task_continuations h ON h.card_id = c.card_id
			WHERE c.run_id = ?
			  AND c.anchor_kind = 'human_task'
			  AND (
				h.card_id IS NULL
				OR h.run_id <> c.run_id
				OR h.state <> 'outcome_dispatched'
				OR h.outcome_event_id IS NULL
				OR c.status NOT IN ('decided', 'expired')
				OR (c.status = 'decided' AND (c.decision_event_id IS NULL OR c.decision_event_id <> h.outcome_event_id))
				OR NOT EXISTS (
					SELECT 1 FROM events e
					WHERE e.run_id = c.run_id
					  AND e.source_event_id = h.outcome_event_id
					  AND e.event_name = CASE
						WHEN c.status = 'expired' THEN 'human_task.expired'
						WHEN c.verdict = 'approve' THEN 'human_task.approved'
						WHEN c.verdict = 'reject' THEN 'human_task.rejected'
						ELSE ''
					  END
				)
			  )
		)
	`, runID).Scan(&unresolved); err != nil {
		return false, fmt.Errorf("check sqlite normal run human-task settlement: %w", err)
	}
	if unresolved {
		return false, nil
	}
	return normalRunCompletionHumanTaskOutcomeEventsPersistedTx(ctx, tx, runID, false)
}

func sqliteNormalRunCompletionGateObligationsSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var active bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM entity_state es, json_each(COALESCE(json_extract(es.accumulator, '$.stage_gates'), '{}')) gate
			WHERE es.run_id = ?
			  AND COALESCE(json_extract(gate.value, '$.status'), '') IN ('open', 'decision_committed')
		)
	`, runID).Scan(&active); err != nil {
		return false, fmt.Errorf("check sqlite normal run gate obligations: %w", err)
	}
	return !active, nil
}

func sqliteNormalRunCompletionPipelinesSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var unsettled bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM events e
			LEFT JOIN event_receipts r
				ON r.event_id = e.event_id
				AND r.subscriber_type = 'platform'
				AND r.subscriber_id = 'pipeline'
			WHERE e.run_id = ?
			  AND e.event_name <> ?
			  AND (r.event_id IS NULL OR COALESCE(r.outcome, '') <> 'success')
		)
	`, runID, runtimeLogEventName).Scan(&unsettled); err != nil {
		return false, fmt.Errorf("check sqlite normal run pipeline settlement: %w", err)
	}
	return !unsettled, nil
}

func sqliteNormalRunCompletionDeliveriesSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	summary, err := sqliteDeliveryAdapter.SummarizeRun(ctx, tx, runID)
	if err != nil {
		return false, err
	}
	return summary.Settled(), nil
}

func sqliteNormalRunCompletionTimersSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var active bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM timers
			WHERE run_id = ?
			  AND status = 'active'
		)
	`, runID).Scan(&active); err != nil {
		return false, fmt.Errorf("check sqlite normal run active timers: %w", err)
	}
	return !active, nil
}

func (s *SQLiteRuntimeStore) sqliteNormalRunCompletionSessionLeasesSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT lease_expires_at
		FROM agent_sessions
		WHERE run_id = ?
		  AND status = 'active'
		  AND lease_holder IS NOT NULL
		  AND lease_expires_at IS NOT NULL
	`, runID)
	if err != nil {
		return false, fmt.Errorf("check sqlite normal run active session leases: %w", err)
	}
	defer rows.Close()
	now := s.now()
	for rows.Next() {
		var raw any
		if err := rows.Scan(&raw); err != nil {
			return false, fmt.Errorf("scan sqlite session lease expiry: %w", err)
		}
		expiresAt, ok, err := sqliteTimeValue(raw)
		if err != nil {
			return false, err
		}
		if ok && expiresAt.After(now) {
			return false, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read sqlite session lease expiries: %w", err)
	}
	return true, nil
}

func sqliteNormalRunCompletionEntitiesTerminalTx(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	workflowTerminals map[string]struct{},
	flowTerminals map[string]map[string]struct{},
) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT
			LOWER(COALESCE(es.current_state, '')),
			COALESCE(es.flow_instance, ''),
			COALESCE(fi.flow_template, '')
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = ?
	`, runID)
	if err != nil {
		return false, fmt.Errorf("load sqlite normal run entity terminality: %w", err)
	}
	defer rows.Close()
	seen := false
	for rows.Next() {
		seen = true
		var state, flowInstance, flowTemplate string
		if err := rows.Scan(&state, &flowInstance, &flowTemplate); err != nil {
			return false, fmt.Errorf("scan sqlite normal run entity terminality: %w", err)
		}
		terminals, ok := normalRunCompletionTerminalSetForEntity(flowTemplate, flowInstance, workflowTerminals, flowTerminals)
		if !ok {
			return false, nil
		}
		if _, ok := terminals[strings.TrimSpace(strings.ToLower(state))]; !ok {
			return false, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read sqlite normal run entity terminality: %w", err)
	}
	return seen, nil
}

func sqliteLoadStandaloneRuntimePlatformRunRecord(ctx context.Context, q sqliteRunCompletionDBTX, eventID string) (standaloneRuntimePlatformRunRecord, bool, error) {
	eventID = sanitizeOptionalUUID(eventID)
	if q == nil || eventID == "" {
		return standaloneRuntimePlatformRunRecord{}, false, nil
	}
	durable, found, err := loadSQLiteEventIdentity(ctx, q, eventID)
	if err != nil || !found {
		return standaloneRuntimePlatformRunRecord{}, found, err
	}
	admitted, err := decodeEventRecord(durable)
	if err != nil {
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("decode sqlite standalone runtime platform event: %w", err)
	}
	event := admitted.Event()
	rec := standaloneRuntimePlatformRunRecord{
		RunID: event.RunID(), EventID: event.ID(), EventClass: string(event.AdmissionClass()),
		EventType: string(event.Type()), ProducedBy: event.SourceAgent(), ProducedByType: string(event.ProducerType()),
		SourceEventID: event.ParentEventID(),
	}
	err = q.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(trigger_event_id, ''), COALESCE(trigger_event_type, '')
		FROM runs WHERE run_id = ?
	`, rec.RunID).Scan(&rec.RunStatus, &rec.TriggerEventID, &rec.TriggerEventType)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return standaloneRuntimePlatformRunRecord{}, false, nil
	case err != nil:
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("load sqlite standalone runtime platform run candidate: %w", err)
	default:
		return rec, true, nil
	}
}
