package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

func (s *SQLiteRuntimeStore) requireRunHeaderAccess() error {
	return s.requireCurrentSchema()
}

func (s *SQLiteRuntimeStore) LoadRunHeader(ctx context.Context, runID string) (RunHeader, error) {
	if err := s.requireRunHeaderAccess(); err != nil {
		return RunHeader{}, err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return RunHeader{}, ErrRunNotFound
	}
	if _, err := uuid.Parse(runID); err != nil {
		return RunHeader{}, ErrRunNotFound
	}
	row := s.DB.QueryRowContext(ctx, sqliteRunHeaderSelectSQL()+`
WHERE r.run_id = ?
`, runID)
	header, err := scanSQLiteRunHeader(row)
	if errors.Is(err, sql.ErrNoRows) {
		return RunHeader{}, ErrRunNotFound
	}
	if err != nil {
		return RunHeader{}, err
	}
	if strings.TrimSpace(header.TriggerEventID) == "" || strings.TrimSpace(header.TriggerEventType) == "" {
		return RunHeader{}, fmt.Errorf("run %s is missing trigger event identity", runID)
	}
	return header, nil
}

func (s *SQLiteRuntimeStore) ListRunHeaders(ctx context.Context, opts RunHeaderListOptions) ([]RunHeader, string, error) {
	if err := s.requireRunHeaderAccess(); err != nil {
		return nil, "", err
	}
	opts = defaultRunHeaderListOptions(opts)
	args := make([]any, 0, 6)
	where := []string{"(NULLIF(r.trigger_event_id, '') IS NOT NULL OR EXISTS (SELECT 1 FROM events e WHERE e.run_id = r.run_id))"}
	if opts.Status != "" {
		args = append(args, opts.Status)
		where = append(where, "lower(r.status) = ?")
	}
	if opts.BundleHash != "" {
		args = append(args, opts.BundleHash)
		where = append(where, "r.bundle_hash = ?")
	}
	if opts.Since != nil {
		args = append(args, opts.Since.UTC())
		where = append(where, "r.started_at >= ?")
	}
	if opts.Until != nil {
		args = append(args, opts.Until.UTC())
		where = append(where, "r.started_at <= ?")
	}
	if opts.Cursor != "" {
		startedAt, runID, err := decodeRunHeaderCursor(opts.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, startedAt.UTC(), startedAt.UTC(), runID)
		where = append(where, "(r.started_at < ? OR (r.started_at = ? AND r.run_id < ?))")
	}
	args = append(args, opts.Limit+1)
	rows, err := s.DB.QueryContext(ctx, sqliteRunHeaderSelectSQL()+`
WHERE `+strings.Join(where, " AND ")+`
ORDER BY r.started_at DESC, r.run_id DESC
LIMIT ?
`, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	headers := make([]RunHeader, 0, opts.Limit)
	for rows.Next() {
		header, err := scanSQLiteRunHeader(rows)
		if err != nil {
			return nil, "", err
		}
		if strings.TrimSpace(header.TriggerEventID) == "" || strings.TrimSpace(header.TriggerEventType) == "" {
			return nil, "", fmt.Errorf("run %s is missing trigger event identity", header.RunID)
		}
		headers = append(headers, header)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(headers) > opts.Limit {
		headers = headers[:opts.Limit]
		nextCursor = encodeRunHeaderCursor(headers[len(headers)-1])
	}
	return headers, nextCursor, nil
}

func (s *SQLiteRuntimeStore) LoadRunDebugReport(ctx context.Context, runID string, opts RunDebugQueryOptions) (RunDebugReport, error) {
	opts = defaultRunDebugQueryOptions(opts)
	header, err := s.LoadRunHeader(ctx, runID)
	if err != nil {
		return RunDebugReport{}, err
	}
	report := RunDebugReport{
		RunID:          header.RunID,
		RunTableStatus: header.Status,
		RootEventID:    header.TriggerEventID,
		RootEventType:  header.TriggerEventType,
		Failure:        runtimefailures.CloneEnvelope(header.Failure),
		ControlReason:  header.ControlReason,
		StartedAt:      header.StartedAt,
		EndedAt:        header.EndedAt,
		EventCount:     header.EventCount,
		EntityCount:    header.EntityCount,
	}
	if lastEventAt, ok, err := s.sqliteRunLastEventAt(ctx, header.RunID); err != nil {
		return RunDebugReport{}, err
	} else if ok {
		report.LastEventAt = lastEventAt
	}
	eventCounts, err := s.sqliteRunDebugEventCounts(ctx, header.RunID)
	if err != nil {
		return RunDebugReport{}, err
	}
	report.EventCounts = eventCounts
	deliveries, err := s.sqliteRunDebugDeliveryCounts(ctx, header.RunID)
	if err != nil {
		return RunDebugReport{}, err
	}
	report.Deliveries = deliveries
	failedDeliveries, err := s.sqliteRunDebugFailureDeliveries(ctx, header.RunID, opts.DeadLetterLimit)
	if err != nil {
		return RunDebugReport{}, err
	}
	report.FailedDeliveries = failedDeliveries
	testQuiescence, err := s.sqliteRunTestQuiescence(ctx, header.RunID)
	if err != nil {
		return RunDebugReport{}, err
	}
	report.TestQuiescence = testQuiescence
	events, err := s.sqliteRunDebugEvents(ctx, header.RunID, opts.EventLimit)
	if err != nil {
		return RunDebugReport{}, err
	}
	report.Events = events
	logs, err := s.sqliteRunDebugRuntimeLogs(ctx, header.RunID, opts)
	if err != nil {
		return RunDebugReport{}, err
	}
	report.RuntimeLogs = logs
	logSummary, warnErrorCount, err := s.sqliteRunDebugRuntimeLogSummary(ctx, header.RunID, opts.Component)
	if err != nil {
		return RunDebugReport{}, err
	}
	report.RuntimeLogSummary = logSummary
	report.WarnErrorLogCount = warnErrorCount
	return report, nil
}

func (s *SQLiteRuntimeStore) sqliteRunTestQuiescence(ctx context.Context, runID string) (RunTestQuiescence, error) {
	var out RunTestQuiescence
	summary, err := s.SummarizeRun(ctx, runID)
	if err != nil {
		return RunTestQuiescence{}, fmt.Errorf("load sqlite run test quiescence active deliveries: %w", err)
	}
	out.ActiveDeliveries = summary.Pending + summary.InProgress + summary.RetryScheduled
	quiescenceArgs := append([]any{runID}, diagnosticDirectReplayEventArgs()...)
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events e
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE e.run_id = ?
		  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		  AND r.event_id IS NULL
	`, quiescenceArgs...).Scan(&out.UnsettledPipelineEvents); err != nil {
		return RunTestQuiescence{}, fmt.Errorf("load sqlite run test quiescence unsettled pipeline events: %w", err)
	}
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM timers
		WHERE run_id = ?
		  AND status = 'active'
		  AND fire_at <= ?
	`, runID, s.now()).Scan(&out.DueTimers); err != nil {
		return RunTestQuiescence{}, fmt.Errorf("load sqlite run test quiescence due timers: %w", err)
	}
	activeSessionLeases, err := s.sqliteRunActiveSessionLeaseCount(ctx, runID)
	if err != nil {
		return RunTestQuiescence{}, err
	}
	out.ActiveSessionLeases = activeSessionLeases
	out.Ready = runTestQuiescenceReady(out)
	return out, nil
}

func (s *SQLiteRuntimeStore) sqliteRunActiveSessionLeaseCount(ctx context.Context, runID string) (int, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT lease_expires_at
		FROM agent_sessions
		WHERE run_id = ?
		  AND status = 'active'
		  AND lease_holder IS NOT NULL
		  AND lease_expires_at IS NOT NULL
	`, runID)
	if err != nil {
		return 0, fmt.Errorf("load sqlite run test quiescence active session leases: %w", err)
	}
	defer rows.Close()
	count := 0
	now := s.now()
	for rows.Next() {
		var raw any
		if err := rows.Scan(&raw); err != nil {
			return 0, fmt.Errorf("scan sqlite run test quiescence session lease expiry: %w", err)
		}
		expiresAt, ok, err := sqliteTimeValue(raw)
		if err != nil {
			return 0, err
		}
		if ok && expiresAt.After(now) {
			count++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read sqlite run test quiescence session lease expiries: %w", err)
	}
	return count, nil
}

func sqliteRunHeaderSelectSQL() string {
	return `
SELECT
	r.run_id,
	lower(COALESCE(r.status, '')),
	COALESCE(NULLIF(r.trigger_event_type, ''), (
		SELECT e.event_name
		FROM events e
		WHERE e.run_id = r.run_id
		ORDER BY e.created_at ASC, e.event_id ASC
		LIMIT 1
	), ''),
	COALESCE(NULLIF(r.trigger_event_id, ''), (
		SELECT e.event_id
		FROM events e
		WHERE e.run_id = r.run_id
		ORDER BY e.created_at ASC, e.event_id ASC
		LIMIT 1
	), ''),
	COALESCE((SELECT COUNT(DISTINCT es.entity_id) FROM entity_state es WHERE es.run_id = r.run_id), 0),
	COALESCE(NULLIF(r.event_count, 0), (SELECT COUNT(*) FROM events e WHERE e.run_id = r.run_id), 0),
	r.started_at,
	r.ended_at,
	COALESCE(r.forked_from_run_id, ''),
	COALESCE(r.continued_as_run_id, ''),
	r.failure,
	COALESCE(rc.reason, '')
FROM runs r
	LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
`
}

func scanSQLiteRunHeader(row runHeaderScanner) (RunHeader, error) {
	var header RunHeader
	var startedRaw, endedRaw any
	var failureRaw any
	if err := row.Scan(
		&header.RunID,
		&header.Status,
		&header.TriggerEventType,
		&header.TriggerEventID,
		&header.EntityCount,
		&header.EventCount,
		&startedRaw,
		&endedRaw,
		&header.ForkedFromRunID,
		&header.ContinuedAsRunID,
		&failureRaw,
		&header.ControlReason,
	); err != nil {
		return RunHeader{}, err
	}
	startedAt, ok, err := sqliteTimeValue(startedRaw)
	if err != nil {
		return RunHeader{}, err
	}
	if ok {
		header.StartedAt = startedAt
	}
	if endedAt, ok, err := sqliteTimeValue(endedRaw); err != nil {
		return RunHeader{}, err
	} else if ok {
		header.EndedAt = &endedAt
	}
	header.Status = strings.ToLower(strings.TrimSpace(header.Status))
	header.TriggerEventType = strings.TrimSpace(header.TriggerEventType)
	header.TriggerEventID = strings.TrimSpace(header.TriggerEventID)
	header.ForkedFromRunID = strings.TrimSpace(header.ForkedFromRunID)
	header.ContinuedAsRunID = strings.TrimSpace(header.ContinuedAsRunID)
	header.ControlReason = strings.TrimSpace(header.ControlReason)
	failure, err := decodeStoredFailure(failureRaw)
	if err != nil {
		return RunHeader{}, err
	}
	header.Failure = failure
	if err := storerunlifecycle.ValidateStatusFailure(header.Status, header.Failure); err != nil {
		return RunHeader{}, fmt.Errorf("run %s terminal evidence: %w", header.RunID, err)
	}
	return header, nil
}

func (s *SQLiteRuntimeStore) sqliteRunLastEventAt(ctx context.Context, runID string) (time.Time, bool, error) {
	var raw any
	if err := s.DB.QueryRowContext(ctx, `SELECT MAX(created_at) FROM events WHERE run_id = ?`, runID).Scan(&raw); err != nil {
		return time.Time{}, false, fmt.Errorf("load sqlite run last event timestamp: %w", err)
	}
	return sqliteTimeValue(raw)
}

func (s *SQLiteRuntimeStore) sqliteRunDebugEventCounts(ctx context.Context, runID string) ([]RunDebugEventCount, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT event_name, COUNT(*)
		FROM events
		WHERE run_id = ?
		GROUP BY event_name
		ORDER BY COUNT(*) DESC, event_name ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("query sqlite run debug event counts: %w", err)
	}
	defer rows.Close()
	out := []RunDebugEventCount{}
	for rows.Next() {
		var item RunDebugEventCount
		if err := rows.Scan(&item.EventName, &item.Count); err != nil {
			return nil, fmt.Errorf("scan sqlite run debug event count: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite run debug event counts: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) sqliteRunDebugDeliveryCounts(ctx context.Context, runID string) ([]RunDebugDeliveryCount, error) {
	snapshots, err := s.deliverySnapshotsForRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("query sqlite run debug delivery counts: %w", err)
	}
	return runDebugDeliveryCountsFromSnapshots(snapshots), nil
}

func (s *SQLiteRuntimeStore) sqliteRunDebugFailureDeliveries(ctx context.Context, runID string, limit int) ([]RunDebugFailureDelivery, error) {
	if limit <= 0 {
		limit = 10
	}
	snapshots, err := s.deliverySnapshotsForRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("query sqlite run failed deliveries: %w", err)
	}
	return runDebugFailuresFromSnapshots(snapshots, limit,
		func(eventID string) (deliveryLifecycleEventMetadata, error) {
			record, found, err := loadSQLiteEventIdentity(ctx, s.DB, eventID)
			if err != nil {
				return deliveryLifecycleEventMetadata{}, err
			}
			if !found {
				return deliveryLifecycleEventMetadata{}, fmt.Errorf("delivery event %s not found", eventID)
			}
			admitted, err := decodeEventRecord(record)
			if err != nil {
				return deliveryLifecycleEventMetadata{}, err
			}
			event := admitted.Event()
			return deliveryLifecycleEventMetadata{EventName: string(event.Type()), RunID: event.RunID(), EntityID: event.EntityID()}, nil
		}, func(deliveryID string, claimVersion int64) ([]OperatorDeadLetterRecord, error) {
			return s.sqliteOperatorDeliveryDeadLetters(ctx, deliveryID, claimVersion)
		})
}

func (s *SQLiteRuntimeStore) sqliteRunDebugEvents(ctx context.Context, runID string, limit int) ([]RunDebugEvent, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT event_id, event_name, COALESCE(entity_id, ''), created_at,
		       COALESCE(produced_by, ''), COALESCE(produced_by_type, ''), payload
		FROM events
		WHERE run_id = ? AND event_name <> 'platform.runtime_log'
		ORDER BY created_at DESC, event_id DESC
		LIMIT ?
	`, runID, limit)
	if err != nil {
		return nil, fmt.Errorf("query sqlite run debug events: %w", err)
	}
	defer rows.Close()
	out := []RunDebugEvent{}
	for rows.Next() {
		var item RunDebugEvent
		var createdRaw, payloadRaw any
		if err := rows.Scan(&item.EventID, &item.EventName, &item.EntityID, &createdRaw, &item.Source, &item.SourceType, &payloadRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite run debug event: %w", err)
		}
		if at, ok, err := sqliteTimeValue(createdRaw); err != nil {
			return nil, err
		} else if ok {
			item.CreatedAt = at
		}
		item.Payload = sqliteJSONRawMessage(payloadRaw)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite run debug events: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) sqliteRunDebugRuntimeLogs(ctx context.Context, runID string, opts RunDebugQueryOptions) ([]RunDebugRuntimeLog, error) {
	where := []string{"run_id = ?", "event_name = 'platform.runtime_log'"}
	args := []any{runID}
	if !opts.LogsAllLevels {
		where = append(where, "COALESCE(json_extract(payload, '$.log_level'), '') IN ('warn', 'error')")
	}
	if opts.Component != "" {
		where = append(where, "json_extract(payload, '$.details.component') = ?")
		args = append(args, opts.Component)
	}
	args = append(args, opts.RuntimeLogLimit)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT event_id, created_at, COALESCE(entity_id, ''), payload
		FROM events
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY created_at DESC, event_id DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("query sqlite run debug runtime logs: %w", err)
	}
	defer rows.Close()
	out := []RunDebugRuntimeLog{}
	for rows.Next() {
		var log OperatorRuntimeLogEntry
		var createdRaw, payloadRaw any
		if err := rows.Scan(&log.LogID, &createdRaw, &log.EntityID, &payloadRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite run debug runtime log: %w", err)
		}
		if at, ok, err := sqliteTimeValue(createdRaw); err != nil {
			return nil, err
		} else if ok {
			log.TS = at
		}
		if err := applySQLiteRuntimeLogPayload(&log, sqliteJSONRawMessage(payloadRaw)); err != nil {
			return nil, fmt.Errorf("decode sqlite run debug runtime log: %w", err)
		}
		detail, _ := json.Marshal(log.Details)
		item := RunDebugRuntimeLog{
			EventID:   strings.TrimSpace(log.LogID),
			Level:     strings.TrimSpace(log.Level),
			Message:   strings.TrimSpace(log.Message),
			Component: strings.TrimSpace(log.Component),
			Action:    strings.TrimSpace(sqliteObservabilityString(log.Details["action"])),
			EventType: strings.TrimSpace(sqliteObservabilityString(log.Details["event_type"])),
			AgentID:   strings.TrimSpace(log.Source),
			EntityID:  strings.TrimSpace(log.EntityID),
			Failure:   runtimefailures.CloneEnvelope(log.Failure),
			Detail:    json.RawMessage(detail),
			CreatedAt: log.TS.UTC(),
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite run debug runtime logs: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) sqliteRunDebugRuntimeLogSummary(ctx context.Context, runID, component string) ([]RunDebugRuntimeSummary, int, error) {
	where := []string{"run_id = ?", "event_name = 'platform.runtime_log'"}
	args := []any{runID}
	component = strings.TrimSpace(component)
	if component != "" {
		where = append(where, "json_extract(payload, '$.details.component') = ?")
		args = append(args, component)
	}
	logLevels := "COALESCE(json_extract(payload, '$.log_level'), '') IN ('warn', 'error')"
	rows, err := s.DB.QueryContext(ctx, `
		SELECT COALESCE(json_extract(payload, '$.log_level'), 'info'),
		       COALESCE(json_extract(payload, '$.details.component'), ''),
		       COALESCE(json_extract(payload, '$.details.action'), ''),
		       COUNT(*)
		FROM events
		WHERE `+strings.Join(where, " AND ")+`
		  AND `+logLevels+`
		GROUP BY COALESCE(json_extract(payload, '$.log_level'), 'info'),
		         COALESCE(json_extract(payload, '$.details.component'), ''),
		         COALESCE(json_extract(payload, '$.details.action'), '')
		ORDER BY COUNT(*) DESC,
		         COALESCE(json_extract(payload, '$.log_level'), 'info') ASC,
		         COALESCE(json_extract(payload, '$.details.component'), '') ASC
	`, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query sqlite run debug runtime log summary: %w", err)
	}
	defer rows.Close()
	out := []RunDebugRuntimeSummary{}
	warnErrorCount := 0
	for rows.Next() {
		var item RunDebugRuntimeSummary
		if err := rows.Scan(&item.Level, &item.Component, &item.Action, &item.Count); err != nil {
			return nil, 0, fmt.Errorf("scan sqlite run debug runtime log summary: %w", err)
		}
		switch strings.ToLower(strings.TrimSpace(item.Level)) {
		case "warn", "warning", "error":
			warnErrorCount += item.Count
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("read sqlite run debug runtime log summary: %w", err)
	}
	return out, warnErrorCount, nil
}
