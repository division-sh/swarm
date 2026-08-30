package operatorsurface

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/google/uuid"
)

func (s *RunSQLite) requireRunHeaderAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunSQLite) LoadRunHeader(ctx context.Context, runID string) (operatorread.RunHeader, error) {
	if err := s.requireRunHeaderAccess(); err != nil {
		return operatorread.RunHeader{}, err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return operatorread.RunHeader{}, operatorread.ErrRunNotFound
	}
	if _, err := uuid.Parse(runID); err != nil {
		return operatorread.RunHeader{}, operatorread.ErrRunNotFound
	}
	row := s.backend.QueryRowContext(ctx, sqliteRunHeaderSelectSQL()+`
WHERE r.run_id = ?
`, runID)
	header, err := scanSQLiteRunHeader(row)
	if errors.Is(err, sql.ErrNoRows) {
		return operatorread.RunHeader{}, operatorread.ErrRunNotFound
	}
	if err != nil {
		return operatorread.RunHeader{}, err
	}
	return header, nil
}

func (s *RunSQLite) LoadRunOrigin(ctx context.Context, runID string) (runtimerunlifecycle.RunOrigin, error) {
	header, err := s.LoadRunHeader(ctx, runID)
	if err != nil {
		return runtimerunlifecycle.RunOrigin{}, err
	}
	return header.Origin, nil
}

func (s *RunSQLite) ListRunHeaders(ctx context.Context, opts operatorread.RunHeaderListOptions) ([]operatorread.RunHeader, string, error) {
	if err := s.requireRunHeaderAccess(); err != nil {
		return nil, "", err
	}
	opts = defaultRunHeaderListOptions(opts)
	args := make([]any, 0, 6)
	where := []string{"1 = 1"}
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
	rows, err := s.backend.QueryContext(ctx, sqliteRunHeaderSelectSQL()+`
WHERE `+strings.Join(where, " AND ")+`
ORDER BY r.started_at DESC, r.run_id DESC
LIMIT ?
`, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	headers := make([]operatorread.RunHeader, 0, opts.Limit)
	for rows.Next() {
		header, err := scanSQLiteRunHeader(rows)
		if err != nil {
			return nil, "", err
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

func (s *RunSQLite) LoadRunDebugReport(ctx context.Context, runID string, opts operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error) {
	opts = defaultRunDebugQueryOptions(opts)
	header, err := s.LoadRunHeader(ctx, runID)
	if err != nil {
		return operatorread.RunDebugReport{}, err
	}
	report := operatorread.RunDebugReport{
		RunID:          header.RunID,
		RunTableStatus: header.Status,
		Failure:        runtimefailures.CloneEnvelope(header.Failure),
		ControlReason:  header.ControlReason,
		StartedAt:      header.StartedAt,
		EndedAt:        header.EndedAt,
		EventCount:     header.EventCount,
		EntityCount:    header.EntityCount,
	}
	if header.Origin.Kind() == runtimerunlifecycle.OriginEvent {
		report.RootEventID = header.Origin.EventID()
		report.RootEventType = header.Origin.EventType()
	}
	if lastEventAt, ok, err := s.sqliteRunLastEventAt(ctx, header.RunID); err != nil {
		return operatorread.RunDebugReport{}, err
	} else if ok {
		report.LastEventAt = lastEventAt
	}
	eventCounts, err := s.sqliteRunDebugEventCounts(ctx, header.RunID)
	if err != nil {
		return operatorread.RunDebugReport{}, err
	}
	report.EventCounts = eventCounts
	deliveries, err := s.sqliteRunDebugDeliveryCounts(ctx, header.RunID)
	if err != nil {
		return operatorread.RunDebugReport{}, err
	}
	report.Deliveries = deliveries
	failedDeliveries, err := s.sqliteRunDebugFailureDeliveries(ctx, header.RunID, opts.DeadLetterLimit)
	if err != nil {
		return operatorread.RunDebugReport{}, err
	}
	report.FailedDeliveries = failedDeliveries
	testQuiescence, err := s.LoadRunTestQuiescence(ctx, header.RunID, s.now())
	if err != nil {
		return operatorread.RunDebugReport{}, err
	}
	report.TestQuiescence = testQuiescence
	events, err := s.sqliteRunDebugEvents(ctx, header.RunID, opts.EventLimit)
	if err != nil {
		return operatorread.RunDebugReport{}, err
	}
	report.Events = events
	logs, err := s.sqliteRunDebugRuntimeLogs(ctx, header.RunID, opts)
	if err != nil {
		return operatorread.RunDebugReport{}, err
	}
	report.RuntimeLogs = logs
	logSummary, warnErrorCount, err := s.sqliteRunDebugRuntimeLogSummary(ctx, header.RunID, opts.Component)
	if err != nil {
		return operatorread.RunDebugReport{}, err
	}
	report.RuntimeLogSummary = logSummary
	report.WarnErrorLogCount = warnErrorCount
	report.FanOut, err = s.pipeline.FanOutRunSummary(ctx, report.RunID, s.now())
	if err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("load sqlite run fan-out diagnostics: %w", err)
	}
	return report, nil
}

func (s *RunSQLite) LoadRunTestQuiescence(ctx context.Context, runID string, observedAt time.Time) (operatorread.RunTestQuiescence, error) {
	var out operatorread.RunTestQuiescence
	summary, err := s.summarizeDeliveryRun(ctx, runID)
	if err != nil {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("load sqlite run test quiescence active deliveries: %w", err)
	}
	out.ActiveDeliveries = summary.Pending + summary.InProgress + summary.RetryScheduled
	if s.pipeline == nil || s.timers == nil {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("run debug runtime diagnostics source is required")
	}
	pipelineSummary, err := s.pipeline.PipelineObligations().SummarizeRun(ctx, runID)
	if err != nil {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("load sqlite run test quiescence unsettled pipeline events: %w", err)
	}
	out.UnsettledPipelineEvents = pipelineSummary.Replayable + pipelineSummary.Deferred
	fanOut, err := s.pipeline.FanOutRunSummary(ctx, runID, observedAt)
	if err != nil {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("load sqlite run test quiescence fan-out obligations: %w", err)
	}
	out.FanOutOwed = fanOut.Owed
	out.FanOutBarriers = fanOut.BarrierArmed + fanOut.BarrierPending
	scope, err := runtimetimerobligation.Run(runID)
	if err != nil {
		return operatorread.RunTestQuiescence{}, err
	}
	timerSnapshot, err := s.timers.ReadTimerObligations(ctx, scope, observedAt)
	if err != nil {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("load sqlite run test quiescence timer obligations: %w", err)
	}
	runTimers, ok := timerSnapshot.Run(runID)
	if !ok {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("load sqlite run test quiescence timer obligations: snapshot omitted requested run")
	}
	out.DueTimers = runTimers.Totals().DueCount
	activeSessionLeases, err := s.sqliteRunActiveSessionLeaseCount(ctx, runID)
	if err != nil {
		return operatorread.RunTestQuiescence{}, err
	}
	out.ActiveSessionLeases = activeSessionLeases
	out.Ready = runTestQuiescenceReady(out)
	return out, nil
}

func (s *RunSQLite) sqliteRunActiveSessionLeaseCount(ctx context.Context, runID string) (int, error) {
	rows, err := s.backend.QueryContext(ctx, `
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
	COALESCE(r.bundle_hash, ''),
	COALESCE(r.origin_kind, ''),
	COALESCE(r.trigger_event_id, ''),
	COALESCE(r.trigger_event_type, ''),
	COALESCE(r.origin_service_id, ''),
	COALESCE(r.origin_generation, 0),
	COALESCE(r.forked_from_run_id, ''),
	COALESCE(r.forked_from_event_id, ''),
	(SELECT COUNT(*) FROM standing_service_generations ssg WHERE ssg.run_id = r.run_id),
	(
		SELECT COUNT(*)
		FROM standing_service_generations ssg
		WHERE ssg.run_id = r.run_id
		  AND ssg.service_id = r.origin_service_id
		  AND ssg.generation = r.origin_generation
	),
	COALESCE((SELECT COUNT(DISTINCT es.entity_id) FROM entity_state es WHERE es.run_id = r.run_id), 0),
	COALESCE(NULLIF(r.event_count, 0), (SELECT COUNT(*) FROM events e WHERE e.run_id = r.run_id), 0),
	r.started_at,
	r.ended_at,
	COALESCE(r.continued_as_run_id, ''),
	r.failure,
	COALESCE(rc.reason, '')
FROM runs r
	LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
`
}

func scanSQLiteRunHeader(row runHeaderScanner) (operatorread.RunHeader, error) {
	var header operatorread.RunHeader
	var startedRaw, endedRaw any
	var failureRaw any
	var bundleHash string
	var originKind, eventID, eventType, serviceID, sourceRunID, sourceEventID string
	var generation int64
	var standingRelationCount, matchingStandingRelationCount int
	if err := row.Scan(
		&header.RunID,
		&header.Status,
		&bundleHash,
		&originKind,
		&eventID,
		&eventType,
		&serviceID,
		&generation,
		&sourceRunID,
		&sourceEventID,
		&standingRelationCount,
		&matchingStandingRelationCount,
		&header.EntityCount,
		&header.EventCount,
		&startedRaw,
		&endedRaw,
		&header.ContinuedAsRunID,
		&failureRaw,
		&header.ControlReason,
	); err != nil {
		return operatorread.RunHeader{}, err
	}
	startedAt, ok, err := sqliteTimeValue(startedRaw)
	if err != nil {
		return operatorread.RunHeader{}, err
	}
	if ok {
		header.StartedAt = startedAt
	}
	if endedAt, ok, err := sqliteTimeValue(endedRaw); err != nil {
		return operatorread.RunHeader{}, err
	} else if ok {
		header.EndedAt = &endedAt
	}
	header.Status = strings.ToLower(strings.TrimSpace(header.Status))
	header.Origin, err = runtimerunlifecycle.DecodeRunOrigin(
		originKind, eventID, eventType, serviceID, generation, sourceRunID, sourceEventID,
	)
	if err != nil {
		return operatorread.RunHeader{}, fmt.Errorf("run %s origin: %w", header.RunID, err)
	}
	if err := validateRunHeaderStandingRelation(
		header.RunID, header.Origin, standingRelationCount, matchingStandingRelationCount,
	); err != nil {
		return operatorread.RunHeader{}, err
	}
	header.ContinuedAsRunID = strings.TrimSpace(header.ContinuedAsRunID)
	header.ControlReason = strings.TrimSpace(header.ControlReason)
	failure, err := decodeStoredFailure(failureRaw)
	if err != nil {
		return operatorread.RunHeader{}, err
	}
	header.Failure = failure
	if err := validateRunHeaderLifecycle(header, bundleHash); err != nil {
		return operatorread.RunHeader{}, err
	}
	return header, nil
}

func (s *RunSQLite) sqliteRunLastEventAt(ctx context.Context, runID string) (time.Time, bool, error) {
	var raw any
	if err := s.backend.QueryRowContext(ctx, `SELECT MAX(created_at) FROM events WHERE run_id = ?`, runID).Scan(&raw); err != nil {
		return time.Time{}, false, fmt.Errorf("load sqlite run last event timestamp: %w", err)
	}
	return sqliteTimeValue(raw)
}

func (s *RunSQLite) sqliteRunDebugEventCounts(ctx context.Context, runID string) ([]operatorread.RunDebugEventCount, error) {
	rows, err := s.backend.QueryContext(ctx, `
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
	out := []operatorread.RunDebugEventCount{}
	for rows.Next() {
		var item operatorread.RunDebugEventCount
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

func (s *RunSQLite) sqliteRunDebugDeliveryCounts(ctx context.Context, runID string) ([]operatorread.RunDebugDeliveryCount, error) {
	counts, err := s.deliveryRunDiagnosticCounts(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("query sqlite run debug delivery counts: %w", err)
	}
	return runDebugDeliveryCounts(counts), nil
}

func (s *RunSQLite) sqliteRunDebugFailureDeliveries(ctx context.Context, runID string, limit int) ([]operatorread.RunDebugFailureDelivery, error) {
	if limit <= 0 {
		limit = 10
	}
	snapshots, err := s.deliveryRunDiagnosticFailures(ctx, runID, limit)
	if err != nil {
		return nil, fmt.Errorf("query sqlite run failed deliveries: %w", err)
	}
	return runDebugFailuresFromSnapshots(snapshots,
		func(eventID string) (deliveryLifecycleEventMetadata, error) {
			record, found, err := loadSQLiteEventIdentity(ctx, s.backend, eventID)
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
		}, func(deliveryID string, claimVersion int64) ([]operatorread.OperatorDeadLetterRecord, error) {
			return s.deadLetters.LoadOperatorDeliveryDeadLetters(ctx, deliveryID, claimVersion)
		})
}

func (s *RunSQLite) sqliteRunDebugEvents(ctx context.Context, runID string, limit int) ([]operatorread.RunDebugEvent, error) {
	rows, err := s.backend.QueryContext(ctx, `
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
	out := []operatorread.RunDebugEvent{}
	for rows.Next() {
		var item operatorread.RunDebugEvent
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

func (s *RunSQLite) sqliteRunDebugRuntimeLogs(ctx context.Context, runID string, opts operatorread.RunDebugQueryOptions) ([]operatorread.RunDebugRuntimeLog, error) {
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
	rows, err := s.backend.QueryContext(ctx, `
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
	out := []operatorread.RunDebugRuntimeLog{}
	for rows.Next() {
		var log operatorread.OperatorRuntimeLogEntry
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
		detail, _ := json.Marshal(log.CanonicalDetail)
		item := operatorread.RunDebugRuntimeLog{
			EventID:   strings.TrimSpace(log.LogID),
			Level:     strings.TrimSpace(log.Level),
			Message:   strings.TrimSpace(log.Message),
			Component: strings.TrimSpace(log.Component),
			Action:    log.Action,
			EventType: log.EventType,
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

func (s *RunSQLite) sqliteRunDebugRuntimeLogSummary(ctx context.Context, runID, component string) ([]operatorread.RunDebugRuntimeSummary, int, error) {
	where := []string{"run_id = ?", "event_name = 'platform.runtime_log'"}
	args := []any{runID}
	component = strings.TrimSpace(component)
	if component != "" {
		where = append(where, "json_extract(payload, '$.details.component') = ?")
		args = append(args, component)
	}
	logLevels := "COALESCE(json_extract(payload, '$.log_level'), '') IN ('warn', 'error')"
	rows, err := s.backend.QueryContext(ctx, `
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
	out := []operatorread.RunDebugRuntimeSummary{}
	warnErrorCount := 0
	for rows.Next() {
		var item operatorread.RunDebugRuntimeSummary
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
