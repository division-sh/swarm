package store

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type runDebugTraceInputs struct {
	events   []RunDebugTraceRow
	turns    map[string][]runDebugTraceTurn
	sessions map[string]runDebugTraceSession
}

type runDebugTraceTurn struct {
	agentID   string
	sessionID string
	row       RunDebugTraceRow
}

type runDebugTraceSession struct {
	runID        string
	kind         string
	memory       bool
	memorySource string
	status       string
	updatedAt    time.Time
}

func (s *PostgresStore) loadProjectedRunDebugTrace(ctx context.Context, runID string, opts RunDebugTraceQueryOptions) ([]RunDebugTraceRow, string, error) {
	inputs, err := loadPostgresRunDebugTraceInputs(ctx, s.DB, runID)
	if err != nil {
		return nil, "", err
	}
	snapshots, err := s.deliverySnapshotsForRun(ctx, runID)
	if err != nil {
		return nil, "", fmt.Errorf("load run debug trace delivery snapshots: %w", err)
	}
	return projectRunDebugTrace(inputs, snapshots, opts)
}

func (s *SQLiteRuntimeStore) loadProjectedRunDebugTrace(ctx context.Context, runID string, opts RunDebugTraceQueryOptions) ([]RunDebugTraceRow, string, error) {
	inputs, err := loadSQLiteRunDebugTraceInputs(ctx, s.DB, runID)
	if err != nil {
		return nil, "", err
	}
	snapshots, err := s.deliverySnapshotsForRun(ctx, runID)
	if err != nil {
		return nil, "", fmt.Errorf("load sqlite run debug trace delivery snapshots: %w", err)
	}
	return projectRunDebugTrace(inputs, snapshots, opts)
}

func loadPostgresRunDebugTraceInputs(ctx context.Context, db *sql.DB, runID string) (runDebugTraceInputs, error) {
	inputs := runDebugTraceInputs{turns: map[string][]runDebugTraceTurn{}, sessions: map[string]runDebugTraceSession{}}
	rows, err := db.QueryContext(ctx, `
		SELECT event_id::text, COALESCE(event_name, ''), COALESCE(source_event_id::text, ''),
		       COALESCE(entity_id::text, ''), COALESCE(produced_by, ''), COALESCE(produced_by_type, ''), created_at
		FROM events
		WHERE run_id = $1::uuid
		ORDER BY created_at ASC, event_id ASC
	`, runID)
	if err != nil {
		return runDebugTraceInputs{}, fmt.Errorf("load run debug trace events: %w", err)
	}
	for rows.Next() {
		var row RunDebugTraceRow
		if err := rows.Scan(&row.EventID, &row.EventName, &row.SourceEventID, &row.EntityID, &row.EventSource, &row.EventSourceType, &row.EventCreatedAt); err != nil {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("scan run debug trace event: %w", err)
		}
		inputs.events = append(inputs.events, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return runDebugTraceInputs{}, fmt.Errorf("read run debug trace events: %w", err)
	}
	rows.Close()

	rows, err = db.QueryContext(ctx, `
		SELECT turn_id::text, COALESCE(trigger_event_id::text, ''), COALESCE(trigger_event_type, ''),
		       COALESCE(flow_instance, ''), COALESCE(memory_enabled, false), COALESCE(memory_source, ''),
		       COALESCE(entity_id::text, ''), COALESCE(task_id, ''), COALESCE(parse_ok, false),
		       COALESCE(retry_count, 0), COALESCE(failure, 'null'::jsonb), created_at,
		       COALESCE(agent_id, ''), COALESCE(session_id::text, '')
		FROM agent_turns
		WHERE run_id = $1::uuid
		ORDER BY created_at ASC, turn_id ASC
	`, runID)
	if err != nil {
		return runDebugTraceInputs{}, fmt.Errorf("load run debug trace turns: %w", err)
	}
	for rows.Next() {
		var turn runDebugTraceTurn
		var rawFailure []byte
		if err := rows.Scan(
			&turn.row.TurnID, &turn.row.TurnTriggerEventID, &turn.row.TurnTriggerEventType,
			&turn.row.TurnFlowInstance, &turn.row.TurnMemory, &turn.row.TurnMemorySource,
			&turn.row.TurnEntityID, &turn.row.TurnTaskID, &turn.row.TurnParseOK,
			&turn.row.TurnRetryCount, &rawFailure, &turn.row.TurnCreatedAt,
			&turn.agentID, &turn.sessionID,
		); err != nil {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("scan run debug trace turn: %w", err)
		}
		turn.row.TurnFailure, err = decodeStoredFailure(rawFailure)
		if err != nil {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("decode run debug trace turn failure: %w", err)
		}
		inputs.turns[turn.row.TurnTriggerEventID] = append(inputs.turns[turn.row.TurnTriggerEventID], turn)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return runDebugTraceInputs{}, fmt.Errorf("read run debug trace turns: %w", err)
	}
	rows.Close()

	rows, err = db.QueryContext(ctx, `
		SELECT session_id::text, COALESCE(run_id::text, ''), session_kind,
		       COALESCE(memory_enabled, false), COALESCE(memory_source, ''), COALESCE(status, ''), updated_at
		FROM (`+runDebugTraceSessionSources()+`) trace_sessions
		WHERE run_id = $1::uuid OR run_id IS NULL
		ORDER BY session_id, session_kind
	`, runID)
	if err != nil {
		return runDebugTraceInputs{}, fmt.Errorf("load run debug trace sessions: %w", err)
	}
	for rows.Next() {
		var sessionID string
		var session runDebugTraceSession
		if err := rows.Scan(&sessionID, &session.runID, &session.kind, &session.memory, &session.memorySource, &session.status, &session.updatedAt); err != nil {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("scan run debug trace session: %w", err)
		}
		if _, duplicate := inputs.sessions[sessionID]; duplicate {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("run debug trace session %s has multiple canonical records", sessionID)
		}
		inputs.sessions[sessionID] = session
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return runDebugTraceInputs{}, fmt.Errorf("read run debug trace sessions: %w", err)
	}
	rows.Close()
	return inputs, nil
}

func loadSQLiteRunDebugTraceInputs(ctx context.Context, db *sql.DB, runID string) (runDebugTraceInputs, error) {
	inputs := runDebugTraceInputs{turns: map[string][]runDebugTraceTurn{}, sessions: map[string]runDebugTraceSession{}}
	rows, err := db.QueryContext(ctx, `
		SELECT event_id, COALESCE(event_name, ''), COALESCE(source_event_id, ''),
		       COALESCE(entity_id, ''), COALESCE(produced_by, ''), COALESCE(produced_by_type, ''), created_at
		FROM events
		WHERE run_id = ?
		ORDER BY created_at ASC, event_id ASC
	`, runID)
	if err != nil {
		return runDebugTraceInputs{}, fmt.Errorf("load sqlite run debug trace events: %w", err)
	}
	for rows.Next() {
		var row RunDebugTraceRow
		var createdRaw any
		if err := rows.Scan(&row.EventID, &row.EventName, &row.SourceEventID, &row.EntityID, &row.EventSource, &row.EventSourceType, &createdRaw); err != nil {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("scan sqlite run debug trace event: %w", err)
		}
		createdAt, ok, err := sqliteTimeValue(createdRaw)
		if err != nil || !ok {
			rows.Close()
			if err == nil {
				err = fmt.Errorf("event %s is missing created_at", row.EventID)
			}
			return runDebugTraceInputs{}, err
		}
		row.EventCreatedAt = createdAt
		inputs.events = append(inputs.events, row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return runDebugTraceInputs{}, fmt.Errorf("read sqlite run debug trace events: %w", err)
	}
	rows.Close()

	rows, err = db.QueryContext(ctx, `
		SELECT turn_id, COALESCE(trigger_event_id, ''), COALESCE(trigger_event_type, ''),
		       COALESCE(flow_instance, ''), COALESCE(memory_enabled, 0), COALESCE(memory_source, ''),
		       COALESCE(entity_id, ''), COALESCE(task_id, ''), COALESCE(parse_ok, 0),
		       COALESCE(retry_count, 0), COALESCE(failure, 'null'), created_at,
		       COALESCE(agent_id, ''), COALESCE(session_id, '')
		FROM agent_turns
		WHERE run_id = ?
		ORDER BY created_at ASC, turn_id ASC
	`, runID)
	if err != nil {
		return runDebugTraceInputs{}, fmt.Errorf("load sqlite run debug trace turns: %w", err)
	}
	for rows.Next() {
		var turn runDebugTraceTurn
		var rawFailure, createdRaw any
		if err := rows.Scan(
			&turn.row.TurnID, &turn.row.TurnTriggerEventID, &turn.row.TurnTriggerEventType,
			&turn.row.TurnFlowInstance, &turn.row.TurnMemory, &turn.row.TurnMemorySource,
			&turn.row.TurnEntityID, &turn.row.TurnTaskID, &turn.row.TurnParseOK,
			&turn.row.TurnRetryCount, &rawFailure, &createdRaw,
			&turn.agentID, &turn.sessionID,
		); err != nil {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("scan sqlite run debug trace turn: %w", err)
		}
		turn.row.TurnFailure, err = decodeStoredFailure(rawFailure)
		if err != nil {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("decode sqlite run debug trace turn failure: %w", err)
		}
		if at, ok, err := sqliteTimeValue(createdRaw); err != nil {
			rows.Close()
			return runDebugTraceInputs{}, err
		} else if ok {
			turn.row.TurnCreatedAt = &at
		}
		inputs.turns[turn.row.TurnTriggerEventID] = append(inputs.turns[turn.row.TurnTriggerEventID], turn)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return runDebugTraceInputs{}, fmt.Errorf("read sqlite run debug trace turns: %w", err)
	}
	rows.Close()

	rows, err = db.QueryContext(ctx, `
		SELECT session_id, COALESCE(run_id, ''), session_kind,
		       COALESCE(memory_enabled, 0), COALESCE(memory_source, ''), COALESCE(status, ''), updated_at
		FROM (
			SELECT session_id, run_id, 'live_session' AS session_kind, memory_enabled, memory_source, status, updated_at FROM agent_sessions
			UNION ALL
			SELECT session_id, run_id, 'turn_audit' AS session_kind, memory_enabled, memory_source, status, updated_at FROM agent_conversation_audits
		) trace_sessions
		WHERE run_id = ? OR run_id IS NULL
		ORDER BY session_id, session_kind
	`, runID)
	if err != nil {
		return runDebugTraceInputs{}, fmt.Errorf("load sqlite run debug trace sessions: %w", err)
	}
	for rows.Next() {
		var sessionID string
		var session runDebugTraceSession
		var updatedRaw any
		if err := rows.Scan(&sessionID, &session.runID, &session.kind, &session.memory, &session.memorySource, &session.status, &updatedRaw); err != nil {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("scan sqlite run debug trace session: %w", err)
		}
		if at, ok, err := sqliteTimeValue(updatedRaw); err != nil {
			rows.Close()
			return runDebugTraceInputs{}, err
		} else if ok {
			session.updatedAt = at
		}
		if _, duplicate := inputs.sessions[sessionID]; duplicate {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("sqlite run debug trace session %s has multiple canonical records", sessionID)
		}
		inputs.sessions[sessionID] = session
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return runDebugTraceInputs{}, fmt.Errorf("read sqlite run debug trace sessions: %w", err)
	}
	rows.Close()
	return inputs, nil
}

func projectRunDebugTrace(inputs runDebugTraceInputs, snapshots []runtimedelivery.Snapshot, opts RunDebugTraceQueryOptions) ([]RunDebugTraceRow, string, error) {
	snapshotsByEvent := map[string][]runtimedelivery.Snapshot{}
	for _, snapshot := range snapshots {
		snapshotsByEvent[snapshot.EventID] = append(snapshotsByEvent[snapshot.EventID], snapshot)
	}
	for eventID := range snapshotsByEvent {
		sort.Slice(snapshotsByEvent[eventID], func(i, j int) bool {
			left, right := snapshotsByEvent[eventID][i], snapshotsByEvent[eventID][j]
			if !left.CreatedAt.Equal(right.CreatedAt) {
				return left.CreatedAt.Before(right.CreatedAt)
			}
			return left.DeliveryID < right.DeliveryID
		})
	}
	var cursor *runDebugTraceCursor
	if opts.Cursor != "" {
		decoded, err := decodeRunDebugTraceCursor(opts.Cursor)
		if err != nil {
			return nil, "", err
		}
		cursor = &decoded
	}
	out := make([]RunDebugTraceRow, 0, opts.Limit+1)
	for _, event := range inputs.events {
		if !traceEventMatchesFilter(event, opts) {
			continue
		}
		eventSnapshots := snapshotsByEvent[event.EventID]
		if len(eventSnapshots) == 0 {
			if traceHasDeliveryFilter(opts.Filter) {
				continue
			}
			turns := inputs.turns[event.EventID]
			if len(turns) == 0 {
				out = appendProjectedTraceRow(out, event, runtimedelivery.Snapshot{}, runDebugTraceTurn{}, inputs.sessions, opts, cursor)
			} else {
				for _, turn := range turns {
					out = appendProjectedTraceRow(out, event, runtimedelivery.Snapshot{}, turn, inputs.sessions, opts, cursor)
				}
			}
		} else {
			for _, snapshot := range eventSnapshots {
				if !traceDeliveryMatchesFilter(snapshot, opts.Filter) {
					continue
				}
				matchingTurns := []runDebugTraceTurn{}
				if snapshot.SubscriberClass == runtimedelivery.SubscriberAgent {
					for _, turn := range inputs.turns[event.EventID] {
						if turn.agentID == snapshot.SubscriberID {
							matchingTurns = append(matchingTurns, turn)
						}
					}
				}
				if len(matchingTurns) == 0 {
					out = appendProjectedTraceRow(out, event, snapshot, runDebugTraceTurn{}, inputs.sessions, opts, cursor)
				} else {
					for _, turn := range matchingTurns {
						out = appendProjectedTraceRow(out, event, snapshot, turn, inputs.sessions, opts, cursor)
					}
				}
			}
		}
		if len(out) > opts.Limit {
			break
		}
	}
	nextCursor := ""
	if len(out) > opts.Limit {
		out = out[:opts.Limit]
		nextCursor = encodeRunDebugTraceCursor(out[len(out)-1])
	}
	return out, nextCursor, nil
}

func appendProjectedTraceRow(out []RunDebugTraceRow, event RunDebugTraceRow, snapshot runtimedelivery.Snapshot, turn runDebugTraceTurn, sessions map[string]runDebugTraceSession, opts RunDebugTraceQueryOptions, cursor *runDebugTraceCursor) []RunDebugTraceRow {
	row := event
	if snapshot.DeliveryID != "" {
		row.DeliveryID = snapshot.DeliveryID
		row.SubscriberType = string(snapshot.SubscriberClass)
		row.SubscriberID = snapshot.SubscriberID
		row.DeliveryStatus = string(snapshot.Status)
		row.DeliveryReasonCode = snapshot.ReasonCode
		row.ReplyContextID = snapshot.Route.Context.ReplyContextID()
		if projection := snapshot.Route.PayloadProjection.Normalized(); !projection.Empty() {
			row.DeliveryPayloadProjection = &projection
		}
		row.DeliveryFailure = runtimefailures.CloneEnvelope(snapshot.Failure)
		row.DeliveryRetryCount = snapshot.RetryCount
		row.DeliveryRetryEligible = snapshot.RetryEligible
		row.DeliveryTerminal = snapshot.Terminal()
		row.ActiveSessionID = snapshot.ActiveSessionID
		row.DeliveryCreatedAt = traceTimePtr(snapshot.CreatedAt)
		row.DeliveryStartedAt = traceTimePtr(snapshot.StartedAt)
		row.DeliveryDeliveredAt = traceTimePtr(snapshot.SettledAt)
	}
	if turn.row.TurnID != "" {
		row.TurnID = turn.row.TurnID
		row.TurnTriggerEventID = turn.row.TurnTriggerEventID
		row.TurnTriggerEventType = turn.row.TurnTriggerEventType
		row.TurnFlowInstance = turn.row.TurnFlowInstance
		row.TurnMemory = turn.row.TurnMemory
		row.TurnMemorySource = turn.row.TurnMemorySource
		row.TurnEntityID = turn.row.TurnEntityID
		row.TurnTaskID = turn.row.TurnTaskID
		row.TurnParseOK = turn.row.TurnParseOK
		row.TurnRetryCount = turn.row.TurnRetryCount
		row.TurnFailure = runtimefailures.CloneEnvelope(turn.row.TurnFailure)
		row.TurnCreatedAt = traceTimePtrFromPointer(turn.row.TurnCreatedAt)
	}
	sessionID := firstNonEmptyStore(turn.sessionID, snapshot.ActiveSessionID)
	if session, ok := sessions[sessionID]; ok {
		row.SessionID = sessionID
		row.SessionKind = session.kind
		row.SessionMemory = session.memory
		row.SessionMemorySource = session.memorySource
		row.SessionStatus = session.status
		row.SessionUpdatedAt = traceTimePtr(session.updatedAt)
	}
	watermark := traceRowWatermark(row)
	if opts.Since != nil && !watermark.After(opts.Since.UTC()) {
		return out
	}
	if opts.Until != nil && watermark.After(opts.Until.UTC()) {
		return out
	}
	if cursor != nil && !traceRowAfterCursor(row, *cursor) {
		return out
	}
	return append(out, row)
}

func traceEventMatchesFilter(row RunDebugTraceRow, opts RunDebugTraceQueryOptions) bool {
	if opts.ExcludeRuntimeLogs && row.EventName == "platform.runtime_log" {
		return false
	}
	return traceStringAllowed(row.EventName, opts.Filter.EventNames) && traceStringAllowed(row.EntityID, opts.Filter.EntityIDs)
}

func traceDeliveryMatchesFilter(snapshot runtimedelivery.Snapshot, filter RunDebugTraceFilter) bool {
	return traceStringAllowed(string(snapshot.Status), filter.DeliveryStatuses) &&
		traceStringAllowed(snapshot.SubscriberID, filter.SubscriberIDs) &&
		traceStringAllowed(string(snapshot.SubscriberClass), filter.SubscriberTypes)
}

func traceHasDeliveryFilter(filter RunDebugTraceFilter) bool {
	return len(filter.DeliveryStatuses) > 0 || len(filter.SubscriberIDs) > 0 || len(filter.SubscriberTypes) > 0
}

func traceStringAllowed(value string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func traceRowWatermark(row RunDebugTraceRow) time.Time {
	watermark := row.EventCreatedAt
	for _, candidate := range []*time.Time{row.DeliveryCreatedAt, row.DeliveryStartedAt, row.DeliveryDeliveredAt, row.SessionUpdatedAt, row.TurnCreatedAt} {
		if candidate != nil && candidate.After(watermark) {
			watermark = *candidate
		}
	}
	return watermark
}

func traceRowAfterCursor(row RunDebugTraceRow, cursor runDebugTraceCursor) bool {
	eventAt, _ := time.Parse(time.RFC3339Nano, cursor.EventCreatedAt)
	if cmp := compareTraceTime(row.EventCreatedAt, eventAt); cmp != 0 {
		return cmp > 0
	}
	if row.EventID != cursor.EventID {
		return row.EventID > cursor.EventID
	}
	if cmp := compareOptionalTraceTime(row.DeliveryCreatedAt, cursor.DeliveryCreatedAt); cmp != 0 {
		return cmp > 0
	}
	if row.DeliveryID != cursor.DeliveryID {
		return row.DeliveryID > cursor.DeliveryID
	}
	if cmp := compareOptionalTraceTime(row.TurnCreatedAt, cursor.TurnCreatedAt); cmp != 0 {
		return cmp > 0
	}
	return row.TurnID > cursor.TurnID
}

func compareOptionalTraceTime(value *time.Time, encoded string) int {
	encoded = strings.TrimSpace(encoded)
	if value == nil && encoded == "" {
		return 0
	}
	if value == nil {
		return -1
	}
	if encoded == "" {
		return 1
	}
	other, _ := time.Parse(time.RFC3339Nano, encoded)
	return compareTraceTime(*value, other)
}

func compareTraceTime(left, right time.Time) int {
	if left.Before(right) {
		return -1
	}
	if left.After(right) {
		return 1
	}
	return 0
}

func traceTimePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	value = value.UTC()
	return &value
}

func traceTimePtrFromPointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	return traceTimePtr(*value)
}
