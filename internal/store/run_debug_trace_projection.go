package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

type runDebugTraceInputs struct {
	events   map[string]RunDebugTraceRow
	turns    map[string]runDebugTraceTurn
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
	query, err := runDebugTracePageQuery(runID, opts)
	if err != nil {
		return nil, "", err
	}
	page, err := s.deliveryRunTraceReferencePage(ctx, query)
	if err != nil {
		return nil, "", fmt.Errorf("load run debug trace references: %w", err)
	}
	inputs, err := loadPostgresRunDebugTraceInputs(ctx, s.DB, runID, page.References)
	if err != nil {
		return nil, "", err
	}
	return projectRunDebugTrace(inputs, page, opts)
}

func (s *SQLiteRuntimeStore) loadProjectedRunDebugTrace(ctx context.Context, runID string, opts RunDebugTraceQueryOptions) ([]RunDebugTraceRow, string, error) {
	query, err := runDebugTracePageQuery(runID, opts)
	if err != nil {
		return nil, "", err
	}
	page, err := s.deliveryRunTraceReferencePage(ctx, query)
	if err != nil {
		return nil, "", fmt.Errorf("load sqlite run debug trace references: %w", err)
	}
	inputs, err := loadSQLiteRunDebugTraceInputs(ctx, s.DB, runID, page.References)
	if err != nil {
		return nil, "", err
	}
	return projectRunDebugTrace(inputs, page, opts)
}

func runDebugTracePageQuery(runID string, opts RunDebugTraceQueryOptions) (runtimedelivery.RunTracePageQuery, error) {
	query := runtimedelivery.RunTracePageQuery{
		RunID:              runID,
		Limit:              opts.Limit,
		Since:              opts.Since,
		Until:              opts.Until,
		EventNames:         append([]string(nil), opts.Filter.EventNames...),
		EntityIDs:          append([]string(nil), opts.Filter.EntityIDs...),
		SubscriberIDs:      append([]string(nil), opts.Filter.SubscriberIDs...),
		ExcludeRuntimeLogs: opts.ExcludeRuntimeLogs,
	}
	for _, raw := range opts.Filter.DeliveryStatuses {
		status, err := runtimedelivery.ParseStatus(raw)
		if err != nil {
			return runtimedelivery.RunTracePageQuery{}, err
		}
		query.DeliveryStatuses = append(query.DeliveryStatuses, status)
	}
	for _, raw := range opts.Filter.SubscriberTypes {
		class, err := runtimedelivery.ParseSubscriberClass(raw)
		if err != nil {
			return runtimedelivery.RunTracePageQuery{}, err
		}
		query.SubscriberClasses = append(query.SubscriberClasses, class)
	}
	if opts.Cursor != "" {
		cursor, err := decodeRunDebugTraceCursor(opts.Cursor)
		if err != nil {
			return runtimedelivery.RunTracePageQuery{}, err
		}
		position := runtimedelivery.RunTracePosition{EventID: cursor.EventID, DeliveryID: cursor.DeliveryID, TurnID: cursor.TurnID}
		position.EventCreatedAt, _ = time.Parse(time.RFC3339Nano, cursor.EventCreatedAt)
		if cursor.DeliveryCreatedAt != "" {
			position.DeliveryCreatedAt, _ = time.Parse(time.RFC3339Nano, cursor.DeliveryCreatedAt)
		}
		if cursor.TurnCreatedAt != "" {
			position.TurnCreatedAt, _ = time.Parse(time.RFC3339Nano, cursor.TurnCreatedAt)
		}
		query.After = &position
	}
	return query, nil
}

func loadPostgresRunDebugTraceInputs(ctx context.Context, db *sql.DB, runID string, references []runtimedelivery.RunTraceReference) (runDebugTraceInputs, error) {
	inputs := runDebugTraceInputs{events: map[string]RunDebugTraceRow{}, turns: map[string]runDebugTraceTurn{}, sessions: map[string]runDebugTraceSession{}}
	for _, reference := range references {
		if _, loaded := inputs.events[reference.EventID]; loaded {
			continue
		}
		var row RunDebugTraceRow
		if err := db.QueryRowContext(ctx, `
		SELECT event_id::text, COALESCE(event_name, ''), COALESCE(source_event_id::text, ''),
		       COALESCE(entity_id::text, ''), COALESCE(produced_by, ''), COALESCE(produced_by_type, ''), created_at
		FROM events
		WHERE run_id = $1::uuid AND event_id = $2::uuid
	`, runID, reference.EventID).Scan(&row.EventID, &row.EventName, &row.SourceEventID, &row.EntityID, &row.EventSource, &row.EventSourceType, &row.EventCreatedAt); err != nil {
			return runDebugTraceInputs{}, fmt.Errorf("load referenced run debug trace event %s: %w", reference.EventID, err)
		}
		inputs.events[reference.EventID] = row
	}
	for _, reference := range references {
		if reference.TurnID == "" {
			continue
		}
		if _, loaded := inputs.turns[reference.TurnID]; loaded {
			continue
		}
		var turn runDebugTraceTurn
		var rawFailure []byte
		if err := db.QueryRowContext(ctx, `
		SELECT turn_id::text, COALESCE(trigger_event_id::text, ''), COALESCE(trigger_event_type, ''),
		       COALESCE(flow_instance, ''), COALESCE(memory_enabled, false), COALESCE(memory_source, ''),
		       COALESCE(entity_id::text, ''), COALESCE(task_id, ''), COALESCE(parse_ok, false),
		       COALESCE(retry_count, 0), COALESCE(failure, 'null'::jsonb), created_at,
		       COALESCE(agent_id, ''), COALESCE(session_id::text, '')
		FROM agent_turns
		WHERE run_id = $1::uuid AND turn_id = $2::uuid
	`, runID, reference.TurnID).Scan(
			&turn.row.TurnID, &turn.row.TurnTriggerEventID, &turn.row.TurnTriggerEventType,
			&turn.row.TurnFlowInstance, &turn.row.TurnMemory, &turn.row.TurnMemorySource,
			&turn.row.TurnEntityID, &turn.row.TurnTaskID, &turn.row.TurnParseOK,
			&turn.row.TurnRetryCount, &rawFailure, &turn.row.TurnCreatedAt,
			&turn.agentID, &turn.sessionID,
		); err != nil {
			return runDebugTraceInputs{}, fmt.Errorf("load referenced run debug trace turn %s: %w", reference.TurnID, err)
		}
		var err error
		turn.row.TurnFailure, err = decodeStoredFailure(rawFailure)
		if err != nil {
			return runDebugTraceInputs{}, fmt.Errorf("decode run debug trace turn failure: %w", err)
		}
		if turn.row.TurnTriggerEventID != reference.EventID {
			return runDebugTraceInputs{}, fmt.Errorf("run debug trace turn %s does not belong to event %s", reference.TurnID, reference.EventID)
		}
		inputs.turns[reference.TurnID] = turn
	}

	for sessionID := range referencedRunDebugTraceSessions(references, inputs.turns) {
		rows, err := db.QueryContext(ctx, `
		SELECT session_id::text, COALESCE(run_id::text, ''), session_kind,
		       COALESCE(memory_enabled, false), COALESCE(memory_source, ''), COALESCE(status, ''), updated_at
		FROM (`+runDebugTraceSessionSources()+`) trace_sessions
		WHERE session_id = $1::uuid AND (run_id = $2::uuid OR run_id IS NULL)
		ORDER BY session_id, session_kind
	`, sessionID, runID)
		if err != nil {
			return runDebugTraceInputs{}, fmt.Errorf("load referenced run debug trace session %s: %w", sessionID, err)
		}
		for rows.Next() {
			var loadedID string
			var session runDebugTraceSession
			if err := rows.Scan(&loadedID, &session.runID, &session.kind, &session.memory, &session.memorySource, &session.status, &session.updatedAt); err != nil {
				rows.Close()
				return runDebugTraceInputs{}, fmt.Errorf("scan referenced run debug trace session: %w", err)
			}
			if _, duplicate := inputs.sessions[loadedID]; duplicate {
				rows.Close()
				return runDebugTraceInputs{}, fmt.Errorf("run debug trace session %s has multiple canonical records", loadedID)
			}
			inputs.sessions[loadedID] = session
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("read referenced run debug trace session: %w", err)
		}
		rows.Close()
	}
	return inputs, nil
}

func loadSQLiteRunDebugTraceInputs(ctx context.Context, db *sql.DB, runID string, references []runtimedelivery.RunTraceReference) (runDebugTraceInputs, error) {
	inputs := runDebugTraceInputs{events: map[string]RunDebugTraceRow{}, turns: map[string]runDebugTraceTurn{}, sessions: map[string]runDebugTraceSession{}}
	for _, reference := range references {
		if _, loaded := inputs.events[reference.EventID]; loaded {
			continue
		}
		var row RunDebugTraceRow
		var createdRaw any
		if err := db.QueryRowContext(ctx, `
		SELECT event_id, COALESCE(event_name, ''), COALESCE(source_event_id, ''),
		       COALESCE(entity_id, ''), COALESCE(produced_by, ''), COALESCE(produced_by_type, ''), created_at
		FROM events
		WHERE run_id = ? AND event_id = ?
	`, runID, reference.EventID).Scan(&row.EventID, &row.EventName, &row.SourceEventID, &row.EntityID, &row.EventSource, &row.EventSourceType, &createdRaw); err != nil {
			return runDebugTraceInputs{}, fmt.Errorf("load referenced sqlite run debug trace event %s: %w", reference.EventID, err)
		}
		createdAt, ok, err := sqliteTimeValue(createdRaw)
		if err != nil || !ok {
			if err == nil {
				err = fmt.Errorf("event %s is missing created_at", row.EventID)
			}
			return runDebugTraceInputs{}, err
		}
		row.EventCreatedAt = createdAt
		inputs.events[reference.EventID] = row
	}

	for _, reference := range references {
		if reference.TurnID == "" {
			continue
		}
		if _, loaded := inputs.turns[reference.TurnID]; loaded {
			continue
		}
		var turn runDebugTraceTurn
		var rawFailure, createdRaw any
		if err := db.QueryRowContext(ctx, `
		SELECT turn_id, COALESCE(trigger_event_id, ''), COALESCE(trigger_event_type, ''),
		       COALESCE(flow_instance, ''), COALESCE(memory_enabled, 0), COALESCE(memory_source, ''),
		       COALESCE(entity_id, ''), COALESCE(task_id, ''), COALESCE(parse_ok, 0),
		       COALESCE(retry_count, 0), COALESCE(failure, 'null'), created_at,
		       COALESCE(agent_id, ''), COALESCE(session_id, '')
		FROM agent_turns
		WHERE run_id = ? AND turn_id = ?
	`, runID, reference.TurnID).Scan(
			&turn.row.TurnID, &turn.row.TurnTriggerEventID, &turn.row.TurnTriggerEventType,
			&turn.row.TurnFlowInstance, &turn.row.TurnMemory, &turn.row.TurnMemorySource,
			&turn.row.TurnEntityID, &turn.row.TurnTaskID, &turn.row.TurnParseOK,
			&turn.row.TurnRetryCount, &rawFailure, &createdRaw,
			&turn.agentID, &turn.sessionID,
		); err != nil {
			return runDebugTraceInputs{}, fmt.Errorf("load referenced sqlite run debug trace turn %s: %w", reference.TurnID, err)
		}
		var err error
		turn.row.TurnFailure, err = decodeStoredFailure(rawFailure)
		if err != nil {
			return runDebugTraceInputs{}, fmt.Errorf("decode sqlite run debug trace turn failure: %w", err)
		}
		if at, ok, err := sqliteTimeValue(createdRaw); err != nil {
			return runDebugTraceInputs{}, err
		} else if ok {
			turn.row.TurnCreatedAt = &at
		}
		if turn.row.TurnTriggerEventID != reference.EventID {
			return runDebugTraceInputs{}, fmt.Errorf("sqlite run debug trace turn %s does not belong to event %s", reference.TurnID, reference.EventID)
		}
		inputs.turns[reference.TurnID] = turn
	}

	for sessionID := range referencedRunDebugTraceSessions(references, inputs.turns) {
		rows, err := db.QueryContext(ctx, `
		SELECT session_id, COALESCE(run_id, ''), session_kind,
		       COALESCE(memory_enabled, 0), COALESCE(memory_source, ''), COALESCE(status, ''), updated_at
		FROM (
			SELECT session_id, run_id, 'live_session' AS session_kind, memory_enabled, memory_source, status, updated_at FROM agent_sessions
			UNION ALL
			SELECT session_id, run_id, 'turn_audit' AS session_kind, memory_enabled, memory_source, status, updated_at FROM agent_conversation_audits
		) trace_sessions
		WHERE session_id = ? AND (run_id = ? OR run_id IS NULL)
		ORDER BY session_id, session_kind
	`, sessionID, runID)
		if err != nil {
			return runDebugTraceInputs{}, fmt.Errorf("load referenced sqlite run debug trace session %s: %w", sessionID, err)
		}
		for rows.Next() {
			var loadedID string
			var session runDebugTraceSession
			var updatedRaw any
			if err := rows.Scan(&loadedID, &session.runID, &session.kind, &session.memory, &session.memorySource, &session.status, &updatedRaw); err != nil {
				rows.Close()
				return runDebugTraceInputs{}, fmt.Errorf("scan referenced sqlite run debug trace session: %w", err)
			}
			if at, ok, err := sqliteTimeValue(updatedRaw); err != nil {
				rows.Close()
				return runDebugTraceInputs{}, err
			} else if ok {
				session.updatedAt = at
			}
			if _, duplicate := inputs.sessions[loadedID]; duplicate {
				rows.Close()
				return runDebugTraceInputs{}, fmt.Errorf("sqlite run debug trace session %s has multiple canonical records", loadedID)
			}
			inputs.sessions[loadedID] = session
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return runDebugTraceInputs{}, fmt.Errorf("read referenced sqlite run debug trace session: %w", err)
		}
		rows.Close()
	}
	return inputs, nil
}

func referencedRunDebugTraceSessions(references []runtimedelivery.RunTraceReference, turns map[string]runDebugTraceTurn) map[string]struct{} {
	sessions := map[string]struct{}{}
	for _, reference := range references {
		turn := turns[reference.TurnID]
		sessionID := turn.sessionID
		if sessionID == "" && reference.Delivery != nil {
			sessionID = reference.Delivery.ActiveSessionID
		}
		if sessionID != "" {
			sessions[sessionID] = struct{}{}
		}
	}
	return sessions
}

func projectRunDebugTrace(inputs runDebugTraceInputs, page runtimedelivery.RunTraceReferencePage, opts RunDebugTraceQueryOptions) ([]RunDebugTraceRow, string, error) {
	out := make([]RunDebugTraceRow, 0, len(page.References))
	for _, reference := range page.References {
		event, ok := inputs.events[reference.EventID]
		if !ok {
			return nil, "", fmt.Errorf("run debug trace event %s was not hydrated", reference.EventID)
		}
		var snapshot runtimedelivery.Snapshot
		if reference.Delivery != nil {
			snapshot = *reference.Delivery
		}
		var turn runDebugTraceTurn
		if reference.TurnID != "" {
			var found bool
			turn, found = inputs.turns[reference.TurnID]
			if !found {
				return nil, "", fmt.Errorf("run debug trace turn %s was not hydrated", reference.TurnID)
			}
		}
		out = appendProjectedTraceRow(out, event, snapshot, turn, inputs.sessions)
		if !projectedRunDebugTraceRowStillMatches(out[len(out)-1], opts) {
			return nil, "", fmt.Errorf("run debug trace row %s changed while its bounded page was hydrated", reference.EventID)
		}
	}
	nextCursor := ""
	if page.HasMore && len(out) > 0 {
		nextCursor = encodeRunDebugTraceCursor(out[len(out)-1])
	}
	return out, nextCursor, nil
}

func projectedRunDebugTraceRowStillMatches(row RunDebugTraceRow, opts RunDebugTraceQueryOptions) bool {
	if opts.ExcludeRuntimeLogs && row.EventName == "platform.runtime_log" {
		return false
	}
	if !traceStringAllowed(row.EventName, opts.Filter.EventNames) || !traceStringAllowed(row.EntityID, opts.Filter.EntityIDs) {
		return false
	}
	if len(opts.Filter.DeliveryStatuses) > 0 || len(opts.Filter.SubscriberIDs) > 0 || len(opts.Filter.SubscriberTypes) > 0 {
		if row.DeliveryID == "" || !traceStringAllowed(row.DeliveryStatus, opts.Filter.DeliveryStatuses) ||
			!traceStringAllowed(row.SubscriberID, opts.Filter.SubscriberIDs) || !traceStringAllowed(row.SubscriberType, opts.Filter.SubscriberTypes) {
			return false
		}
	}
	watermark := row.EventCreatedAt
	for _, candidate := range []*time.Time{row.DeliveryCreatedAt, row.DeliveryStartedAt, row.DeliveryDeliveredAt, row.SessionUpdatedAt, row.TurnCreatedAt} {
		if candidate != nil && candidate.After(watermark) {
			watermark = *candidate
		}
	}
	return (opts.Since == nil || watermark.After(opts.Since.UTC())) && (opts.Until == nil || !watermark.After(opts.Until.UTC()))
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

func appendProjectedTraceRow(out []RunDebugTraceRow, event RunDebugTraceRow, snapshot runtimedelivery.Snapshot, turn runDebugTraceTurn, sessions map[string]runDebugTraceSession) []RunDebugTraceRow {
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
	return append(out, row)
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
