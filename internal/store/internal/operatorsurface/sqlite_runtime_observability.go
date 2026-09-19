package operatorsurface

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
)

func (s *RunSQLite) LoadRunDebugTracePage(ctx context.Context, runID string, opts operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, string, error) {
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil, "", operatorread.ErrRunNotFound
	}
	opts = defaultRunDebugTraceQueryOptions(opts)
	var exists bool
	if err := s.backend.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = ?)`, runID).Scan(&exists); err != nil {
		return nil, "", fmt.Errorf("load sqlite run trace run existence: %w", err)
	}
	if !exists {
		return nil, "", operatorread.ErrRunNotFound
	}
	return s.loadProjectedRunDebugTrace(ctx, runID, opts)
}

func (s *ObservabilitySQLite) ListOperatorEvents(ctx context.Context, opts operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error) {
	var result operatorread.OperatorEventListResult
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = s.listOperatorEvents(ctx, tx, opts)
		return err
	})
	if err != nil {
		return operatorread.OperatorEventListResult{}, err
	}
	return result, nil
}

func (s *ObservabilitySQLite) listOperatorEvents(ctx context.Context, tx *sql.Tx, opts operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error) {
	opts = defaultOperatorEventListOptions(opts)
	where := []string{"1=1"}
	args := []any{}
	if opts.Filter.RunID != "" {
		where = append(where, "e.run_id = ?")
		args = append(args, nullUUIDString(opts.Filter.RunID))
	}
	if opts.Filter.EventName != "" {
		where = append(where, "e.event_name = ?")
		args = append(args, opts.Filter.EventName)
	}
	if opts.Filter.EntityID != "" {
		where = append(where, "COALESCE(e.entity_id, '') = ?")
		args = append(args, nullUUIDString(opts.Filter.EntityID))
	}
	if opts.Source != "" {
		where = append(where, "COALESCE(e.produced_by, '') = ?")
		args = append(args, opts.Source)
	}
	if opts.Since != nil {
		where = append(where, "e.created_at > ?")
		args = append(args, opts.Since.UTC())
	}
	if opts.Until != nil {
		where = append(where, "e.created_at <= ?")
		args = append(args, opts.Until.UTC())
	}
	if opts.ExcludeRuntimeLogs {
		where = append(where, "e.event_name <> 'platform.runtime_log'")
	}
	if opts.Filter.HasDeadLetter != nil {
		exists := "EXISTS"
		if !*opts.Filter.HasDeadLetter {
			exists = "NOT EXISTS"
		}
		where = append(where, exists+" (SELECT 1 FROM dead_letters dl WHERE dl.original_event_id = e.event_id)")
	}
	var scanCreatedAt time.Time
	var scanEventID string
	if opts.Cursor != "" {
		cursor, err := decodeObservabilityPositionCursor(opts.Cursor, "event.list")
		if err != nil || (cursor.Order != "" && cursor.Order != opts.Order) {
			return operatorread.OperatorEventListResult{}, operatorread.ErrInvalidObservabilityCursor
		}
		createdAt, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
		if err != nil || strings.TrimSpace(cursor.ID) == "" {
			return operatorread.OperatorEventListResult{}, operatorread.ErrInvalidObservabilityCursor
		}
		scanCreatedAt, scanEventID = createdAt.UTC(), cursor.ID
	}
	order := "DESC"
	comparison := "<"
	if opts.Order == "asc" {
		order = "ASC"
		comparison = ">"
	}
	result := operatorread.OperatorEventListResult{Events: []operatorread.OperatorEventFull{}}
	for len(result.Events) <= opts.Limit {
		pageLimit := operatorEventBatchSize(opts.Limit, len(result.Events))
		pageWhere := append([]string(nil), where...)
		pageArgs := append([]any(nil), args...)
		if scanEventID != "" {
			pageWhere = append(pageWhere, "(e.created_at "+comparison+" ? OR (e.created_at = ? AND e.event_id "+comparison+" ?))")
			pageArgs = append(pageArgs, scanCreatedAt, scanCreatedAt, scanEventID)
		}
		pageArgs = append(pageArgs, pageLimit)
		rows, err := tx.QueryContext(ctx, `
			SELECT e.event_id, e.created_at
			FROM events e
			WHERE `+strings.Join(pageWhere, " AND ")+`
			ORDER BY e.created_at `+order+`, e.event_id `+order+`
			LIMIT ?
		`, pageArgs...)
		if err != nil {
			return operatorread.OperatorEventListResult{}, fmt.Errorf("query sqlite operator events: %w", err)
		}
		ids := make([]string, 0, pageLimit)
		for rows.Next() {
			var eventID string
			var createdRaw any
			if err := rows.Scan(&eventID, &createdRaw); err != nil {
				rows.Close()
				return operatorread.OperatorEventListResult{}, fmt.Errorf("scan sqlite operator event id: %w", err)
			}
			createdAt, ok, err := sqliteTimeValue(createdRaw)
			if err != nil || !ok {
				rows.Close()
				if err == nil {
					err = fmt.Errorf("operator event %s is missing created_at", eventID)
				}
				return operatorread.OperatorEventListResult{}, err
			}
			ids = append(ids, eventID)
			scanEventID, scanCreatedAt = eventID, createdAt
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return operatorread.OperatorEventListResult{}, fmt.Errorf("read sqlite operator events: %w", err)
		}
		if err := rows.Close(); err != nil {
			return operatorread.OperatorEventListResult{}, err
		}
		batch, err := s.loadOperatorEventBatch(ctx, tx, ids)
		if err != nil {
			return operatorread.OperatorEventListResult{}, err
		}
		for _, event := range batch {
			if operatorEventMatchesListFilter(event, opts.Filter) {
				result.Events = append(result.Events, event)
			}
		}
		if len(ids) < pageLimit || len(result.Events) > opts.Limit {
			break
		}
	}
	if len(result.Events) > opts.Limit {
		result.Events = result.Events[:opts.Limit]
		last := result.Events[len(result.Events)-1]
		result.NextCursor = encodeObservabilityPositionCursor(observabilityPositionCursor{
			Kind: "event.list", CreatedAt: last.CreatedAt.UTC().Format(time.RFC3339Nano), ID: last.EventID, Order: opts.Order,
		})
	}
	return result, nil
}

func (s *ObservabilitySQLite) LoadOperatorEvent(ctx context.Context, eventID string) (operatorread.OperatorEventFull, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return operatorread.OperatorEventFull{}, operatorread.ErrEventNotFound
	}
	var result operatorread.OperatorEventFull
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = s.loadOperatorEvent(ctx, tx, eventID)
		return err
	})
	if err != nil {
		return operatorread.OperatorEventFull{}, err
	}
	return result, nil
}

func (s *ObservabilitySQLite) loadOperatorEvent(ctx context.Context, tx *sql.Tx, eventID string) (operatorread.OperatorEventFull, error) {
	decoded, settlement, found, err := eventrecordsqlite.LoadAdmitted(ctx, tx, eventID)
	if err != nil {
		return operatorread.OperatorEventFull{}, fmt.Errorf("load sqlite operator event: %w", err)
	}
	if !found {
		return operatorread.OperatorEventFull{}, operatorread.ErrEventNotFound
	}
	deadLetters, err := s.sqliteOperatorEventDeadLetters(ctx, tx, eventID)
	if err != nil {
		return operatorread.OperatorEventFull{}, err
	}
	deliveries, err := s.sqliteOperatorEventDeliveries(ctx, tx, eventID)
	if err != nil {
		return operatorread.OperatorEventFull{}, err
	}
	return assembleOperatorEvent(decoded, settlement, deliveries, deadLetters)
}

func (s *ObservabilitySQLite) ListOperatorRuntimeLogs(ctx context.Context, opts operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error) {
	if opts.Cursor != "" {
		return operatorread.OperatorRuntimeLogListResult{}, operatorread.ErrInvalidObservabilityCursor
	}
	opts = defaultOperatorRuntimeLogListOptions(opts)
	where := []string{"event_name = 'platform.runtime_log'"}
	args := []any{}
	if opts.RunID != "" {
		where = append(where, "run_id = ?")
		args = append(args, nullUUIDString(opts.RunID))
	}
	if opts.EntityID != "" {
		where = append(where, "COALESCE(entity_id, json_extract(payload, '$.details.entity_id'), '') = ?")
		args = append(args, nullUUIDString(opts.EntityID))
	}
	if opts.Level != "" {
		where = append(where, "json_extract(payload, '$.log_level') = ?")
		args = append(args, opts.Level)
	}
	if opts.Component != "" {
		where = append(where, "json_extract(payload, '$.details.component') = ?")
		args = append(args, opts.Component)
	}
	if opts.Source != "" {
		where = append(where, "COALESCE(NULLIF(TRIM(json_extract(payload, '$.details.agent_id')), ''), NULLIF(TRIM(produced_by), ''), 'runtime') = ?")
		args = append(args, opts.Source)
	}
	if opts.SessionID != "" {
		where = append(where, "json_extract(payload, '$.details.session_id') = ?")
		args = append(args, opts.SessionID)
	}
	if opts.ErrorCode != "" {
		where = append(where, "json_extract(payload, '$.details.failure.detail.code') = ?")
		args = append(args, opts.ErrorCode)
	}
	if opts.BundleHash != "" {
		where = append(where, "EXISTS (SELECT 1 FROM runs r WHERE r.run_id = events.run_id AND r.bundle_hash = ?)")
		args = append(args, opts.BundleHash)
	}
	if opts.ActionOrEventType != "" {
		where = append(where, "(json_extract(payload, '$.details.action') = ? OR COALESCE(NULLIF(TRIM(json_extract(payload, '$.details.event_name')), ''), json_extract(payload, '$.details.event_type'), '') = ?)")
		args = append(args, opts.ActionOrEventType, opts.ActionOrEventType)
	}
	if opts.Since != nil {
		where = append(where, "created_at > ?")
		args = append(args, opts.Since.UTC())
	}
	if opts.Until != nil {
		where = append(where, "created_at <= ?")
		args = append(args, opts.Until.UTC())
	}
	order := "DESC"
	if strings.EqualFold(opts.Order, "asc") {
		order = "ASC"
	}
	args = append(args, opts.Limit)
	rows, err := s.backend.QueryContext(ctx, `
		SELECT event_id, created_at, COALESCE(run_id, ''), COALESCE(entity_id, ''), COALESCE(produced_by, ''), payload
		FROM events
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY created_at `+order+`, event_id `+order+`
		LIMIT ?
	`, args...)
	if err != nil {
		return operatorread.OperatorRuntimeLogListResult{}, fmt.Errorf("query sqlite runtime logs: %w", err)
	}
	defer rows.Close()
	result := operatorread.OperatorRuntimeLogListResult{Logs: []operatorread.OperatorRuntimeLogEntry{}}
	for rows.Next() {
		var createdRaw, payloadRaw any
		var logID, runID, entityID, producedBy string
		if err := rows.Scan(&logID, &createdRaw, &runID, &entityID, &producedBy, &payloadRaw); err != nil {
			return operatorread.OperatorRuntimeLogListResult{}, fmt.Errorf("scan sqlite runtime log: %w", err)
		}
		var createdAt time.Time
		if at, ok, err := sqliteTimeValue(createdRaw); err != nil {
			return operatorread.OperatorRuntimeLogListResult{}, err
		} else if ok {
			createdAt = at
		}
		log, err := operatorRuntimeLogEntry(logID, runID, entityID, producedBy, createdAt, sqliteJSONRawMessage(payloadRaw))
		if err != nil {
			return operatorread.OperatorRuntimeLogListResult{}, err
		}
		result.Logs = append(result.Logs, log)
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorRuntimeLogListResult{}, fmt.Errorf("read sqlite runtime logs: %w", err)
	}
	return result, nil
}

func (s *ObservabilitySQLite) ListOperatorRuntimeIncidents(ctx context.Context, opts operatorread.OperatorRuntimeIncidentListOptions) (operatorread.OperatorRuntimeIncidentListResult, error) {
	if s == nil || s.backend == nil || s.schemaGuard == nil {
		return operatorread.OperatorRuntimeIncidentListResult{}, fmt.Errorf("operator observability read surface is required")
	}
	if err := s.schemaGuard(); err != nil {
		return operatorread.OperatorRuntimeIncidentListResult{}, err
	}
	return listOperatorRuntimeIncidents(ctx, s.backend, false, opts)
}

func (s *ObservabilitySQLite) sqliteOperatorEventDeliveries(ctx context.Context, tx *sql.Tx, eventID string) ([]operatorread.OperatorEventDelivery, error) {
	snapshots, err := s.deliverySnapshotsForEvent(ctx, tx, eventID)
	if err != nil {
		return nil, fmt.Errorf("query sqlite operator event deliveries: %w", err)
	}
	out := make([]operatorread.OperatorEventDelivery, 0, len(snapshots))
	for _, snapshot := range snapshots {
		out = append(out, operatorEventDeliveryFromSnapshot(snapshot))
	}
	return out, nil
}

func (s *ObservabilitySQLite) sqliteOperatorEventDeadLetters(ctx context.Context, tx *sql.Tx, eventID string) ([]operatorread.OperatorDeadLetterRecord, error) {
	rows, err := loadOperatorEventDeadLetterBatch(ctx, tx, false, []string{eventID})
	if err != nil {
		return nil, err
	}
	return rows[eventID], nil
}

func (s *ObservabilitySQLite) LoadOperatorDeliveryDeadLetters(ctx context.Context, deliveryID string, claimVersion int64) ([]operatorread.OperatorDeadLetterRecord, error) {
	rows, err := s.backend.QueryContext(ctx, `
		SELECT dead_letter_id, delivery_id, claim_version, failure,
		       COALESCE(retry_count, 0), COALESCE(chain_depth, 0), COALESCE(handler_node, ''), created_at
		FROM dead_letters
		WHERE delivery_id = ?
		  AND claim_version = ?
		ORDER BY claim_version ASC, created_at ASC, dead_letter_id ASC
	`, strings.TrimSpace(deliveryID), claimVersion)
	if err != nil {
		return nil, fmt.Errorf("query sqlite operator delivery dead letters: %w", err)
	}
	defer rows.Close()
	out := []operatorread.OperatorDeadLetterRecord{}
	for rows.Next() {
		var item operatorread.OperatorDeadLetterRecord
		var rawFailure any
		var createdRaw any
		if err := rows.Scan(&item.DeadLetterID, &item.DeliveryID, &item.ClaimVersion, &rawFailure, &item.RetryCount, &item.ChainDepth, &item.HandlerNode, &createdRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite operator delivery dead letter: %w", err)
		}
		failure, err := decodeStoredFailure(rawFailure)
		if err != nil || failure == nil {
			return nil, fmt.Errorf("decode sqlite operator delivery dead-letter failure")
		}
		item.Failure = *failure
		if at, ok, err := sqliteTimeValue(createdRaw); err != nil {
			return nil, err
		} else if ok {
			item.CreatedAt = at
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite operator delivery dead letters: %w", err)
	}
	return out, nil
}

func applySQLiteRuntimeLogPayload(log *operatorread.OperatorRuntimeLogEntry, raw json.RawMessage) error {
	if log == nil {
		return fmt.Errorf("runtime log target is required")
	}
	payload, err := runtimepkg.DecodeCanonicalRuntimeLogPayload(raw)
	if err != nil {
		return err
	}
	log.Level = strings.TrimSpace(payload.LogLevel)
	log.Message = strings.TrimSpace(payload.Message)
	log.CanonicalDetail = payload.Detail
	log.Component = strings.TrimSpace(payload.Component)
	log.Source = strings.TrimSpace(payload.AgentID)
	log.SessionID = strings.TrimSpace(payload.SessionID)
	log.ErrorCode = strings.TrimSpace(payload.ErrorCode)
	log.Failure = runtimefailures.CloneEnvelope(payload.Failure)
	log.EventID = strings.TrimSpace(payload.EventID)
	log.Action = strings.TrimSpace(payload.Action)
	log.EventType = strings.TrimSpace(payload.EventType)
	log.ParentEventID = strings.TrimSpace(payload.ParentEventID)
	log.HandlerID = strings.TrimSpace(payload.HandlerID)
	log.AgentID = strings.TrimSpace(payload.AgentID)
	log.DurationUS = payload.DurationUS
	log.DeliveryState = strings.TrimSpace(payload.DeliveryState)
	log.PreviousState = strings.TrimSpace(payload.PreviousState)
	log.Transition = strings.TrimSpace(payload.Transition)
	log.Reason = strings.TrimSpace(payload.Reason)
	log.Terminal = strings.TrimSpace(payload.Terminal)
	log.RetryCount = payload.RetryCount
	log.Correlation = payload.Correlation
	return nil
}
