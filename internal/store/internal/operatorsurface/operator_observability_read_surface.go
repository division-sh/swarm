package operatorsurface

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type observabilityPositionCursor struct {
	Kind      string `json:"kind"`
	CreatedAt string `json:"created_at,omitempty"`
	ID        string `json:"id,omitempty"`
	Order     string `json:"order,omitempty"`
	LastSeen  string `json:"last_seen,omitempty"`
}

func (s *ObservabilityPostgres) LoadOperatorDeliveryDeadLetters(ctx context.Context, deliveryID string, claimVersion int64) ([]operatorread.OperatorDeadLetterRecord, error) {
	return s.loadOperatorDeliveryDeadLetters(ctx, deliveryID, claimVersion)
}

func (r *ObservabilityPostgres) requireOperatorObservabilityAccess() error {
	if r == nil || r.backend == nil {
		return fmt.Errorf("operator observability read surface is required")
	}
	return r.requireCurrentSchema()
}

func (r *ObservabilityPostgres) ListOperatorEvents(ctx context.Context, opts operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error) {
	if err := r.requireOperatorObservabilityAccess(); err != nil {
		return operatorread.OperatorEventListResult{}, err
	}
	opts = defaultOperatorEventListOptions(opts)
	args := make([]any, 0, 12)
	where := []string{"TRUE"}
	add := func(value any) int {
		args = append(args, value)
		return len(args)
	}
	if opts.Filter.RunID != "" {
		n := add(opts.Filter.RunID)
		where = append(where, fmt.Sprintf("e.run_id::text = $%d", n))
	}
	if opts.Filter.EntityID != "" {
		n := add(opts.Filter.EntityID)
		where = append(where, fmt.Sprintf("e.entity_id::text = $%d", n))
	}
	if opts.Filter.EventName != "" {
		n := add(opts.Filter.EventName)
		where = append(where, fmt.Sprintf("e.event_name = $%d", n))
	}
	if opts.Source != "" {
		n := add(opts.Source)
		where = append(where, fmt.Sprintf("COALESCE(e.produced_by, '') = $%d", n))
	}
	if opts.Filter.HasDeadLetter != nil {
		exists := "EXISTS"
		if !*opts.Filter.HasDeadLetter {
			exists = "NOT EXISTS"
		}
		where = append(where, fmt.Sprintf("%s (SELECT 1 FROM dead_letters dl WHERE dl.original_event_id = e.event_id)", exists))
	}
	if opts.ExcludeRuntimeLogs {
		where = append(where, "e.event_name <> 'platform.runtime_log'")
	}
	if opts.Since != nil {
		n := add(opts.Since.UTC())
		where = append(where, fmt.Sprintf("e.created_at > $%d", n))
	}
	if opts.Until != nil {
		n := add(opts.Until.UTC())
		where = append(where, fmt.Sprintf("e.created_at <= $%d", n))
	}
	var scanCreatedAt time.Time
	var scanEventID string
	if opts.Cursor != "" {
		cursor, err := decodeObservabilityPositionCursor(opts.Cursor, "event.list")
		if err != nil {
			return operatorread.OperatorEventListResult{}, err
		}
		if cursor.Order != "" && cursor.Order != opts.Order {
			return operatorread.OperatorEventListResult{}, operatorread.ErrInvalidObservabilityCursor
		}
		createdAt, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
		if err != nil || strings.TrimSpace(cursor.ID) == "" {
			return operatorread.OperatorEventListResult{}, operatorread.ErrInvalidObservabilityCursor
		}
		scanCreatedAt = createdAt.UTC()
		scanEventID = cursor.ID
	}
	orderSQL := "DESC"
	if opts.Order == "asc" {
		orderSQL = "ASC"
	}
	events := make([]operatorread.OperatorEventFull, 0, opts.Limit+1)
	for len(events) <= opts.Limit {
		pageArgs := append([]any(nil), args...)
		pageWhere := append([]string(nil), where...)
		if scanEventID != "" {
			pageArgs = append(pageArgs, scanCreatedAt, scanEventID)
			timeArg, idArg := len(pageArgs)-1, len(pageArgs)
			comparison := "<"
			if opts.Order == "asc" {
				comparison = ">"
			}
			pageWhere = append(pageWhere, fmt.Sprintf("(e.created_at %s $%d OR (e.created_at = $%d AND e.event_id::text %s $%d))", comparison, timeArg, timeArg, comparison, idArg))
		}
		pageArgs = append(pageArgs, opts.Limit+1)
		rows, err := r.backend.QueryContext(ctx, `
			SELECT e.event_id::text, e.created_at
			FROM events e
			WHERE `+strings.Join(pageWhere, " AND ")+fmt.Sprintf(`
			ORDER BY e.created_at %s, e.event_id::text %s
			LIMIT $%d
		`, orderSQL, orderSQL, len(pageArgs)), pageArgs...)
		if err != nil {
			return operatorread.OperatorEventListResult{}, fmt.Errorf("list operator events: %w", err)
		}
		candidates := 0
		for rows.Next() {
			var id string
			var createdAt time.Time
			if err := rows.Scan(&id, &createdAt); err != nil {
				rows.Close()
				return operatorread.OperatorEventListResult{}, fmt.Errorf("scan operator event id: %w", err)
			}
			candidates++
			scanEventID, scanCreatedAt = id, createdAt.UTC()
			event, err := r.LoadOperatorEvent(ctx, id)
			if err != nil {
				rows.Close()
				return operatorread.OperatorEventListResult{}, err
			}
			if operatorEventMatchesListFilter(event, opts.Filter) {
				events = append(events, event)
				if len(events) > opts.Limit {
					break
				}
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return operatorread.OperatorEventListResult{}, fmt.Errorf("read operator event ids: %w", err)
		}
		rows.Close()
		if candidates < opts.Limit+1 || len(events) > opts.Limit {
			break
		}
	}
	nextCursor := ""
	if len(events) > opts.Limit {
		events = events[:opts.Limit]
		last := events[len(events)-1]
		nextCursor = encodeObservabilityPositionCursor(observabilityPositionCursor{
			Kind:      "event.list",
			CreatedAt: last.CreatedAt.UTC().Format(time.RFC3339Nano),
			ID:        last.EventID,
			Order:     opts.Order,
		})
	}
	if events == nil {
		events = []operatorread.OperatorEventFull{}
	}
	return operatorread.OperatorEventListResult{Events: events, NextCursor: nextCursor}, nil
}

func operatorEventMatchesListFilter(event operatorread.OperatorEventFull, filter operatorread.OperatorEventListFilter) bool {
	if filter.DeliveryStatus != "" || filter.SubscriberID != "" || filter.SubscriberType != "" {
		matched := false
		for _, delivery := range event.Deliveries {
			if (filter.DeliveryStatus == "" || delivery.Status == filter.DeliveryStatus) &&
				(filter.SubscriberID == "" || delivery.SubscriberID == filter.SubscriberID) &&
				(filter.SubscriberType == "" || delivery.SubscriberType == filter.SubscriberType) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if filter.ReasonCode != "" {
		for _, delivery := range event.Deliveries {
			if delivery.ReasonCode == filter.ReasonCode {
				return true
			}
		}
		for _, deadLetter := range event.DeadLetters {
			if string(deadLetter.Failure.Class) == filter.ReasonCode || deadLetter.Failure.Detail.Code == filter.ReasonCode {
				return true
			}
		}
		if event.NoDelivery != nil && event.NoDelivery.Reason == filter.ReasonCode {
			return true
		}
		return false
	}
	return true
}

func (r *ObservabilityPostgres) LoadOperatorEvent(ctx context.Context, eventID string) (operatorread.OperatorEventFull, error) {
	if err := r.requireOperatorObservabilityAccess(); err != nil {
		return operatorread.OperatorEventFull{}, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return operatorread.OperatorEventFull{}, operatorread.ErrEventNotFound
	}
	parsedEventID, err := uuid.Parse(eventID)
	if err != nil {
		return operatorread.OperatorEventFull{}, operatorread.ErrEventNotFound
	}
	row, found, err := loadPostgresEventIdentity(ctx, r.backend, parsedEventID.String())
	if err != nil {
		return operatorread.OperatorEventFull{}, fmt.Errorf("load operator event: %w", err)
	}
	if !found {
		return operatorread.OperatorEventFull{}, operatorread.ErrEventNotFound
	}
	decoded, err := decodeEventRecord(row)
	if err != nil {
		return operatorread.OperatorEventFull{}, fmt.Errorf("load operator event: %w", err)
	}
	event, err := operatorread.NewOperatorEventFull(decoded.Event())
	if err != nil {
		return operatorread.OperatorEventFull{}, err
	}
	deadLetters, err := r.loadOperatorEventDeadLetters(ctx, event.EventID)
	if err != nil {
		return operatorread.OperatorEventFull{}, err
	}
	deliveries, err := r.loadOperatorEventDeliveries(ctx, event.EventID)
	if err != nil {
		return operatorread.OperatorEventFull{}, err
	}
	event.Deliveries = operatorread.EnrichOperatorDeliveryFailureEvidence(deliveries, deadLetters)
	event.DeadLetters = deadLetters
	settlement, err := row.DecodeSettlement()
	if err != nil {
		return operatorread.OperatorEventFull{}, fmt.Errorf("load operator event settlement: %w", err)
	}
	if err := applyRouteSettlement(&event, settlement); err != nil {
		return operatorread.OperatorEventFull{}, fmt.Errorf("load operator event settlement: %w", err)
	}
	if event.Deliveries == nil {
		event.Deliveries = []operatorread.OperatorEventDelivery{}
	}
	if event.DeadLetters == nil {
		event.DeadLetters = []operatorread.OperatorDeadLetterRecord{}
	}
	return event, nil
}

func (r *ObservabilityPostgres) loadOperatorEventDeliveries(ctx context.Context, eventID string) ([]operatorread.OperatorEventDelivery, error) {
	snapshots, err := r.deliverySnapshotsForEvent(ctx, eventID)
	if err != nil {
		return nil, fmt.Errorf("load operator event deliveries: %w", err)
	}
	out := make([]operatorread.OperatorEventDelivery, 0, len(snapshots))
	for _, snapshot := range snapshots {
		item := operatorEventDeliveryFromSnapshot(snapshot)
		out = append(out, item)
	}
	return out, nil
}

func operatorEventDeliveryFromSnapshot(snapshot runtimedelivery.Snapshot) operatorread.OperatorEventDelivery {
	owner := snapshot.Route.Target
	target := owner.Route()
	item := operatorread.OperatorEventDelivery{
		DeliveryID: snapshot.DeliveryID, SubscriberType: string(snapshot.SubscriberClass),
		SubscriberID: snapshot.SubscriberID, Route: snapshot.Route.Normalized(), SessionID: snapshot.ActiveSessionID,
		Target: operatorread.OperatorDeliveryTarget{Kind: owner.Code(), FlowID: target.FlowID, FlowInstance: target.FlowInstance, EntityID: target.EntityID},
		Status: string(snapshot.Status), ReasonCode: snapshot.ReasonCode,
		Failure: runtimefailures.CloneEnvelope(snapshot.Failure), RetryCount: snapshot.RetryCount,
		RetryScheduled: snapshot.RetryScheduled, Terminal: snapshot.Terminal(),
		ClaimVersion: snapshot.ClaimVersion,
	}
	if !snapshot.CreatedAt.IsZero() {
		created := snapshot.CreatedAt
		item.CreatedAt = &created
	}
	if !snapshot.StartedAt.IsZero() {
		started := snapshot.StartedAt
		item.StartedAt = &started
	}
	if !snapshot.SettledAt.IsZero() {
		settled := snapshot.SettledAt
		item.FinishedAt = &settled
	}
	return item
}

func applyRouteSettlement(e *operatorread.OperatorEventFull, settlement events.RouteSettlement) error {
	if e == nil {
		return fmt.Errorf("operator event is required")
	}
	routes := make([]events.DeliveryRoute, 0, len(e.Deliveries))
	for _, delivery := range e.Deliveries {
		routes = append(routes, delivery.Route)
	}
	if err := settlement.Validate(routes); err != nil {
		return err
	}
	e.NoDelivery = nil
	if settlement.Delivered() {
		return nil
	}
	plans := make([]operatorread.OperatorConnectPlanEvaluation, 0, len(settlement.Ledger().Plans()))
	for _, plan := range settlement.Ledger().Plans() {
		targets := make([]operatorread.OperatorConnectPlanTarget, 0, len(plan.Targets()))
		for _, target := range plan.Targets() {
			target = target.Normalized()
			targets = append(targets, operatorread.OperatorConnectPlanTarget{FlowID: target.FlowID, FlowInstance: target.FlowInstance, EntityID: target.EntityID})
		}
		candidates := make([]operatorread.OperatorConnectCandidateEvidence, 0, len(plan.Candidates()))
		for _, candidate := range plan.Candidates() {
			agent := ""
			if !candidate.AgentPlan().IsZero() {
				agent = candidate.AgentPlan().Description()
			}
			candidates = append(candidates, operatorread.OperatorConnectCandidateEvidence{
				ReceiverSHA256: candidate.Receiver().String(), RecipientKind: candidate.Recipient().Code(),
				RecipientID: candidate.Recipient().ID(), Path: candidate.Path(), AgentIdentity: agent,
				Outcome: candidate.Outcome().Code(),
			})
		}
		plans = append(plans, operatorread.OperatorConnectPlanEvaluation{
			PlanSHA256: plan.PlanIdentity().String(), Resolution: plan.Resolution().Code(), Targets: targets, Candidates: candidates,
		})
	}
	e.NoDelivery = &operatorread.OperatorNoDelivery{Reason: settlement.Reason().Code(), Plans: plans}
	return nil
}

func (r *ObservabilityPostgres) loadOperatorEventDeadLetters(ctx context.Context, eventID string) ([]operatorread.OperatorDeadLetterRecord, error) {
	rows, err := r.backend.QueryContext(ctx, `
		SELECT
			dl.dead_letter_id::text,
			COALESCE(dl.delivery_id::text, ''),
			COALESCE(dl.claim_version, 0),
			dl.failure,
			COALESCE(dl.retry_count, 0),
			COALESCE(dl.chain_depth, 0),
			COALESCE(dl.handler_node, ''),
			dl.created_at
		FROM dead_letters dl
		WHERE dl.original_event_id::text = $1
		ORDER BY dl.created_at ASC, dl.dead_letter_id::text ASC
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("load operator event dead letters: %w", err)
	}
	defer rows.Close()
	out := []operatorread.OperatorDeadLetterRecord{}
	for rows.Next() {
		var item operatorread.OperatorDeadLetterRecord
		var rawFailure []byte
		if err := rows.Scan(&item.DeadLetterID, &item.DeliveryID, &item.ClaimVersion, &rawFailure, &item.RetryCount, &item.ChainDepth, &item.HandlerNode, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan operator event dead letter: %w", err)
		}
		failure, err := decodeStoredFailure(rawFailure)
		if err != nil || failure == nil {
			return nil, fmt.Errorf("decode operator event dead letter failure: %w", err)
		}
		item.Failure = *failure
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read operator event dead letters: %w", err)
	}
	return out, nil
}

func (r *ObservabilityPostgres) loadOperatorDeliveryDeadLetters(ctx context.Context, deliveryID string, claimVersion int64) ([]operatorread.OperatorDeadLetterRecord, error) {
	rows, err := r.backend.QueryContext(ctx, `
		SELECT
			dl.dead_letter_id::text,
			dl.delivery_id::text,
			dl.claim_version,
			dl.failure,
			COALESCE(dl.retry_count, 0),
			COALESCE(dl.chain_depth, 0),
			COALESCE(dl.handler_node, ''),
			dl.created_at
		FROM dead_letters dl
		WHERE dl.delivery_id::text = $1
		  AND dl.claim_version = $2
		ORDER BY dl.claim_version ASC, dl.created_at ASC, dl.dead_letter_id::text ASC
	`, strings.TrimSpace(deliveryID), claimVersion)
	if err != nil {
		return nil, fmt.Errorf("load operator delivery dead letters: %w", err)
	}
	defer rows.Close()
	out := []operatorread.OperatorDeadLetterRecord{}
	for rows.Next() {
		var item operatorread.OperatorDeadLetterRecord
		var rawFailure []byte
		if err := rows.Scan(&item.DeadLetterID, &item.DeliveryID, &item.ClaimVersion, &rawFailure, &item.RetryCount, &item.ChainDepth, &item.HandlerNode, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan operator delivery dead letter: %w", err)
		}
		failure, err := decodeStoredFailure(rawFailure)
		if err != nil || failure == nil {
			return nil, fmt.Errorf("decode operator delivery dead letter failure: %w", err)
		}
		item.Failure = *failure
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read operator delivery dead letters: %w", err)
	}
	return out, nil
}

func (r *ObservabilityPostgres) ListOperatorRuntimeLogs(ctx context.Context, opts operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error) {
	if err := r.requireOperatorObservabilityAccess(); err != nil {
		return operatorread.OperatorRuntimeLogListResult{}, err
	}
	opts = defaultOperatorRuntimeLogListOptions(opts)
	cursorClause := ""
	args := []any{opts.RunID, opts.EntityID, opts.Component, opts.Level, opts.ErrorCode, opts.Source, opts.ActionOrEventType, opts.SessionID, opts.BundleHash}
	if opts.Since != nil {
		args = append(args, opts.Since.UTC())
		cursorClause += fmt.Sprintf(" AND e.created_at > $%d", len(args))
	}
	if opts.Until != nil {
		args = append(args, opts.Until.UTC())
		cursorClause += fmt.Sprintf(" AND e.created_at <= $%d", len(args))
	}
	if opts.Cursor != "" {
		cursor, err := decodeObservabilityPositionCursor(opts.Cursor, "runtime.logs")
		if err != nil {
			return operatorread.OperatorRuntimeLogListResult{}, err
		}
		if cursor.Order != opts.Order {
			return operatorread.OperatorRuntimeLogListResult{}, operatorread.ErrInvalidObservabilityCursor
		}
		createdAt, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
		if err != nil || strings.TrimSpace(cursor.ID) == "" {
			return operatorread.OperatorRuntimeLogListResult{}, operatorread.ErrInvalidObservabilityCursor
		}
		args = append(args, createdAt.UTC(), cursor.ID)
		if opts.Order == "asc" {
			cursorClause += fmt.Sprintf(" AND (e.created_at > $%d OR (e.created_at = $%d AND e.event_id::text > $%d))", len(args)-1, len(args)-1, len(args))
		} else {
			cursorClause += fmt.Sprintf(" AND (e.created_at < $%d OR (e.created_at = $%d AND e.event_id::text < $%d))", len(args)-1, len(args)-1, len(args))
		}
	}
	orderSQL := "DESC"
	compareOrder := "DESC"
	if opts.Order == "asc" {
		orderSQL = "ASC"
		compareOrder = "ASC"
	}
	args = append(args, opts.Limit+1)
	rows, err := r.backend.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			e.event_id::text,
			COALESCE(e.run_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			e.created_at,
			COALESCE(e.produced_by, ''),
			COALESCE(e.payload, '{}'::jsonb)
		FROM events e
		WHERE e.event_name = 'platform.runtime_log'
		  AND ($1 = '' OR e.run_id::text = $1)
		  AND ($2 = '' OR COALESCE(e.entity_id::text, e.payload->'details'->>'entity_id', '') = $2)
		  AND ($3 = '' OR COALESCE(e.payload->'details'->>'component', '') = $3)
		  AND ($4 = '' OR COALESCE(e.payload->>'log_level', '') = $4)
		  AND ($5 = '' OR COALESCE(e.payload->'details'->'failure'->'detail'->>'code', '') = $5)
		  AND ($6 = '' OR COALESCE(NULLIF(BTRIM(e.payload->'details'->>'agent_id'), ''), NULLIF(BTRIM(e.produced_by), ''), 'runtime') = $6)
		  AND ($7 = '' OR COALESCE(e.payload->'details'->>'action', '') = $7 OR COALESCE(e.payload->'details'->>'event_name', e.payload->'details'->>'event_type', '') = $7)
		  AND ($8 = '' OR COALESCE(e.payload->'details'->>'session_id', '') = $8)
		  AND ($9 = '' OR EXISTS (
		  	SELECT 1
		  	FROM runs r
		  	WHERE r.run_id = e.run_id
		  	  AND r.bundle_hash = $9
		  ))
		  %s
		ORDER BY e.created_at %s, e.event_id::text %s
		LIMIT $%d
	`, cursorClause, orderSQL, compareOrder, len(args)), args...)
	if err != nil {
		return operatorread.OperatorRuntimeLogListResult{}, fmt.Errorf("list operator runtime logs: %w", err)
	}
	defer rows.Close()
	logs := []operatorread.OperatorRuntimeLogEntry{}
	for rows.Next() {
		var (
			eventID    string
			runID      string
			entityID   string
			createdAt  time.Time
			producedBy string
			payloadRaw []byte
		)
		if err := rows.Scan(&eventID, &runID, &entityID, &createdAt, &producedBy, &payloadRaw); err != nil {
			return operatorread.OperatorRuntimeLogListResult{}, fmt.Errorf("scan operator runtime log: %w", err)
		}
		entry, err := operatorRuntimeLogEntry(eventID, runID, entityID, producedBy, createdAt, payloadRaw)
		if err != nil {
			return operatorread.OperatorRuntimeLogListResult{}, err
		}
		logs = append(logs, entry)
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorRuntimeLogListResult{}, fmt.Errorf("read operator runtime logs: %w", err)
	}
	nextCursor := ""
	if len(logs) > opts.Limit {
		logs = logs[:opts.Limit]
		last := logs[len(logs)-1]
		nextCursor = encodeObservabilityPositionCursor(observabilityPositionCursor{
			Kind:      "runtime.logs",
			CreatedAt: last.TS.UTC().Format(time.RFC3339Nano),
			ID:        last.LogID,
			Order:     opts.Order,
		})
	}
	if logs == nil {
		logs = []operatorread.OperatorRuntimeLogEntry{}
	}
	return operatorread.OperatorRuntimeLogListResult{Logs: logs, NextCursor: nextCursor}, nil
}

func (r *ObservabilityPostgres) ListOperatorRuntimeIncidents(ctx context.Context, opts operatorread.OperatorRuntimeIncidentListOptions) (operatorread.OperatorRuntimeIncidentListResult, error) {
	if err := r.requireOperatorObservabilityAccess(); err != nil {
		return operatorread.OperatorRuntimeIncidentListResult{}, err
	}
	opts = defaultOperatorRuntimeIncidentListOptions(opts)
	cutoff := time.Now().UTC().Add(-time.Duration(opts.SinceHours) * time.Hour)
	rows, err := r.backend.QueryContext(ctx, `
		SELECT
			e.event_id::text,
			COALESCE(e.run_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			e.created_at,
			COALESCE(e.produced_by, ''),
			COALESCE(e.payload, '{}'::jsonb)
		FROM events e
		WHERE e.event_name = 'platform.runtime_log'
		  AND e.created_at >= $1
		  AND ($2 = '' OR COALESCE(e.payload->'details'->>'component', '') = $2)
		  AND ($3 = '' OR COALESCE(e.payload->>'log_level', '') = $3)
		  AND ($4 = '' OR EXISTS (
		  	SELECT 1
		  	FROM runs r
		  	WHERE r.run_id = e.run_id
		  	  AND r.bundle_hash = $4
		  ))
		ORDER BY e.created_at DESC, e.event_id::text DESC
	`, cutoff, opts.Component, opts.Level, opts.BundleHash)
	if err != nil {
		return operatorread.OperatorRuntimeIncidentListResult{}, fmt.Errorf("list operator runtime incident logs: %w", err)
	}
	defer rows.Close()
	type aggregate struct {
		item       operatorread.OperatorRuntimeIncident
		agents     map[string]struct{}
		actions    map[string]struct{}
		components map[string]struct{}
	}
	aggregates := map[string]*aggregate{}
	for rows.Next() {
		var (
			eventID    string
			runID      string
			entityID   string
			createdAt  time.Time
			producedBy string
			payloadRaw []byte
		)
		if err := rows.Scan(&eventID, &runID, &entityID, &createdAt, &producedBy, &payloadRaw); err != nil {
			return operatorread.OperatorRuntimeIncidentListResult{}, fmt.Errorf("scan operator runtime incident log: %w", err)
		}
		logEntry, err := operatorRuntimeLogEntry(eventID, runID, entityID, producedBy, createdAt, payloadRaw)
		if err != nil {
			return operatorread.OperatorRuntimeIncidentListResult{}, err
		}
		if opts.MCPOnly && !strings.HasPrefix(strings.TrimSpace(logEntry.Component), "mcp") {
			continue
		}
		if strings.TrimSpace(logEntry.ErrorCode) == "" {
			continue
		}
		key := strings.Join([]string{logEntry.ErrorCode, logEntry.Component, logEntry.Level}, "\x00")
		agg := aggregates[key]
		if agg == nil {
			agg = &aggregate{
				item: operatorread.OperatorRuntimeIncident{
					IncidentID:    operatorIncidentID(key),
					FirstSeen:     logEntry.TS,
					LastSeen:      logEntry.TS,
					Level:         logEntry.Level,
					Component:     logEntry.Component,
					ErrorCode:     logEntry.ErrorCode,
					SampleMessage: operatorRuntimeLogFailureMessage(logEntry),
					SampleLogIDs:  []string{},
				},
				agents:     map[string]struct{}{},
				actions:    map[string]struct{}{},
				components: map[string]struct{}{},
			}
			aggregates[key] = agg
		}
		agg.item.Count++
		if logEntry.TS.Before(agg.item.FirstSeen) {
			agg.item.FirstSeen = logEntry.TS
		}
		if logEntry.TS.After(agg.item.LastSeen) {
			agg.item.LastSeen = logEntry.TS
		}
		if len(agg.item.SampleLogIDs) < 5 {
			agg.item.SampleLogIDs = append(agg.item.SampleLogIDs, logEntry.LogID)
		}
		if agg.item.SampleMessage == "" {
			agg.item.SampleMessage = operatorRuntimeLogFailureMessage(logEntry)
		}
		if agentID := strings.TrimSpace(logEntry.AgentID); agentID != "" {
			agg.agents[agentID] = struct{}{}
		}
		if action := strings.TrimSpace(logEntry.Action); action != "" {
			agg.actions[action] = struct{}{}
		}
		if logEntry.Component != "" {
			agg.components[logEntry.Component] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorRuntimeIncidentListResult{}, fmt.Errorf("read operator runtime incident logs: %w", err)
	}
	out := make([]operatorread.OperatorRuntimeIncident, 0, len(aggregates))
	for _, agg := range aggregates {
		agg.item.Agents = sortedStoreStringSet(agg.agents)
		agg.item.Actions = sortedStoreStringSet(agg.actions)
		agg.item.Components = sortedStoreStringSet(agg.components)
		out = append(out, agg.item)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].LastSeen.Equal(out[j].LastSeen) {
			return out[i].LastSeen.After(out[j].LastSeen)
		}
		return out[i].IncidentID < out[j].IncidentID
	})
	if opts.Cursor != "" {
		cursor, err := decodeObservabilityPositionCursor(opts.Cursor, "runtime.incidents")
		if err != nil {
			return operatorread.OperatorRuntimeIncidentListResult{}, err
		}
		lastSeen, err := time.Parse(time.RFC3339Nano, cursor.LastSeen)
		if err != nil || cursor.ID == "" {
			return operatorread.OperatorRuntimeIncidentListResult{}, operatorread.ErrInvalidObservabilityCursor
		}
		filtered := out[:0]
		for _, item := range out {
			if item.LastSeen.Before(lastSeen) || (item.LastSeen.Equal(lastSeen) && item.IncidentID > cursor.ID) {
				filtered = append(filtered, item)
			}
		}
		out = filtered
	}
	nextCursor := ""
	if len(out) > opts.Limit {
		out = out[:opts.Limit]
		last := out[len(out)-1]
		nextCursor = encodeObservabilityPositionCursor(observabilityPositionCursor{
			Kind:     "runtime.incidents",
			LastSeen: last.LastSeen.UTC().Format(time.RFC3339Nano),
			ID:       last.IncidentID,
		})
	}
	if out == nil {
		out = []operatorread.OperatorRuntimeIncident{}
	}
	return operatorread.OperatorRuntimeIncidentListResult{Incidents: out, NextCursor: nextCursor}, nil
}

func operatorRuntimeLogFailureMessage(logEntry operatorread.OperatorRuntimeLogEntry) string {
	if logEntry.Failure != nil {
		return strings.TrimSpace(logEntry.Failure.Message)
	}
	return strings.TrimSpace(logEntry.Message)
}

func operatorRuntimeLogEntry(eventID, runID, rowEntityID, producedBy string, createdAt time.Time, payloadRaw []byte) (operatorread.OperatorRuntimeLogEntry, error) {
	payload, err := runtimepkg.DecodeCanonicalRuntimeLogPayload(payloadRaw)
	if err != nil {
		return operatorread.OperatorRuntimeLogEntry{}, fmt.Errorf("decode canonical runtime log payload: %w", err)
	}
	details := map[string]any{}
	for key, value := range payload.Detail {
		details[key] = value
	}
	return operatorread.OperatorRuntimeLogEntry{
		LogID:           strings.TrimSpace(eventID),
		TS:              createdAt.UTC(),
		Level:           strings.TrimSpace(payload.LogLevel),
		Component:       strings.TrimSpace(payload.Component),
		Source:          firstNonEmptyStore(payload.AgentID, producedBy, "runtime"),
		RunID:           strings.TrimSpace(runID),
		EntityID:        firstNonEmptyStore(payload.EntityID, rowEntityID),
		SessionID:       strings.TrimSpace(payload.SessionID),
		ErrorCode:       strings.TrimSpace(payload.ErrorCode),
		Failure:         runtimefailures.CloneEnvelope(payload.Failure),
		Message:         strings.TrimSpace(payload.Message),
		EventID:         strings.TrimSpace(payload.EventID),
		Action:          strings.TrimSpace(payload.Action),
		EventType:       strings.TrimSpace(payload.EventType),
		ParentEventID:   strings.TrimSpace(payload.ParentEventID),
		HandlerID:       strings.TrimSpace(payload.HandlerID),
		AgentID:         strings.TrimSpace(payload.AgentID),
		DurationUS:      payload.DurationUS,
		DeliveryState:   strings.TrimSpace(payload.DeliveryState),
		PreviousState:   strings.TrimSpace(payload.PreviousState),
		Transition:      strings.TrimSpace(payload.Transition),
		Reason:          strings.TrimSpace(payload.Reason),
		Terminal:        strings.TrimSpace(payload.Terminal),
		RetryCount:      payload.RetryCount,
		Correlation:     payload.Correlation,
		CanonicalDetail: details,
	}, nil
}

func defaultOperatorEventListOptions(opts operatorread.OperatorEventListOptions) operatorread.OperatorEventListOptions {
	opts.Filter.RunID = strings.TrimSpace(opts.Filter.RunID)
	opts.Filter.EntityID = strings.TrimSpace(opts.Filter.EntityID)
	opts.Filter.EventName = strings.TrimSpace(opts.Filter.EventName)
	opts.Filter.DeliveryStatus = strings.TrimSpace(opts.Filter.DeliveryStatus)
	opts.Filter.SubscriberID = strings.TrimSpace(opts.Filter.SubscriberID)
	opts.Filter.SubscriberType = strings.TrimSpace(opts.Filter.SubscriberType)
	opts.Filter.ReasonCode = strings.TrimSpace(opts.Filter.ReasonCode)
	opts.Source = strings.TrimSpace(opts.Source)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	opts.Order = strings.ToLower(strings.TrimSpace(opts.Order))
	if opts.Order == "" {
		opts.Order = "desc"
	}
	if opts.Order != "asc" && opts.Order != "desc" {
		opts.Order = "desc"
	}
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	if opts.Limit > 1000 {
		opts.Limit = 1000
	}
	return opts
}

func defaultOperatorRuntimeLogListOptions(opts operatorread.OperatorRuntimeLogListOptions) operatorread.OperatorRuntimeLogListOptions {
	opts.RunID = strings.TrimSpace(opts.RunID)
	opts.BundleHash = strings.TrimSpace(opts.BundleHash)
	opts.EntityID = strings.TrimSpace(opts.EntityID)
	opts.SessionID = strings.TrimSpace(opts.SessionID)
	opts.Component = strings.TrimSpace(opts.Component)
	opts.Level = strings.TrimSpace(opts.Level)
	opts.ErrorCode = strings.TrimSpace(opts.ErrorCode)
	opts.Source = strings.TrimSpace(opts.Source)
	opts.ActionOrEventType = strings.TrimSpace(opts.ActionOrEventType)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	opts.Order = strings.ToLower(strings.TrimSpace(opts.Order))
	if opts.Order == "" {
		opts.Order = "desc"
	}
	if opts.Order != "asc" {
		opts.Order = "desc"
	}
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	if opts.Limit > 1000 {
		opts.Limit = 1000
	}
	return opts
}

func defaultOperatorRuntimeIncidentListOptions(opts operatorread.OperatorRuntimeIncidentListOptions) operatorread.OperatorRuntimeIncidentListOptions {
	opts.BundleHash = strings.TrimSpace(opts.BundleHash)
	opts.Component = strings.TrimSpace(opts.Component)
	opts.Level = strings.TrimSpace(opts.Level)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.SinceHours <= 0 {
		opts.SinceHours = 24
	}
	if opts.SinceHours > 720 {
		opts.SinceHours = 720
	}
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	return opts
}

func encodeObservabilityPositionCursor(cursor observabilityPositionCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeObservabilityPositionCursor(raw string, kind string) (observabilityPositionCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return observabilityPositionCursor{}, operatorread.ErrInvalidObservabilityCursor
	}
	var cursor observabilityPositionCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return observabilityPositionCursor{}, operatorread.ErrInvalidObservabilityCursor
	}
	if strings.TrimSpace(cursor.Kind) != kind {
		return observabilityPositionCursor{}, operatorread.ErrInvalidObservabilityCursor
	}
	return cursor, nil
}

func operatorIncidentID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "inc_" + hex.EncodeToString(sum[:8])
}

func firstNonEmptyStore(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func sortedStoreStringSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
	}
	sort.Strings(out)
	return out
}
