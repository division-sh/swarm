package operatorsurface

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimetimerobligation "github.com/division-sh/swarm/internal/runtime/timerobligation"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func defaultRunDebugQueryOptions(opts operatorread.RunDebugQueryOptions) operatorread.RunDebugQueryOptions {
	if opts.EventLimit <= 0 {
		opts.EventLimit = 15
	}
	if opts.MutationLimit <= 0 {
		opts.MutationLimit = 20
	}
	if opts.RuntimeLogLimit <= 0 {
		opts.RuntimeLogLimit = 20
	}
	if opts.DeadLetterLimit <= 0 {
		opts.DeadLetterLimit = 10
	}
	opts.Component = strings.TrimSpace(opts.Component)
	return opts
}

func defaultRunDebugTraceQueryOptions(opts operatorread.RunDebugTraceQueryOptions) operatorread.RunDebugTraceQueryOptions {
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.Limit <= 0 {
		opts.Limit = 200
	}
	if opts.Limit > 2000 {
		opts.Limit = 2000
	}
	opts.Filter.EventNames = normalizedUniqueStrings(opts.Filter.EventNames)
	opts.Filter.EntityIDs = normalizedUniqueStrings(opts.Filter.EntityIDs)
	opts.Filter.DeliveryStatuses = normalizedUniqueStrings(opts.Filter.DeliveryStatuses)
	opts.Filter.SubscriberIDs = normalizedUniqueStrings(opts.Filter.SubscriberIDs)
	opts.Filter.SubscriberTypes = normalizedUniqueStrings(opts.Filter.SubscriberTypes)
	return opts
}

func normalizedUniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (s *RunPostgres) requireRunDebugAccess() error {
	return s.requireCurrentSchema()
}

func (s *RunPostgres) ResolveLatestRunDebugRunID(ctx context.Context) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunDebugAccess(); err != nil {
		return "", err
	}
	var runID string
	if err := s.backend.QueryRowContext(ctx, `
		SELECT r.run_id::text
		FROM runs r
		WHERE EXISTS (
			SELECT 1
			FROM events e
			WHERE e.run_id = r.run_id
		)
		ORDER BY r.started_at DESC, r.run_id DESC
		LIMIT 1
	`).Scan(&runID); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("no current run found")
		}
		return "", fmt.Errorf("resolve latest run: %w", err)
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "", fmt.Errorf("no current run found")
	}
	return runID, nil
}

func (s *RunPostgres) ListRunDebugRuns(ctx context.Context, limit int) ([]operatorread.RunDebugRunSummary, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunDebugAccess(); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT
			r.run_id::text,
			COALESCE(r.status, ''),
			COALESCE(root.event_id::text, ''),
			COALESCE(root.event_name, ''),
			COALESCE(root.created_at, r.started_at, now()),
			summary.last_event_at,
			r.ended_at,
			COALESCE(summary.event_count, 0),
			COALESCE(entity_summary.entity_count, 0)
		FROM runs r
		LEFT JOIN LATERAL (
			SELECT e.event_id, e.event_name, e.created_at
			FROM events e
			WHERE e.run_id = r.run_id
			ORDER BY e.created_at ASC, e.event_id ASC
			LIMIT 1
		) root ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::int AS event_count, MAX(created_at) AS last_event_at
			FROM events
			WHERE run_id = r.run_id
		) summary ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(DISTINCT es.entity_id)::int AS entity_count
			FROM entity_state es
			WHERE es.run_id = r.run_id
		) entity_summary ON TRUE
		WHERE root.event_id IS NOT NULL
		ORDER BY COALESCE(summary.last_event_at, root.created_at, r.started_at) DESC, r.run_id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list run debug runs: %w", err)
	}
	defer rows.Close()
	out := make([]operatorread.RunDebugRunSummary, 0, limit)
	for rows.Next() {
		var (
			item      operatorread.RunDebugRunSummary
			lastEvent sql.NullTime
			endedAt   sql.NullTime
		)
		if err := rows.Scan(
			&item.RunID,
			&item.RunTableStatus,
			&item.RootEventID,
			&item.RootEventType,
			&item.StartedAt,
			&lastEvent,
			&endedAt,
			&item.EventCount,
			&item.EntityCount,
		); err != nil {
			return nil, fmt.Errorf("scan run debug summary: %w", err)
		}
		if lastEvent.Valid {
			item.LastEventAt = lastEvent.Time
		}
		if endedAt.Valid {
			tm := endedAt.Time
			item.EndedAt = &tm
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read run debug summaries: %w", err)
	}
	return out, nil
}

func (s *RunPostgres) LoadRunDebugReport(ctx context.Context, runID string, opts operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error) {
	if s == nil || s.backend == nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunDebugAccess(); err != nil {
		return operatorread.RunDebugReport{}, err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return operatorread.RunDebugReport{}, fmt.Errorf("run_id is required")
	}
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
	var lastEventAt sql.NullTime
	if err := s.backend.QueryRowContext(ctx, `
		SELECT COUNT(*), MAX(created_at)
		FROM events
		WHERE run_id = $1::uuid
	`, report.RunID).Scan(&report.EventCount, &lastEventAt); err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("load event summary: %w", err)
	}
	if lastEventAt.Valid {
		report.LastEventAt = lastEventAt.Time.UTC()
	}

	eventCountRows, err := s.backend.QueryContext(ctx, `
		SELECT event_name, COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		GROUP BY event_name
		ORDER BY event_name
	`, report.RunID)
	if err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("load event counts: %w", err)
	}
	defer eventCountRows.Close()
	for eventCountRows.Next() {
		var item operatorread.RunDebugEventCount
		if err := eventCountRows.Scan(&item.EventName, &item.Count); err != nil {
			return operatorread.RunDebugReport{}, fmt.Errorf("scan event counts: %w", err)
		}
		report.EventCounts = append(report.EventCounts, item)
	}
	if err := eventCountRows.Err(); err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("read event counts: %w", err)
	}

	deliveryCounts, err := s.deliveryRunDiagnosticCounts(ctx, report.RunID)
	if err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("load deliveries: %w", err)
	}
	report.Deliveries = runDebugDeliveryCounts(deliveryCounts)
	failedDeliveries, err := s.loadRunDebugFailureDeliveries(ctx, report.RunID, opts.DeadLetterLimit)
	if err != nil {
		return operatorread.RunDebugReport{}, err
	}
	report.FailedDeliveries = failedDeliveries
	testQuiescence, err := s.LoadRunTestQuiescence(ctx, report.RunID, time.Now().UTC())
	if err != nil {
		return operatorread.RunDebugReport{}, err
	}
	report.TestQuiescence = testQuiescence

	eventRows, err := s.backend.QueryContext(ctx, `
		SELECT
			event_id::text,
			event_name,
			COALESCE(entity_id::text, ''),
			created_at,
			COALESCE(produced_by, ''),
			COALESCE(produced_by_type, ''),
			COALESCE(payload, '{}'::jsonb)
		FROM events
		WHERE run_id = $1::uuid
		ORDER BY created_at DESC, event_id DESC
		LIMIT $2
	`, report.RunID, opts.EventLimit)
	if err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("load run events: %w", err)
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var item operatorread.RunDebugEvent
		var payload []byte
		if err := eventRows.Scan(&item.EventID, &item.EventName, &item.EntityID, &item.CreatedAt, &item.Source, &item.SourceType, &payload); err != nil {
			return operatorread.RunDebugReport{}, fmt.Errorf("scan run events: %w", err)
		}
		item.Payload = append(json.RawMessage(nil), payload...)
		report.Events = append(report.Events, item)
	}
	if err := eventRows.Err(); err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("read run events: %w", err)
	}

	deadRows, err := s.backend.QueryContext(ctx, `
		SELECT
			COALESCE(dl.original_event, ''),
			COALESCE(dl.entity_id::text, ''),
			dl.failure,
			COALESCE(dl.handler_node, ''),
			dl.created_at
		FROM dead_letters dl
		INNER JOIN events e ON e.event_id = dl.original_event_id
		WHERE e.run_id = $1::uuid
		ORDER BY dl.created_at DESC
		LIMIT $2
	`, report.RunID, opts.DeadLetterLimit)
	if err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("load dead letters: %w", err)
	}
	defer deadRows.Close()
	for deadRows.Next() {
		var item operatorread.RunDebugDeadLetter
		var rawFailure []byte
		if err := deadRows.Scan(&item.OriginalEvent, &item.EntityID, &rawFailure, &item.HandlerNode, &item.CreatedAt); err != nil {
			return operatorread.RunDebugReport{}, fmt.Errorf("scan dead letters: %w", err)
		}
		failure, err := decodeStoredFailure(rawFailure)
		if err != nil || failure == nil {
			return operatorread.RunDebugReport{}, fmt.Errorf("decode run dead letter failure")
		}
		item.Failure = *failure
		report.DeadLetters = append(report.DeadLetters, item)
	}
	if err := deadRows.Err(); err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("read dead letters: %w", err)
	}

	turnRows, err := s.backend.QueryContext(ctx, `
		SELECT agent_id, COUNT(*), COUNT(*) FILTER (WHERE failure IS NOT NULL), MAX(created_at)
		FROM agent_turns
		WHERE run_id = $1::uuid
		GROUP BY agent_id
		ORDER BY agent_id
	`, report.RunID)
	if err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("load agent turns: %w", err)
	}
	defer turnRows.Close()
	for turnRows.Next() {
		var item operatorread.RunDebugAgentTurn
		if err := turnRows.Scan(&item.AgentID, &item.Turns, &item.ErrorCount, &item.LastAt); err != nil {
			return operatorread.RunDebugReport{}, fmt.Errorf("scan agent turns: %w", err)
		}
		report.AgentTurns = append(report.AgentTurns, item)
	}
	if err := turnRows.Err(); err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("read agent turns: %w", err)
	}

	mutationRows, err := s.backend.QueryContext(ctx, `
		SELECT
			mutation_id::text,
			COALESCE(entity_id::text, ''),
			COALESCE(domain, ''),
			COALESCE(path, ''),
			COALESCE(old_value, 'null'::jsonb),
			COALESCE(new_value, 'null'::jsonb),
			COALESCE(writer_type, ''),
			COALESCE(writer_id, ''),
			COALESCE(handler_step, ''),
			COALESCE(caused_by_event::text, ''),
			created_at
		FROM entity_mutations
		WHERE run_id = $1::uuid
		ORDER BY created_at DESC, mutation_id DESC
		LIMIT $2
	`, report.RunID, opts.MutationLimit)
	if err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("load run mutations: %w", err)
	}
	defer mutationRows.Close()
	for mutationRows.Next() {
		var (
			item     operatorread.RunDebugMutation
			oldValue []byte
			newValue []byte
		)
		if err := mutationRows.Scan(
			&item.MutationID,
			&item.EntityID,
			&item.Domain,
			&item.Path,
			&oldValue,
			&newValue,
			&item.WriterType,
			&item.WriterID,
			&item.HandlerStep,
			&item.CausedByEvent,
			&item.CreatedAt,
		); err != nil {
			return operatorread.RunDebugReport{}, fmt.Errorf("scan run mutations: %w", err)
		}
		item.OldValue = append(json.RawMessage(nil), oldValue...)
		item.NewValue = append(json.RawMessage(nil), newValue...)
		report.Mutations = append(report.Mutations, item)
	}
	if err := mutationRows.Err(); err != nil {
		return operatorread.RunDebugReport{}, fmt.Errorf("read run mutations: %w", err)
	}

	if err := s.loadRunDebugRuntimeLogs(ctx, report.RunID, opts, &report); err != nil {
		return operatorread.RunDebugReport{}, err
	}

	return report, nil
}

func (s *RunPostgres) LoadRunTestQuiescence(ctx context.Context, runID string, observedAt time.Time) (operatorread.RunTestQuiescence, error) {
	var out operatorread.RunTestQuiescence
	summary, err := s.summarizeDeliveryRun(ctx, runID)
	if err != nil {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("load run test quiescence active deliveries: %w", err)
	}
	out.ActiveDeliveries = summary.Pending + summary.InProgress + summary.RetryScheduled
	if s.pipeline == nil || s.timers == nil {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("run debug runtime diagnostics source is required")
	}
	pipelineSummary, err := s.pipeline.PipelineObligations().SummarizeRun(ctx, runID)
	if err != nil {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("load run test quiescence unsettled pipeline events: %w", err)
	}
	out.UnsettledPipelineEvents = pipelineSummary.Replayable + pipelineSummary.Deferred
	scope, err := runtimetimerobligation.Run(runID)
	if err != nil {
		return operatorread.RunTestQuiescence{}, err
	}
	timerSnapshot, err := s.timers.ReadTimerObligations(ctx, scope, observedAt)
	if err != nil {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("load run test quiescence timer obligations: %w", err)
	}
	runTimers, ok := timerSnapshot.Run(runID)
	if !ok {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("load run test quiescence timer obligations: snapshot omitted requested run")
	}
	out.DueTimers = runTimers.Totals().DueCount
	if err := s.backend.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_sessions
		WHERE run_id = $1::uuid
		  AND status = 'active'
		  AND lease_holder IS NOT NULL
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at > now()
	`, runID).Scan(&out.ActiveSessionLeases); err != nil {
		return operatorread.RunTestQuiescence{}, fmt.Errorf("load run test quiescence active session leases: %w", err)
	}
	out.Ready = runTestQuiescenceReady(out)
	return out, nil
}

func runTestQuiescenceReady(value operatorread.RunTestQuiescence) bool {
	return value.ActiveDeliveries == 0 &&
		value.UnsettledPipelineEvents == 0 &&
		value.DueTimers == 0 &&
		value.ActiveSessionLeases == 0
}

func (s *RunPostgres) loadRunDebugFailureDeliveries(ctx context.Context, runID string, limit int) ([]operatorread.RunDebugFailureDelivery, error) {
	if limit <= 0 {
		limit = 10
	}
	snapshots, err := s.deliveryRunDiagnosticFailures(ctx, runID, limit)
	if err != nil {
		return nil, fmt.Errorf("load run failed deliveries: %w", err)
	}
	return runDebugFailuresFromSnapshots(snapshots,
		func(eventID string) (deliveryLifecycleEventMetadata, error) {
			record, found, err := loadPostgresEventIdentity(ctx, s.backend, eventID)
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

func normalizeRunDebugFailureDelivery(item *operatorread.RunDebugFailureDelivery) {
	if item == nil {
		return
	}
	item.Status = strings.TrimSpace(item.Status)
	item.ReasonCode = strings.TrimSpace(item.ReasonCode)
}

func runDebugDeliveryCounts(counts []runtimedelivery.RunDiagnosticCount) []operatorread.RunDebugDeliveryCount {
	out := make([]operatorread.RunDebugDeliveryCount, 0, len(counts))
	for _, count := range counts {
		out = append(out, operatorread.RunDebugDeliveryCount{
			SubscriberID: count.SubscriberID,
			Status:       string(count.Status),
			Count:        count.Count,
		})
	}
	return out
}

func runDebugFailuresFromSnapshots(
	snapshots []runtimedelivery.Snapshot,
	loadEvent func(string) (deliveryLifecycleEventMetadata, error),
	loadDeliveryDeadLetters func(string, int64) ([]operatorread.OperatorDeadLetterRecord, error),
) ([]operatorread.RunDebugFailureDelivery, error) {
	for _, snapshot := range snapshots {
		if snapshot.Status != runtimedelivery.StatusFailed && snapshot.Status != runtimedelivery.StatusDeadLetter {
			return nil, fmt.Errorf("canonical run diagnostic failure page returned delivery status %q", snapshot.Status)
		}
	}
	out := make([]operatorread.RunDebugFailureDelivery, 0, len(snapshots))
	for _, snapshot := range snapshots {
		metadata, err := loadEvent(snapshot.EventID)
		if err != nil {
			return nil, err
		}
		item := operatorread.RunDebugFailureDelivery{
			EventID: snapshot.EventID, EventName: metadata.EventName, EntityID: metadata.EntityID,
			DeliveryID: snapshot.DeliveryID, SubscriberType: string(snapshot.SubscriberClass),
			SubscriberID: snapshot.SubscriberID, SessionID: snapshot.ActiveSessionID,
			Status: string(snapshot.Status), ReasonCode: snapshot.ReasonCode,
			Failure: runtimefailures.CloneEnvelope(snapshot.Failure), RetryCount: snapshot.RetryCount,
			RetryScheduled: snapshot.RetryScheduled, Terminal: snapshot.Terminal(),
		}
		if !snapshot.CreatedAt.IsZero() {
			at := snapshot.CreatedAt
			item.CreatedAt = &at
		}
		if !snapshot.StartedAt.IsZero() {
			at := snapshot.StartedAt
			item.StartedAt = &at
		}
		if !snapshot.SettledAt.IsZero() {
			at := snapshot.SettledAt
			item.FinishedAt = &at
		}
		if snapshot.Status == runtimedelivery.StatusDeadLetter {
			item.DeadLetters, err = loadDeliveryDeadLetters(snapshot.DeliveryID, snapshot.ClaimVersion)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, item)
	}
	return out, nil
}

func (s *RunPostgres) LoadRunDebugTrace(ctx context.Context, runID string, opts operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, error) {
	rows, _, err := s.LoadRunDebugTracePage(ctx, runID, opts)
	return rows, err
}

func (s *RunPostgres) LoadRunDebugTracePage(ctx context.Context, runID string, opts operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, string, error) {
	if s == nil || s.backend == nil {
		return nil, "", fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunDebugAccess(); err != nil {
		return nil, "", err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, "", operatorread.ErrRunNotFound
	}
	if _, err := uuid.Parse(runID); err != nil {
		return nil, "", operatorread.ErrRunNotFound
	}
	opts = defaultRunDebugTraceQueryOptions(opts)
	var exists bool
	if err := s.backend.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1::uuid)`, runID).Scan(&exists); err != nil {
		return nil, "", fmt.Errorf("check run debug trace run: %w", err)
	}
	if !exists {
		return nil, "", operatorread.ErrRunNotFound
	}

	return s.loadProjectedRunDebugTrace(ctx, runID, opts)
}

type runDebugTraceCursor struct {
	EventCreatedAt    string `json:"event_created_at"`
	EventID           string `json:"event_id"`
	DeliveryCreatedAt string `json:"delivery_created_at,omitempty"`
	DeliveryID        string `json:"delivery_id,omitempty"`
	TurnCreatedAt     string `json:"turn_created_at,omitempty"`
	TurnID            string `json:"turn_id,omitempty"`
}

func encodeRunDebugTraceCursor(row operatorread.RunDebugTraceRow) string {
	cursor := runDebugTraceCursor{
		EventCreatedAt: row.EventCreatedAt.UTC().Format(time.RFC3339Nano),
		EventID:        strings.TrimSpace(row.EventID),
		DeliveryID:     strings.TrimSpace(row.DeliveryID),
		TurnID:         strings.TrimSpace(row.TurnID),
	}
	if row.DeliveryCreatedAt != nil {
		cursor.DeliveryCreatedAt = row.DeliveryCreatedAt.UTC().Format(time.RFC3339Nano)
	}
	if row.TurnCreatedAt != nil {
		cursor.TurnCreatedAt = row.TurnCreatedAt.UTC().Format(time.RFC3339Nano)
	}
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeRunDebugTraceCursor(cursor string) (runDebugTraceCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return runDebugTraceCursor{}, operatorread.ErrInvalidObservabilityCursor
	}
	var decoded runDebugTraceCursor
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return runDebugTraceCursor{}, operatorread.ErrInvalidObservabilityCursor
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(decoded.EventCreatedAt)); err != nil {
		return runDebugTraceCursor{}, operatorread.ErrInvalidObservabilityCursor
	}
	if strings.TrimSpace(decoded.EventID) == "" {
		return runDebugTraceCursor{}, operatorread.ErrInvalidObservabilityCursor
	}
	for _, timestamp := range []string{decoded.DeliveryCreatedAt, decoded.TurnCreatedAt} {
		if strings.TrimSpace(timestamp) == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(timestamp)); err != nil {
			return runDebugTraceCursor{}, operatorread.ErrInvalidObservabilityCursor
		}
	}
	return decoded, nil
}

func nullableCursorTimestamp(timestamp string) string {
	if strings.TrimSpace(timestamp) == "" {
		return "-infinity"
	}
	return strings.TrimSpace(timestamp)
}

func RunDebugTraceSessionSources() string {
	sources := []string{`
			SELECT
				session_id,
				run_id,
				'live_session' AS session_kind,
				memory_enabled,
				memory_source,
				COALESCE(status, '') AS status,
				updated_at
			FROM agent_sessions
		`, `
			SELECT
				session_id,
				run_id,
				'turn_audit' AS session_kind,
				memory_enabled,
				memory_source,
				COALESCE(status, '') AS status,
				updated_at
			FROM agent_conversation_audits
		`}
	return strings.Join(sources, "\nUNION ALL\n")
}

func nullableTimePtr(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	tm := value.Time
	return &tm
}

func (s *RunPostgres) loadRunDebugRuntimeLogs(ctx context.Context, runID string, opts operatorread.RunDebugQueryOptions, report *operatorread.RunDebugReport) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres store is required")
	}
	if report == nil {
		return fmt.Errorf("report is required")
	}
	logLevels := []string{"warn", "error"}
	if opts.LogsAllLevels {
		logLevels = []string{"info", "warn", "error"}
	}
	if err := s.backend.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->>'log_level' = ANY($2::text[])
		  AND ($3 = '' OR payload->'details'->>'component' = $3)
	`, runID, pq.Array(logLevels), opts.Component).Scan(&report.WarnErrorLogCount); err != nil {
		return fmt.Errorf("load runtime log summary: %w", err)
	}
	logSummaryRows, err := s.backend.QueryContext(ctx, `
		SELECT
			COALESCE(payload->>'log_level', ''),
			COALESCE(payload->'details'->>'component', ''),
			COALESCE(payload->'details'->>'action', ''),
			COUNT(*)
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->>'log_level' = ANY($2::text[])
		  AND ($3 = '' OR payload->'details'->>'component' = $3)
		GROUP BY payload->>'log_level', payload->'details'->>'component', payload->'details'->>'action'
		ORDER BY COUNT(*) DESC, payload->'details'->>'component', payload->'details'->>'action'
		LIMIT 12
	`, runID, pq.Array(logLevels), opts.Component)
	if err != nil {
		return fmt.Errorf("load runtime log rollup: %w", err)
	}
	defer logSummaryRows.Close()
	for logSummaryRows.Next() {
		var item operatorread.RunDebugRuntimeSummary
		if err := logSummaryRows.Scan(&item.Level, &item.Component, &item.Action, &item.Count); err != nil {
			return fmt.Errorf("scan runtime log rollup: %w", err)
		}
		report.RuntimeLogSummary = append(report.RuntimeLogSummary, item)
	}
	if err := logSummaryRows.Err(); err != nil {
		return fmt.Errorf("read runtime log rollup: %w", err)
	}
	logRows, err := s.backend.QueryContext(ctx, `
		SELECT
			event_id::text,
			COALESCE(payload->>'log_level', ''),
			COALESCE(payload->>'message', ''),
			COALESCE(payload->'details'->>'component', ''),
			COALESCE(payload->'details'->>'action', ''),
			COALESCE(payload->'details'->>'event_type', ''),
			COALESCE(payload->'details'->>'agent_id', ''),
			COALESCE(payload->'details'->>'entity_id', ''),
			COALESCE(payload->'details'->'failure', 'null'::jsonb),
			COALESCE(payload->'details', '{}'::jsonb),
			created_at
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->>'log_level' = ANY($2::text[])
		  AND ($3 = '' OR payload->'details'->>'component' = $3)
		ORDER BY created_at DESC
		LIMIT $4
	`, runID, pq.Array(logLevels), opts.Component, opts.RuntimeLogLimit)
	if err != nil {
		return fmt.Errorf("load runtime logs: %w", err)
	}
	defer logRows.Close()
	for logRows.Next() {
		var item operatorread.RunDebugRuntimeLog
		var detail []byte
		var failureRaw []byte
		if err := logRows.Scan(&item.EventID, &item.Level, &item.Message, &item.Component, &item.Action, &item.EventType, &item.AgentID, &item.EntityID, &failureRaw, &detail, &item.CreatedAt); err != nil {
			return fmt.Errorf("scan runtime logs: %w", err)
		}
		failure, err := decodeStoredFailure(failureRaw)
		if err != nil {
			return fmt.Errorf("decode runtime log failure: %w", err)
		}
		item.Failure = failure
		item.Detail = append(json.RawMessage(nil), detail...)
		report.RuntimeLogs = append(report.RuntimeLogs, item)
	}
	if err := logRows.Err(); err != nil {
		return fmt.Errorf("read runtime logs: %w", err)
	}
	return nil
}
