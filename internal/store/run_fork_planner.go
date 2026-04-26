package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/runtime/mutationlog"
)

const (
	RunForkPendingClassificationDeliveredCompleted = "delivered_completed"
	RunForkPendingClassificationPending            = "pending"
	RunForkPendingClassificationInProgress         = "in_progress"
	RunForkPendingClassificationFailedRetryable    = "failed_retryable"
	RunForkPendingClassificationFailedTerminal     = "failed_terminal"
	RunForkPendingClassificationDeadLetter         = "dead_letter"
	RunForkPendingClassificationCommittedReplay    = "committed_replay_scope"
)

type RunForkPlanRequest struct {
	SourceRunID string
	At          string
}

type RunForkPlan struct {
	SourceRunID              string                      `json:"source_run_id"`
	SourceRunStatus          string                      `json:"source_run_status,omitempty"`
	SourceRunStartedAt       *time.Time                  `json:"source_run_started_at,omitempty"`
	SourceRunEndedAt         *time.Time                  `json:"source_run_ended_at,omitempty"`
	ForkPoint                RunForkPoint                `json:"fork_point"`
	EventCountAtFork         int                         `json:"event_count_at_fork"`
	ReconstructedEntityCount int                         `json:"reconstructed_entity_count"`
	PendingWorkCount         int                         `json:"pending_work_count"`
	UnsupportedBlockerCount  int                         `json:"unsupported_blocker_count"`
	ExecutionReady           bool                        `json:"execution_ready"`
	Entities                 []RunForkEntityState        `json:"entities,omitempty"`
	PendingWork              []RunForkPendingWork        `json:"pending_work,omitempty"`
	UnsupportedBlockers      []RunForkUnsupportedBlocker `json:"unsupported_blockers,omitempty"`
}

type RunForkPoint struct {
	Input     string    `json:"input"`
	EventID   string    `json:"event_id"`
	EventName string    `json:"event_name,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type RunForkEntityState struct {
	EntityID     string         `json:"entity_id"`
	CurrentState string         `json:"current_state,omitempty"`
	Fields       map[string]any `json:"fields,omitempty"`
	Gates        map[string]any `json:"gates,omitempty"`
	Accumulator  map[string]any `json:"accumulator,omitempty"`
}

type RunForkPendingWork struct {
	EventID         string     `json:"event_id"`
	EventName       string     `json:"event_name"`
	DeliveryID      string     `json:"delivery_id,omitempty"`
	SubscriberType  string     `json:"subscriber_type,omitempty"`
	SubscriberID    string     `json:"subscriber_id,omitempty"`
	Classification  string     `json:"classification"`
	Status          string     `json:"status,omitempty"`
	RetryCount      int        `json:"retry_count,omitempty"`
	ReasonCode      string     `json:"reason_code,omitempty"`
	ActiveSessionID string     `json:"active_session_id,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	DeliveredAt     *time.Time `json:"delivered_at,omitempty"`
	ReceiptOutcome  string     `json:"receipt_outcome,omitempty"`
	ReceiptAt       *time.Time `json:"receipt_at,omitempty"`
}

type RunForkUnsupportedBlocker struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type runForkEventCursor struct {
	EventID   string
	EventName string
	CreatedAt time.Time
}

func RequireCanonicalRunForkPlannerCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	switch {
	case !caps.Events.HasRuns:
		return fmt.Errorf("run fork planner requires canonical runs table")
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case !caps.Events.LogRunID:
		return fmt.Errorf("run fork planner requires canonical events.run_id")
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case !caps.Events.DeliveryRunID:
		return fmt.Errorf("run fork planner requires canonical event_deliveries.run_id")
	case caps.Events.Receipts != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
	required := map[string][]string{
		"runs":             {"run_id", "status"},
		"events":           {"event_id", "run_id", "event_name", "created_at"},
		"event_deliveries": {"delivery_id", "run_id", "event_id", "subscriber_type", "subscriber_id", "status", "retry_count", "reason_code", "active_session_id", "started_at", "delivered_at", "created_at"},
		"event_receipts":   {"event_id", "subscriber_type", "subscriber_id", "outcome", "reason_code", "processed_at"},
		"dead_letters":     {"original_event_id", "handler_node", "created_at"},
		"entity_mutations": {"mutation_id", "run_id", "entity_id", "field", "new_value", "caused_by_event", "created_at"},
	}
	for tableName, columns := range required {
		if catalog.hasColumns(tableName, columns...) {
			continue
		}
		return fmt.Errorf("run fork planner requires %s columns %v", tableName, columns)
	}
	return nil
}

func (s *PostgresStore) requireRunForkPlannerCapabilities(ctx context.Context) (schemaColumnCatalog, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return schemaColumnCatalog{}, err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return schemaColumnCatalog{}, err
	}
	return catalog, RequireCanonicalRunForkPlannerCapabilities(caps, catalog)
}

func (s *PostgresStore) PlanRunFork(ctx context.Context, req RunForkPlanRequest) (RunForkPlan, error) {
	if s == nil || s.DB == nil {
		return RunForkPlan{}, fmt.Errorf("postgres store is required")
	}
	catalog, err := s.requireRunForkPlannerCapabilities(ctx)
	if err != nil {
		return RunForkPlan{}, err
	}
	runID := strings.TrimSpace(req.SourceRunID)
	if runID == "" {
		return RunForkPlan{}, fmt.Errorf("source run_id is required")
	}
	if _, err := uuid.Parse(runID); err != nil {
		return RunForkPlan{}, fmt.Errorf("source run_id must be a UUID: %w", err)
	}
	at := strings.TrimSpace(req.At)
	if at == "" {
		return RunForkPlan{}, fmt.Errorf("fork point --at value is required")
	}

	plan := RunForkPlan{SourceRunID: runID}
	if err := s.loadRunForkSourceSummary(ctx, &plan); err != nil {
		return RunForkPlan{}, err
	}
	cursor, err := s.resolveRunForkPoint(ctx, runID, at)
	if err != nil {
		return RunForkPlan{}, err
	}
	plan.ForkPoint = RunForkPoint{
		Input:     at,
		EventID:   cursor.EventID,
		EventName: cursor.EventName,
		Timestamp: cursor.CreatedAt,
	}
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND (created_at, event_id) <= ($2::timestamptz, $3::uuid)
	`, runID, cursor.CreatedAt, cursor.EventID).Scan(&plan.EventCountAtFork); err != nil {
		return RunForkPlan{}, fmt.Errorf("count fork events: %w", err)
	}

	entities, err := s.loadRunForkEntityStates(ctx, runID, cursor)
	if err != nil {
		return RunForkPlan{}, err
	}
	plan.Entities = entities
	plan.ReconstructedEntityCount = len(entities)

	pending, err := s.loadRunForkPendingWork(ctx, runID, cursor)
	if err != nil {
		return RunForkPlan{}, err
	}
	plan.PendingWork = pending
	plan.PendingWorkCount = len(pending)
	plan.UnsupportedBlockers = runForkUnsupportedBlockers(catalog, pending)
	plan.UnsupportedBlockerCount = len(plan.UnsupportedBlockers)
	plan.ExecutionReady = plan.UnsupportedBlockerCount == 0
	return plan, nil
}

func (s *PostgresStore) loadRunForkSourceSummary(ctx context.Context, plan *RunForkPlan) error {
	var started, ended sql.NullTime
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), started_at, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, plan.SourceRunID).Scan(&plan.SourceRunStatus, &started, &ended); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("source run %s not found", plan.SourceRunID)
		}
		return fmt.Errorf("load source run: %w", err)
	}
	if started.Valid {
		tm := started.Time
		plan.SourceRunStartedAt = &tm
	}
	if ended.Valid {
		tm := ended.Time
		plan.SourceRunEndedAt = &tm
	}
	return nil
}

func (s *PostgresStore) resolveRunForkPoint(ctx context.Context, runID, at string) (runForkEventCursor, error) {
	if _, err := uuid.Parse(at); err == nil {
		var cursor runForkEventCursor
		if err := s.DB.QueryRowContext(ctx, `
			SELECT event_id::text, event_name, created_at
			FROM events
			WHERE run_id = $1::uuid
			  AND event_id = $2::uuid
		`, runID, at).Scan(&cursor.EventID, &cursor.EventName, &cursor.CreatedAt); err != nil {
			if err == sql.ErrNoRows {
				return runForkEventCursor{}, fmt.Errorf("fork point event %s not found in source run %s", at, runID)
			}
			return runForkEventCursor{}, fmt.Errorf("resolve fork event: %w", err)
		}
		return cursor, nil
	}
	atTime, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return runForkEventCursor{}, fmt.Errorf("fork point --at must be an event UUID or RFC3339 timestamp: %w", err)
	}
	var cursor runForkEventCursor
	if err := s.DB.QueryRowContext(ctx, `
		SELECT event_id::text, event_name, created_at
		FROM events
		WHERE run_id = $1::uuid
		  AND created_at <= $2::timestamptz
		ORDER BY created_at DESC, event_id DESC
		LIMIT 1
	`, runID, atTime).Scan(&cursor.EventID, &cursor.EventName, &cursor.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return runForkEventCursor{}, fmt.Errorf("no source-run event exists at or before fork timestamp %s", at)
		}
		return runForkEventCursor{}, fmt.Errorf("resolve fork timestamp: %w", err)
	}
	return cursor, nil
}

func (s *PostgresStore) loadRunForkEntityStates(ctx context.Context, runID string, cursor runForkEventCursor) ([]RunForkEntityState, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			m.entity_id::text,
			m.field,
			COALESCE(m.new_value, 'null'::jsonb)
		FROM entity_mutations m
		LEFT JOIN events e
			ON e.event_id = m.caused_by_event
		   AND e.run_id = m.run_id
		WHERE m.run_id = $1::uuid
		  AND (
				(e.event_id IS NOT NULL AND (e.created_at, e.event_id) <= ($2::timestamptz, $3::uuid))
				OR
				(e.event_id IS NULL AND m.created_at <= $2::timestamptz)
		  )
		ORDER BY m.entity_id ASC, m.created_at ASC, m.mutation_id ASC
	`, runID, cursor.CreatedAt, cursor.EventID)
	if err != nil {
		return nil, fmt.Errorf("load fork entity mutations: %w", err)
	}
	defer rows.Close()

	grouped := map[string][]mutationlog.ProjectionMutation{}
	entityOrder := []string{}
	seen := map[string]struct{}{}
	for rows.Next() {
		var entityID, field string
		var raw []byte
		if err := rows.Scan(&entityID, &field, &raw); err != nil {
			return nil, fmt.Errorf("scan fork entity mutation: %w", err)
		}
		var value any
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("decode fork entity mutation %s/%s: %w", entityID, field, err)
		}
		entityID = strings.TrimSpace(entityID)
		if _, ok := seen[entityID]; !ok {
			seen[entityID] = struct{}{}
			entityOrder = append(entityOrder, entityID)
		}
		grouped[entityID] = append(grouped[entityID], mutationlog.ProjectionMutation{
			Field:    field,
			NewValue: value,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read fork entity mutations: %w", err)
	}

	out := make([]RunForkEntityState, 0, len(entityOrder))
	for _, entityID := range entityOrder {
		projection, err := mutationlog.ReconstructEntityStateProjection(grouped[entityID])
		if err != nil {
			return nil, fmt.Errorf("reconstruct entity %s at fork point: %w", entityID, err)
		}
		out = append(out, RunForkEntityState{
			EntityID:     entityID,
			CurrentState: projection.CurrentState,
			Fields:       projection.Fields,
			Gates:        projection.Gates,
			Accumulator:  projection.Accumulator,
		})
	}
	return out, nil
}

func (s *PostgresStore) loadRunForkPendingWork(ctx context.Context, runID string, cursor runForkEventCursor) ([]RunForkPendingWork, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			e.event_id::text,
			e.event_name,
			d.delivery_id::text,
			COALESCE(d.subscriber_type, ''),
			COALESCE(d.subscriber_id, ''),
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.reason_code, ''),
			COALESCE(d.active_session_id::text, ''),
			d.created_at,
			d.started_at,
			d.delivered_at,
			COALESCE(r.outcome, ''),
			r.processed_at,
			EXISTS (
				SELECT 1
				FROM dead_letters dl
				WHERE dl.original_event_id = e.event_id
				  AND dl.created_at <= $2::timestamptz
				  AND COALESCE(dl.handler_node, '') <> ''
				  AND COALESCE(d.subscriber_type, '') = 'node'
				  AND dl.handler_node = d.subscriber_id
			)
		FROM events e
		INNER JOIN event_deliveries d ON d.event_id = e.event_id
		LEFT JOIN event_receipts r
			ON r.event_id = d.event_id
		   AND r.subscriber_type = d.subscriber_type
		   AND r.subscriber_id = d.subscriber_id
		   AND r.processed_at <= $2::timestamptz
		WHERE e.run_id = $1::uuid
		  AND (e.created_at, e.event_id) <= ($2::timestamptz, $3::uuid)
		  AND d.created_at <= $2::timestamptz
		ORDER BY e.created_at ASC, e.event_id ASC, d.created_at ASC, d.delivery_id ASC
	`, runID, cursor.CreatedAt, cursor.EventID)
	if err != nil {
		return nil, fmt.Errorf("load fork pending work: %w", err)
	}
	defer rows.Close()

	out := []RunForkPendingWork{}
	for rows.Next() {
		var item RunForkPendingWork
		var started, delivered, receipt sql.NullTime
		var deadLetter bool
		if err := rows.Scan(
			&item.EventID,
			&item.EventName,
			&item.DeliveryID,
			&item.SubscriberType,
			&item.SubscriberID,
			&item.Status,
			&item.RetryCount,
			&item.ReasonCode,
			&item.ActiveSessionID,
			&item.CreatedAt,
			&started,
			&delivered,
			&item.ReceiptOutcome,
			&receipt,
			&deadLetter,
		); err != nil {
			return nil, fmt.Errorf("scan fork pending work: %w", err)
		}
		item.StartedAt = nullableTimePtr(started)
		item.DeliveredAt = nullableTimePtr(delivered)
		item.ReceiptAt = nullableTimePtr(receipt)
		item.Classification = classifyRunForkPendingWork(item, deadLetter)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read fork pending work: %w", err)
	}
	return out, nil
}

func classifyRunForkPendingWork(item RunForkPendingWork, deadLetter bool) string {
	if item.SubscriberType == replayScopeMarkerSubscriberType && item.SubscriberID == replayScopeMarkerSubscriberID {
		if _, ok := committedReplayScopeFromReasonCode(item.ReasonCode); ok {
			return RunForkPendingClassificationCommittedReplay
		}
	}
	if deadLetter || strings.TrimSpace(item.Status) == "dead_letter" || strings.TrimSpace(item.ReceiptOutcome) == "dead_letter" {
		return RunForkPendingClassificationDeadLetter
	}
	switch strings.TrimSpace(item.Status) {
	case "pending":
		return RunForkPendingClassificationPending
	case "in_progress":
		return RunForkPendingClassificationInProgress
	case "failed":
		if item.RetryCount < 2 {
			return RunForkPendingClassificationFailedRetryable
		}
		return RunForkPendingClassificationFailedTerminal
	case "delivered":
		return RunForkPendingClassificationDeliveredCompleted
	default:
		if item.ReceiptAt != nil {
			return RunForkPendingClassificationDeliveredCompleted
		}
		return RunForkPendingClassificationPending
	}
}

func runForkUnsupportedBlockers(catalog schemaColumnCatalog, pending []RunForkPendingWork) []RunForkUnsupportedBlocker {
	blockers := []RunForkUnsupportedBlocker{}
	add := func(code, message string) {
		for _, existing := range blockers {
			if existing.Code == code {
				return
			}
		}
		blockers = append(blockers, RunForkUnsupportedBlocker{Code: code, Message: message})
	}
	if len(pending) > 0 {
		add("delivery_history_unproven", "event_deliveries stores current delivery state; arbitrary historical delivery transitions at the fork point are not append-only proven")
	}
	if catalog.hasTable("timers") {
		add("timer_history_unproven", "timers are current-state rows and timer creation/cancellation is not represented in the mutation log")
	}
	if catalog.hasTable("routing_rules") {
		add("flow_route_history_unproven", "routing_rules are current-state rows and cannot prove historical flow-route membership at the fork point")
	}
	for _, item := range pending {
		if strings.TrimSpace(item.ActiveSessionID) == "" {
			continue
		}
		add("session_history_unproven", "delivery active_session_id references current session rows without append-only session-state proof at the fork point")
		add("active_turn_history_unproven", "active turn ownership at the fork point cannot be proven from current delivery/session rows alone")
		break
	}
	return blockers
}
