package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	DefaultAgentDeliveryDiagnosticsLimit = 50
	MaxAgentDeliveryDiagnosticsLimit     = 200
)

var ErrInvalidAgentDeliveryDiagnosticsCursor = errors.New("invalid agent delivery diagnostics cursor")

type AgentDeliveryDiagnosticsCursorError struct {
	Field string
}

func (e AgentDeliveryDiagnosticsCursorError) Error() string {
	field := strings.TrimSpace(e.Field)
	if field == "" {
		field = "cursor"
	}
	return fmt.Sprintf("invalid agent delivery diagnostics %s", field)
}

func (e AgentDeliveryDiagnosticsCursorError) Unwrap() error {
	return ErrInvalidAgentDeliveryDiagnosticsCursor
}

type OperatorAgentDeliveryDiagnosticsOptions struct {
	FailureLimit     int
	FailureCursor    string
	DeadLetterLimit  int
	DeadLetterCursor string
}

type OperatorAgentDeliveryDiagnostics struct {
	AgentID               string                                  `json:"agent_id"`
	Summary               OperatorAgentDeliveryDiagnosticsSummary `json:"summary"`
	Failures              []OperatorAgentDeliveryFailure          `json:"failures"`
	FailuresNextCursor    string                                  `json:"failures_next_cursor,omitempty"`
	DeadLetters           []OperatorAgentDeadLetterDelivery       `json:"dead_letters"`
	DeadLettersNextCursor string                                  `json:"dead_letters_next_cursor,omitempty"`
}

type OperatorAgentDeliveryDiagnosticsSummary struct {
	Failures24h    int `json:"failures_24h"`
	DeadLetters24h int `json:"dead_letters_24h"`
}

type OperatorAgentDeliveryFailure struct {
	DeliveryID string    `json:"delivery_id"`
	EventID    string    `json:"event_id"`
	EventName  string    `json:"event_name"`
	RunID      string    `json:"run_id,omitempty"`
	EntityID   string    `json:"entity_id,omitempty"`
	Status     string    `json:"status"`
	ReasonCode string    `json:"reason_code,omitempty"`
	LastError  string    `json:"last_error,omitempty"`
	RetryCount int       `json:"retry_count"`
	OccurredAt time.Time `json:"occurred_at"`
}

type OperatorAgentDeadLetterDelivery struct {
	DeliveryID        string                     `json:"delivery_id"`
	EventID           string                     `json:"event_id"`
	EventName         string                     `json:"event_name"`
	RunID             string                     `json:"run_id,omitempty"`
	EntityID          string                     `json:"entity_id,omitempty"`
	Status            string                     `json:"status"`
	ReasonCode        string                     `json:"reason_code,omitempty"`
	LastError         string                     `json:"last_error,omitempty"`
	RetryCount        int                        `json:"retry_count"`
	OccurredAt        time.Time                  `json:"occurred_at"`
	DeadLetterRecords []OperatorDeadLetterRecord `json:"dead_letter_records"`
}

type agentDeliveryDiagnosticsCursor struct {
	Kind       string `json:"kind"`
	OccurredAt string `json:"occurred_at"`
	DeliveryID string `json:"delivery_id"`
}

func (s *PostgresStore) LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, agentID string, opts OperatorAgentDeliveryDiagnosticsOptions) (OperatorAgentDeliveryDiagnostics, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).LoadOperatorAgentDeliveryDiagnostics(ctx, agentID, opts)
}

func (r *OperatorAgentConversationReadSurface) LoadOperatorAgentDeliveryDiagnostics(ctx context.Context, agentID string, opts OperatorAgentDeliveryDiagnosticsOptions) (OperatorAgentDeliveryDiagnostics, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentDeliveryDiagnostics{}, ErrAgentNotFound
	}
	if _, err := r.LoadOperatorAgent(ctx, agentID); err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	if err := r.requireAgentDeliveryDiagnosticsCapabilities(ctx); err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}

	opts = defaultOperatorAgentDeliveryDiagnosticsOptions(opts)
	summary, err := r.loadAgentDeliveryDiagnosticsSummary(ctx, agentID)
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	failures, failuresNext, err := r.listAgentDeliveryFailures(ctx, agentID, opts.FailureLimit, opts.FailureCursor)
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	if err := r.assertAgentDeadLetterDeliveriesHaveRecords(ctx, agentID); err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	deadLetters, deadLettersNext, err := r.listAgentDeadLetterDeliveries(ctx, agentID, opts.DeadLetterLimit, opts.DeadLetterCursor)
	if err != nil {
		return OperatorAgentDeliveryDiagnostics{}, err
	}
	result := OperatorAgentDeliveryDiagnostics{
		AgentID:               agentID,
		Summary:               summary,
		Failures:              failures,
		FailuresNextCursor:    failuresNext,
		DeadLetters:           deadLetters,
		DeadLettersNextCursor: deadLettersNext,
	}
	if result.Failures == nil {
		result.Failures = []OperatorAgentDeliveryFailure{}
	}
	if result.DeadLetters == nil {
		result.DeadLetters = []OperatorAgentDeadLetterDelivery{}
	}
	return result, nil
}

func defaultOperatorAgentDeliveryDiagnosticsOptions(opts OperatorAgentDeliveryDiagnosticsOptions) OperatorAgentDeliveryDiagnosticsOptions {
	if opts.FailureLimit <= 0 {
		opts.FailureLimit = DefaultAgentDeliveryDiagnosticsLimit
	}
	if opts.FailureLimit > MaxAgentDeliveryDiagnosticsLimit {
		opts.FailureLimit = MaxAgentDeliveryDiagnosticsLimit
	}
	if opts.DeadLetterLimit <= 0 {
		opts.DeadLetterLimit = DefaultAgentDeliveryDiagnosticsLimit
	}
	if opts.DeadLetterLimit > MaxAgentDeliveryDiagnosticsLimit {
		opts.DeadLetterLimit = MaxAgentDeliveryDiagnosticsLimit
	}
	opts.FailureCursor = strings.TrimSpace(opts.FailureCursor)
	opts.DeadLetterCursor = strings.TrimSpace(opts.DeadLetterCursor)
	return opts
}

func (r *OperatorAgentConversationReadSurface) requireAgentDeliveryDiagnosticsCapabilities(ctx context.Context) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("operator agent delivery diagnostics read owner requires postgres store")
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return err
	}
	switch {
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case !caps.Events.LogRunID:
		return fmt.Errorf("agent delivery diagnostics read owner requires canonical events.run_id")
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	catalog, err := loadSchemaColumnCatalog(ctx, r.db)
	if err != nil {
		return err
	}
	required := map[string][]string{
		"events": {
			"event_id", "run_id", "event_name", "entity_id", "created_at",
		},
		"event_deliveries": {
			"delivery_id", "event_id", "subscriber_type", "subscriber_id",
			"status", "retry_count", "reason_code", "last_error", "delivered_at", "created_at",
		},
		"dead_letters": {
			"dead_letter_id", "original_event_id", "failure_type", "error_message",
			"retry_count", "chain_depth", "handler_node", "created_at",
		},
	}
	for table, columns := range required {
		if !catalog.hasColumns(table, columns...) {
			return fmt.Errorf("agent delivery diagnostics read owner requires canonical %s columns: %s", table, strings.Join(columns, ", "))
		}
	}
	return nil
}

func (r *OperatorAgentConversationReadSurface) loadAgentDeliveryDiagnosticsSummary(ctx context.Context, agentID string) (OperatorAgentDeliveryDiagnosticsSummary, error) {
	var summary OperatorAgentDeliveryDiagnosticsSummary
	if err := r.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'failed')::int,
			COUNT(*) FILTER (WHERE status = 'dead_letter')::int
		FROM event_deliveries
		WHERE subscriber_type = 'agent'
		  AND subscriber_id = $1
		  AND COALESCE(delivered_at, created_at) >= now() - interval '24 hours'
	`, agentID).Scan(&summary.Failures24h, &summary.DeadLetters24h); err != nil {
		return OperatorAgentDeliveryDiagnosticsSummary{}, fmt.Errorf("load agent delivery diagnostics summary: %w", err)
	}
	return summary, nil
}

func (r *OperatorAgentConversationReadSurface) listAgentDeliveryFailures(ctx context.Context, agentID string, limit int, cursorRaw string) ([]OperatorAgentDeliveryFailure, string, error) {
	cursorClause, args, err := agentDeliveryDiagnosticsCursorClause(agentID, cursorRaw, "agent.delivery_diagnostics.failures", "failure_cursor")
	if err != nil {
		return nil, "", err
	}
	args = append(args, limit+1)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			d.delivery_id::text,
			d.event_id::text,
			COALESCE(e.event_name, ''),
			COALESCE(e.run_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.last_error, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.delivered_at, d.created_at) AS occurred_at
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id = $1
		  AND d.status = 'failed'
		  %s
		ORDER BY occurred_at DESC, d.delivery_id::text DESC
		LIMIT $%d
	`, cursorClause, len(args)), args...)
	if err != nil {
		return nil, "", fmt.Errorf("list agent delivery failures: %w", err)
	}
	defer rows.Close()

	out := []OperatorAgentDeliveryFailure{}
	for rows.Next() {
		var item OperatorAgentDeliveryFailure
		if err := rows.Scan(
			&item.DeliveryID,
			&item.EventID,
			&item.EventName,
			&item.RunID,
			&item.EntityID,
			&item.ReasonCode,
			&item.LastError,
			&item.RetryCount,
			&item.OccurredAt,
		); err != nil {
			return nil, "", fmt.Errorf("scan agent delivery failure: %w", err)
		}
		item.Status = "failed"
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read agent delivery failures: %w", err)
	}
	nextCursor := ""
	if len(out) > limit {
		nextCursor = encodeAgentDeliveryDiagnosticsCursor("agent.delivery_diagnostics.failures", out[limit-1].OccurredAt, out[limit-1].DeliveryID)
		out = out[:limit]
	}
	return out, nextCursor, nil
}

func (r *OperatorAgentConversationReadSurface) assertAgentDeadLetterDeliveriesHaveRecords(ctx context.Context, agentID string) error {
	var deliveryID string
	err := r.db.QueryRowContext(ctx, `
		SELECT d.delivery_id::text
		FROM event_deliveries d
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id = $1
		  AND d.status = 'dead_letter'
		  AND NOT EXISTS (
		  	SELECT 1
		  	FROM dead_letters dl
		  	WHERE dl.original_event_id = d.event_id
		  )
		ORDER BY COALESCE(d.delivered_at, d.created_at) DESC, d.delivery_id::text DESC
		LIMIT 1
	`, agentID).Scan(&deliveryID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("check agent dead-letter delivery reconciliation: %w", err)
	}
	return fmt.Errorf("agent delivery diagnostics owner found dead_letter delivery %s without a dead_letters record", deliveryID)
}

func (r *OperatorAgentConversationReadSurface) listAgentDeadLetterDeliveries(ctx context.Context, agentID string, limit int, cursorRaw string) ([]OperatorAgentDeadLetterDelivery, string, error) {
	cursorClause, args, err := agentDeliveryDiagnosticsCursorClause(agentID, cursorRaw, "agent.delivery_diagnostics.dead_letters", "dead_letter_cursor")
	if err != nil {
		return nil, "", err
	}
	args = append(args, limit+1)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			d.delivery_id::text,
			d.event_id::text,
			COALESCE(e.event_name, ''),
			COALESCE(e.run_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.last_error, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.delivered_at, d.created_at) AS occurred_at,
			COALESCE(jsonb_agg(jsonb_build_object(
				'dead_letter_id', dl.dead_letter_id::text,
				'failure_type', COALESCE(dl.failure_type, ''),
				'error_message', COALESCE(dl.error_message, ''),
				'retry_count', COALESCE(dl.retry_count, 0),
				'chain_depth', COALESCE(dl.chain_depth, 0),
				'handler_node', COALESCE(dl.handler_node, ''),
				'created_at', dl.created_at
			) ORDER BY dl.created_at ASC, dl.dead_letter_id::text ASC) FILTER (WHERE dl.dead_letter_id IS NOT NULL), '[]'::jsonb)
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		LEFT JOIN dead_letters dl ON dl.original_event_id = d.event_id
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id = $1
		  AND d.status = 'dead_letter'
		  %s
		GROUP BY d.delivery_id, d.event_id, e.event_name, e.run_id, e.entity_id, d.reason_code, d.last_error, d.retry_count, d.delivered_at, d.created_at
		ORDER BY occurred_at DESC, d.delivery_id::text DESC
		LIMIT $%d
	`, cursorClause, len(args)), args...)
	if err != nil {
		return nil, "", fmt.Errorf("list agent dead-letter deliveries: %w", err)
	}
	defer rows.Close()

	out := []OperatorAgentDeadLetterDelivery{}
	for rows.Next() {
		var (
			item       OperatorAgentDeadLetterDelivery
			recordsRaw []byte
		)
		if err := rows.Scan(
			&item.DeliveryID,
			&item.EventID,
			&item.EventName,
			&item.RunID,
			&item.EntityID,
			&item.ReasonCode,
			&item.LastError,
			&item.RetryCount,
			&item.OccurredAt,
			&recordsRaw,
		); err != nil {
			return nil, "", fmt.Errorf("scan agent dead-letter delivery: %w", err)
		}
		item.Status = "dead_letter"
		if err := json.Unmarshal(recordsRaw, &item.DeadLetterRecords); err != nil {
			return nil, "", fmt.Errorf("decode agent dead-letter records: %w", err)
		}
		if len(item.DeadLetterRecords) == 0 {
			return nil, "", fmt.Errorf("agent delivery diagnostics owner returned dead_letter delivery %s without a dead_letters record", item.DeliveryID)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read agent dead-letter deliveries: %w", err)
	}
	nextCursor := ""
	if len(out) > limit {
		nextCursor = encodeAgentDeliveryDiagnosticsCursor("agent.delivery_diagnostics.dead_letters", out[limit-1].OccurredAt, out[limit-1].DeliveryID)
		out = out[:limit]
	}
	return out, nextCursor, nil
}

func agentDeliveryDiagnosticsCursorClause(agentID, rawCursor, kind, field string) (string, []any, error) {
	args := []any{agentID}
	rawCursor = strings.TrimSpace(rawCursor)
	if rawCursor == "" {
		return "", args, nil
	}
	cursor, err := decodeAgentDeliveryDiagnosticsCursor(rawCursor, kind, field)
	if err != nil {
		return "", nil, err
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, cursor.OccurredAt)
	if err != nil || strings.TrimSpace(cursor.DeliveryID) == "" {
		return "", nil, AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	args = append(args, occurredAt.UTC(), strings.TrimSpace(cursor.DeliveryID))
	return fmt.Sprintf("AND (COALESCE(d.delivered_at, d.created_at) < $%d OR (COALESCE(d.delivered_at, d.created_at) = $%d AND d.delivery_id::text < $%d))", len(args)-1, len(args)-1, len(args)), args, nil
}

func encodeAgentDeliveryDiagnosticsCursor(kind string, occurredAt time.Time, deliveryID string) string {
	raw, _ := json.Marshal(agentDeliveryDiagnosticsCursor{
		Kind:       strings.TrimSpace(kind),
		OccurredAt: occurredAt.UTC().Format(time.RFC3339Nano),
		DeliveryID: strings.TrimSpace(deliveryID),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeAgentDeliveryDiagnosticsCursor(raw, kind, field string) (agentDeliveryDiagnosticsCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return agentDeliveryDiagnosticsCursor{}, AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	var cursor agentDeliveryDiagnosticsCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return agentDeliveryDiagnosticsCursor{}, AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	if strings.TrimSpace(cursor.Kind) != strings.TrimSpace(kind) {
		return agentDeliveryDiagnosticsCursor{}, AgentDeliveryDiagnosticsCursorError{Field: field}
	}
	return cursor, nil
}
