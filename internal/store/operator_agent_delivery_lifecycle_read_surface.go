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

	"github.com/lib/pq"
)

const (
	DefaultAgentDeliveryLifecycleLimit = 50
	MaxAgentDeliveryLifecycleLimit     = 200
)

var (
	ErrInvalidAgentDeliveryLifecycleCursor = errors.New("invalid agent delivery lifecycle cursor")
	ErrInvalidAgentDeliveryLifecycleStatus = errors.New("invalid agent delivery lifecycle status")
)

type AgentDeliveryLifecycleCursorError struct{}

func (AgentDeliveryLifecycleCursorError) Error() string {
	return "invalid agent delivery lifecycle cursor"
}

func (AgentDeliveryLifecycleCursorError) Unwrap() error {
	return ErrInvalidAgentDeliveryLifecycleCursor
}

type AgentDeliveryLifecycleStatusError struct {
	Status string
}

func (e AgentDeliveryLifecycleStatusError) Error() string {
	status := strings.TrimSpace(e.Status)
	if status == "" {
		return "invalid agent delivery lifecycle status"
	}
	return fmt.Sprintf("invalid agent delivery lifecycle status %q", status)
}

func (e AgentDeliveryLifecycleStatusError) Unwrap() error {
	return ErrInvalidAgentDeliveryLifecycleStatus
}

type OperatorAgentDeliveryLifecycleOptions struct {
	RunID    string
	Statuses []string
	Limit    int
	Cursor   string
}

type OperatorAgentDeliveryLifecycleList struct {
	AgentID    string                              `json:"agent_id"`
	Deliveries []OperatorAgentDeliveryLifecycleRow `json:"deliveries"`
	NextCursor string                              `json:"next_cursor,omitempty"`
}

type OperatorAgentDeliveryLifecycleRow struct {
	DeliveryID          string     `json:"delivery_id"`
	EventID             string     `json:"event_id"`
	EventName           string     `json:"event_name"`
	RunID               string     `json:"run_id,omitempty"`
	EntityID            string     `json:"entity_id,omitempty"`
	Status              string     `json:"status"`
	RetryCount          int        `json:"retry_count"`
	ReasonCode          string     `json:"reason_code,omitempty"`
	LastError           string     `json:"last_error,omitempty"`
	DeliveryCreatedAt   time.Time  `json:"delivery_created_at"`
	DeliveryStartedAt   *time.Time `json:"delivery_started_at,omitempty"`
	DeliveryDeliveredAt *time.Time `json:"delivery_delivered_at,omitempty"`
}

type agentDeliveryLifecycleCursor struct {
	Kind              string `json:"kind"`
	DeliveryCreatedAt string `json:"delivery_created_at"`
	DeliveryID        string `json:"delivery_id"`
}

var agentDeliveryLifecycleStatuses = map[string]struct{}{
	"pending":     {},
	"in_progress": {},
	"delivered":   {},
	"failed":      {},
	"dead_letter": {},
}

func (s *PostgresStore) LoadOperatorAgentDeliveryLifecycle(ctx context.Context, agentID string, opts OperatorAgentDeliveryLifecycleOptions) (OperatorAgentDeliveryLifecycleList, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).LoadOperatorAgentDeliveryLifecycle(ctx, agentID, opts)
}

func (r *OperatorAgentConversationReadSurface) LoadOperatorAgentDeliveryLifecycle(ctx context.Context, agentID string, opts OperatorAgentDeliveryLifecycleOptions) (OperatorAgentDeliveryLifecycleList, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentDeliveryLifecycleList{}, ErrAgentNotFound
	}
	opts, err := defaultOperatorAgentDeliveryLifecycleOptions(opts)
	if err != nil {
		return OperatorAgentDeliveryLifecycleList{}, err
	}
	if err := r.requireAgentDeliveryLifecycleCapabilities(ctx); err != nil {
		return OperatorAgentDeliveryLifecycleList{}, err
	}
	if err := r.ensureAgentDeliveryLifecycleAgentExists(ctx, agentID); err != nil {
		return OperatorAgentDeliveryLifecycleList{}, err
	}
	deliveries, next, err := r.listAgentDeliveryLifecycleRows(ctx, agentID, opts)
	if err != nil {
		return OperatorAgentDeliveryLifecycleList{}, err
	}
	if deliveries == nil {
		deliveries = []OperatorAgentDeliveryLifecycleRow{}
	}
	return OperatorAgentDeliveryLifecycleList{
		AgentID:    agentID,
		Deliveries: deliveries,
		NextCursor: next,
	}, nil
}

func (s *SQLiteRuntimeStore) LoadOperatorAgentDeliveryLifecycle(ctx context.Context, agentID string, opts OperatorAgentDeliveryLifecycleOptions) (OperatorAgentDeliveryLifecycleList, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentDeliveryLifecycleList{}, ErrAgentNotFound
	}
	opts, err := defaultOperatorAgentDeliveryLifecycleOptions(opts)
	if err != nil {
		return OperatorAgentDeliveryLifecycleList{}, err
	}
	if err := s.requireSQLiteAgentDeliveryLifecycleCapabilities(ctx); err != nil {
		return OperatorAgentDeliveryLifecycleList{}, err
	}
	if err := s.ensureSQLiteAgentDeliveryLifecycleAgentExists(ctx, agentID); err != nil {
		return OperatorAgentDeliveryLifecycleList{}, err
	}
	deliveries, next, err := s.listSQLiteAgentDeliveryLifecycleRows(ctx, agentID, opts)
	if err != nil {
		return OperatorAgentDeliveryLifecycleList{}, err
	}
	if deliveries == nil {
		deliveries = []OperatorAgentDeliveryLifecycleRow{}
	}
	return OperatorAgentDeliveryLifecycleList{
		AgentID:    agentID,
		Deliveries: deliveries,
		NextCursor: next,
	}, nil
}

func defaultOperatorAgentDeliveryLifecycleOptions(opts OperatorAgentDeliveryLifecycleOptions) (OperatorAgentDeliveryLifecycleOptions, error) {
	opts.RunID = strings.TrimSpace(opts.RunID)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.Limit <= 0 {
		opts.Limit = DefaultAgentDeliveryLifecycleLimit
	}
	if opts.Limit > MaxAgentDeliveryLifecycleLimit {
		opts.Limit = MaxAgentDeliveryLifecycleLimit
	}
	statuses := make([]string, 0, len(opts.Statuses))
	seen := map[string]struct{}{}
	for _, raw := range opts.Statuses {
		status := strings.TrimSpace(raw)
		if status == "" {
			continue
		}
		if _, ok := agentDeliveryLifecycleStatuses[status]; !ok {
			return OperatorAgentDeliveryLifecycleOptions{}, AgentDeliveryLifecycleStatusError{Status: status}
		}
		if _, ok := seen[status]; ok {
			continue
		}
		seen[status] = struct{}{}
		statuses = append(statuses, status)
	}
	opts.Statuses = statuses
	return opts, nil
}

func (r *OperatorAgentConversationReadSurface) requireAgentDeliveryLifecycleCapabilities(ctx context.Context) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("operator agent delivery lifecycle read owner requires postgres store")
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return err
	}
	if err := requireAgentDeliveryLifecycleSchemaCapabilities(caps); err != nil {
		return err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, r.db)
	if err != nil {
		return err
	}
	if err := requireAgentDeliveryLifecycleColumnCatalog(catalog); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteRuntimeStore) requireSQLiteAgentDeliveryLifecycleCapabilities(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	caps, err := s.ResolveSchemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if err := requireAgentDeliveryLifecycleSchemaCapabilities(caps); err != nil {
		return err
	}
	catalog, err := loadSQLiteSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	if err := requireAgentDeliveryLifecycleColumnCatalog(catalog); err != nil {
		return err
	}
	return nil
}

func requireAgentDeliveryLifecycleSchemaCapabilities(caps StoreSchemaCapabilities) error {
	switch {
	case caps.Agents != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("agents", caps.Agents)
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case !caps.Events.LogRunID:
		return fmt.Errorf("agent delivery lifecycle read owner requires canonical events.run_id")
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	return nil
}

func requireAgentDeliveryLifecycleColumnCatalog(catalog schemaColumnCatalog) error {
	required := map[string][]string{
		"agents": {
			"agent_id", "status",
		},
		"events": {
			"event_id", "run_id", "event_name", "entity_id",
		},
		"event_deliveries": {
			"delivery_id", "run_id", "event_id", "subscriber_type", "subscriber_id",
			"status", "retry_count", "reason_code", "last_error", "started_at", "delivered_at", "created_at",
		},
	}
	for table, columns := range required {
		if !catalog.hasColumns(table, columns...) {
			return fmt.Errorf("agent delivery lifecycle read owner requires canonical %s columns: %s", table, strings.Join(columns, ", "))
		}
	}
	return nil
}

func (r *OperatorAgentConversationReadSurface) ensureAgentDeliveryLifecycleAgentExists(ctx context.Context, agentID string) error {
	var exists bool
	if err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM agents
			WHERE agent_id = $1
			  AND status NOT IN ('terminated', 'ephemeral')
		)
	`, agentID).Scan(&exists); err != nil {
		return fmt.Errorf("load agent delivery lifecycle agent: %w", err)
	}
	if !exists {
		return ErrAgentNotFound
	}
	return nil
}

func (s *SQLiteRuntimeStore) ensureSQLiteAgentDeliveryLifecycleAgentExists(ctx context.Context, agentID string) error {
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM agents
			WHERE agent_id = ?
			  AND status NOT IN ('terminated', 'ephemeral')
		)
	`, agentID).Scan(&exists); err != nil {
		return fmt.Errorf("load sqlite agent delivery lifecycle agent: %w", err)
	}
	if !exists {
		return ErrAgentNotFound
	}
	return nil
}

func (r *OperatorAgentConversationReadSurface) listAgentDeliveryLifecycleRows(ctx context.Context, agentID string, opts OperatorAgentDeliveryLifecycleOptions) ([]OperatorAgentDeliveryLifecycleRow, string, error) {
	where := []string{"d.subscriber_type = 'agent'", "d.subscriber_id = $1"}
	args := []any{agentID}
	if opts.RunID != "" {
		args = append(args, opts.RunID)
		where = append(where, fmt.Sprintf("COALESCE(d.run_id::text, e.run_id::text, '') = $%d", len(args)))
	}
	if len(opts.Statuses) > 0 {
		args = append(args, pq.Array(opts.Statuses))
		where = append(where, fmt.Sprintf("d.status = ANY($%d::text[])", len(args)))
	}
	if opts.Cursor != "" {
		createdAt, deliveryID, err := decodeAgentDeliveryLifecycleCursorPosition(opts.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args, createdAt.UTC(), deliveryID)
		where = append(where, fmt.Sprintf("(d.created_at < $%d OR (d.created_at = $%d AND d.delivery_id::text < $%d))", len(args)-1, len(args)-1, len(args)))
	}
	args = append(args, opts.Limit+1)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			d.delivery_id::text,
			d.event_id::text,
			COALESCE(e.event_name, ''),
			COALESCE(d.run_id::text, e.run_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.reason_code, ''),
			COALESCE(d.last_error, ''),
			d.created_at,
			d.started_at,
			d.delivered_at
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		WHERE %s
		ORDER BY d.created_at DESC, d.delivery_id::text DESC
		LIMIT $%d
	`, strings.Join(where, " AND "), len(args)), args...)
	if err != nil {
		return nil, "", fmt.Errorf("list agent delivery lifecycle rows: %w", err)
	}
	defer rows.Close()

	out := []OperatorAgentDeliveryLifecycleRow{}
	for rows.Next() {
		var (
			item        OperatorAgentDeliveryLifecycleRow
			startedAt   sql.NullTime
			deliveredAt sql.NullTime
		)
		if err := rows.Scan(
			&item.DeliveryID,
			&item.EventID,
			&item.EventName,
			&item.RunID,
			&item.EntityID,
			&item.Status,
			&item.RetryCount,
			&item.ReasonCode,
			&item.LastError,
			&item.DeliveryCreatedAt,
			&startedAt,
			&deliveredAt,
		); err != nil {
			return nil, "", fmt.Errorf("scan agent delivery lifecycle row: %w", err)
		}
		item.DeliveryStartedAt = nullableTimePtr(startedAt)
		item.DeliveryDeliveredAt = nullableTimePtr(deliveredAt)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read agent delivery lifecycle rows: %w", err)
	}
	return trimAgentDeliveryLifecyclePage(out, opts.Limit), agentDeliveryLifecycleNextCursor(out, opts.Limit), nil
}

func (s *SQLiteRuntimeStore) listSQLiteAgentDeliveryLifecycleRows(ctx context.Context, agentID string, opts OperatorAgentDeliveryLifecycleOptions) ([]OperatorAgentDeliveryLifecycleRow, string, error) {
	where := []string{"d.subscriber_type = 'agent'", "d.subscriber_id = ?"}
	args := []any{agentID}
	if opts.RunID != "" {
		where = append(where, "COALESCE(d.run_id, e.run_id, '') = ?")
		args = append(args, opts.RunID)
	}
	if len(opts.Statuses) > 0 {
		where = append(where, "d.status IN ("+sqlitePlaceholders(len(opts.Statuses))+")")
		for _, status := range opts.Statuses {
			args = append(args, status)
		}
	}
	if opts.Cursor != "" {
		createdAt, deliveryID, err := decodeAgentDeliveryLifecycleCursorPosition(opts.Cursor)
		if err != nil {
			return nil, "", err
		}
		where = append(where, "(d.created_at < ? OR (d.created_at = ? AND d.delivery_id < ?))")
		args = append(args, createdAt.UTC(), createdAt.UTC(), deliveryID)
	}
	args = append(args, opts.Limit+1)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			d.delivery_id,
			d.event_id,
			COALESCE(e.event_name, ''),
			COALESCE(d.run_id, e.run_id, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.reason_code, ''),
			COALESCE(d.last_error, ''),
			d.created_at,
			d.started_at,
			d.delivered_at
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY d.created_at DESC, d.delivery_id DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list sqlite agent delivery lifecycle rows: %w", err)
	}
	defer rows.Close()

	out := []OperatorAgentDeliveryLifecycleRow{}
	for rows.Next() {
		var (
			item                                 OperatorAgentDeliveryLifecycleRow
			createdRaw, startedRaw, deliveredRaw any
		)
		if err := rows.Scan(
			&item.DeliveryID,
			&item.EventID,
			&item.EventName,
			&item.RunID,
			&item.EntityID,
			&item.Status,
			&item.RetryCount,
			&item.ReasonCode,
			&item.LastError,
			&createdRaw,
			&startedRaw,
			&deliveredRaw,
		); err != nil {
			return nil, "", fmt.Errorf("scan sqlite agent delivery lifecycle row: %w", err)
		}
		createdAt, ok, err := sqliteTimeValue(createdRaw)
		if err != nil {
			return nil, "", fmt.Errorf("scan sqlite agent delivery lifecycle created_at: %w", err)
		}
		if !ok {
			return nil, "", fmt.Errorf("agent delivery lifecycle owner found delivery %s without created_at", item.DeliveryID)
		}
		item.DeliveryCreatedAt = createdAt
		item.DeliveryStartedAt = sqliteTraceTimePtr(startedRaw)
		item.DeliveryDeliveredAt = sqliteTraceTimePtr(deliveredRaw)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read sqlite agent delivery lifecycle rows: %w", err)
	}
	return trimAgentDeliveryLifecyclePage(out, opts.Limit), agentDeliveryLifecycleNextCursor(out, opts.Limit), nil
}

func trimAgentDeliveryLifecyclePage(rows []OperatorAgentDeliveryLifecycleRow, limit int) []OperatorAgentDeliveryLifecycleRow {
	if limit < 0 {
		limit = 0
	}
	if len(rows) > limit {
		return rows[:limit]
	}
	return rows
}

func agentDeliveryLifecycleNextCursor(rows []OperatorAgentDeliveryLifecycleRow, limit int) string {
	if limit <= 0 || len(rows) <= limit {
		return ""
	}
	last := rows[limit-1]
	return encodeAgentDeliveryLifecycleCursor(last.DeliveryCreatedAt, last.DeliveryID)
}

func decodeAgentDeliveryLifecycleCursorPosition(raw string) (time.Time, string, error) {
	cursor, err := decodeAgentDeliveryLifecycleCursor(raw)
	if err != nil {
		return time.Time{}, "", err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, cursor.DeliveryCreatedAt)
	if err != nil || strings.TrimSpace(cursor.DeliveryID) == "" {
		return time.Time{}, "", AgentDeliveryLifecycleCursorError{}
	}
	return createdAt.UTC(), strings.TrimSpace(cursor.DeliveryID), nil
}

func encodeAgentDeliveryLifecycleCursor(createdAt time.Time, deliveryID string) string {
	raw, _ := json.Marshal(agentDeliveryLifecycleCursor{
		Kind:              "agent.delivery_lifecycle",
		DeliveryCreatedAt: createdAt.UTC().Format(time.RFC3339Nano),
		DeliveryID:        strings.TrimSpace(deliveryID),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeAgentDeliveryLifecycleCursor(raw string) (agentDeliveryLifecycleCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return agentDeliveryLifecycleCursor{}, AgentDeliveryLifecycleCursorError{}
	}
	var cursor agentDeliveryLifecycleCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return agentDeliveryLifecycleCursor{}, AgentDeliveryLifecycleCursorError{}
	}
	if strings.TrimSpace(cursor.Kind) != "agent.delivery_lifecycle" {
		return agentDeliveryLifecycleCursor{}, AgentDeliveryLifecycleCursorError{}
	}
	return cursor, nil
}
