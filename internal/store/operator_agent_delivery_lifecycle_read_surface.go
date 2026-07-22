package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
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
	DeliveryID          string                    `json:"delivery_id"`
	EventID             string                    `json:"event_id"`
	EventName           string                    `json:"event_name"`
	RunID               string                    `json:"run_id,omitempty"`
	EntityID            string                    `json:"entity_id,omitempty"`
	Status              string                    `json:"status"`
	RetryCount          int                       `json:"retry_count"`
	ReasonCode          string                    `json:"reason_code,omitempty"`
	Failure             *runtimefailures.Envelope `json:"failure,omitempty"`
	DeliveryCreatedAt   time.Time                 `json:"delivery_created_at"`
	DeliveryStartedAt   *time.Time                `json:"delivery_started_at,omitempty"`
	DeliveryDeliveredAt *time.Time                `json:"delivery_delivered_at,omitempty"`
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
	if err := r.requireAgentDeliveryLifecycleAccess(); err != nil {
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
	if err := s.requireSQLiteAgentDeliveryLifecycleAccess(); err != nil {
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

func (r *OperatorAgentConversationReadSurface) requireAgentDeliveryLifecycleAccess() error {
	if r == nil || r.db == nil {
		return fmt.Errorf("operator agent delivery lifecycle read owner requires postgres store")
	}
	return r.owner.requireCurrentSchema()
}

func (s *SQLiteRuntimeStore) requireSQLiteAgentDeliveryLifecycleAccess() error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	return s.requireCurrentSchema()
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
	reader, ok := r.owner.(interface {
		deliveryLifecycleSnapshotPageForAgent(context.Context, runtimedelivery.AgentLifecyclePageQuery) (runtimedelivery.SnapshotPage, error)
	})
	if !ok {
		return nil, "", fmt.Errorf("operator agent delivery lifecycle requires canonical bounded delivery snapshots")
	}
	query, err := agentDeliveryLifecyclePageQuery(agentID, opts)
	if err != nil {
		return nil, "", err
	}
	page, err := reader.deliveryLifecycleSnapshotPageForAgent(ctx, query)
	if err != nil {
		return nil, "", fmt.Errorf("list agent delivery lifecycle rows: %w", err)
	}
	rows, err := deliveryLifecycleRowsFromSnapshots(page.Snapshots, func(eventID string) (deliveryLifecycleEventMetadata, error) {
		record, found, err := loadPostgresEventIdentity(ctx, r.db, eventID)
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
	})
	return rows, agentDeliveryLifecyclePageCursor(rows, page.HasMore), err
}

func (s *SQLiteRuntimeStore) listSQLiteAgentDeliveryLifecycleRows(ctx context.Context, agentID string, opts OperatorAgentDeliveryLifecycleOptions) ([]OperatorAgentDeliveryLifecycleRow, string, error) {
	query, err := agentDeliveryLifecyclePageQuery(agentID, opts)
	if err != nil {
		return nil, "", err
	}
	page, err := s.deliveryLifecycleSnapshotPageForAgent(ctx, query)
	if err != nil {
		return nil, "", fmt.Errorf("list sqlite agent delivery lifecycle rows: %w", err)
	}
	rows, err := deliveryLifecycleRowsFromSnapshots(page.Snapshots, func(eventID string) (deliveryLifecycleEventMetadata, error) {
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
	})
	return rows, agentDeliveryLifecyclePageCursor(rows, page.HasMore), err
}

type deliveryLifecycleEventMetadata struct {
	EventName string
	RunID     string
	EntityID  string
}

func deliveryLifecycleRowsFromSnapshots(
	snapshots []runtimedelivery.Snapshot,
	loadEvent func(string) (deliveryLifecycleEventMetadata, error),
) ([]OperatorAgentDeliveryLifecycleRow, error) {
	rows := make([]OperatorAgentDeliveryLifecycleRow, 0, len(snapshots))
	for _, snapshot := range snapshots {
		metadata, err := loadEvent(snapshot.EventID)
		if err != nil {
			return nil, err
		}
		runID := snapshot.RunID
		if runID == "" {
			runID = metadata.RunID
		}
		row := OperatorAgentDeliveryLifecycleRow{
			DeliveryID: snapshot.DeliveryID, EventID: snapshot.EventID, EventName: metadata.EventName,
			RunID: runID, EntityID: metadata.EntityID, Status: string(snapshot.Status),
			RetryCount: snapshot.RetryCount, ReasonCode: snapshot.ReasonCode,
			Failure: runtimefailures.CloneEnvelope(snapshot.Failure), DeliveryCreatedAt: snapshot.CreatedAt,
		}
		if !snapshot.StartedAt.IsZero() {
			started := snapshot.StartedAt
			row.DeliveryStartedAt = &started
		}
		if !snapshot.SettledAt.IsZero() {
			settled := snapshot.SettledAt
			row.DeliveryDeliveredAt = &settled
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func agentDeliveryLifecyclePageQuery(agentID string, opts OperatorAgentDeliveryLifecycleOptions) (runtimedelivery.AgentLifecyclePageQuery, error) {
	query := runtimedelivery.AgentLifecyclePageQuery{
		AgentID: agentID,
		RunID:   opts.RunID,
		Limit:   opts.Limit,
	}
	for _, raw := range opts.Statuses {
		status, err := runtimedelivery.ParseStatus(raw)
		if err != nil {
			return runtimedelivery.AgentLifecyclePageQuery{}, AgentDeliveryLifecycleStatusError{Status: raw}
		}
		query.Statuses = append(query.Statuses, status)
	}
	if opts.Cursor != "" {
		createdAt, deliveryID, err := decodeAgentDeliveryLifecycleCursorPosition(opts.Cursor)
		if err != nil {
			return runtimedelivery.AgentLifecyclePageQuery{}, err
		}
		query.BeforeCreatedAt = createdAt
		query.BeforeDeliveryID = deliveryID
	}
	return query, nil
}

func agentDeliveryLifecyclePageCursor(rows []OperatorAgentDeliveryLifecycleRow, hasMore bool) string {
	if !hasMore || len(rows) == 0 {
		return ""
	}
	last := rows[len(rows)-1]
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
