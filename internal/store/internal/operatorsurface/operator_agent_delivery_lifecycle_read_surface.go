package operatorsurface

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

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

func (r *AgentPostgres) LoadOperatorAgentDeliveryLifecycle(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryLifecycleOptions) (operatorread.OperatorAgentDeliveryLifecycleList, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return operatorread.OperatorAgentDeliveryLifecycleList{}, operatorread.ErrAgentNotFound
	}
	opts, err := defaultOperatorAgentDeliveryLifecycleOptions(opts)
	if err != nil {
		return operatorread.OperatorAgentDeliveryLifecycleList{}, err
	}
	if err := r.requireAgentDeliveryLifecycleAccess(); err != nil {
		return operatorread.OperatorAgentDeliveryLifecycleList{}, err
	}
	if err := r.ensureAgentDeliveryLifecycleAgentExists(ctx, identity); err != nil {
		return operatorread.OperatorAgentDeliveryLifecycleList{}, err
	}
	deliveries, next, err := r.listAgentDeliveryLifecycleRows(ctx, identity, opts)
	if err != nil {
		return operatorread.OperatorAgentDeliveryLifecycleList{}, err
	}
	if deliveries == nil {
		deliveries = []operatorread.OperatorAgentDeliveryLifecycleRow{}
	}
	return operatorread.OperatorAgentDeliveryLifecycleList{
		AgentID:    identity.AgentID(),
		Deliveries: deliveries,
		NextCursor: next,
	}, nil
}

func (s *AgentSQLite) LoadOperatorAgentDeliveryLifecycle(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryLifecycleOptions) (operatorread.OperatorAgentDeliveryLifecycleList, error) {
	identity = identity.Normalize()
	if err := identity.Validate(); err != nil {
		return operatorread.OperatorAgentDeliveryLifecycleList{}, operatorread.ErrAgentNotFound
	}
	opts, err := defaultOperatorAgentDeliveryLifecycleOptions(opts)
	if err != nil {
		return operatorread.OperatorAgentDeliveryLifecycleList{}, err
	}
	if err := s.requireSQLiteAgentDeliveryLifecycleAccess(); err != nil {
		return operatorread.OperatorAgentDeliveryLifecycleList{}, err
	}
	if err := s.ensureSQLiteAgentDeliveryLifecycleAgentExists(ctx, identity); err != nil {
		return operatorread.OperatorAgentDeliveryLifecycleList{}, err
	}
	deliveries, next, err := s.listSQLiteAgentDeliveryLifecycleRows(ctx, identity, opts)
	if err != nil {
		return operatorread.OperatorAgentDeliveryLifecycleList{}, err
	}
	if deliveries == nil {
		deliveries = []operatorread.OperatorAgentDeliveryLifecycleRow{}
	}
	return operatorread.OperatorAgentDeliveryLifecycleList{
		AgentID:    identity.AgentID(),
		Deliveries: deliveries,
		NextCursor: next,
	}, nil
}

func defaultOperatorAgentDeliveryLifecycleOptions(opts operatorread.OperatorAgentDeliveryLifecycleOptions) (operatorread.OperatorAgentDeliveryLifecycleOptions, error) {
	opts.RunID = strings.TrimSpace(opts.RunID)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.Limit <= 0 {
		opts.Limit = operatorread.DefaultAgentDeliveryLifecycleLimit
	}
	if opts.Limit > operatorread.MaxAgentDeliveryLifecycleLimit {
		opts.Limit = operatorread.MaxAgentDeliveryLifecycleLimit
	}
	statuses := make([]string, 0, len(opts.Statuses))
	seen := map[string]struct{}{}
	for _, raw := range opts.Statuses {
		status := strings.TrimSpace(raw)
		if status == "" {
			continue
		}
		if _, ok := agentDeliveryLifecycleStatuses[status]; !ok {
			return operatorread.OperatorAgentDeliveryLifecycleOptions{}, operatorread.AgentDeliveryLifecycleStatusError{Status: status}
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

func (r *AgentPostgres) requireAgentDeliveryLifecycleAccess() error {
	if r == nil || r.backend == nil {
		return fmt.Errorf("operator agent delivery lifecycle read owner requires postgres store")
	}
	return r.requireCurrentSchema()
}

func (s *AgentSQLite) requireSQLiteAgentDeliveryLifecycleAccess() error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite runtime store is required")
	}
	return s.requireCurrentSchema()
}

func (r *AgentPostgres) ensureAgentDeliveryLifecycleAgentExists(ctx context.Context, identity agentidentity.Identity) error {
	fields, err := agentIdentityFields(identity)
	if err != nil {
		return operatorread.ErrAgentNotFound
	}
	var exists bool
	if err := r.backend.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM agents
			WHERE agent_id = $1
			  AND agent_name_owner = $2
			  AND agent_name_source = $3
			  AND agent_route_presence = $4
			  AND flow_scope_key = $5
			  AND flow_instance_id = $6
			  AND flow_instance = $7
			  AND status NOT IN ('terminated', 'ephemeral')
		)
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&exists); err != nil {
		return fmt.Errorf("load agent delivery lifecycle agent: %w", err)
	}
	if !exists {
		return operatorread.ErrAgentNotFound
	}
	return nil
}

func (s *AgentSQLite) ensureSQLiteAgentDeliveryLifecycleAgentExists(ctx context.Context, identity agentidentity.Identity) error {
	fields, err := agentIdentityFields(identity)
	if err != nil {
		return operatorread.ErrAgentNotFound
	}
	var exists bool
	if err := s.backend.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM agents
			WHERE agent_id = ?
			  AND agent_name_owner = ?
			  AND agent_name_source = ?
			  AND agent_route_presence = ?
			  AND flow_scope_key = ?
			  AND flow_instance_id = ?
			  AND flow_instance = ?
			  AND status NOT IN ('terminated', 'ephemeral')
		)
	`, fields.AgentID, fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath).Scan(&exists); err != nil {
		return fmt.Errorf("load sqlite agent delivery lifecycle agent: %w", err)
	}
	if !exists {
		return operatorread.ErrAgentNotFound
	}
	return nil
}

func (r *AgentPostgres) listAgentDeliveryLifecycleRows(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryLifecycleOptions) ([]operatorread.OperatorAgentDeliveryLifecycleRow, string, error) {
	query, err := agentDeliveryLifecyclePageQuery(identity, opts)
	if err != nil {
		return nil, "", err
	}
	page, err := r.delivery.DeliveryLifecycleSnapshotPageForAgent(ctx, query)
	if err != nil {
		return nil, "", fmt.Errorf("list agent delivery lifecycle rows: %w", err)
	}
	rows, err := deliveryLifecycleRowsFromSnapshots(page.Snapshots, func(eventID string) (deliveryLifecycleEventMetadata, error) {
		record, found, err := loadPostgresEventIdentity(ctx, r.backend, eventID)
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

func (s *AgentSQLite) listSQLiteAgentDeliveryLifecycleRows(ctx context.Context, identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryLifecycleOptions) ([]operatorread.OperatorAgentDeliveryLifecycleRow, string, error) {
	query, err := agentDeliveryLifecyclePageQuery(identity, opts)
	if err != nil {
		return nil, "", err
	}
	page, err := s.delivery.DeliveryLifecycleSnapshotPageForAgent(ctx, query)
	if err != nil {
		return nil, "", fmt.Errorf("list sqlite agent delivery lifecycle rows: %w", err)
	}
	rows, err := deliveryLifecycleRowsFromSnapshots(page.Snapshots, func(eventID string) (deliveryLifecycleEventMetadata, error) {
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
	})
	return rows, agentDeliveryLifecyclePageCursor(rows, page.HasMore), err
}

func deliveryLifecycleRowsFromSnapshots(
	snapshots []runtimedelivery.Snapshot,
	loadEvent func(string) (deliveryLifecycleEventMetadata, error),
) ([]operatorread.OperatorAgentDeliveryLifecycleRow, error) {
	rows := make([]operatorread.OperatorAgentDeliveryLifecycleRow, 0, len(snapshots))
	for _, snapshot := range snapshots {
		metadata, err := loadEvent(snapshot.EventID)
		if err != nil {
			return nil, err
		}
		runID := snapshot.RunID
		if runID == "" {
			runID = metadata.RunID
		}
		row := operatorread.OperatorAgentDeliveryLifecycleRow{
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

func agentDeliveryLifecyclePageQuery(identity agentidentity.Identity, opts operatorread.OperatorAgentDeliveryLifecycleOptions) (runtimedelivery.AgentLifecyclePageQuery, error) {
	query := runtimedelivery.AgentLifecyclePageQuery{
		AgentIdentity: identity,
		RunID:         opts.RunID,
		Limit:         opts.Limit,
	}
	for _, raw := range opts.Statuses {
		status, err := runtimedelivery.ParseStatus(raw)
		if err != nil {
			return runtimedelivery.AgentLifecyclePageQuery{}, operatorread.AgentDeliveryLifecycleStatusError{Status: raw}
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

func agentDeliveryLifecyclePageCursor(rows []operatorread.OperatorAgentDeliveryLifecycleRow, hasMore bool) string {
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
		return time.Time{}, "", operatorread.AgentDeliveryLifecycleCursorError{}
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
		return agentDeliveryLifecycleCursor{}, operatorread.AgentDeliveryLifecycleCursorError{}
	}
	var cursor agentDeliveryLifecycleCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return agentDeliveryLifecycleCursor{}, operatorread.AgentDeliveryLifecycleCursorError{}
	}
	if strings.TrimSpace(cursor.Kind) != "agent.delivery_lifecycle" {
		return agentDeliveryLifecycleCursor{}, operatorread.AgentDeliveryLifecycleCursorError{}
	}
	return cursor, nil
}
