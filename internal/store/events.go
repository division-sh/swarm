package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

const (
	replayScopeMarkerSubscriberType = "node"
	replayScopeMarkerSubscriberID   = "__runtime_replay_scope__"
	replayScopeReasonDirect         = "replay_scope_direct"
	replayScopeReasonSubscribed     = "replay_scope_subscribed"
)

type rowQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type execQueryer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func (s *PostgresStore) SetEventPayloadValidator(validator func(eventType string, payload []byte) error) {
	if s == nil {
		return
	}
	s.eventPayloadValidator = EventPayloadValidator(validator)
}

// validateEventPayload is the store-side canonical admission guard for append
// paths that may not pass through an emit-surface owner immediately before
// persistence.
func (s *PostgresStore) validateEventPayload(eventType string, payload []byte) error {
	if s == nil || s.eventPayloadValidator == nil {
		return nil
	}
	if err := s.eventPayloadValidator(strings.TrimSpace(eventType), payload); err != nil {
		return fmt.Errorf("validate event payload: %w", err)
	}
	return nil
}

func (s *PostgresStore) AppendEvent(ctx context.Context, evt events.Event) error {
	return s.AppendEventTx(ctx, nil, evt)
}

func (s *PostgresStore) BeginEventTx(ctx context.Context) (*sql.Tx, error) {
	return s.DB.BeginTx(ctx, nil)
}

func (s *PostgresStore) AppendEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	return withEventStoreRetry(ctx, tx, func() error {
		evt = s.enrichEventCorrelation(ctx, caps, chooseRowQueryer(s.DB, tx), evt)
		switch caps.Events.Log {
		case SchemaFlavorCanonical:
			if tx == nil && persistedBundleRunCreationValidationRequired(ctx, caps) {
				return s.appendEventSpecWithRunCreationValidationTx(ctx, caps, evt)
			}
			return s.appendEventSpec(ctx, caps, tx, evt)
		default:
			return unsupportedSchemaCapability("events", caps.Events.Log)
		}
	})
}

func (s *PostgresStore) appendEventSpecWithRunCreationValidationTx(ctx context.Context, caps StoreSchemaCapabilities, evt events.Event) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event source validation tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := s.appendEventSpec(ctx, caps, tx, evt); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit event source validation tx: %w", err)
	}
	committed = true
	return nil
}

func (s *PostgresStore) PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	evt = s.enrichEventCorrelation(ctx, caps, chooseRowQueryer(s.DB, nil), evt)
	switch {
	case caps.Events.Log == SchemaFlavorCanonical && caps.Events.Deliveries == SchemaFlavorCanonical:
		return s.persistEventWithDeliveriesSpec(ctx, caps, evt, agentIDs)
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	return nil
}

func (s *PostgresStore) PersistEventWithDeliveriesAndScope(
	ctx context.Context,
	evt events.Event,
	agentIDs []string,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	evt = s.enrichEventCorrelation(ctx, caps, chooseRowQueryer(s.DB, nil), evt)
	switch {
	case caps.Events.Log == SchemaFlavorCanonical && caps.Events.Deliveries == SchemaFlavorCanonical:
		return s.persistEventWithDeliveriesAndScopeSpec(ctx, caps, evt, agentIDs, scope)
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	return nil
}

func (s *PostgresStore) PersistEventWithDeliveryRoutesAndScope(
	ctx context.Context,
	evt events.Event,
	agentIDs []string,
	deliveryTargets map[string]events.RouteIdentity,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	evt = s.enrichEventCorrelation(ctx, caps, chooseRowQueryer(s.DB, nil), evt)
	switch {
	case caps.Events.Log == SchemaFlavorCanonical && caps.Events.Deliveries == SchemaFlavorCanonical:
		return s.persistEventWithDeliveryRoutesAndScopeSpec(ctx, caps, evt, agentIDs, deliveryTargets, scope)
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	return nil
}

func (s *PostgresStore) PersistEventWithDeliveryRouteSetAndScope(
	ctx context.Context,
	evt events.Event,
	deliveryRoutes []events.DeliveryRoute,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	evt = s.enrichEventCorrelation(ctx, caps, chooseRowQueryer(s.DB, nil), evt)
	switch {
	case caps.Events.Log == SchemaFlavorCanonical && caps.Events.Deliveries == SchemaFlavorCanonical:
		return s.persistEventWithDeliveryRouteSetAndScopeSpec(ctx, caps, evt, deliveryRoutes, scope)
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	return nil
}

func (s *PostgresStore) enrichEventCorrelation(ctx context.Context, caps StoreSchemaCapabilities, q rowQueryer, evt events.Event) events.Event {
	if strings.TrimSpace(evt.RunID) == "" {
		if lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx); ok {
			evt.RunID = strings.TrimSpace(lineage.RunID)
		}
	}
	parentID := strings.TrimSpace(evt.ParentEventID)
	if parentID == "" {
		if lineageParentID := runtimecorrelation.RuntimeLineageParentForEvent(ctx, evt.ID); lineageParentID != "" {
			parentID = lineageParentID
			evt.ParentEventID = lineageParentID
		}
	}
	if parentID == "" {
		if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
			if inboundID := strings.TrimSpace(inbound.ID); inboundID != "" && inboundID != strings.TrimSpace(evt.ID) {
				parentID = inboundID
				evt.ParentEventID = inboundID
			}
		}
	}
	if strings.TrimSpace(evt.RunID) == "" && parentID != "" {
		if runID := lookupEventRunID(ctx, caps, q, parentID); runID != "" {
			evt.RunID = runID
		}
	}
	_, evt = runtimecorrelation.CorrelateEvent(ctx, evt)
	return evt
}

func sanitizeOptionalUUID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

func validateOptionalEntityUUID(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if _, err := uuid.Parse(raw); err != nil {
		return "", fmt.Errorf("invalid entity_id %q: must be a UUID", raw)
	}
	return raw, nil
}

func (s *PostgresStore) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	return s.InsertEventDeliveriesTx(ctx, nil, eventID, agentIDs)
}

func (s *PostgresStore) InsertEventDeliveriesTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string) error {
	if len(agentIDs) == 0 {
		return nil
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	switch {
	case caps.Events.Deliveries == SchemaFlavorCanonical && caps.Events.Log == SchemaFlavorCanonical:
		return s.insertEventDeliveriesSpec(ctx, caps, tx, eventID, agentIDs)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	}
	return nil
}

func (s *PostgresStore) UpsertCommittedReplayScope(
	ctx context.Context,
	eventID string,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	return s.UpsertCommittedReplayScopeTx(ctx, nil, eventID, scope)
}

func (s *PostgresStore) UpsertCommittedReplayScopeTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	switch {
	case caps.Events.Deliveries == SchemaFlavorCanonical && caps.Events.Log == SchemaFlavorCanonical:
		return s.upsertCommittedReplayScopeSpec(ctx, caps, tx, eventID, scope)
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	}
	return nil
}

func (s *PostgresStore) UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, errText)
}

func (s *PostgresStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		status = "processed"
	}
	if strings.TrimSpace(errText) != "" && status == "processed" {
		status = "error"
	}
	if caps.Events.Receipts != SchemaFlavorCanonical || caps.Events.Log != SchemaFlavorCanonical {
		if caps.Events.Receipts != SchemaFlavorCanonical {
			return unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
		}
		return unsupportedSchemaCapability("events", caps.Events.Log)
	}
	return s.upsertPipelineReceiptSpec(ctx, tx, eventID, status, errText)
}

func (s *PostgresStore) ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	if caps.Events.Receipts != SchemaFlavorCanonical || caps.Events.Log != SchemaFlavorCanonical {
		if caps.Events.Receipts != SchemaFlavorCanonical {
			return nil, unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
		}
		return nil, unsupportedSchemaCapability("events", caps.Events.Log)
	}
	return s.listEventsMissingPipelineReceiptSpec(ctx, caps, since, limit)
}

func (s *PostgresStore) ListEventsMissingPipelineReceiptForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	if caps.Events.Receipts != SchemaFlavorCanonical || caps.Events.Log != SchemaFlavorCanonical {
		if caps.Events.Receipts != SchemaFlavorCanonical {
			return nil, unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
		}
		return nil, unsupportedSchemaCapability("events", caps.Events.Log)
	}
	if !caps.Events.LogRunID {
		return nil, fmt.Errorf("list missing pipeline receipts by run requires canonical events.run_id")
	}
	return s.listEventsMissingPipelineReceiptForRunSpec(ctx, caps, runID, since, limit)
}

func (s *PostgresStore) ClaimPipelineReplay(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, false, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, false, fmt.Errorf("event_id is required")
	}
	if caps.Events.Receipts != SchemaFlavorCanonical || caps.Events.Log != SchemaFlavorCanonical {
		if caps.Events.Receipts != SchemaFlavorCanonical {
			return nil, false, unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
		}
		return nil, false, unsupportedSchemaCapability("events", caps.Events.Log)
	}
	lease, claimed, err := acquireAdvisoryLockLease(ctx, s.DB, replayClaimLockKey(eventID))
	if err != nil || !claimed {
		return nil, claimed, err
	}
	var pending bool
	err = lease.conn.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM events e
			LEFT JOIN event_receipts r
				ON r.event_id = e.event_id
				AND r.subscriber_type = 'platform'
				AND r.subscriber_id = 'pipeline'
			WHERE e.event_id = $1::uuid
			  AND r.event_id IS NULL
		)
	`, eventID).Scan(&pending)
	if err != nil {
		_ = lease.Release(ctx)
		return nil, false, fmt.Errorf("claim pipeline replay: %w", err)
	}
	if !pending {
		_ = lease.Release(ctx)
		return nil, false, nil
	}
	return lease, true, nil
}

func (s *PostgresStore) EventExists(ctx context.Context, eventID string) (bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return false, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM events WHERE event_id = $1::uuid)`
	switch caps.Events.Log {
	case SchemaFlavorCanonical:
	default:
		return false, unsupportedSchemaCapability("events", caps.Events.Log)
	}
	if err := s.DB.QueryRowContext(ctx, query, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("event exists lookup: %w", err)
	}
	return exists, nil
}

func (s *PostgresStore) ListEventDeliveryRecipients(ctx context.Context, eventID string) ([]string, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	query := `
		SELECT subscriber_id
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		ORDER BY subscriber_id ASC
	`
	switch caps.Events.Deliveries {
	case SchemaFlavorCanonical:
	default:
		return nil, unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	rows, err := s.DB.QueryContext(ctx, query, eventID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery recipients: %w", err)
	}
	defer rows.Close()

	recipients := make([]string, 0, 8)
	for rows.Next() {
		var agentID string
		if err := rows.Scan(&agentID); err != nil {
			return nil, fmt.Errorf("scan event delivery recipient: %w", err)
		}
		agentID = strings.TrimSpace(agentID)
		if agentID != "" {
			recipients = append(recipients, agentID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event delivery recipients: %w", err)
	}
	return recipients, nil
}

func (s *PostgresStore) ListEventDeliveryTargets(ctx context.Context, eventID string) (map[string]events.RouteIdentity, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	if caps.Events.Deliveries != SchemaFlavorCanonical {
		return nil, unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	if !caps.Events.DeliveryTargetRoute {
		return nil, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT subscriber_id, COALESCE(delivery_target_route, '{}'::jsonb)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		ORDER BY created_at ASC, subscriber_id ASC
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery targets: %w", err)
	}
	defer rows.Close()
	out := map[string]events.RouteIdentity{}
	for rows.Next() {
		var subscriberID string
		var raw json.RawMessage
		if err := rows.Scan(&subscriberID, &raw); err != nil {
			return nil, fmt.Errorf("scan event delivery target: %w", err)
		}
		route := decodeRouteIdentityJSON(raw)
		if route.Empty() {
			continue
		}
		out[strings.TrimSpace(subscriberID)] = route
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event delivery targets: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (s *PostgresStore) ListEventDeliveryRoutes(ctx context.Context, eventID string) ([]events.DeliveryRoute, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	if caps.Events.Deliveries != SchemaFlavorCanonical {
		return nil, unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	routeSelect := `'{}'::jsonb`
	if caps.Events.DeliveryTargetRoute {
		routeSelect = `COALESCE(delivery_target_route, '{}'::jsonb)`
	}
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT subscriber_type, subscriber_id, %s
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND NOT (subscriber_type = $2 AND subscriber_id = $3)
		ORDER BY created_at ASC, delivery_id ASC
	`, routeSelect), eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID)
	if err != nil {
		return nil, fmt.Errorf("list event delivery routes: %w", err)
	}
	defer rows.Close()
	out := make([]events.DeliveryRoute, 0, 8)
	for rows.Next() {
		var subscriberType, subscriberID string
		var raw json.RawMessage
		if err := rows.Scan(&subscriberType, &subscriberID, &raw); err != nil {
			return nil, fmt.Errorf("scan event delivery route: %w", err)
		}
		out = append(out, events.DeliveryRoute{
			SubscriberType: subscriberType,
			SubscriberID:   subscriberID,
			Target:         decodeRouteIdentityJSON(raw),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event delivery routes: %w", err)
	}
	return events.NormalizeDeliveryRoutes(out), nil
}

func (s *PostgresStore) LoadCommittedReplayScope(
	ctx context.Context,
	eventID string,
) (runtimereplayclaim.CommittedReplayScope, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return "", err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	}
	switch caps.Events.Deliveries {
	case SchemaFlavorCanonical:
	default:
		return "", unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
	var reasonCode string
	err = s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = $2
		  AND subscriber_id = $3
		ORDER BY created_at DESC, delivery_id DESC
		LIMIT 1
	`, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID).Scan(&reasonCode)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	case err != nil:
		return "", fmt.Errorf("load committed replay scope: %w", err)
	}
	scope, ok := committedReplayScopeFromReasonCode(reasonCode)
	if !ok {
		return "", fmt.Errorf("load committed replay scope: unrecognized reason_code %q", strings.TrimSpace(reasonCode))
	}
	return scope, nil
}

func (s *PostgresStore) appendEventSpec(ctx context.Context, caps StoreSchemaCapabilities, tx *sql.Tx, evt events.Event) error {
	id, runID, name, entityID, flowInstance, scope, payload, chainDepth, producedBy, producedByType, sourceEventID, createdAt, err := eventStorageEnvelope(evt)
	if err != nil {
		return err
	}
	sourceRoute, targetRoute, targetSet := eventRouteStorageEnvelope(evt)
	if err := s.validateEventPayload(name, payload); err != nil {
		return err
	}
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	q := `
		INSERT INTO events (
			event_id, event_name, entity_id, flow_instance, scope, payload,
			chain_depth, produced_by, produced_by_type, source_event_id, created_at
		)
		VALUES (
			$1::uuid, $2, NULLIF($3,'')::uuid, NULLIF($4,''), $5, $6::jsonb,
			$7, NULLIF($8,''), $9, NULLIF($10,'')::uuid, $11
		)
		ON CONFLICT (event_id) DO NOTHING
	`
	args := []any{id, name, entityID, flowInstance, scope, string(payload), chainDepth, producedBy, producedByType, sourceEventID, createdAt}
	if caps.Events.LogRouteIdentity {
		q = `
			INSERT INTO events (
				event_id, event_name, entity_id, flow_instance, scope, payload,
				chain_depth, produced_by, produced_by_type, source_event_id, created_at,
				source_route, target_route, target_set
			)
			VALUES (
				$1::uuid, $2, NULLIF($3,'')::uuid, NULLIF($4,''), $5, $6::jsonb,
				$7, NULLIF($8,''), $9, NULLIF($10,'')::uuid, $11,
				$12::jsonb, $13::jsonb, $14::jsonb
			)
			ON CONFLICT (event_id) DO NOTHING
		`
		args = []any{id, name, entityID, flowInstance, scope, string(payload), chainDepth, producedBy, producedByType, sourceEventID, createdAt, string(sourceRoute), string(targetRoute), string(targetSet)}
	} else if eventHasRouteIdentity(evt) {
		return fmt.Errorf("events source_route/target_route/target_set columns required for routed event")
	}
	if caps.Events.LogRunID {
		if err := s.ensureRunRow(ctx, caps, tx, runID, id, name, false); err != nil {
			return err
		}
		q = `
			INSERT INTO events (
				event_id, run_id, event_name, entity_id, flow_instance, scope, payload,
				chain_depth, produced_by, produced_by_type, source_event_id, created_at
			)
			VALUES (
				$1::uuid, NULLIF($2,'')::uuid, $3, NULLIF($4,'')::uuid, NULLIF($5,''), $6, $7::jsonb,
				$8, NULLIF($9,''), $10, NULLIF($11,'')::uuid, $12
			)
			ON CONFLICT (event_id) DO NOTHING
		`
		args = []any{id, runID, name, entityID, flowInstance, scope, string(payload), chainDepth, producedBy, producedByType, sourceEventID, createdAt}
		if caps.Events.LogRouteIdentity {
			q = `
				INSERT INTO events (
					event_id, run_id, event_name, entity_id, flow_instance, scope, payload,
					chain_depth, produced_by, produced_by_type, source_event_id, created_at,
					source_route, target_route, target_set
				)
				VALUES (
					$1::uuid, NULLIF($2,'')::uuid, $3, NULLIF($4,'')::uuid, NULLIF($5,''), $6, $7::jsonb,
					$8, NULLIF($9,''), $10, NULLIF($11,'')::uuid, $12,
					$13::jsonb, $14::jsonb, $15::jsonb
				)
				ON CONFLICT (event_id) DO NOTHING
			`
			args = []any{id, runID, name, entityID, flowInstance, scope, string(payload), chainDepth, producedBy, producedByType, sourceEventID, createdAt, string(sourceRoute), string(targetRoute), string(targetSet)}
		}
	}
	res, err := execFn(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	if caps.Events.LogRunID {
		rows, rowsErr := res.RowsAffected()
		if rowsErr == nil && rows == 0 {
			return nil
		}
		if err := s.ensureRunRow(ctx, caps, tx, runID, id, name, true); err != nil {
			return err
		}
		if caps.Events.RunCounterColumns {
			if err := storerunlifecycle.SyncCounts(ctx, chooseExecQueryer(s.DB, tx), runID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *PostgresStore) persistEventWithDeliveriesSpec(ctx context.Context, caps StoreSchemaCapabilities, evt events.Event, agentIDs []string) error {
	return withEventStoreRetry(ctx, nil, func() error {
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin event tx: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		if err := s.appendEventSpec(ctx, caps, tx, evt); err != nil {
			return err
		}
		if err := s.insertEventDeliveriesSpec(ctx, caps, tx, evt.ID, agentIDs); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit event tx: %w", err)
		}
		return nil
	})
}

func (s *PostgresStore) persistEventWithDeliveriesAndScopeSpec(
	ctx context.Context,
	caps StoreSchemaCapabilities,
	evt events.Event,
	agentIDs []string,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	return withEventStoreRetry(ctx, nil, func() error {
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin event tx: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		if err := s.appendEventSpec(ctx, caps, tx, evt); err != nil {
			return err
		}
		if err := s.insertEventDeliveriesSpec(ctx, caps, tx, evt.ID, agentIDs); err != nil {
			return err
		}
		if err := s.upsertCommittedReplayScopeSpec(ctx, caps, tx, evt.ID, scope); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit event tx: %w", err)
		}
		return nil
	})
}

func (s *PostgresStore) persistEventWithDeliveryRoutesAndScopeSpec(
	ctx context.Context,
	caps StoreSchemaCapabilities,
	evt events.Event,
	agentIDs []string,
	deliveryTargets map[string]events.RouteIdentity,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	return withEventStoreRetry(ctx, nil, func() error {
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin event tx: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		if err := s.appendEventSpec(ctx, caps, tx, evt); err != nil {
			return err
		}
		if err := s.insertEventDeliveriesWithTargetsSpec(ctx, caps, tx, evt.ID, agentIDs, deliveryTargets); err != nil {
			return err
		}
		if err := s.upsertCommittedReplayScopeSpec(ctx, caps, tx, evt.ID, scope); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit event tx: %w", err)
		}
		return nil
	})
}

func (s *PostgresStore) persistEventWithDeliveryRouteSetAndScopeSpec(
	ctx context.Context,
	caps StoreSchemaCapabilities,
	evt events.Event,
	deliveryRoutes []events.DeliveryRoute,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	return withEventStoreRetry(ctx, nil, func() error {
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin event tx: %w", err)
		}
		defer func() { _ = tx.Rollback() }()
		if err := s.appendEventSpec(ctx, caps, tx, evt); err != nil {
			return err
		}
		if err := s.insertEventDeliveryRoutesSpec(ctx, caps, tx, evt.ID, deliveryRoutes); err != nil {
			return err
		}
		if err := s.upsertCommittedReplayScopeSpec(ctx, caps, tx, evt.ID, scope); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit event tx: %w", err)
		}
		return nil
	})
}

func withEventStoreRetry(ctx context.Context, tx *sql.Tx, fn func() error) error {
	if fn == nil {
		return nil
	}
	attempts := 1
	if tx == nil {
		attempts = 3
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
		lastErr = fn()
		if !isTransientEventStoreConnectionError(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

func isTransientEventStoreConnectionError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, sql.ErrConnDone) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "bad connection")
}

func (s *PostgresStore) insertEventDeliveriesSpec(ctx context.Context, caps StoreSchemaCapabilities, tx *sql.Tx, eventID string, agentIDs []string) error {
	return s.insertEventDeliveriesWithTargetsSpec(ctx, caps, tx, eventID, agentIDs, nil)
}

func (s *PostgresStore) InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	switch caps.Events.Deliveries {
	case SchemaFlavorCanonical:
		return s.insertEventDeliveriesWithTargetsSpec(ctx, caps, nil, eventID, agentIDs, deliveryTargets)
	default:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
}

func (s *PostgresStore) InsertEventDeliveriesWithTargetsTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	switch caps.Events.Deliveries {
	case SchemaFlavorCanonical:
		return s.insertEventDeliveriesWithTargetsSpec(ctx, caps, tx, eventID, agentIDs, deliveryTargets)
	default:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
}

func (s *PostgresStore) InsertEventDeliveryRoutes(ctx context.Context, eventID string, deliveryRoutes []events.DeliveryRoute) error {
	return s.InsertEventDeliveryRoutesTx(ctx, nil, eventID, deliveryRoutes)
}

func (s *PostgresStore) InsertEventDeliveryRoutesTx(ctx context.Context, tx *sql.Tx, eventID string, deliveryRoutes []events.DeliveryRoute) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	switch caps.Events.Deliveries {
	case SchemaFlavorCanonical:
		return s.insertEventDeliveryRoutesSpec(ctx, caps, tx, eventID, deliveryRoutes)
	default:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
}

func (s *PostgresStore) insertEventDeliveryRoutesSpec(ctx context.Context, caps StoreSchemaCapabilities, tx *sql.Tx, eventID string, deliveryRoutes []events.DeliveryRoute) error {
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if len(deliveryRoutes) == 0 {
		return nil
	}
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	withTarget := caps.Events.DeliveryTargetRoute
	q := `
		INSERT INTO event_deliveries (event_id, subscriber_type, subscriber_id, reason_code, created_at)
		VALUES ($1::uuid, $2, $3, $4, now())
		ON CONFLICT DO NOTHING
	`
	if withTarget {
		q = `
			INSERT INTO event_deliveries (event_id, subscriber_type, subscriber_id, reason_code, delivery_target_route, created_at)
			VALUES ($1::uuid, $2, $3, $4, $5::jsonb, now())
			ON CONFLICT DO NOTHING
		`
	}
	if caps.Events.DeliveryRunID {
		q = `
			INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, reason_code, created_at)
			SELECT e.run_id, e.event_id, $2, $3, $4, now()
			FROM events e
			WHERE e.event_id = $1::uuid
			ON CONFLICT DO NOTHING
		`
		if withTarget {
			q = `
				INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, reason_code, delivery_target_route, created_at)
				SELECT e.run_id, e.event_id, $2, $3, $4, $5::jsonb, now()
				FROM events e
				WHERE e.event_id = $1::uuid
				ON CONFLICT DO NOTHING
			`
		}
	}
	for _, route := range deliveryRoutes {
		route = route.Normalized()
		if route.SubscriberType == "" || route.SubscriberID == "" {
			continue
		}
		args := []any{eventID, route.SubscriberType, route.SubscriberID, deliveryRouteReasonCode(route)}
		if withTarget {
			args = append(args, string(routeIdentityJSON(route.Target)))
		} else if !route.Target.Empty() {
			return fmt.Errorf("event_deliveries.delivery_target_route required for target-routed delivery")
		}
		if _, err := execFn(ctx, q, args...); err != nil {
			return fmt.Errorf("insert event delivery (%s=%s): %w", route.SubscriberType, route.SubscriberID, err)
		}
	}
	return nil
}

func deliveryRouteReasonCode(route events.DeliveryRoute) string {
	switch strings.TrimSpace(route.SubscriberType) {
	case "agent":
		return "matched_agent_subscription"
	case "node":
		return "matched_node_subscription"
	default:
		return "matched_subscription"
	}
}

func (s *PostgresStore) insertEventDeliveriesWithTargetsSpec(ctx context.Context, caps StoreSchemaCapabilities, tx *sql.Tx, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	q := `
		INSERT INTO event_deliveries (event_id, subscriber_type, subscriber_id, reason_code, created_at)
		VALUES ($1::uuid, 'agent', $2, 'matched_agent_subscription', now())
		ON CONFLICT DO NOTHING
	`
	withTarget := caps.Events.DeliveryTargetRoute
	if withTarget {
		q = `
			INSERT INTO event_deliveries (event_id, subscriber_type, subscriber_id, reason_code, delivery_target_route, created_at)
			VALUES ($1::uuid, 'agent', $2, 'matched_agent_subscription', $3::jsonb, now())
			ON CONFLICT DO NOTHING
		`
	}
	if caps.Events.DeliveryRunID {
		q = `
			INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, reason_code, created_at)
			SELECT e.run_id, e.event_id, 'agent', $2, 'matched_agent_subscription', now()
			FROM events e
			WHERE e.event_id = $1::uuid
			ON CONFLICT DO NOTHING
		`
		if withTarget {
			q = `
				INSERT INTO event_deliveries (run_id, event_id, subscriber_type, subscriber_id, reason_code, delivery_target_route, created_at)
				SELECT e.run_id, e.event_id, 'agent', $2, 'matched_agent_subscription', $3::jsonb, now()
				FROM events e
				WHERE e.event_id = $1::uuid
				ON CONFLICT DO NOTHING
			`
		}
	}
	seen := make(map[string]struct{}, len(agentIDs))
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		if _, ok := seen[agentID]; ok {
			continue
		}
		seen[agentID] = struct{}{}
		args := []any{eventID, agentID}
		if withTarget {
			args = append(args, string(routeIdentityJSON(deliveryTargets[agentID])))
		} else if target := deliveryTargets[agentID]; !target.Empty() {
			return fmt.Errorf("event_deliveries.delivery_target_route required for target-routed delivery")
		}
		if _, err := execFn(ctx, q, args...); err != nil {
			return fmt.Errorf("insert event delivery (agent=%s): %w", agentID, err)
		}
	}
	return nil
}

func (s *PostgresStore) upsertCommittedReplayScopeSpec(
	ctx context.Context,
	caps StoreSchemaCapabilities,
	tx *sql.Tx,
	eventID string,
	scope runtimereplayclaim.CommittedReplayScope,
) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	reasonCode, err := committedReplayScopeReasonCode(scope)
	if err != nil {
		return err
	}
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	res, err := execFn(ctx, `
		UPDATE event_deliveries
		SET reason_code = $4,
		    status = 'delivered',
		    delivered_at = now()
		WHERE event_id = $1::uuid
		  AND subscriber_type = $2
		  AND subscriber_id = $3
	`, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID, reasonCode)
	if err != nil {
		return fmt.Errorf("update committed replay scope: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update committed replay scope rows: %w", err)
	}
	if rows > 0 {
		return nil
	}
	q := `
		INSERT INTO event_deliveries (
			event_id, subscriber_type, subscriber_id, status, reason_code, delivered_at, created_at
		)
		VALUES ($1::uuid, $2, $3, 'delivered', $4, now(), now())
	`
	if caps.Events.DeliveryRunID {
		q = `
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, status, reason_code, delivered_at, created_at
			)
			SELECT
				e.run_id, e.event_id, $2, $3, 'delivered', $4, now(), now()
			FROM events e
			WHERE e.event_id = $1::uuid
		`
	}
	if _, err := execFn(ctx, q, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID, reasonCode); err != nil {
		return fmt.Errorf("insert committed replay scope: %w", err)
	}
	return nil
}

func committedReplayScopeReasonCode(scope runtimereplayclaim.CommittedReplayScope) (string, error) {
	switch scope {
	case runtimereplayclaim.CommittedReplayScopeDirect:
		return replayScopeReasonDirect, nil
	case runtimereplayclaim.CommittedReplayScopeSubscribed:
		return replayScopeReasonSubscribed, nil
	default:
		return "", fmt.Errorf("committed replay scope: unsupported scope %q", strings.TrimSpace(string(scope)))
	}
}

func committedReplayScopeFromReasonCode(reasonCode string) (runtimereplayclaim.CommittedReplayScope, bool) {
	switch strings.TrimSpace(reasonCode) {
	case replayScopeReasonDirect:
		return runtimereplayclaim.CommittedReplayScopeDirect, true
	case replayScopeReasonSubscribed:
		return runtimereplayclaim.CommittedReplayScopeSubscribed, true
	default:
		return "", false
	}
}

func (s *PostgresStore) upsertPipelineReceiptSpec(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error {
	reasonCode := pipelineReceiptReasonCode(status, errText)
	sideEffects, err := marshalPipelineReceiptSideEffects(newPipelineReceiptSideEffects(status, reasonCode, errText))
	if err != nil {
		return fmt.Errorf("marshal pipeline receipt side effects: %w", err)
	}
	outcome := mapPipelineStatusToOutcome(status)
	const q = `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance,
			$2, NULLIF($3,''), $4::jsonb, now()
		FROM events e
		WHERE e.event_id = $1::uuid
		  AND NOT EXISTS (
			SELECT 1
			FROM event_deliveries d
			WHERE d.event_id = e.event_id
			  AND d.status = 'dead_letter'
			  AND d.reason_code IN ($5, $6)
		  )
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			side_effects = EXCLUDED.side_effects,
			processed_at = now()
		WHERE COALESCE(event_receipts.reason_code, '') NOT IN ($5, $6)
	`
	execFn := s.DB.ExecContext
	if tx != nil {
		execFn = tx.ExecContext
	}
	if _, err := execFn(ctx, q, eventID, outcome, reasonCode, string(sideEffects), destructivereset.QuiescenceReasonCode, runtimerunquiescence.ServeAbandonReasonCode); err != nil {
		return fmt.Errorf("upsert pipeline receipt: %w", err)
	}
	return nil
}

func (s *PostgresStore) listEventsMissingPipelineReceiptSpec(ctx context.Context, caps StoreSchemaCapabilities, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	runIDExpr := `COALESCE(e.run_id::text, '')`
	if !caps.Events.LogRunID {
		runIDExpr = `''`
	}
	routeSelect := ` '{}'::jsonb, '{}'::jsonb, '[]'::jsonb`
	if caps.Events.LogRouteIdentity {
		routeSelect = `COALESCE(e.source_route, '{}'::jsonb), COALESCE(e.target_route, '{}'::jsonb), COALESCE(e.target_set, '[]'::jsonb)`
	}
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			e.event_id::text, %s, e.event_name, COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'),
			e.payload, e.created_at, COALESCE(e.source_event_id::text, ''),
			%s
		FROM events e
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE r.event_id IS NULL
		  AND e.created_at >= $1
		ORDER BY e.created_at ASC
		LIMIT $2
	`, runIDExpr, routeSelect), since, limit)
	if err != nil {
		return nil, fmt.Errorf("list events missing pipeline receipt: %w", err)
	}
	defer rows.Close()

	out := make([]events.PersistedReplayEvent, 0, limit)
	for rows.Next() {
		var evt events.Event
		var entityID, flowInstance, scope string
		var sourceRoute, targetRoute, targetSet json.RawMessage
		if err := rows.Scan(
			&evt.ID,
			&evt.RunID,
			&evt.Type,
			&evt.SourceAgent,
			&entityID,
			&flowInstance,
			&scope,
			&evt.Payload,
			&evt.CreatedAt,
			&evt.ParentEventID,
			&sourceRoute,
			&targetRoute,
			&targetSet,
		); err != nil {
			return nil, fmt.Errorf("scan missing pipeline receipt event: %w", err)
		}
		evt = evt.WithEnvelope(eventEnvelopeFromStorage(entityID, flowInstance, scope, sourceRoute, targetRoute, targetSet))
		record := events.PersistedReplayEvent{Event: evt}
		if !caps.Events.LogRunID {
			record.ReplayError = "missing run_id schema capability"
		} else if strings.TrimSpace(evt.RunID) == "" {
			record.ReplayError = "missing canonical run_id"
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read missing pipeline receipt events: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) listEventsMissingPipelineReceiptForRunSpec(ctx context.Context, caps StoreSchemaCapabilities, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	routeSelect := ` '{}'::jsonb, '{}'::jsonb, '[]'::jsonb`
	if caps.Events.LogRouteIdentity {
		routeSelect = `COALESCE(e.source_route, '{}'::jsonb), COALESCE(e.target_route, '{}'::jsonb), COALESCE(e.target_set, '[]'::jsonb)`
	}
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			e.event_id::text, COALESCE(e.run_id::text, ''), e.event_name, COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'),
			e.payload, e.created_at, COALESCE(e.source_event_id::text, ''),
			%s
		FROM events e
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE r.event_id IS NULL
		  AND e.run_id = $1::uuid
		  AND e.created_at >= $2
		ORDER BY e.created_at ASC
		LIMIT $3
	`, routeSelect), runID, since, limit)
	if err != nil {
		return nil, fmt.Errorf("list run events missing pipeline receipt: %w", err)
	}
	defer rows.Close()

	out := make([]events.PersistedReplayEvent, 0, limit)
	for rows.Next() {
		var evt events.Event
		var entityID, flowInstance, scope string
		var sourceRoute, targetRoute, targetSet json.RawMessage
		if err := rows.Scan(
			&evt.ID,
			&evt.RunID,
			&evt.Type,
			&evt.SourceAgent,
			&entityID,
			&flowInstance,
			&scope,
			&evt.Payload,
			&evt.CreatedAt,
			&evt.ParentEventID,
			&sourceRoute,
			&targetRoute,
			&targetSet,
		); err != nil {
			return nil, fmt.Errorf("scan run missing pipeline receipt event: %w", err)
		}
		evt = evt.WithEnvelope(eventEnvelopeFromStorage(entityID, flowInstance, scope, sourceRoute, targetRoute, targetSet))
		record := events.PersistedReplayEvent{Event: evt}
		if strings.TrimSpace(evt.RunID) == "" {
			record.ReplayError = "missing canonical run_id"
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read run missing pipeline receipt events: %w", err)
	}
	return out, nil
}

func chooseRowQueryer(db *sql.DB, tx *sql.Tx) rowQueryer {
	if tx != nil {
		return tx
	}
	return db
}

func chooseExecQueryer(db *sql.DB, tx *sql.Tx) execQueryer {
	if tx != nil {
		return tx
	}
	return db
}

func lookupEventRunID(ctx context.Context, caps StoreSchemaCapabilities, q rowQueryer, eventID string) string {
	eventID = strings.TrimSpace(eventID)
	if q == nil || eventID == "" || caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogRunID {
		return ""
	}
	var runID string
	if err := q.QueryRowContext(ctx, `
		SELECT COALESCE(run_id::text, '')
		FROM events
		WHERE event_id = $1::uuid
		LIMIT 1
	`, eventID).Scan(&runID); err != nil {
		return ""
	}
	return strings.TrimSpace(runID)
}

func (s *PostgresStore) ensureRunRow(ctx context.Context, caps StoreSchemaCapabilities, tx *sql.Tx, runID, triggerEventID, triggerEventType string, reopenCompleted bool) error {
	runID = nullUUIDString(runID)
	if runID == "" || !caps.Events.HasRuns {
		return nil
	}
	opts := runLifecycleOptions(caps)
	opts.ReopenCompleted = reopenCompleted
	if fact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); ok {
		opts.BundleHash = fact.BundleHash
		opts.BundleSource = fact.BundleSource
		opts.BundleFingerprint = fact.BundleFingerprint
	} else {
		opts.BundleSource = storerunlifecycle.BundleSourceLegacy
		opts.BundleFingerprint = runtimecorrelation.BundleFingerprintFromContext(ctx)
	}
	return storerunlifecycle.EnsureActive(ctx, chooseExecQueryer(s.DB, tx), runID, triggerEventID, triggerEventType, opts)
}

func persistedBundleRunCreationValidationRequired(ctx context.Context, caps StoreSchemaCapabilities) bool {
	if !caps.Events.HasRuns || !caps.Events.RunBundleHash || !caps.Events.RunBundleSource {
		return false
	}
	fact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		return false
	}
	source, err := storerunlifecycle.CanonicalBundleSource(fact.BundleSource)
	if err != nil {
		return false
	}
	return source == storerunlifecycle.BundleSourcePersisted && strings.TrimSpace(fact.BundleHash) != ""
}

func canonicalRunTerminalStatus(raw string) (string, error) {
	return storerunlifecycle.CanonicalTerminalStatus(raw)
}

func (s *PostgresStore) LoadRunLifecycleSnapshot(ctx context.Context, runID string) (runtimebus.RunLifecycleSnapshot, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	snap, err := storerunlifecycle.LoadSnapshot(ctx, s.DB, nullUUIDString(runID), runLifecycleOptions(caps))
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	return runtimebus.RunLifecycleSnapshot{
		RunID:        snap.RunID,
		Status:       snap.Status,
		EventCount:   snap.EventCount,
		EntityCount:  snap.EntityCount,
		ErrorSummary: snap.ErrorSummary,
		StartedAt:    snap.StartedAt,
		EndedAt:      snap.EndedAt,
	}, nil
}

func (s *PostgresStore) MarkRunTerminal(ctx context.Context, runID, status, errorSummary string, endedAt time.Time) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	runID = nullUUIDString(runID)
	if runID == "" {
		return fmt.Errorf("run_id is required")
	}
	if !caps.Events.HasRuns {
		return fmt.Errorf("runs table is required")
	}
	status, err = canonicalRunTerminalStatus(status)
	if err != nil {
		return err
	}
	errorSummary = strings.TrimSpace(errorSummary)
	if status != "failed" {
		errorSummary = ""
	}
	if endedAt.IsZero() {
		endedAt = time.Now().UTC()
	}
	_, err = storerunlifecycle.MarkTerminal(ctx, s.DB, runID, status, errorSummary, endedAt, runLifecycleOptions(caps))
	return err
}

func (s *PostgresStore) ConvergeStandaloneRuntimePlatformRun(ctx context.Context, evt events.Event) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	return s.convergeStandaloneRuntimePlatformRunByEventID(ctx, s.DB, caps, strings.TrimSpace(evt.ID))
}

func runLifecycleOptions(caps StoreSchemaCapabilities) storerunlifecycle.EnsureActiveOptions {
	return storerunlifecycle.EnsureActiveOptions{
		HasStartedAtCol:         caps.Events.RunStartedAt,
		HasTriggerCols:          caps.Events.RunTriggerColumns,
		HasCounterCols:          caps.Events.RunCounterColumns,
		HasTerminalCols:         caps.Events.RunTerminalFields,
		HasBundleHashCol:        caps.Events.RunBundleHash,
		HasBundleSourceCol:      caps.Events.RunBundleSource,
		HasBundleFingerprintCol: caps.Events.RunBundleFingerprint,
	}
}

type standaloneRuntimePlatformRunRecord struct {
	RunID            string
	RunStatus        string
	EventID          string
	EventType        string
	ProducedBy       string
	ProducedByType   string
	SourceEventID    string
	TriggerEventID   string
	TriggerEventType string
}

func isStandaloneRuntimePlatformEventType(eventType string) bool {
	switch strings.TrimSpace(eventType) {
	case "platform.boot",
		"platform.recovery_failed",
		"platform.event_quarantined",
		"platform.agent_panic",
		"platform.agent_failed",
		"platform.auth_required",
		"platform.paused",
		"platform.resumed",
		"platform.dead_letter_escalation",
		"platform.budget_threshold_crossed":
		return true
	default:
		return false
	}
}

func loadStandaloneRuntimePlatformRunRecord(ctx context.Context, db storerunlifecycle.DBTX, eventID string) (standaloneRuntimePlatformRunRecord, bool, error) {
	eventID = sanitizeOptionalUUID(eventID)
	if db == nil || eventID == "" {
		return standaloneRuntimePlatformRunRecord{}, false, nil
	}
	var rec standaloneRuntimePlatformRunRecord
	err := db.QueryRowContext(ctx, `
		SELECT
			COALESCE(r.run_id::text, ''),
			COALESCE(r.status, ''),
			COALESCE(e.event_id::text, ''),
			COALESCE(e.event_name, ''),
			COALESCE(e.produced_by, ''),
			COALESCE(e.produced_by_type, ''),
			COALESCE(e.source_event_id::text, ''),
			COALESCE(r.trigger_event_id::text, ''),
			COALESCE(r.trigger_event_type, '')
		FROM events e
		INNER JOIN runs r ON r.run_id = e.run_id
		WHERE e.event_id = $1::uuid
		LIMIT 1
	`, eventID).Scan(
		&rec.RunID,
		&rec.RunStatus,
		&rec.EventID,
		&rec.EventType,
		&rec.ProducedBy,
		&rec.ProducedByType,
		&rec.SourceEventID,
		&rec.TriggerEventID,
		&rec.TriggerEventType,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return standaloneRuntimePlatformRunRecord{}, false, nil
	case err != nil:
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("load standalone runtime platform run candidate: %w", err)
	default:
		return rec, true, nil
	}
}

func isStandaloneRuntimePlatformRunRecord(rec standaloneRuntimePlatformRunRecord) bool {
	if strings.TrimSpace(rec.RunID) == "" {
		return false
	}
	if !isStandaloneRuntimePlatformEventType(rec.EventType) {
		return false
	}
	if strings.TrimSpace(rec.ProducedByType) != "platform" {
		return false
	}
	producedBy := strings.TrimSpace(rec.ProducedBy)
	if producedBy != "" && producedBy != "runtime" {
		return false
	}
	if strings.TrimSpace(rec.SourceEventID) != "" {
		return false
	}
	if strings.TrimSpace(rec.TriggerEventID) != strings.TrimSpace(rec.EventID) {
		return false
	}
	if strings.TrimSpace(rec.TriggerEventType) != strings.TrimSpace(rec.EventType) {
		return false
	}
	return true
}

func (s *PostgresStore) convergeStandaloneRuntimePlatformRunByEventID(
	ctx context.Context,
	db storerunlifecycle.DBTX,
	caps StoreSchemaCapabilities,
	eventID string,
) error {
	eventID = sanitizeOptionalUUID(eventID)
	if db == nil || eventID == "" || !caps.Events.HasRuns || caps.Events.Log != SchemaFlavorCanonical || !caps.Events.LogRunID {
		return nil
	}
	rec, found, err := loadStandaloneRuntimePlatformRunRecord(ctx, db, eventID)
	if err != nil || !found || !isStandaloneRuntimePlatformRunRecord(rec) {
		return err
	}
	switch strings.TrimSpace(rec.RunStatus) {
	case "completed":
		return nil
	case "failed", "cancelled", "forked":
		return fmt.Errorf("standalone runtime platform run %s already terminal with status %s", rec.RunID, strings.TrimSpace(rec.RunStatus))
	}
	active, err := storerunlifecycle.HasActiveDeliveries(ctx, db, rec.RunID)
	if err != nil {
		return err
	}
	if active {
		return nil
	}
	_, err = storerunlifecycle.MarkTerminal(ctx, db, rec.RunID, "completed", "", time.Now().UTC(), runLifecycleOptions(caps))
	if err != nil {
		return fmt.Errorf("converge standalone runtime platform run: %w", err)
	}
	return nil
}

func runIDOrEventID(runID, eventID string) string {
	if runID = nullUUIDString(runID); runID != "" {
		return runID
	}
	return nullUUIDString(eventID)
}

func eventStorageEnvelope(evt events.Event) (id string, runID string, eventName string, entityID string, flowInstance string, scope string, payload []byte, chainDepth int, producedBy string, producedByType string, sourceEventID string, createdAt time.Time, err error) {
	id = strings.TrimSpace(evt.ID)
	if id == "" {
		id = uuid.NewString()
	}
	runID = runIDOrEventID(evt.RunID, id)
	eventName = strings.TrimSpace(string(evt.Type))
	payload = eventPayloadForStorage(evt)
	envelope := evt.NormalizedEnvelope()
	entityID, err = validateOptionalEntityUUID(envelope.EntityID)
	if err != nil {
		return "", "", "", "", "", "", nil, 0, "", "", "", time.Time{}, err
	}
	flowInstance = envelope.FlowInstance
	scope = string(envelope.Scope)
	if scope == "" {
		scope = string(events.EventScopeGlobal)
	}
	chainDepth = evt.ChainDepth
	if chainDepth < 0 {
		chainDepth = 0
	}
	producedBy = strings.TrimSpace(evt.SourceAgent)
	producedByType = "agent"
	if producedBy == "" || producedBy == "runtime" {
		producedByType = "platform"
	}
	sourceEventID = sanitizeOptionalUUID(evt.ParentEventID)
	createdAt = evt.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	return
}

func eventPayloadForStorage(evt events.Event) []byte {
	taskID := sanitizeOptionalUUID(evt.TaskID)
	if taskID == "" {
		if len(evt.Payload) == 0 {
			return []byte("{}")
		}
		return evt.Payload
	}
	payload := map[string]any{}
	if len(evt.Payload) > 0 {
		if err := json.Unmarshal(evt.Payload, &payload); err != nil || payload == nil {
			return evt.Payload
		}
	}
	if _, exists := payload["task_id"]; !exists {
		payload["task_id"] = taskID
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return evt.Payload
	}
	return encoded
}

func eventRouteStorageEnvelope(evt events.Event) (sourceRoute, targetRoute, targetSet []byte) {
	envelope := evt.NormalizedEnvelope()
	sourceRoute = routeIdentityJSON(envelope.Source)
	targetRoute = routeIdentityJSON(envelope.Target)
	targetSet = routeIdentitySetJSON(envelope.TargetSet)
	return sourceRoute, targetRoute, targetSet
}

func eventHasRouteIdentity(evt events.Event) bool {
	envelope := evt.NormalizedEnvelope()
	return !envelope.Source.Empty() || !envelope.Target.Empty() || len(envelope.TargetSet) > 0
}

func routeIdentityJSON(route events.RouteIdentity) []byte {
	route = route.Normalized()
	if route.Empty() {
		return []byte("{}")
	}
	payload := map[string]string{}
	if route.FlowInstance != "" {
		payload["flow_instance"] = route.FlowInstance
	}
	if route.EntityID != "" {
		payload["entity_id"] = route.EntityID
	}
	if route.FlowID != "" {
		payload["flow_id"] = route.FlowID
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return []byte("{}")
	}
	return encoded
}

func routeIdentitySetJSON(routes []events.RouteIdentity) []byte {
	if len(routes) == 0 {
		return []byte("[]")
	}
	payload := make([]map[string]string, 0, len(routes))
	for _, route := range routes {
		route = route.Normalized()
		if route.Empty() {
			continue
		}
		item := map[string]string{}
		if route.FlowInstance != "" {
			item["flow_instance"] = route.FlowInstance
		}
		if route.EntityID != "" {
			item["entity_id"] = route.EntityID
		}
		if route.FlowID != "" {
			item["flow_id"] = route.FlowID
		}
		payload = append(payload, item)
	}
	if len(payload) == 0 {
		return []byte("[]")
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return []byte("[]")
	}
	return encoded
}

func eventEnvelopeFromStorage(entityID, flowInstance, scope string, sourceRouteRaw, targetRouteRaw, targetSetRaw []byte) events.EventEnvelope {
	return events.EventEnvelope{
		EntityID:     entityID,
		FlowInstance: flowInstance,
		Scope:        events.EventScope(scope),
		Source:       decodeRouteIdentityJSON(sourceRouteRaw),
		Target:       decodeRouteIdentityJSON(targetRouteRaw),
		TargetSet:    decodeRouteIdentitySetJSON(targetSetRaw),
	}.Normalized()
}

func decodeRouteIdentityJSON(raw []byte) events.RouteIdentity {
	if len(raw) == 0 {
		return events.RouteIdentity{}
	}
	var route events.RouteIdentity
	if err := json.Unmarshal(raw, &route); err != nil {
		return events.RouteIdentity{}
	}
	return route.Normalized()
}

func decodeRouteIdentitySetJSON(raw []byte) []events.RouteIdentity {
	if len(raw) == 0 {
		return nil
	}
	var routes []events.RouteIdentity
	if err := json.Unmarshal(raw, &routes); err != nil {
		return nil
	}
	out := make([]events.RouteIdentity, 0, len(routes))
	for _, route := range routes {
		if route = route.Normalized(); !route.Empty() {
			out = append(out, route)
		}
	}
	return out
}

func mapPipelineStatusToOutcome(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "error", "dead_letter":
		return "dead_letter"
	default:
		return "success"
	}
}

func pipelineReceiptReasonCode(status, errText string) string {
	status = strings.TrimSpace(strings.ToLower(status))
	if strings.TrimSpace(errText) != "" {
		return "pipeline_error"
	}
	switch status {
	case "dead_letter":
		return "pipeline_dead_letter"
	case "error":
		return "pipeline_error"
	default:
		return "pipeline_persisted"
	}
}
