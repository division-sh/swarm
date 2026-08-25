package operatorsurface

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/google/uuid"
)

const pendingAgentDeliveryCursorKind = "agent.diagnose.queue"

type pendingAgentDeliveryCursor struct {
	Kind       string `json:"kind"`
	EnqueuedAt string `json:"enqueued_at"`
	EventID    string `json:"event_id"`
	DeliveryID string `json:"delivery_id"`
}

func (s *AgentPostgres) ListPendingAgentDeliveryFacts(ctx context.Context, identities []agentidentity.Identity, since time.Time) (map[agentidentity.Identity]operatorread.PendingAgentDeliveryFacts, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	normalized, err := normalizePendingAgentIdentities(identities)
	if err != nil {
		return nil, err
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	asOf, err := operatorPostgresDelivery.CaptureSnapshotTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	aggregates, err := operatorPostgresDelivery.AgentPendingAggregates(ctx, tx, normalized, since, asOf)
	if err != nil {
		return nil, err
	}
	result := pendingAgentDeliveryFactsFromAggregates(normalized, aggregates, asOf)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit postgres pending agent facts snapshot: %w", err)
	}
	return result, nil
}

func (s *AgentPostgres) ListPendingAgentDeliveryDetails(ctx context.Context, opts operatorread.PendingAgentDeliveryListOptions) (operatorread.PendingAgentDeliveryPage, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	defer tx.Rollback()
	asOf, err := postgresAgentSnapshotTime(ctx, tx)
	if err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	result, err := s.listPendingAgentDeliveryDetailsTx(ctx, tx, opts, asOf)
	if err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	if err := tx.Commit(); err != nil {
		return operatorread.PendingAgentDeliveryPage{}, fmt.Errorf("commit postgres pending agent delivery snapshot: %w", err)
	}
	return result, nil
}

func (s *AgentPostgres) listPendingAgentDeliveryDetailsTx(ctx context.Context, tx *sql.Tx, opts operatorread.PendingAgentDeliveryListOptions, asOf time.Time) (operatorread.PendingAgentDeliveryPage, error) {
	if tx == nil {
		return operatorread.PendingAgentDeliveryPage{}, fmt.Errorf("postgres pending agent delivery transaction is required")
	}
	opts, cursor, empty, err := normalizePendingAgentDeliveryOptions(opts)
	if err != nil || empty {
		return operatorread.PendingAgentDeliveryPage{PendingDeliveries: []operatorread.PendingAgentDeliveryDetail{}}, err
	}
	aggregates, err := operatorPostgresDelivery.AgentPendingAggregates(ctx, tx, []agentidentity.Identity{opts.AgentIdentity}, opts.Since, asOf)
	if err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	page, err := operatorPostgresDelivery.AgentPendingReferencePage(ctx, tx, runtimedelivery.AgentPendingPageQuery{
		AgentIdentity: opts.AgentIdentity,
		Since:         opts.Since,
		Limit:         opts.Limit,
		After:         cursor,
	}, asOf)
	if err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	return pendingAgentDeliveryPageFromProjection(ctx, opts.AgentIdentity, aggregates, page, asOf, func(ctx context.Context, eventID string) (eventrecord.Record, bool, error) {
		return eventrecordpostgres.Load(ctx, tx, eventID)
	})
}

func (s *AgentSQLite) ListPendingAgentDeliveryFacts(ctx context.Context, identities []agentidentity.Identity, since time.Time) (map[agentidentity.Identity]operatorread.PendingAgentDeliveryFacts, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return nil, err
	}
	normalized, err := normalizePendingAgentIdentities(identities)
	if err != nil {
		return nil, err
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	asOf, err := operatorSQLiteDelivery.CaptureSnapshotTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	aggregates, err := operatorSQLiteDelivery.AgentPendingAggregates(ctx, tx, normalized, since, asOf)
	if err != nil {
		return nil, err
	}
	result := pendingAgentDeliveryFactsFromAggregates(normalized, aggregates, asOf)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit sqlite pending agent facts snapshot: %w", err)
	}
	return result, nil
}

func (s *AgentSQLite) ListPendingAgentDeliveryDetails(ctx context.Context, opts operatorread.PendingAgentDeliveryListOptions) (operatorread.PendingAgentDeliveryPage, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	defer tx.Rollback()
	asOf, err := operatorSQLiteDelivery.CaptureSnapshotTime(ctx, tx)
	if err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	result, err := s.listPendingAgentDeliveryDetailsTx(ctx, tx, opts, asOf)
	if err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	if err := tx.Commit(); err != nil {
		return operatorread.PendingAgentDeliveryPage{}, fmt.Errorf("commit sqlite pending agent delivery snapshot: %w", err)
	}
	return result, nil
}

func (s *AgentSQLite) listPendingAgentDeliveryDetailsTx(ctx context.Context, tx *sql.Tx, opts operatorread.PendingAgentDeliveryListOptions, asOf time.Time) (operatorread.PendingAgentDeliveryPage, error) {
	if tx == nil {
		return operatorread.PendingAgentDeliveryPage{}, fmt.Errorf("sqlite pending agent delivery transaction is required")
	}
	opts, cursor, empty, err := normalizePendingAgentDeliveryOptions(opts)
	if err != nil || empty {
		return operatorread.PendingAgentDeliveryPage{PendingDeliveries: []operatorread.PendingAgentDeliveryDetail{}}, err
	}
	aggregates, err := operatorSQLiteDelivery.AgentPendingAggregates(ctx, tx, []agentidentity.Identity{opts.AgentIdentity}, opts.Since, asOf)
	if err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	page, err := operatorSQLiteDelivery.AgentPendingReferencePage(ctx, tx, runtimedelivery.AgentPendingPageQuery{
		AgentIdentity: opts.AgentIdentity, Since: opts.Since, Limit: opts.Limit, After: cursor,
	}, asOf)
	if err != nil {
		return operatorread.PendingAgentDeliveryPage{}, err
	}
	return pendingAgentDeliveryPageFromProjection(ctx, opts.AgentIdentity, aggregates, page, asOf, func(ctx context.Context, eventID string) (eventrecord.Record, bool, error) {
		return eventrecordsqlite.Load(ctx, tx, eventID)
	})
}

func normalizePendingAgentIdentities(identities []agentidentity.Identity) ([]agentidentity.Identity, error) {
	seen := make(map[agentidentity.Identity]struct{}, len(identities))
	out := make([]agentidentity.Identity, 0, len(identities))
	for _, identity := range identities {
		identity = identity.Normalize()
		if err := identity.Validate(); err != nil {
			return nil, fmt.Errorf("pending agent identity: %w", err)
		}
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		out = append(out, identity)
	}
	return out, nil
}

func normalizePendingAgentDeliveryOptions(opts operatorread.PendingAgentDeliveryListOptions) (operatorread.PendingAgentDeliveryListOptions, *runtimedelivery.AgentPendingPosition, bool, error) {
	opts.AgentIdentity = opts.AgentIdentity.Normalize()
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.AgentIdentity.IsZero() {
		return opts, nil, true, nil
	}
	if err := opts.AgentIdentity.Validate(); err != nil {
		return opts, nil, false, fmt.Errorf("pending agent delivery identity: %w", err)
	}
	if opts.Limit == 0 {
		opts.Limit = operatorread.DefaultPendingAgentDeliveryDetailLimit
	}
	if opts.Limit < 0 || opts.Limit > operatorread.MaxPendingAgentDeliveryDetailLimit {
		return opts, nil, false, fmt.Errorf("pending agent delivery detail limit must be from 1 to %d", operatorread.MaxPendingAgentDeliveryDetailLimit)
	}
	if opts.Cursor == "" {
		return opts, nil, false, nil
	}
	cursor, err := decodePendingAgentDeliveryCursor(opts.Cursor)
	if err != nil {
		return opts, nil, false, err
	}
	return opts, &cursor, false, nil
}

func pendingAgentDeliveryFactsFromAggregates(identities []agentidentity.Identity, aggregates []runtimedelivery.AgentPendingAggregate, now time.Time) map[agentidentity.Identity]operatorread.PendingAgentDeliveryFacts {
	out := make(map[agentidentity.Identity]operatorread.PendingAgentDeliveryFacts, len(identities))
	for _, identity := range identities {
		out[identity] = operatorread.PendingAgentDeliveryFacts{}
	}
	for _, aggregate := range aggregates {
		age := int(now.Sub(aggregate.OldestEventAt).Seconds())
		if age < 0 {
			age = 0
		}
		out[aggregate.AgentIdentity] = operatorread.PendingAgentDeliveryFacts{
			PendingCount:        aggregate.Count,
			OldestPendingAgeSec: age,
		}
	}
	return out
}

func pendingAgentDeliveryPageFromProjection(
	ctx context.Context,
	identity agentidentity.Identity,
	aggregates []runtimedelivery.AgentPendingAggregate,
	page runtimedelivery.AgentPendingReferencePage,
	now time.Time,
	load func(context.Context, string) (eventrecord.Record, bool, error),
) (operatorread.PendingAgentDeliveryPage, error) {
	facts := pendingAgentDeliveryFactsFromAggregates([]agentidentity.Identity{identity}, aggregates, now)[identity]
	out := operatorread.PendingAgentDeliveryPage{
		PendingCount:        facts.PendingCount,
		OldestPendingAgeSec: facts.OldestPendingAgeSec,
		PendingDeliveries:   make([]operatorread.PendingAgentDeliveryDetail, 0, len(page.References)),
	}
	for _, reference := range page.References {
		durable, found, err := load(ctx, reference.Snapshot.EventID)
		if err != nil || !found {
			if err == nil {
				err = eventrecord.Missing(reference.Snapshot.EventID)
			}
			return operatorread.PendingAgentDeliveryPage{}, err
		}
		admitted, err := durable.Decode()
		if err != nil {
			return operatorread.PendingAgentDeliveryPage{}, err
		}
		event := admitted.Event()
		if event.ID() != reference.Snapshot.EventID || !event.CreatedAt().UTC().Equal(reference.EventCreatedAt.UTC()) {
			return operatorread.PendingAgentDeliveryPage{}, fmt.Errorf("pending agent delivery event %s changed during hydration", reference.Snapshot.EventID)
		}
		delivery, err := events.NewDeliveryEvent(event, reference.Snapshot.Route)
		if err != nil {
			return operatorread.PendingAgentDeliveryPage{}, err
		}
		detail, err := pendingAgentDeliveryDetailFromReference(reference, delivery.Event())
		if err != nil {
			return operatorread.PendingAgentDeliveryPage{}, err
		}
		out.PendingDeliveries = append(out.PendingDeliveries, detail)
	}
	if page.HasMore && len(out.PendingDeliveries) > 0 {
		out.NextCursor = encodePendingAgentDeliveryCursor(out.PendingDeliveries[len(out.PendingDeliveries)-1])
	}
	return out, nil
}

func pendingAgentDeliveryDetailFromReference(reference runtimedelivery.AgentPendingReference, event events.Event) (operatorread.PendingAgentDeliveryDetail, error) {
	detail := operatorread.PendingAgentDeliveryDetail{
		DeliveryID: strings.TrimSpace(reference.Snapshot.DeliveryID),
		EventID:    strings.TrimSpace(event.ID()),
		EventName:  strings.TrimSpace(string(event.Type())),
		EnqueuedAt: reference.EventCreatedAt.UTC(),
		Attempts:   reference.Snapshot.RetryCount,
		Event:      event,
	}
	if detail.DeliveryID == "" {
		return operatorread.PendingAgentDeliveryDetail{}, fmt.Errorf("pending agent delivery detail delivery_id is required")
	}
	if detail.EventID == "" {
		return operatorread.PendingAgentDeliveryDetail{}, fmt.Errorf("pending agent delivery detail event_id is required")
	}
	if detail.EventName == "" {
		return operatorread.PendingAgentDeliveryDetail{}, fmt.Errorf("pending agent delivery detail event_name is required")
	}
	if detail.EnqueuedAt.IsZero() {
		return operatorread.PendingAgentDeliveryDetail{}, fmt.Errorf("pending agent delivery detail enqueued_at is required")
	}
	if detail.Attempts < 0 {
		return operatorread.PendingAgentDeliveryDetail{}, fmt.Errorf("pending agent delivery detail attempts must be non-negative")
	}
	return detail, nil
}

func encodePendingAgentDeliveryCursor(detail operatorread.PendingAgentDeliveryDetail) string {
	raw, _ := json.Marshal(pendingAgentDeliveryCursor{
		Kind:       pendingAgentDeliveryCursorKind,
		EnqueuedAt: detail.EnqueuedAt.UTC().Format(time.RFC3339Nano),
		EventID:    strings.TrimSpace(detail.EventID),
		DeliveryID: strings.TrimSpace(detail.DeliveryID),
	})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodePendingAgentDeliveryCursor(raw string) (runtimedelivery.AgentPendingPosition, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return runtimedelivery.AgentPendingPosition{}, operatorread.ErrInvalidPendingAgentDeliveryCursor
	}
	var cursor pendingAgentDeliveryCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return runtimedelivery.AgentPendingPosition{}, operatorread.ErrInvalidPendingAgentDeliveryCursor
	}
	if strings.TrimSpace(cursor.Kind) != pendingAgentDeliveryCursorKind ||
		strings.TrimSpace(cursor.EventID) == "" ||
		strings.TrimSpace(cursor.DeliveryID) == "" ||
		strings.TrimSpace(cursor.EnqueuedAt) == "" {
		return runtimedelivery.AgentPendingPosition{}, operatorread.ErrInvalidPendingAgentDeliveryCursor
	}
	enqueuedAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(cursor.EnqueuedAt))
	if err != nil {
		return runtimedelivery.AgentPendingPosition{}, operatorread.ErrInvalidPendingAgentDeliveryCursor
	}
	if _, err := uuid.Parse(strings.TrimSpace(cursor.EventID)); err != nil {
		return runtimedelivery.AgentPendingPosition{}, operatorread.ErrInvalidPendingAgentDeliveryCursor
	}
	if _, err := uuid.Parse(strings.TrimSpace(cursor.DeliveryID)); err != nil {
		return runtimedelivery.AgentPendingPosition{}, operatorread.ErrInvalidPendingAgentDeliveryCursor
	}
	return runtimedelivery.AgentPendingPosition{
		EventCreatedAt: enqueuedAt.UTC(),
		EventID:        strings.TrimSpace(cursor.EventID),
		DeliveryID:     strings.TrimSpace(cursor.DeliveryID),
	}, nil
}
