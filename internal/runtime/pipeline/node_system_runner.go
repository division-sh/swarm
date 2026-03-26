package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"swarm/internal/events"
	runtimedeadletters "swarm/internal/runtime/deadletters"
	"github.com/google/uuid"
)

type systemNodeBus interface {
	Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event
	Publish(ctx context.Context, evt events.Event) error
}

type systemNodeRunner struct {
	nodeID string
	bus    systemNodeBus
	db     *sql.DB

	retryLimit int
	backoffFn  func(int) time.Duration

	subscriptionsFn func() []events.EventType
	handleFn        func(context.Context, events.Event) error
	overrideHandle  func(context.Context, events.Event) error
	onSubscribe     func()

	ledgerMu      sync.Mutex
	ledgerChecked bool
	ledgerEnabled bool
}

func newSystemNodeRunner(nodeID string, bus systemNodeBus, db *sql.DB, subscriptionsFn func() []events.EventType, handleFn func(context.Context, events.Event) error) *systemNodeRunner {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" || bus == nil || handleFn == nil {
		return nil
	}
	return &systemNodeRunner{
		nodeID:          nodeID,
		bus:             bus,
		db:              db,
		retryLimit:      DefaultSystemNodeRetryLimit,
		backoffFn:       defaultSystemNodeBackoff,
		subscriptionsFn: subscriptionsFn,
		handleFn:        handleFn,
	}
}

func (n *systemNodeRunner) Run(ctx context.Context) {
	if n == nil || n.bus == nil || n.handleFn == nil {
		return
	}
	ch := n.subscribe()
	if n.onSubscribe != nil {
		n.onSubscribe()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				ch = n.subscribe()
				if n.onSubscribe != nil {
					n.onSubscribe()
				}
				continue
			}
			n.ProcessEventForTest(ctx, evt)
		}
	}
}

func (n *systemNodeRunner) ProcessEventForTest(ctx context.Context, evt events.Event) {
	if n == nil {
		return
	}
	eventID := strings.TrimSpace(evt.ID)
	if eventID == "" {
		return
	}
	if n.isProcessed(ctx, evt) {
		return
	}
	var lastErr error
	retryLimit := n.retryLimit
	if retryLimit <= 0 {
		retryLimit = DefaultSystemNodeRetryLimit
	}
	backoffFn := n.backoffFn
	if backoffFn == nil {
		backoffFn = defaultSystemNodeBackoff
	}
	for attempt := 1; attempt <= retryLimit; attempt++ {
		if err := n.handle(ctx, evt); err == nil {
			n.markProcessed(ctx, evt)
			return
		} else {
			lastErr = err
		}
		if attempt >= retryLimit {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoffFn(attempt)):
		}
	}
	n.emitDeadLetter(ctx, evt, lastErr)
	n.markProcessed(ctx, evt)
}

func (n *systemNodeRunner) SetRetryPolicyForTest(limit int, backoff func(int) time.Duration) {
	if n == nil {
		return
	}
	n.retryLimit = limit
	n.backoffFn = backoff
}

func (n *systemNodeRunner) SetOverrideHandleForTest(fn func(context.Context, events.Event) error) {
	if n == nil {
		return
	}
	n.overrideHandle = fn
}

func (n *systemNodeRunner) SetOnSubscribeForTest(fn func()) {
	if n == nil {
		return
	}
	n.onSubscribe = fn
}

func (n *systemNodeRunner) subscribe() <-chan events.Event {
	if n == nil || n.bus == nil {
		return nil
	}
	subscriptions := []events.EventType(nil)
	if n.subscriptionsFn != nil {
		subscriptions = n.subscriptionsFn()
	}
	return n.bus.Subscribe(n.nodeID, subscriptions...)
}

func (n *systemNodeRunner) handle(ctx context.Context, evt events.Event) error {
	if n == nil || n.handleFn == nil {
		return nil
	}
	if n.overrideHandle != nil {
		return n.overrideHandle(ctx, evt)
	}
	return n.handleFn(ctx, evt)
}

func (n *systemNodeRunner) emitDeadLetter(ctx context.Context, evt events.Event, cause error) {
	if n == nil || n.bus == nil {
		return
	}
	msg := "unknown error"
	if cause != nil {
		msg = strings.TrimSpace(cause.Error())
		if msg == "" {
			msg = "unknown error"
		}
	}
	payload := map[string]any{
		"original_event":   strings.TrimSpace(string(evt.Type)),
		"original_payload": json.RawMessage(evt.Payload),
		"entity_id":        workflowEventEntityID(evt),
		"flow_instance":    "runtime",
		"failure_type":     "retry_exhausted",
		"error_message":    msg,
		"retry_count":      maxInt(n.retryLimit, 1),
		"chain_depth":      evt.ChainDepth,
		"handler_node":     n.nodeID,
		"timestamp":        time.Now().UTC().Format(time.RFC3339Nano),
	}
	if n.db != nil {
		_ = runtimedeadletters.Insert(ctx, n.db, runtimedeadletters.Record{
			OriginalEventID: strings.TrimSpace(evt.ID),
			OriginalEvent:   strings.TrimSpace(string(evt.Type)),
			OriginalPayload: evt.Payload,
			EntityID:        workflowEventEntityID(evt),
			FailureType:     "retry_exhausted",
			ErrorMessage:    msg,
			RetryCount:      maxInt(n.retryLimit, 1),
			ChainDepth:      evt.ChainDepth,
			HandlerNode:     n.nodeID,
		})
	}
	if err := n.bus.Publish(ctx, (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("platform.dead_letter"),
		SourceAgent: n.nodeID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(workflowEventEntityID(evt))); err != nil {
		slog.Warn("system node dead letter publish failed", "node_id", n.nodeID, "event_id", strings.TrimSpace(evt.ID), "error", err)
	}
}

func (n *systemNodeRunner) isProcessed(ctx context.Context, evt events.Event) bool {
	eventID := strings.TrimSpace(evt.ID)
	if n == nil || n.db == nil || eventID == "" {
		return false
	}
	if !n.ledgerAvailable(ctx) {
		return false
	}
	var ok bool
	if eventReceiptsAvailable(ctx, n.db) {
		err := n.db.QueryRowContext(ctx, `
			SELECT EXISTS(
				SELECT 1
				FROM event_receipts
				WHERE subscriber_type = 'node'
				  AND subscriber_id = $1
				  AND idempotency_key = $2
			)
		`, n.nodeID, systemNodeReceiptIdempotencyKey(n.nodeID, eventID)).Scan(&ok)
		if err == nil {
			return ok
		}
	}
	err := n.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM system_node_ledger
			WHERE event_id = $1::uuid AND node_id = $2
		)
	`, eventID, n.nodeID).Scan(&ok)
	return err == nil && ok
}

func (n *systemNodeRunner) markProcessed(ctx context.Context, evt events.Event) {
	eventID := strings.TrimSpace(evt.ID)
	if n == nil || n.db == nil || eventID == "" {
		return
	}
	if !n.ledgerAvailable(ctx) {
		return
	}
	if eventReceiptsAvailable(ctx, n.db) {
		sideEffects, _ := json.Marshal(map[string]any{
			"idempotency_key": systemNodeReceiptIdempotencyKey(n.nodeID, eventID),
		})
		_, err := n.db.ExecContext(ctx, `
			INSERT INTO event_receipts (
				event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
				outcome, side_effects, idempotency_key, processed_at
			)
			SELECT
				e.event_id, 'node', $2, e.entity_id, e.flow_instance,
				'no_op', $3::jsonb, $4, now()
			FROM events e
			WHERE e.event_id = $1::uuid
			ON CONFLICT (event_id, subscriber_id) DO NOTHING
		`, eventID, n.nodeID, string(sideEffects), systemNodeReceiptIdempotencyKey(n.nodeID, eventID))
		if err == nil {
			return
		}
	}
	if _, err := n.db.ExecContext(ctx, `
		INSERT INTO system_node_ledger (event_id, node_id, processed_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, node_id) DO NOTHING
	`, eventID, n.nodeID); err != nil {
		slog.Warn("system node mark processed failed", "node_id", n.nodeID, "event_id", eventID, "error", err)
	}
}

func (n *systemNodeRunner) ledgerAvailable(ctx context.Context) bool {
	if n == nil || n.db == nil {
		return false
	}
	n.ledgerMu.Lock()
	defer n.ledgerMu.Unlock()
	if n.ledgerChecked {
		return n.ledgerEnabled
	}
	var exists bool
	if err := n.db.QueryRowContext(ctx, `
		SELECT (
			to_regclass('public.event_receipts') IS NOT NULL
			OR to_regclass('public.system_node_ledger') IS NOT NULL
		)
	`).Scan(&exists); err != nil {
		exists = false
	}
	n.ledgerChecked = true
	n.ledgerEnabled = exists
	return n.ledgerEnabled
}

func (n *systemNodeRunner) String() string {
	if n == nil {
		return ""
	}
	return n.nodeID
}

func defaultSystemNodeBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(attempt*attempt) * 50 * time.Millisecond
	if d > 2*time.Second {
		return 2 * time.Second
	}
	return d
}

func eventReceiptsAvailable(ctx context.Context, db *sql.DB) bool {
	if db == nil {
		return false
	}
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'event_receipts'
			  AND column_name = 'subscriber_id'
		)
	`).Scan(&exists); err != nil {
		return false
	}
	return exists
}

func systemNodeReceiptIdempotencyKey(nodeID, eventID string) string {
	return strings.TrimSpace(nodeID) + ":" + strings.TrimSpace(eventID)
}
