package pipeline

import (
	"context"
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
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
	if n.isProcessed(ctx, eventID) {
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
			n.markProcessed(ctx, eventID)
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
	n.markProcessed(ctx, eventID)
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
		"node_id":     n.nodeID,
		"event_id":    strings.TrimSpace(evt.ID),
		"event_type":  strings.TrimSpace(string(evt.Type)),
		"last_error":  msg,
		"retry_count": maxInt(n.retryLimit, 1),
	}
	if verticalID := workflowEventEntityID(evt); verticalID != "" {
		payload["vertical_id"] = verticalID
	}
	if err := n.bus.Publish(ctx, (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("pipeline.dead_letter"),
		SourceAgent: n.nodeID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(workflowEventEntityID(evt))); err != nil {
		log.Printf("%s: emit dead letter failed event=%s err=%v", n.nodeID, strings.TrimSpace(evt.ID), err)
	}
}

func (n *systemNodeRunner) isProcessed(ctx context.Context, eventID string) bool {
	if n == nil || n.db == nil || strings.TrimSpace(eventID) == "" {
		return false
	}
	if !n.ledgerAvailable(ctx) {
		return false
	}
	var ok bool
	err := n.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM system_node_ledger
			WHERE event_id = $1::uuid AND node_id = $2
		)
	`, strings.TrimSpace(eventID), n.nodeID).Scan(&ok)
	if err != nil {
		return false
	}
	return ok
}

func (n *systemNodeRunner) markProcessed(ctx context.Context, eventID string) {
	if n == nil || n.db == nil || strings.TrimSpace(eventID) == "" {
		return
	}
	if !n.ledgerAvailable(ctx) {
		return
	}
	if _, err := n.db.ExecContext(ctx, `
		INSERT INTO system_node_ledger (event_id, node_id, processed_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, node_id) DO NOTHING
	`, strings.TrimSpace(eventID), n.nodeID); err != nil {
		log.Printf("%s: mark processed failed event=%s err=%v", n.nodeID, strings.TrimSpace(eventID), err)
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
	if err := n.db.QueryRowContext(ctx, `SELECT to_regclass('public.system_node_ledger') IS NOT NULL`).Scan(&exists); err != nil {
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
