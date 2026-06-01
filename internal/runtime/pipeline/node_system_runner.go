package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimerterr "github.com/division-sh/swarm/internal/runtime/rterrors"
	"github.com/google/uuid"
)

type systemNodeBus interface {
	Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event
	Publish(ctx context.Context, evt events.Event) error
}

type systemNodeRuntimeLogger interface {
	LogRuntime(context.Context, RuntimeLogEntry) error
}

type systemNodeNormalRunCompletionConverger interface {
	ConvergeNormalRunCompletionForEvent(context.Context, string) error
}

type systemNodeRunner struct {
	nodeID       string
	bus          systemNodeBus
	db           *sql.DB
	receiptStore SystemNodeReceiptPersistence

	retryLimit int
	backoffFn  func(int) time.Duration

	subscriptionsFn         func() []events.EventType
	handleFn                func(context.Context, events.Event) error
	overrideHandle          func(context.Context, events.Event) error
	onSubscribe             func()
	eventReceiptsCapability func(context.Context) (bool, error)

	receiptsMu      sync.Mutex
	receiptsChecked bool
	receiptsEnabled bool
}

func newSystemNodeRunner(nodeID string, bus systemNodeBus, db *sql.DB, subscriptionsFn func() []events.EventType, handleFn func(context.Context, events.Event) error, eventReceiptsCapability ...func(context.Context) (bool, error)) *systemNodeRunner {
	return newSystemNodeRunnerWithRetryBase(nodeID, bus, db, subscriptionsFn, handleFn, 0, eventReceiptsCapability...)
}

func newSystemNodeRunnerWithRetryBase(nodeID string, bus systemNodeBus, db *sql.DB, subscriptionsFn func() []events.EventType, handleFn func(context.Context, events.Event) error, retryBase time.Duration, eventReceiptsCapability ...func(context.Context) (bool, error)) *systemNodeRunner {
	return newSystemNodeRunnerWithReceiptStoreAndRetryBase(nodeID, bus, db, NewWorkflowInstanceStore(db), subscriptionsFn, handleFn, retryBase, eventReceiptsCapability...)
}

func newSystemNodeRunnerWithReceiptStoreAndRetryBase(nodeID string, bus systemNodeBus, db *sql.DB, receiptStore SystemNodeReceiptPersistence, subscriptionsFn func() []events.EventType, handleFn func(context.Context, events.Event) error, retryBase time.Duration, eventReceiptsCapability ...func(context.Context) (bool, error)) *systemNodeRunner {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" || bus == nil || handleFn == nil {
		return nil
	}
	if retryBase <= 0 {
		retryBase = time.Second
	}
	var receiptsCapability func(context.Context) (bool, error)
	if len(eventReceiptsCapability) > 0 {
		receiptsCapability = eventReceiptsCapability[0]
	}
	return &systemNodeRunner{
		nodeID:                  nodeID,
		bus:                     bus,
		db:                      db,
		receiptStore:            receiptStore,
		retryLimit:              DefaultSystemNodeRetryLimit,
		backoffFn:               func(attempt int) time.Duration { return defaultSystemNodeBackoff(retryBase, attempt) },
		subscriptionsFn:         subscriptionsFn,
		handleFn:                handleFn,
		eventReceiptsCapability: receiptsCapability,
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
	if n.isActiveRunQuiesced(ctx, evt) {
		return
	}
	if n.isProcessed(ctx, evt) {
		return
	}
	retryLimit := n.retryLimit
	if retryLimit <= 0 {
		retryLimit = DefaultSystemNodeRetryLimit
	}
	var lastErr error
	failureType := "retry_exhausted"
	retryCount := maxInt(retryLimit, 1)
	backoffFn := n.backoffFn
	if backoffFn == nil {
		backoffFn = func(attempt int) time.Duration { return defaultSystemNodeBackoff(time.Second, attempt) }
	}
	for attempt := 1; attempt <= retryLimit; attempt++ {
		if err := n.handle(ctx, evt); err == nil {
			if !n.isActiveRunQuiesced(ctx, evt) {
				n.markProcessed(ctx, evt)
			}
			return
		} else {
			lastErr = err
			if isNonRetryableHandlerError(err) {
				failureType = "handler_error"
				retryCount = 0
				break
			}
			retryCount = attempt
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
	if n.isActiveRunQuiesced(ctx, evt) {
		return
	}
	n.emitDeadLetter(ctx, evt, lastErr, failureType, retryCount)
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
	if internalBus, ok := any(n.bus).(interface {
		SubscribeInternal(string, ...events.EventType) <-chan events.Event
	}); ok {
		return internalBus.SubscribeInternal(n.nodeID, subscriptions...)
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

func (n *systemNodeRunner) emitDeadLetter(ctx context.Context, evt events.Event, cause error, failureType string, retryCount int) {
	if n == nil || n.bus == nil {
		return
	}
	failureType = strings.TrimSpace(failureType)
	if failureType == "" {
		failureType = "retry_exhausted"
	}
	if retryCount < 0 {
		retryCount = 0
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
		"failure_type":     failureType,
		"error_message":    msg,
		"retry_count":      retryCount,
		"chain_depth":      evt.ChainDepth,
		"handler_node":     n.nodeID,
		"timestamp":        time.Now().UTC().Format(time.RFC3339Nano),
	}
	if n.db != nil {
		if err := runtimedeadletters.Insert(ctx, n.db, runtimedeadletters.Record{
			OriginalEventID: strings.TrimSpace(evt.ID),
			OriginalEvent:   strings.TrimSpace(string(evt.Type)),
			OriginalPayload: evt.Payload,
			EntityID:        workflowEventEntityID(evt),
			FailureType:     failureType,
			ErrorMessage:    msg,
			RetryCount:      retryCount,
			ChainDepth:      evt.ChainDepth,
			HandlerNode:     n.nodeID,
		}); err != nil {
			slog.Error("system node dead letter persist failed", "node_id", n.nodeID, "event_id", strings.TrimSpace(evt.ID), "error", err)
			if logger, ok := n.bus.(systemNodeRuntimeLogger); ok && logger != nil {
				logger.LogRuntime(ctx, RuntimeLogEntry{
					Level:     "error",
					Message:   "Persisting the system node dead letter failed",
					Component: n.nodeID,
					Action:    "dead_letter_persist_failed",
					EventID:   strings.TrimSpace(evt.ID),
					EventType: strings.TrimSpace(string(evt.Type)),
					EntityID:  workflowEventEntityID(evt),
					Error:     strings.TrimSpace(err.Error()),
				})
			}
		}
	}
	if err := n.bus.Publish(ctx, (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("platform.dead_letter"),
		SourceAgent: n.nodeID,
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now().UTC(),
	}).WithEntityID(workflowEventEntityID(evt))); err != nil {
		slog.Error("system node dead letter publish failed", "node_id", n.nodeID, "event_id", strings.TrimSpace(evt.ID), "error", err)
		if logger, ok := n.bus.(systemNodeRuntimeLogger); ok && logger != nil {
			logger.LogRuntime(ctx, RuntimeLogEntry{
				Level:     "error",
				Message:   "Publishing the system node dead letter failed",
				Component: n.nodeID,
				Action:    "dead_letter_publish_failed",
				EventID:   strings.TrimSpace(evt.ID),
				EventType: strings.TrimSpace(string(evt.Type)),
				EntityID:  workflowEventEntityID(evt),
				Error:     strings.TrimSpace(err.Error()),
			})
		}
	}
}

func isNonRetryableHandlerError(err error) bool {
	if err == nil {
		return false
	}
	runtimeErr, ok := runtimerterr.AsRuntimeError(err)
	return ok && !runtimeErr.Retryable
}

func (n *systemNodeRunner) isProcessed(ctx context.Context, evt events.Event) bool {
	eventID := strings.TrimSpace(evt.ID)
	if n == nil || n.receiptStore == nil || eventID == "" {
		return false
	}
	if !n.eventReceiptsAvailable(ctx) {
		return false
	}
	ok, err := n.receiptStore.SystemNodeProcessed(ctx, n.nodeID, eventID)
	return err == nil && ok
}

func (n *systemNodeRunner) markProcessed(ctx context.Context, evt events.Event) {
	eventID := strings.TrimSpace(evt.ID)
	if n == nil || n.receiptStore == nil || eventID == "" {
		return
	}
	if n.isActiveRunQuiesced(ctx, evt) {
		return
	}
	if !n.eventReceiptsAvailable(ctx) {
		return
	}
	sideEffects := systemNodeProcessedReceiptSideEffects(n.nodeID, eventID)
	if err := n.persistProcessedReceiptAndSettleDelivery(ctx, eventID, sideEffects); err != nil {
		slog.Error("system node mark processed failed", "node_id", n.nodeID, "event_id", eventID, "error", err)
		if logger, ok := n.bus.(systemNodeRuntimeLogger); ok && logger != nil {
			logger.LogRuntime(ctx, RuntimeLogEntry{
				Level:     "error",
				Message:   "Marking the system node event as processed failed",
				Component: n.nodeID,
				Action:    "mark_processed_failed",
				EventID:   eventID,
				EventType: strings.TrimSpace(string(evt.Type)),
				EntityID:  workflowEventEntityID(evt),
				Error:     strings.TrimSpace(err.Error()),
			})
		}
		return
	}
	n.convergeNormalRunCompletion(ctx, evt)
}

func (n *systemNodeRunner) persistProcessedReceiptAndSettleDelivery(ctx context.Context, eventID, sideEffects string) error {
	if n == nil || n.receiptStore == nil {
		return nil
	}
	return n.receiptStore.MarkSystemNodeProcessedAndSettleDelivery(ctx, n.nodeID, eventID, sideEffects)
}

func systemNodeProcessedReceiptSideEffects(nodeID, eventID string) string {
	sideEffects, _ := json.Marshal(map[string]any{
		"idempotency_key": SystemNodeReceiptIdempotencyKey(nodeID, eventID),
	})
	return string(sideEffects)
}

func persistSystemNodeProcessedReceiptAndSettleDelivery(ctx context.Context, db *sql.DB, nodeID, eventID, sideEffects string) error {
	if db == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if nodeID == "" || eventID == "" {
		return nil
	}
	if tx, ok := PipelineSQLTxFromContext(ctx); ok {
		return persistSystemNodeProcessedReceiptAndSettleDeliveryTx(ctx, tx, nodeID, eventID, sideEffects)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin system node processed receipt tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if err := persistSystemNodeProcessedReceiptAndSettleDeliveryTx(ctx, tx, nodeID, eventID, sideEffects); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit system node processed receipt tx: %w", err)
	}
	committed = true
	return nil
}

func persistSystemNodeProcessedReceiptAndSettleDeliveryTx(ctx context.Context, tx *sql.Tx, nodeID, eventID, sideEffects string) error {
	if tx == nil {
		return nil
	}
	res, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, idempotency_key, processed_at
		)
		SELECT
			e.event_id, 'node', $2, e.entity_id, e.flow_instance,
			'no_op', 'idempotent_no_op', $3::jsonb, $4, now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			side_effects = EXCLUDED.side_effects,
			idempotency_key = EXCLUDED.idempotency_key,
			processed_at = now()
	`, eventID, nodeID, sideEffects, SystemNodeReceiptIdempotencyKey(nodeID, eventID))
	if err != nil {
		return fmt.Errorf("upsert system node receipt: %w", err)
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("upsert system node receipt: event %s not found", eventID)
	}
	if _, err := tx.ExecContext(ctx, `
		WITH settled AS (
			UPDATE event_deliveries
			SET
				status = 'delivered',
				retry_count = COALESCE(retry_count, 0),
				reason_code = 'node_processed',
				last_error = NULL,
				active_session_id = NULL,
				started_at = COALESCE(started_at, created_at),
				delivered_at = now()
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $2
			  AND (
				status IN ('pending', 'in_progress')
				OR (status = 'failed' AND COALESCE(retry_count, 0) < 2)
			  )
			RETURNING delivery_id
		)
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count,
			reason_code, last_error, active_session_id, started_at, delivered_at, created_at
		)
		SELECT
			e.run_id, e.event_id, 'node', $2, 'delivered', 0,
			'node_processed', NULL, NULL, now(), now(), now()
		FROM events e
		WHERE e.event_id = $1::uuid
		  AND NOT EXISTS (
			SELECT 1
			FROM settled
		  )
		  AND NOT EXISTS (
			SELECT 1
			FROM event_deliveries d
			WHERE d.event_id = $1::uuid
			  AND d.subscriber_type = 'node'
			  AND d.subscriber_id = $2
		  )
	`, eventID, nodeID); err != nil {
		return fmt.Errorf("settle system node delivery: %w", err)
	}
	return nil
}

func (n *systemNodeRunner) convergeNormalRunCompletion(ctx context.Context, evt events.Event) {
	if n == nil || n.bus == nil {
		return
	}
	eventID := strings.TrimSpace(evt.ID)
	if eventID == "" {
		return
	}
	converger, ok := n.bus.(systemNodeNormalRunCompletionConverger)
	if !ok || converger == nil {
		return
	}
	if err := converger.ConvergeNormalRunCompletionForEvent(ctx, eventID); err != nil {
		slog.Error("system node normal run completion convergence failed", "node_id", n.nodeID, "event_id", eventID, "error", err)
		if logger, ok := n.bus.(systemNodeRuntimeLogger); ok && logger != nil {
			logger.LogRuntime(ctx, RuntimeLogEntry{
				Level:     "error",
				Message:   "Converging normal run completion after system node receipt failed",
				Component: n.nodeID,
				Action:    "normal_run_completion_failed",
				EventID:   eventID,
				EventType: strings.TrimSpace(string(evt.Type)),
				EntityID:  workflowEventEntityID(evt),
				Error:     strings.TrimSpace(err.Error()),
			})
		}
	}
}

func (n *systemNodeRunner) isActiveRunQuiesced(ctx context.Context, evt events.Event) bool {
	eventID := strings.TrimSpace(evt.ID)
	if n == nil || n.receiptStore == nil || eventID == "" {
		return false
	}
	if _, err := uuid.Parse(eventID); err != nil {
		return false
	}
	ok, err := n.receiptStore.SystemNodeDeliveryQuiesced(ctx, n.nodeID, eventID)
	if err != nil {
		slog.Error("system node active run quiescence check failed", "node_id", n.nodeID, "event_id", eventID, "error", err)
		return true
	}
	return ok
}

func (n *systemNodeRunner) eventReceiptsAvailable(ctx context.Context) bool {
	if n == nil || n.receiptStore == nil {
		return false
	}
	n.receiptsMu.Lock()
	defer n.receiptsMu.Unlock()
	if n.receiptsChecked {
		return n.receiptsEnabled
	}
	enabled := false
	if n.eventReceiptsCapability != nil {
		if ok, err := n.eventReceiptsCapability(ctx); err == nil {
			enabled = ok
		}
	}
	n.receiptsChecked = true
	n.receiptsEnabled = enabled
	return n.receiptsEnabled
}

func (n *systemNodeRunner) String() string {
	if n == nil {
		return ""
	}
	return n.nodeID
}

func defaultSystemNodeBackoff(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if attempt < 1 {
		attempt = 1
	}
	multiplier := time.Duration(30)
	switch attempt {
	case 1:
		multiplier = 1
	case 2:
		multiplier = 5
	}
	d := base * multiplier
	if d > 30*base {
		return 30 * base
	}
	return d
}

func SystemNodeReceiptIdempotencyKey(nodeID, eventID string) string {
	return strings.TrimSpace(nodeID) + ":" + strings.TrimSpace(eventID)
}
