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
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/google/uuid"
)

type systemNodeBus interface {
	Publish(ctx context.Context, evt events.Event) error
}

type ownedInternalSubscriptionBus interface {
	SubscribeInternal(subscriberID string, eventTypes ...events.EventType) <-chan *worklifetime.EventDelivery
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

	subscriptionsFn    func() []events.EventType
	handleFn           func(context.Context, events.Event) error
	overrideHandle     func(context.Context, events.Event) error
	subscribeHookMu    sync.Mutex
	onSubscribeHooks   []func()
	testLifecycleProbe runtimelifecycleprobe.Observer
}

func newSystemNodeRunner(nodeID string, bus systemNodeBus, db *sql.DB, subscriptionsFn func() []events.EventType, handleFn func(context.Context, events.Event) error) *systemNodeRunner {
	return newSystemNodeRunnerWithRetryBase(nodeID, bus, db, subscriptionsFn, handleFn, 0)
}

func newSystemNodeRunnerWithRetryBase(nodeID string, bus systemNodeBus, db *sql.DB, subscriptionsFn func() []events.EventType, handleFn func(context.Context, events.Event) error, retryBase time.Duration) *systemNodeRunner {
	return newSystemNodeRunnerWithReceiptStoreAndRetryBase(nodeID, bus, db, NewWorkflowInstanceStore(db), subscriptionsFn, handleFn, retryBase)
}

func newSystemNodeRunnerWithReceiptStoreAndRetryBase(nodeID string, bus systemNodeBus, db *sql.DB, receiptStore SystemNodeReceiptPersistence, subscriptionsFn func() []events.EventType, handleFn func(context.Context, events.Event) error, retryBase time.Duration) *systemNodeRunner {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" || bus == nil || handleFn == nil {
		return nil
	}
	if retryBase <= 0 {
		retryBase = time.Second
	}
	return &systemNodeRunner{
		nodeID:          nodeID,
		bus:             bus,
		db:              db,
		receiptStore:    receiptStore,
		retryLimit:      DefaultSystemNodeRetryLimit,
		backoffFn:       func(attempt int) time.Duration { return defaultSystemNodeBackoff(retryBase, attempt) },
		subscriptionsFn: subscriptionsFn,
		handleFn:        handleFn,
	}
}

func (n *systemNodeRunner) Run(ctx context.Context) {
	if n == nil || n.bus == nil || n.handleFn == nil {
		return
	}
	ch := n.subscribe()
	n.notifySubscribed()
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-ch:
			if !ok {
				ch = n.subscribe()
				n.notifySubscribed()
				continue
			}
			func() {
				defer func() { _ = delivery.Complete() }()
				n.ProcessEventForTest(delivery.Context(), delivery.Event())
			}()
		}
	}
}

func (n *systemNodeRunner) ProcessEventForTest(ctx context.Context, evt events.Event) {
	if n == nil {
		return
	}
	eventID := strings.TrimSpace(evt.ID())
	if eventID == "" {
		return
	}
	if n.isActiveRunQuiesced(ctx, evt) {
		return
	}
	if n.isProcessed(ctx, evt) {
		return
	}
	if !n.deliveryAuthorized(ctx, evt) {
		return
	}
	retryLimit := n.effectiveRetryLimit()
	var lastFailure *runtimefailures.Envelope
	retryCount := maxInt(retryLimit, 1)
	backoffFn := n.backoffFn
	if backoffFn == nil {
		backoffFn = func(attempt int) time.Duration { return defaultSystemNodeBackoff(time.Second, attempt) }
	}
	for attempt := 1; attempt <= retryLimit; attempt++ {
		if !n.markDeliveryInProgress(ctx, evt) {
			return
		}
		n.notifyTestLifecycleHandlerStarted(ctx, evt)
		if err := n.handle(ctx, evt); err == nil {
			n.notifyTestLifecycleHandlerCompleted(ctx, evt, "completed")
			if !n.isActiveRunQuiesced(ctx, evt) {
				n.markProcessed(ctx, evt)
			}
			return
		} else {
			n.notifyTestLifecycleHandlerCompleted(ctx, evt, "failed")
			failure := runtimefailures.FromError(err, n.nodeID, "handle_event")
			lastFailure = runtimefailures.CloneEnvelope(&failure.Failure)
			if runtimeengine.FailureDispositionFor(failure) != runtimeengine.FailureDispositionRetry {
				retryCount = 0
				break
			}
			retryCount = attempt
		}
		if attempt >= retryLimit {
			break
		}
		n.markDeliveryFailed(ctx, evt, "handler_failure", lastFailure, retryCount)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoffFn(attempt)):
		}
	}
	if n.isActiveRunQuiesced(ctx, evt) {
		return
	}
	terminalFailure := lastFailure
	if terminalFailure == nil {
		missing := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassInternalFailure, "missing_handler_failure", n.nodeID, "handle_event", nil), n.nodeID, "handle_event")
		terminalFailure = &missing.Failure
	} else if runtimeengine.FailureDispositionFor(runtimefailures.FromEnvelope(*terminalFailure)) == runtimeengine.FailureDispositionRetry {
		exhausted := runtimefailures.FromError(runtimefailures.New(runtimefailures.ClassRetryExhausted, "delivery_retry_exhausted", n.nodeID, "apply_retry_policy", map[string]any{
			"attempts": retryCount, "last_failure": *terminalFailure,
		}), n.nodeID, "apply_retry_policy")
		terminalFailure = &exhausted.Failure
	}
	n.emitDeadLetter(ctx, evt, *terminalFailure, retryCount)
	n.markDeliveryDeadLetter(ctx, evt, "handler_terminal_failure", terminalFailure, retryCount)
}

func (n *systemNodeRunner) SetRetryPolicyForTest(limit int, backoff func(int) time.Duration) {
	if n == nil {
		return
	}
	n.retryLimit = limit
	n.backoffFn = backoff
}

func (n *systemNodeRunner) effectiveRetryLimit() int {
	if n == nil {
		return DefaultSystemNodeRetryLimit
	}
	return normalizeSystemNodeRetryLimit(n.retryLimit)
}

func (n *systemNodeRunner) SetOverrideHandleForTest(fn func(context.Context, events.Event) error) {
	if n == nil {
		return
	}
	n.overrideHandle = fn
}

func (n *systemNodeRunner) SetOnSubscribeForTest(fn func()) {
	n.AddSubscriptionReadyHook(fn)
}

func (n *systemNodeRunner) AddSubscriptionReadyHook(fn func()) {
	if n == nil {
		return
	}
	if fn == nil {
		return
	}
	n.subscribeHookMu.Lock()
	n.onSubscribeHooks = append(n.onSubscribeHooks, fn)
	n.subscribeHookMu.Unlock()
}

func (n *systemNodeRunner) notifySubscribed() {
	if n == nil {
		return
	}
	n.subscribeHookMu.Lock()
	hooks := append([]func(){}, n.onSubscribeHooks...)
	n.subscribeHookMu.Unlock()
	for _, hook := range hooks {
		if hook != nil {
			hook()
		}
	}
}

func (n *systemNodeRunner) SetTestLifecycleProbe(probe runtimelifecycleprobe.Observer) {
	if n == nil {
		return
	}
	n.testLifecycleProbe = probe
}

func (n *systemNodeRunner) subscribe() <-chan *worklifetime.EventDelivery {
	if n == nil || n.bus == nil {
		return nil
	}
	bus, ok := n.bus.(ownedInternalSubscriptionBus)
	if !ok {
		return nil
	}
	subscriptions := []events.EventType(nil)
	if n.subscriptionsFn != nil {
		subscriptions = n.subscriptionsFn()
	}
	return bus.SubscribeInternal(n.nodeID, subscriptions...)
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

func (n *systemNodeRunner) emitDeadLetter(ctx context.Context, evt events.Event, failure runtimefailures.Envelope, retryCount int) {
	if n == nil || n.bus == nil {
		return
	}
	if retryCount < 0 {
		retryCount = 0
	}
	payload := map[string]any{
		"original_event":   strings.TrimSpace(string(evt.Type())),
		"original_payload": json.RawMessage(evt.Payload()),
		"entity_id":        workflowEventEntityID(evt),
		"flow_instance":    "runtime",
		"failure":          failure,
		"retry_count":      retryCount,
		"chain_depth":      evt.ChainDepth(),
		"handler_node":     n.nodeID,
		"timestamp":        time.Now().UTC().Format(time.RFC3339Nano),
	}
	if n.db != nil {
		if err := recordPipelineDeadLetter(ctx, n.db, runtimedeadletters.Record{
			OriginalEventID: strings.TrimSpace(evt.ID()),
			OriginalEvent:   strings.TrimSpace(string(evt.Type())),
			OriginalPayload: evt.Payload(),
			EntityID:        workflowEventEntityID(evt),
			Failure:         failure,
			RetryCount:      retryCount,
			ChainDepth:      evt.ChainDepth(),
			HandlerNode:     n.nodeID,
		}); err != nil {
			slog.Error("system node dead letter persist failed", "node_id", n.nodeID, "event_id", strings.TrimSpace(evt.ID()), "error", err)
			if logger, ok := n.bus.(systemNodeRuntimeLogger); ok && logger != nil {
				logger.LogRuntime(ctx, RuntimeLogEntry{
					Level:     "error",
					Message:   "Persisting the system node dead letter failed",
					Component: n.nodeID,
					Action:    "dead_letter_persist_failed",
					EventID:   strings.TrimSpace(evt.ID()),
					EventType: strings.TrimSpace(string(evt.Type())),
					EntityID:  workflowEventEntityID(evt),
					Failure:   pipelineDependencyFailure(err, "dead_letter_persist_failed", n.nodeID, "persist_dead_letter"),
				})
			}
		}
	}
	deadLetter, constructErr := events.NewCausalRuntimeDiagnosticEvent(events.CausalRuntimeEventInput{Facts: events.EventFacts{
		ID: uuid.NewString(), Type: events.EventType("platform.dead_letter"),
		Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "runtime"},
		Payload:  mustJSON(payload), Envelope: events.EventEnvelope{EntityID: workflowEventEntityID(evt)},
		CreatedAt: time.Now().UTC(), ExecutionMode: evt.ExecutionMode(),
	}, Lineage: events.LineageFromEvent(evt)})
	if constructErr != nil {
		slog.Error("system node dead letter construction failed", "node_id", n.nodeID, "event_id", strings.TrimSpace(evt.ID()), "error", constructErr)
		return
	}
	if err := n.bus.Publish(ctx, deadLetter); err != nil {
		slog.Error("system node dead letter publish failed", "node_id", n.nodeID, "event_id", strings.TrimSpace(evt.ID()), "error", err)
		if logger, ok := n.bus.(systemNodeRuntimeLogger); ok && logger != nil {
			logger.LogRuntime(ctx, RuntimeLogEntry{
				Level:     "error",
				Message:   "Publishing the system node dead letter failed",
				Component: n.nodeID,
				Action:    "dead_letter_publish_failed",
				EventID:   strings.TrimSpace(evt.ID()),
				EventType: strings.TrimSpace(string(evt.Type())),
				EntityID:  workflowEventEntityID(evt),
				Failure:   pipelineDependencyFailure(err, "dead_letter_publish_failed", n.nodeID, "publish_dead_letter"),
			})
		}
	}
}

func (n *systemNodeRunner) isProcessed(ctx context.Context, evt events.Event) bool {
	eventID := strings.TrimSpace(evt.ID())
	if n == nil || n.receiptStore == nil || eventID == "" {
		return false
	}
	if !n.eventReceiptsAvailable(ctx) {
		return false
	}
	if target := systemNodeDeliveryTarget(evt); !target.Empty() {
		targetStore, ok := n.receiptStore.(SystemNodeTargetReceiptPersistence)
		if !ok {
			return false
		}
		ok, err := targetStore.SystemNodeProcessedForTarget(ctx, n.nodeID, eventID, target)
		return err == nil && ok
	}
	ok, err := n.receiptStore.SystemNodeProcessed(ctx, n.nodeID, eventID)
	return err == nil && ok
}

func (n *systemNodeRunner) markProcessed(ctx context.Context, evt events.Event) {
	eventID := strings.TrimSpace(evt.ID())
	if n == nil || n.receiptStore == nil || eventID == "" {
		return
	}
	if n.isActiveRunQuiesced(ctx, evt) {
		return
	}
	if !n.eventReceiptsAvailable(ctx) {
		return
	}
	target := systemNodeDeliveryTarget(evt)
	sideEffects := systemNodeProcessedReceiptSideEffects(n.nodeID, eventID, target)
	if err := n.persistProcessedReceiptAndSettleDelivery(ctx, eventID, target, sideEffects); err != nil {
		slog.Error("system node mark processed failed", "node_id", n.nodeID, "event_id", eventID, "error", err)
		if logger, ok := n.bus.(systemNodeRuntimeLogger); ok && logger != nil {
			logger.LogRuntime(ctx, RuntimeLogEntry{
				Level:     "error",
				Message:   "Marking the system node event as processed failed",
				Component: n.nodeID,
				Action:    "mark_processed_failed",
				EventID:   eventID,
				EventType: strings.TrimSpace(string(evt.Type())),
				EntityID:  workflowEventEntityID(evt),
				Failure:   pipelineDependencyFailure(err, "mark_processed_failed", n.nodeID, "settle_delivery"),
			})
		}
		return
	}
	n.convergeNormalRunCompletion(ctx, evt)
	n.notifyTestLifecycleDeliveryStatus(ctx, evt, "delivered")
}

func (n *systemNodeRunner) markDeliveryInProgress(ctx context.Context, evt events.Event) bool {
	eventID := strings.TrimSpace(evt.ID())
	if n == nil || n.receiptStore == nil || eventID == "" {
		return false
	}
	if n.isActiveRunQuiesced(ctx, evt) {
		return false
	}
	if !n.eventReceiptsAvailable(ctx) {
		return false
	}
	if target := systemNodeDeliveryTarget(evt); !target.Empty() {
		targetStore, ok := n.receiptStore.(SystemNodeTargetReceiptPersistence)
		if !ok {
			n.logSystemNodeDeliveryTransitionError(ctx, evt, "mark_delivery_in_progress_failed", "Marking the targeted system node delivery in progress failed", ErrSystemNodeDeliveryAuthorityMissing)
			return false
		}
		if err := targetStore.MarkSystemNodeDeliveryInProgressForTarget(ctx, n.nodeID, eventID, target, n.effectiveRetryLimit()); err != nil {
			n.logSystemNodeDeliveryTransitionError(ctx, evt, "mark_delivery_in_progress_failed", "Marking the targeted system node delivery in progress failed", err)
			return false
		}
		n.notifyTestLifecycleDeliveryStatus(ctx, evt, "in_progress")
		return true
	}
	if err := n.receiptStore.MarkSystemNodeDeliveryInProgress(ctx, n.nodeID, eventID, n.effectiveRetryLimit()); err != nil {
		n.logSystemNodeDeliveryTransitionError(ctx, evt, "mark_delivery_in_progress_failed", "Marking the system node delivery in progress failed", err)
		return false
	}
	n.notifyTestLifecycleDeliveryStatus(ctx, evt, "in_progress")
	return true
}

func (n *systemNodeRunner) markDeliveryFailed(ctx context.Context, evt events.Event, reasonCode string, failure *runtimefailures.Envelope, retryCount int) {
	eventID := strings.TrimSpace(evt.ID())
	if n == nil || n.receiptStore == nil || eventID == "" {
		return
	}
	if n.isActiveRunQuiesced(ctx, evt) {
		return
	}
	if !n.eventReceiptsAvailable(ctx) {
		return
	}
	if target := systemNodeDeliveryTarget(evt); !target.Empty() {
		targetStore, ok := n.receiptStore.(SystemNodeTargetReceiptPersistence)
		if !ok {
			n.logSystemNodeDeliveryTransitionError(ctx, evt, "mark_delivery_failed_failed", "Marking the targeted system node delivery as failed failed", ErrSystemNodeDeliveryAuthorityMissing)
			return
		}
		if err := targetStore.MarkSystemNodeDeliveryFailedForTarget(ctx, n.nodeID, eventID, target, reasonCode, failure, retryCount, n.effectiveRetryLimit()); err != nil {
			n.logSystemNodeDeliveryTransitionError(ctx, evt, "mark_delivery_failed_failed", "Marking the targeted system node delivery as failed failed", err)
			return
		}
		n.notifyTestLifecycleDeliveryStatus(ctx, evt, "failed")
		return
	}
	if err := n.receiptStore.MarkSystemNodeDeliveryFailed(ctx, n.nodeID, eventID, reasonCode, failure, retryCount, n.effectiveRetryLimit()); err != nil {
		n.logSystemNodeDeliveryTransitionError(ctx, evt, "mark_delivery_failed_failed", "Marking the system node delivery as failed failed", err)
		return
	}
	n.notifyTestLifecycleDeliveryStatus(ctx, evt, "failed")
}

func (n *systemNodeRunner) markDeliveryDeadLetter(ctx context.Context, evt events.Event, reasonCode string, failure *runtimefailures.Envelope, retryCount int) {
	eventID := strings.TrimSpace(evt.ID())
	if n == nil || n.receiptStore == nil || eventID == "" {
		return
	}
	if n.isActiveRunQuiesced(ctx, evt) {
		return
	}
	if !n.eventReceiptsAvailable(ctx) {
		return
	}
	target := systemNodeDeliveryTarget(evt)
	sideEffects := systemNodeDeadLetterReceiptSideEffects(n.nodeID, eventID, reasonCode, retryCount, target)
	if !target.Empty() {
		targetStore, ok := n.receiptStore.(SystemNodeTargetReceiptPersistence)
		if !ok {
			n.logSystemNodeDeliveryTransitionError(ctx, evt, "mark_delivery_dead_letter_failed", "Marking the targeted system node delivery as dead_letter failed", ErrSystemNodeDeliveryAuthorityMissing)
			return
		}
		if err := targetStore.MarkSystemNodeDeliveryDeadLetterForTarget(ctx, n.nodeID, eventID, target, reasonCode, failure, retryCount, sideEffects); err != nil {
			n.logSystemNodeDeliveryTransitionError(ctx, evt, "mark_delivery_dead_letter_failed", "Marking the targeted system node delivery as dead_letter failed", err)
			return
		}
		n.convergeNormalRunCompletion(ctx, evt)
		n.notifyTestLifecycleDeliveryStatus(ctx, evt, "dead_letter")
		return
	}
	if err := n.receiptStore.MarkSystemNodeDeliveryDeadLetter(ctx, n.nodeID, eventID, reasonCode, failure, retryCount, sideEffects); err != nil {
		n.logSystemNodeDeliveryTransitionError(ctx, evt, "mark_delivery_dead_letter_failed", "Marking the system node delivery as dead_letter failed", err)
		return
	}
	n.convergeNormalRunCompletion(ctx, evt)
	n.notifyTestLifecycleDeliveryStatus(ctx, evt, "dead_letter")
}

func (n *systemNodeRunner) logSystemNodeDeliveryTransitionError(ctx context.Context, evt events.Event, action, message string, err error) {
	if n == nil || err == nil {
		return
	}
	eventID := strings.TrimSpace(evt.ID())
	slog.Error(message, "node_id", n.nodeID, "event_id", eventID, "error", err)
	if logger, ok := n.bus.(systemNodeRuntimeLogger); ok && logger != nil {
		logger.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "error",
			Message:   message,
			Component: n.nodeID,
			Action:    action,
			EventID:   eventID,
			EventType: strings.TrimSpace(string(evt.Type())),
			EntityID:  workflowEventEntityID(evt),
			Failure:   pipelineDependencyFailure(err, action, n.nodeID, "delivery_transition"),
		})
	}
}

func (n *systemNodeRunner) persistProcessedReceiptAndSettleDelivery(ctx context.Context, eventID string, target events.RouteIdentity, sideEffects string) error {
	if n == nil || n.receiptStore == nil {
		return nil
	}
	target = target.Normalized()
	if !target.Empty() {
		targetStore, ok := n.receiptStore.(SystemNodeTargetReceiptPersistence)
		if !ok {
			return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, n.nodeID, eventID, systemNodeRouteIdentityJSON(target))
		}
		return targetStore.MarkSystemNodeProcessedAndSettleDeliveryForTarget(ctx, n.nodeID, eventID, target, sideEffects)
	}
	return n.receiptStore.MarkSystemNodeProcessedAndSettleDelivery(ctx, n.nodeID, eventID, sideEffects)
}

func systemNodeProcessedReceiptSideEffects(nodeID, eventID string, target ...events.RouteIdentity) string {
	deliveryTarget := optionalSystemNodeTarget(target)
	sideEffects := map[string]any{
		"idempotency_key": systemNodeReceiptIdempotencyKeyForTarget(nodeID, eventID, deliveryTarget),
	}
	if !deliveryTarget.Empty() {
		sideEffects["delivery_target_route"] = deliveryTarget
	}
	encoded, _ := json.Marshal(sideEffects)
	return string(encoded)
}

func systemNodeDeadLetterReceiptSideEffects(nodeID, eventID, reasonCode string, retryCount int, target ...events.RouteIdentity) string {
	deliveryTarget := optionalSystemNodeTarget(target)
	sideEffects := map[string]any{
		"idempotency_key": systemNodeReceiptIdempotencyKeyForTarget(nodeID, eventID, deliveryTarget),
		"reason_code":     strings.TrimSpace(reasonCode),
		"retry_count":     sanitizedSystemNodeRetryCount(retryCount),
	}
	if !deliveryTarget.Empty() {
		sideEffects["delivery_target_route"] = deliveryTarget
	}
	encoded, _ := json.Marshal(sideEffects)
	return string(encoded)
}

func systemNodeDeliveryTarget(evt events.Event) events.RouteIdentity {
	return evt.TargetRoute().Normalized()
}

func optionalSystemNodeTarget(targets []events.RouteIdentity) events.RouteIdentity {
	if len(targets) == 0 {
		return events.RouteIdentity{}
	}
	return targets[0].Normalized()
}

func pipelineFailureJSON(failure *runtimefailures.Envelope) (string, error) {
	if failure == nil {
		return "", fmt.Errorf("canonical failure is required")
	}
	raw, err := runtimefailures.MarshalEnvelope(*failure)
	if err != nil {
		return "", fmt.Errorf("encode canonical failure: %w", err)
	}
	return string(raw), nil
}

func SystemNodeReceiptIdempotencyKey(nodeID, eventID string) string {
	return strings.TrimSpace(nodeID) + ":" + strings.TrimSpace(eventID)
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

func (n *systemNodeRunner) deliveryAuthorized(ctx context.Context, evt events.Event) bool {
	eventID := strings.TrimSpace(evt.ID())
	if n == nil || n.receiptStore == nil || eventID == "" {
		return false
	}
	if !n.eventReceiptsAvailable(ctx) {
		return false
	}
	if target := systemNodeDeliveryTarget(evt); !target.Empty() {
		targetStore, ok := n.receiptStore.(SystemNodeTargetReceiptPersistence)
		if !ok {
			n.logMissingDeliveryAuthority(ctx, evt)
			return false
		}
		ok, err := targetStore.SystemNodeDeliveryAuthorizedForTarget(ctx, n.nodeID, eventID, target, n.effectiveRetryLimit())
		if err != nil {
			n.logDeliveryAuthorityCheckError(ctx, evt, err)
			return false
		}
		if !ok {
			n.logMissingDeliveryAuthority(ctx, evt)
		}
		return ok
	}
	ok, err := n.receiptStore.SystemNodeDeliveryAuthorized(ctx, n.nodeID, eventID, n.effectiveRetryLimit())
	if err != nil {
		n.logDeliveryAuthorityCheckError(ctx, evt, err)
		return false
	}
	if !ok {
		n.logMissingDeliveryAuthority(ctx, evt)
	}
	return ok
}

func (n *systemNodeRunner) logDeliveryAuthorityCheckError(ctx context.Context, evt events.Event, err error) {
	if n == nil || err == nil {
		return
	}
	eventID := strings.TrimSpace(evt.ID())
	slog.Error("system node delivery authority check failed", "node_id", n.nodeID, "event_id", eventID, "error", err)
	if logger, ok := n.bus.(systemNodeRuntimeLogger); ok && logger != nil {
		logger.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "error",
			Message:   "Checking system node delivery authority failed",
			Component: n.nodeID,
			Action:    "delivery_authority_check_failed",
			EventID:   eventID,
			EventType: strings.TrimSpace(string(evt.Type())),
			EntityID:  workflowEventEntityID(evt),
			Failure:   pipelineDependencyFailure(err, "delivery_authority_check_failed", n.nodeID, "check_delivery_authority"),
		})
	}
}

func (n *systemNodeRunner) logMissingDeliveryAuthority(ctx context.Context, evt events.Event) {
	if n == nil {
		return
	}
	if logger, ok := n.bus.(systemNodeRuntimeLogger); ok && logger != nil {
		logger.LogRuntime(ctx, RuntimeLogEntry{
			Level:     "error",
			Message:   "System node delivery authority is missing; handler execution skipped",
			Component: n.nodeID,
			Action:    "delivery_authority_missing",
			EventID:   strings.TrimSpace(evt.ID()),
			EventType: strings.TrimSpace(string(evt.Type())),
			EntityID:  workflowEventEntityID(evt),
		})
	}
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
	return runPostgresAuthorActivityMutation(ctx, db, "system node processed receipt", func(txctx context.Context, tx *sql.Tx) error {
		return persistSystemNodeProcessedReceiptAndSettleDeliveryTx(txctx, tx, nodeID, eventID, sideEffects)
	})
}

func persistSystemNodeProcessedReceiptAndSettleDeliveryForTarget(ctx context.Context, db *sql.DB, nodeID, eventID string, target events.RouteIdentity, sideEffects string) error {
	target = target.Normalized()
	if target.Empty() {
		return persistSystemNodeProcessedReceiptAndSettleDelivery(ctx, db, nodeID, eventID, sideEffects)
	}
	if db == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if nodeID == "" || eventID == "" {
		return nil
	}
	return runPostgresAuthorActivityMutation(ctx, db, "targeted system node processed receipt", func(txctx context.Context, tx *sql.Tx) error {
		return persistSystemNodeProcessedReceiptAndSettleDeliveryForTargetTx(txctx, tx, nodeID, eventID, target, sideEffects)
	})
}

func persistSystemNodeProcessedReceiptAndSettleDeliveryTx(ctx context.Context, tx *sql.Tx, nodeID, eventID, sideEffects string) error {
	if tx == nil {
		return nil
	}
	retryLimit := normalizeSystemNodeRetryLimit(DefaultSystemNodeRetryLimit)
	authorized, err := postgresSystemNodeDeliveryAuthorizedTx(ctx, tx, nodeID, eventID, retryLimit)
	if err != nil {
		return fmt.Errorf("query system node delivery authority: %w", err)
	}
	if !authorized {
		return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
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
		UPDATE event_deliveries
		SET
			status = 'delivered',
			retry_count = COALESCE(retry_count, 0),
			reason_code = 'node_processed',
			failure = NULL,
			active_session_id = NULL,
			started_at = COALESCE(started_at, created_at),
			delivered_at = now()
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
			  AND subscriber_id = $2
			  AND COALESCE(delivery_target_route, '{}'::jsonb) = '{}'::jsonb
			  AND (
				status IN ('pending', 'in_progress')
				OR (status = 'failed' AND COALESCE(retry_count, 0) < $3)
			  )
		`, eventID, nodeID, retryLimit); err != nil {
		return fmt.Errorf("settle system node delivery: %w", err)
	}
	return nil
}

func persistSystemNodeProcessedReceiptAndSettleDeliveryForTargetTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, target events.RouteIdentity, sideEffects string) error {
	if tx == nil {
		return nil
	}
	target = target.Normalized()
	targetJSON := systemNodeRouteIdentityJSON(target)
	retryLimit := normalizeSystemNodeRetryLimit(DefaultSystemNodeRetryLimit)
	authorized, err := postgresSystemNodeDeliveryAuthorizedForTargetTx(ctx, tx, nodeID, eventID, target, retryLimit)
	if err != nil {
		return fmt.Errorf("query targeted system node delivery authority: %w", err)
	}
	if !authorized {
		return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
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
	`, eventID, nodeID, sideEffects, systemNodeReceiptIdempotencyKeyForTarget(nodeID, eventID, target))
	if err != nil {
		return fmt.Errorf("upsert targeted system node receipt: %w", err)
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("upsert targeted system node receipt: event %s not found", eventID)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET
			status = 'delivered',
			retry_count = COALESCE(retry_count, 0),
			reason_code = 'node_processed',
			failure = NULL,
			active_session_id = NULL,
			started_at = COALESCE(started_at, created_at),
			delivered_at = now()
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND COALESCE(delivery_target_route, '{}'::jsonb) = $3::jsonb
		  AND (
			status IN ('pending', 'in_progress')
			OR (status = 'failed' AND COALESCE(retry_count, 0) < $4)
		  )
		`, eventID, nodeID, targetJSON, retryLimit); err != nil {
		return fmt.Errorf("settle targeted system node delivery: %w", err)
	}
	return nil
}

func markPostgresSystemNodeDeliveryInProgress(ctx context.Context, db *sql.DB, nodeID, eventID string, retryLimit int) error {
	if db == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if nodeID == "" || eventID == "" {
		return nil
	}
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	return runPostgresAuthorActivityMutation(ctx, db, "system node delivery start", func(txctx context.Context, tx *sql.Tx) error {
		return markPostgresSystemNodeDeliveryInProgressTx(txctx, tx, nodeID, eventID, retryLimit)
	})
}

func markPostgresSystemNodeDeliveryInProgressForTarget(ctx context.Context, db *sql.DB, nodeID, eventID string, target events.RouteIdentity, retryLimit int) error {
	target = target.Normalized()
	if target.Empty() {
		return markPostgresSystemNodeDeliveryInProgress(ctx, db, nodeID, eventID, retryLimit)
	}
	if db == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if nodeID == "" || eventID == "" {
		return nil
	}
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	return runPostgresAuthorActivityMutation(ctx, db, "targeted system node delivery start", func(txctx context.Context, tx *sql.Tx) error {
		return markPostgresSystemNodeDeliveryInProgressForTargetTx(txctx, tx, nodeID, eventID, target, retryLimit)
	})
}

func markPostgresSystemNodeDeliveryInProgressTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, retryLimit int) error {
	if tx == nil {
		return nil
	}
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	authorized, err := postgresSystemNodeDeliveryAuthorizedTx(ctx, tx, nodeID, eventID, retryLimit)
	if err != nil {
		return fmt.Errorf("query system node delivery authority: %w", err)
	}
	if !authorized {
		return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET
			status = 'in_progress',
			reason_code = 'node_processing',
			failure = NULL,
			active_session_id = NULL,
			started_at = COALESCE(started_at, now()),
			delivered_at = NULL
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
			  AND subscriber_id = $2
			  AND COALESCE(delivery_target_route, '{}'::jsonb) = '{}'::jsonb
			  AND (
				status IN ('pending', 'in_progress')
				OR (status = 'failed' AND COALESCE(retry_count, 0) < $3)
			  )
	`, eventID, nodeID, retryLimit)
	if err != nil {
		return fmt.Errorf("mark system node delivery in progress: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
	}
	return nil
}

func markPostgresSystemNodeDeliveryInProgressForTargetTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, target events.RouteIdentity, retryLimit int) error {
	if tx == nil {
		return nil
	}
	target = target.Normalized()
	targetJSON := systemNodeRouteIdentityJSON(target)
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	authorized, err := postgresSystemNodeDeliveryAuthorizedForTargetTx(ctx, tx, nodeID, eventID, target, retryLimit)
	if err != nil {
		return fmt.Errorf("query targeted system node delivery authority: %w", err)
	}
	if !authorized {
		return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET
			status = 'in_progress',
			reason_code = 'node_processing',
			failure = NULL,
			active_session_id = NULL,
			started_at = COALESCE(started_at, now()),
			delivered_at = NULL
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND COALESCE(delivery_target_route, '{}'::jsonb) = $3::jsonb
		  AND (
			status IN ('pending', 'in_progress')
			OR (status = 'failed' AND COALESCE(retry_count, 0) < $4)
		  )
	`, eventID, nodeID, targetJSON, retryLimit)
	if err != nil {
		return fmt.Errorf("mark targeted system node delivery in progress: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
	}
	return nil
}

func markPostgresSystemNodeDeliveryFailed(ctx context.Context, db *sql.DB, nodeID, eventID, reasonCode string, failure *runtimefailures.Envelope, retryCount, retryLimit int) error {
	if db == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if nodeID == "" || eventID == "" {
		return nil
	}
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	return runPostgresAuthorActivityMutation(ctx, db, "system node delivery failure", func(txctx context.Context, tx *sql.Tx) error {
		return markPostgresSystemNodeDeliveryFailedTx(txctx, tx, nodeID, eventID, reasonCode, failure, retryCount, retryLimit)
	})
}

func markPostgresSystemNodeDeliveryFailedForTarget(ctx context.Context, db *sql.DB, nodeID, eventID string, target events.RouteIdentity, reasonCode string, failure *runtimefailures.Envelope, retryCount, retryLimit int) error {
	target = target.Normalized()
	if target.Empty() {
		return markPostgresSystemNodeDeliveryFailed(ctx, db, nodeID, eventID, reasonCode, failure, retryCount, retryLimit)
	}
	if db == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if nodeID == "" || eventID == "" {
		return nil
	}
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	return runPostgresAuthorActivityMutation(ctx, db, "targeted system node delivery failure", func(txctx context.Context, tx *sql.Tx) error {
		return markPostgresSystemNodeDeliveryFailedForTargetTx(txctx, tx, nodeID, eventID, target, reasonCode, failure, retryCount, retryLimit)
	})
}

func markPostgresSystemNodeDeliveryFailedTx(ctx context.Context, tx *sql.Tx, nodeID, eventID, reasonCode string, failure *runtimefailures.Envelope, retryCount, retryLimit int) error {
	if tx == nil {
		return nil
	}
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	authorized, err := postgresSystemNodeDeliveryAuthorizedTx(ctx, tx, nodeID, eventID, retryLimit)
	if err != nil {
		return fmt.Errorf("query system node delivery authority: %w", err)
	}
	if !authorized {
		return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
	}
	failureJSON, err := pipelineFailureJSON(failure)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET
			status = 'failed',
			retry_count = $3,
			reason_code = NULLIF($4, ''),
			failure = NULLIF($5, '')::jsonb,
			active_session_id = NULL,
			started_at = COALESCE(started_at, created_at),
			delivered_at = now()
	WHERE event_id = $1::uuid
	  AND subscriber_type = 'node'
	  AND subscriber_id = $2
	  AND COALESCE(delivery_target_route, '{}'::jsonb) = '{}'::jsonb
	  AND status IN ('pending', 'in_progress', 'failed')
	  AND COALESCE(retry_count, 0) < $6
	`, eventID, nodeID, sanitizedSystemNodeRetryCount(retryCount), sanitizeSystemNodeReasonCode(reasonCode, "handler_failure"), failureJSON, retryLimit)
	if err != nil {
		return fmt.Errorf("mark system node delivery failed: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
	}
	return nil
}

func markPostgresSystemNodeDeliveryFailedForTargetTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, target events.RouteIdentity, reasonCode string, failure *runtimefailures.Envelope, retryCount, retryLimit int) error {
	if tx == nil {
		return nil
	}
	target = target.Normalized()
	targetJSON := systemNodeRouteIdentityJSON(target)
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	authorized, err := postgresSystemNodeDeliveryAuthorizedForTargetTx(ctx, tx, nodeID, eventID, target, retryLimit)
	if err != nil {
		return fmt.Errorf("query targeted system node delivery authority: %w", err)
	}
	if !authorized {
		return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
	}
	failureJSON, err := pipelineFailureJSON(failure)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET
			status = 'failed',
			retry_count = $4,
			reason_code = NULLIF($5, ''),
			failure = NULLIF($6, '')::jsonb,
			active_session_id = NULL,
			started_at = COALESCE(started_at, created_at),
			delivered_at = now()
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND COALESCE(delivery_target_route, '{}'::jsonb) = $3::jsonb
		  AND status IN ('pending', 'in_progress', 'failed')
		  AND COALESCE(retry_count, 0) < $7
	`, eventID, nodeID, targetJSON, sanitizedSystemNodeRetryCount(retryCount), sanitizeSystemNodeReasonCode(reasonCode, "handler_failure"), failureJSON, retryLimit)
	if err != nil {
		return fmt.Errorf("mark targeted system node delivery failed: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
	}
	return nil
}

func markPostgresSystemNodeDeliveryDeadLetter(ctx context.Context, db *sql.DB, nodeID, eventID, reasonCode string, failure *runtimefailures.Envelope, retryCount int, sideEffects string) error {
	if db == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if nodeID == "" || eventID == "" {
		return nil
	}
	return runPostgresAuthorActivityMutation(ctx, db, "system node delivery dead-letter", func(txctx context.Context, tx *sql.Tx) error {
		return markPostgresSystemNodeDeliveryDeadLetterTx(txctx, tx, nodeID, eventID, reasonCode, failure, retryCount, sideEffects)
	})
}

func markPostgresSystemNodeDeliveryDeadLetterForTarget(ctx context.Context, db *sql.DB, nodeID, eventID string, target events.RouteIdentity, reasonCode string, failure *runtimefailures.Envelope, retryCount int, sideEffects string) error {
	target = target.Normalized()
	if target.Empty() {
		return markPostgresSystemNodeDeliveryDeadLetter(ctx, db, nodeID, eventID, reasonCode, failure, retryCount, sideEffects)
	}
	if db == nil {
		return nil
	}
	nodeID = strings.TrimSpace(nodeID)
	eventID = strings.TrimSpace(eventID)
	if nodeID == "" || eventID == "" {
		return nil
	}
	return runPostgresAuthorActivityMutation(ctx, db, "targeted system node delivery dead-letter", func(txctx context.Context, tx *sql.Tx) error {
		return markPostgresSystemNodeDeliveryDeadLetterForTargetTx(txctx, tx, nodeID, eventID, target, reasonCode, failure, retryCount, sideEffects)
	})
}

func commitSystemNodeRevisionTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := runforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func markPostgresSystemNodeDeliveryDeadLetterTx(ctx context.Context, tx *sql.Tx, nodeID, eventID, reasonCode string, failure *runtimefailures.Envelope, retryCount int, sideEffects string) error {
	if tx == nil {
		return nil
	}
	exists, err := postgresSystemNodeDeliveryRowExistsTx(ctx, tx, nodeID, eventID)
	if err != nil {
		return fmt.Errorf("query system node delivery row: %w", err)
	}
	if !exists {
		return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
	}
	reasonCode = sanitizeSystemNodeReasonCode(reasonCode, "retry_exhausted")
	failureJSON, err := pipelineFailureJSON(failure)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET
			status = 'dead_letter',
			retry_count = $3,
			reason_code = NULLIF($4, ''),
			failure = NULLIF($5, '')::jsonb,
			active_session_id = NULL,
			started_at = COALESCE(started_at, created_at),
			delivered_at = now()
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND COALESCE(delivery_target_route, '{}'::jsonb) = '{}'::jsonb
		  AND status IN ('pending', 'in_progress', 'failed')
	`, eventID, nodeID, sanitizedSystemNodeRetryCount(retryCount), reasonCode, failureJSON)
	if err != nil {
		return fmt.Errorf("dead-letter system node delivery: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("%w: node %s event %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID)
	}
	res, err = tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, idempotency_key, processed_at
		)
		SELECT
			e.event_id, 'node', $2, e.entity_id, e.flow_instance,
			'dead_letter', NULLIF($3, ''), $4::jsonb, $5::jsonb, $6, now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			failure = EXCLUDED.failure,
			side_effects = EXCLUDED.side_effects,
			idempotency_key = EXCLUDED.idempotency_key,
			processed_at = now()
	`, eventID, nodeID, reasonCode, failureJSON, sqliteNodeJSON(sideEffects), SystemNodeReceiptIdempotencyKey(nodeID, eventID))
	if err != nil {
		return fmt.Errorf("upsert system node dead-letter receipt: %w", err)
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("upsert system node dead-letter receipt: event %s not found", eventID)
	}
	return nil
}

func markPostgresSystemNodeDeliveryDeadLetterForTargetTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, target events.RouteIdentity, reasonCode string, failure *runtimefailures.Envelope, retryCount int, sideEffects string) error {
	if tx == nil {
		return nil
	}
	target = target.Normalized()
	targetJSON := systemNodeRouteIdentityJSON(target)
	exists, err := postgresSystemNodeDeliveryRowExistsForTargetTx(ctx, tx, nodeID, eventID, target)
	if err != nil {
		return fmt.Errorf("query targeted system node delivery row: %w", err)
	}
	if !exists {
		return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
	}
	reasonCode = sanitizeSystemNodeReasonCode(reasonCode, "retry_exhausted")
	failureJSON, err := pipelineFailureJSON(failure)
	if err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET
			status = 'dead_letter',
			retry_count = $4,
			reason_code = NULLIF($5, ''),
			failure = NULLIF($6, '')::jsonb,
			active_session_id = NULL,
			started_at = COALESCE(started_at, created_at),
			delivered_at = now()
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND COALESCE(delivery_target_route, '{}'::jsonb) = $3::jsonb
		  AND status IN ('pending', 'in_progress', 'failed')
	`, eventID, nodeID, targetJSON, sanitizedSystemNodeRetryCount(retryCount), reasonCode, failureJSON)
	if err != nil {
		return fmt.Errorf("dead-letter targeted system node delivery: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("%w: node %s event %s target %s", ErrSystemNodeDeliveryAuthorityMissing, nodeID, eventID, targetJSON)
	}
	res, err = tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, idempotency_key, processed_at
		)
		SELECT
			e.event_id, 'node', $2, e.entity_id, e.flow_instance,
			'dead_letter', NULLIF($3, ''), $4::jsonb, $5::jsonb, $6, now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			failure = EXCLUDED.failure,
			side_effects = EXCLUDED.side_effects,
			idempotency_key = EXCLUDED.idempotency_key,
			processed_at = now()
	`, eventID, nodeID, reasonCode, failureJSON, sqliteNodeJSON(sideEffects), systemNodeReceiptIdempotencyKeyForTarget(nodeID, eventID, target))
	if err != nil {
		return fmt.Errorf("upsert targeted system node dead-letter receipt: %w", err)
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		return fmt.Errorf("upsert targeted system node dead-letter receipt: event %s not found", eventID)
	}
	return nil
}

func postgresSystemNodeDeliveryAuthorizedTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, retryLimit int) (bool, error) {
	if tx == nil {
		return false, nil
	}
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	if _, err := uuid.Parse(strings.TrimSpace(eventID)); err != nil {
		return false, nil
	}
	var ok bool
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
	WHERE event_id = $1::uuid
	  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND COALESCE(delivery_target_route, '{}'::jsonb) = '{}'::jsonb
		  AND (
			status IN ('pending', 'in_progress')
			OR (status = 'failed' AND COALESCE(retry_count, 0) < $3)
				  )
			)
	`, strings.TrimSpace(eventID), strings.TrimSpace(nodeID), retryLimit).Scan(&ok)
	return ok, err
}

func postgresSystemNodeDeliveryAuthorizedForTargetTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, target events.RouteIdentity, retryLimit int) (bool, error) {
	if tx == nil {
		return false, nil
	}
	retryLimit = normalizeSystemNodeRetryLimit(retryLimit)
	if _, err := uuid.Parse(strings.TrimSpace(eventID)); err != nil {
		return false, nil
	}
	var ok bool
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $2
			  AND COALESCE(delivery_target_route, '{}'::jsonb) = $3::jsonb
			  AND (
				status IN ('pending', 'in_progress')
				OR (status = 'failed' AND COALESCE(retry_count, 0) < $4)
			  )
			)
	`, strings.TrimSpace(eventID), strings.TrimSpace(nodeID), systemNodeRouteIdentityJSON(target), retryLimit).Scan(&ok)
	return ok, err
}

func postgresSystemNodeDeliveryRowExistsTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string) (bool, error) {
	if tx == nil {
		return false, nil
	}
	if _, err := uuid.Parse(strings.TrimSpace(eventID)); err != nil {
		return false, nil
	}
	var ok bool
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $2
			  AND COALESCE(delivery_target_route, '{}'::jsonb) = '{}'::jsonb
		)
	`, strings.TrimSpace(eventID), strings.TrimSpace(nodeID)).Scan(&ok)
	return ok, err
}

func postgresSystemNodeDeliveryRowExistsForTargetTx(ctx context.Context, tx *sql.Tx, nodeID, eventID string, target events.RouteIdentity) (bool, error) {
	if tx == nil {
		return false, nil
	}
	if _, err := uuid.Parse(strings.TrimSpace(eventID)); err != nil {
		return false, nil
	}
	var ok bool
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = $2
			  AND COALESCE(delivery_target_route, '{}'::jsonb) = $3::jsonb
		)
	`, strings.TrimSpace(eventID), strings.TrimSpace(nodeID), systemNodeRouteIdentityJSON(target)).Scan(&ok)
	return ok, err
}

func (n *systemNodeRunner) convergeNormalRunCompletion(ctx context.Context, evt events.Event) {
	if n == nil || n.bus == nil {
		return
	}
	eventID := strings.TrimSpace(evt.ID())
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
				EventType: strings.TrimSpace(string(evt.Type())),
				EntityID:  workflowEventEntityID(evt),
				Failure:   pipelineDependencyFailure(err, "normal_run_completion_failed", n.nodeID, "converge_run_completion"),
			})
		}
	}
}

func (n *systemNodeRunner) isActiveRunQuiesced(ctx context.Context, evt events.Event) bool {
	eventID := strings.TrimSpace(evt.ID())
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
	return n != nil && n.receiptStore != nil
}

func (n *systemNodeRunner) String() string {
	if n == nil {
		return ""
	}
	return n.nodeID
}
