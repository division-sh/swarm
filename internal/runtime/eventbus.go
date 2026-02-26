package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

type EventStore interface {
	AppendEvent(ctx context.Context, evt events.Event) error
	InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error
}

// ActiveAgentLister is an optional capability for broadcast-style events.
// PostgresStore implements this; InMemoryEventStore does not.
type ActiveAgentLister interface {
	ListActiveAgentIDs(ctx context.Context) ([]string, error)
}

// PipelineReceiptPersistence is an optional capability for marking whether
// persisted events were fully routed/delivered by the runtime publish path.
type PipelineReceiptPersistence interface {
	UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error
}

// AtomicEventPersistence is an optional capability for transactionally
// persisting an event row and its delivery manifest together.
type AtomicEventPersistence interface {
	PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error
}

// TransactionalEventStore is an optional capability for full publish-time
// transactional semantics: interceptor state writes + event persistence +
// deferred event persistence in one DB transaction.
type TransactionalEventStore interface {
	BeginEventTx(ctx context.Context) (*sql.Tx, error)
	AppendEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error
	InsertEventDeliveriesTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string) error
	UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error
}

// EventInterceptor runs deterministic runtime coordination in the publish path.
// It may consume the inbound event and/or emit deferred events.
type EventInterceptor interface {
	Intercept(ctx context.Context, evt events.Event) (passthrough bool, deferred []events.Event, err error)
}

type InMemoryEventStore struct{}

func (InMemoryEventStore) AppendEvent(_ context.Context, _ events.Event) error { return nil }
func (InMemoryEventStore) InsertEventDeliveries(_ context.Context, _ string, _ []string) error {
	return nil
}

type EventBus struct {
	mu            sync.RWMutex
	channels      map[events.EventType]map[string]chan events.Event
	agentChans    map[string]chan events.Event
	subscriptions map[string][]events.EventType
	routingTable  map[string]*RoutingTable
	interceptors  []EventInterceptor
	store         EventStore
	logger        *RuntimeLogger
}

const deliverySendTimeout = 250 * time.Millisecond

var ErrStaleRuntimeEpoch = errors.New("stale runtime epoch")

type RoutingTable struct {
	VerticalID string
	Routes     []Route
}

type Route struct {
	EventPattern string
	SubscriberID string
	Status       string // active | proposed | deactivated
}

type eventDeliveryPlan struct {
	Event               events.Event
	Recipients          []string
	PersistedRecipients []string
	ExtraDetail         map[string]any
	ContradictionReason string
}

var eventTypeTokenPattern = regexp.MustCompile(`^[a-z0-9_]+$`)

// Spec v2.0.4: factory vs OpCo-internal routing classification is based on
// event type prefix, never on vertical_id presence.
var factoryEventPrefixes = []string{
	"agent.",
	"runtime.",
	"ops.",
	"system.",
	"timer.",
	"heartbeat.",
	"scan.",
	"scanner.",
	"campaign.",
	"dedup.",
	"synthesis.",
	"vertical.",
	"scoring.",
	"market_research.",
	"trend_research.",
	"validation.",
	"research.",
	"spec.",
	"spec_review.",
	"cto.",
	"brand.",
	"template.",
	"budget.",
	"human_task.",
	"analyst.",
	"portfolio.",
	"mailbox.",
	"board.",
	"review.",
	"founder_input.",
	"spend.",
	"source.",
	"score.",
	"category.",
	"trend.",
	"devops.",
	"opco.",
}

func NewEventBus(store EventStore) *EventBus {
	if store == nil {
		store = InMemoryEventStore{}
	}
	return &EventBus{
		channels:      make(map[events.EventType]map[string]chan events.Event),
		agentChans:    make(map[string]chan events.Event),
		subscriptions: make(map[string][]events.EventType),
		routingTable:  make(map[string]*RoutingTable),
		store:         store,
	}
}

func (eb *EventBus) SetRuntimeLogger(logger *RuntimeLogger) {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	eb.logger = logger
	eb.mu.Unlock()
}

func (eb *EventBus) SetInterceptors(interceptors ...EventInterceptor) {
	if eb == nil {
		return
	}
	filtered := make([]EventInterceptor, 0, len(interceptors))
	for _, it := range interceptors {
		if it != nil {
			filtered = append(filtered, it)
		}
	}
	eb.mu.Lock()
	eb.interceptors = filtered
	eb.mu.Unlock()
}

// ResetInMemoryState clears process-local EventBus state (subscriptions,
// delivery channels, and routing tables) without touching the persistent store.
// This is used during runtime reset flows where DB state is truncated and
// agents are re-seeded from scratch.
func (eb *EventBus) ResetInMemoryState() {
	if eb == nil {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	for _, ch := range eb.agentChans {
		close(ch)
	}
	eb.channels = make(map[events.EventType]map[string]chan events.Event)
	eb.agentChans = make(map[string]chan events.Event)
	eb.subscriptions = make(map[string][]events.EventType)
	eb.routingTable = make(map[string]*RoutingTable)
}

func (eb *EventBus) Publish(ctx context.Context, evt events.Event) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	start := time.Now()
	if evt.Type == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}

	deferredTransitions := make([]deferredPipelineTransition, 0, 8)
	ictx := withPipelineTransitionCollector(ctx, &deferredTransitions)
	if txStore, ok := eb.store.(TransactionalEventStore); ok {
		return eb.publishTransactional(ictx, evt, start, &deferredTransitions, txStore)
	}

	persisted := false
	passthrough := true
	deferred := []events.Event{}
	defer func() {
		if !persisted {
			return
		}
		status := "processed"
		errText := ""
		if err != nil {
			status = "error"
			errText = err.Error()
		}
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()

	// Interceptors execute before fan-out and can consume the event.
	// Deferred events are persisted after the inbound event commits.
	if pass, out, ierr := eb.runInterceptors(ictx, evt); ierr != nil {
		return ierr
	} else {
		passthrough = pass
		deferred = out
	}

	if passthrough {
		if err := eb.routeAndDeliver(ctx, evt); err != nil {
			return err
		}
		persisted = true
	} else {
		if err := eb.persistEventRecord(ctx, evt, nil); err != nil {
			return err
		}
		persisted = true
	}
	eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))
	flushDeferredPipelineTransitions(ctx, deferredTransitions)
	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

func (eb *EventBus) publishTransactional(
	ctx context.Context,
	evt events.Event,
	start time.Time,
	deferredTransitions *[]deferredPipelineTransition,
	txStore TransactionalEventStore,
) error {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	tx, err := txStore.BeginEventTx(ctx)
	if err != nil {
		return fmt.Errorf("begin publish tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	txCtx := withSQLTxContext(ctx, tx)
	passthrough, deferred, err := eb.runInterceptors(txCtx, evt)
	if err != nil {
		return err
	}
	receiptTableExists := txTableExists(txCtx, tx, "pipeline_receipts")

	var inboundPlan eventDeliveryPlan
	if passthrough {
		inboundPlan, err = eb.buildDeliveryPlan(txCtx, evt)
		if err != nil {
			return err
		}
	}

	if err := txStore.AppendEventTx(txCtx, tx, evt); err != nil {
		return fmt.Errorf("persist event: %w", err)
	}
	if passthrough && len(inboundPlan.PersistedRecipients) > 0 {
		if err := txStore.InsertEventDeliveriesTx(txCtx, tx, evt.ID, inboundPlan.PersistedRecipients); err != nil {
			return fmt.Errorf("persist event deliveries: %w", err)
		}
	}
	if receiptTableExists {
		if err := txStore.UpsertPipelineReceiptTx(txCtx, tx, evt.ID, "processed", ""); err != nil {
			return fmt.Errorf("persist pipeline receipt: %w", err)
		}
	}

	deferredPlans := make([]eventDeliveryPlan, 0, len(deferred))
	for _, d := range deferred {
		plan, perr := eb.buildDeliveryPlan(txCtx, d)
		if perr != nil {
			return perr
		}
		if err := txStore.AppendEventTx(txCtx, tx, d); err != nil {
			return fmt.Errorf("persist deferred event: %w", err)
		}
		if len(plan.PersistedRecipients) > 0 {
			if err := txStore.InsertEventDeliveriesTx(txCtx, tx, d.ID, plan.PersistedRecipients); err != nil {
				return fmt.Errorf("persist deferred deliveries: %w", err)
			}
		}
		if receiptTableExists {
			if err := txStore.UpsertPipelineReceiptTx(txCtx, tx, d.ID, "processed", ""); err != nil {
				return fmt.Errorf("persist deferred pipeline receipt: %w", err)
			}
		}
		deferredPlans = append(deferredPlans, plan)
	}

	if deferredTransitions != nil {
		flushDeferredPipelineTransitions(txCtx, *deferredTransitions)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit publish tx: %w", err)
	}
	committed = true

	if passthrough {
		if len(inboundPlan.Recipients) > 0 {
			eb.deliverToAgents(ctx, evt, inboundPlan.Recipients)
			eb.logDelivery(ctx, evt, inboundPlan.Recipients, inboundPlan.ExtraDetail)
		}
		if strings.TrimSpace(inboundPlan.ContradictionReason) != "" {
			_ = eb.emitContradiction(ctx, evt, inboundPlan.ContradictionReason)
		}
	}
	if !receiptTableExists {
		eb.markPipelineReceipt(ctx, evt.ID, "processed", "")
	}
	eb.logPublished(ctx, evt, int(time.Since(start)/time.Microsecond))

	for _, plan := range deferredPlans {
		if len(plan.Recipients) > 0 {
			eb.deliverToAgents(ctx, plan.Event, plan.Recipients)
			eb.logDelivery(ctx, plan.Event, plan.Recipients, plan.ExtraDetail)
		}
		if strings.TrimSpace(plan.ContradictionReason) != "" {
			_ = eb.emitContradiction(ctx, plan.Event, plan.ContradictionReason)
		}
		if !receiptTableExists {
			eb.markPipelineReceipt(ctx, plan.Event.ID, "processed", "")
		}
		eb.logPublished(ctx, plan.Event, 0)
	}
	return nil
}

func txTableExists(ctx context.Context, tx *sql.Tx, table string) bool {
	if tx == nil || strings.TrimSpace(table) == "" {
		return false
	}
	var ok bool
	if err := tx.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+strings.TrimSpace(table)).Scan(&ok); err != nil {
		return false
	}
	return ok
}

func (eb *EventBus) runInterceptors(ctx context.Context, evt events.Event) (bool, []events.Event, error) {
	eb.mu.RLock()
	interceptors := append([]EventInterceptor(nil), eb.interceptors...)
	eb.mu.RUnlock()
	if len(interceptors) == 0 {
		return true, nil, nil
	}
	passthrough := true
	deferred := make([]events.Event, 0, 4)
	for _, it := range interceptors {
		pass, out, err := it.Intercept(ctx, evt)
		if err != nil {
			return true, nil, err
		}
		if !pass {
			passthrough = false
		}
		for _, d := range out {
			if d.ID == "" {
				d.ID = uuid.NewString()
			}
			if d.CreatedAt.IsZero() {
				d.CreatedAt = time.Now()
			}
			deferred = append(deferred, d)
		}
	}
	return passthrough, deferred, nil
}

func (eb *EventBus) publishDeferred(ctx context.Context, evt events.Event) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	if evt.Type == "" {
		return errors.New("deferred event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid deferred event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	if strings.TrimSpace(evt.SourceAgent) == "" {
		evt.SourceAgent = "runtime"
	}
	persisted := false
	defer func() {
		if !persisted {
			return
		}
		status := "processed"
		errText := ""
		if err != nil {
			status = "error"
			errText = err.Error()
		}
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()
	if err := eb.routeAndDeliver(ctx, evt); err != nil {
		return err
	}
	persisted = true
	eb.logPublished(ctx, evt, 0)
	return nil
}

func (eb *EventBus) logPublished(ctx context.Context, evt events.Event, durationUS int) {
	eb.logRuntime(ctx, RuntimeLogEntry{
		Level:      "debug",
		Component:  "eventbus",
		Action:     "published",
		EventID:    evt.ID,
		EventType:  string(evt.Type),
		AgentID:    evt.SourceAgent,
		VerticalID: evt.VerticalID,
		DurationUS: durationUS,
		Detail: map[string]any{
			"type":   string(evt.Type),
			"source": evt.SourceAgent,
		},
	})
}

func (eb *EventBus) routeAndDeliver(ctx context.Context, evt events.Event) error {
	plan, err := eb.buildDeliveryPlan(ctx, evt)
	if err != nil {
		return err
	}
	if err := eb.persistEventRecord(ctx, evt, plan.PersistedRecipients); err != nil {
		return err
	}
	if len(plan.Recipients) > 0 {
		eb.deliverToAgents(ctx, evt, plan.Recipients)
		eb.logDelivery(ctx, evt, plan.Recipients, plan.ExtraDetail)
	}
	if strings.TrimSpace(plan.ContradictionReason) != "" {
		_ = eb.emitContradiction(ctx, evt, plan.ContradictionReason)
	}
	return nil
}

func (eb *EventBus) buildDeliveryPlan(ctx context.Context, evt events.Event) (eventDeliveryPlan, error) {
	plan := eventDeliveryPlan{Event: evt}
	// Budget events are broadcast guardrails. Deliver via delivery manifest so
	// operating (OpCo) agents also receive them during backlog replay.
	if strings.HasPrefix(string(evt.Type), "budget.") {
		recipients := []string{}
		if lister, ok := eb.store.(ActiveAgentLister); ok {
			if ids, err := lister.ListActiveAgentIDs(ctx); err == nil {
				recipients = ids
			}
		}
		if len(recipients) == 0 {
			// Best-effort fallback: deliver to currently subscribed agents.
			recipients = eb.resolveSubscribedRecipients(string(evt.Type))
		}
		plan.Recipients = uniqueStrings(recipients)
		plan.PersistedRecipients = eb.persistableRecipients(ctx, plan.Recipients)
		return plan, nil
	}

	// Human task events must always reach the requesting agent (even if operating
	// and not subscribed) and should also be visible to subscribers like the
	// Empire Coordinator. Treat them as global events, not OpCo-routed events.
	if strings.HasPrefix(string(evt.Type), "human_task.") {
		recipients := eb.resolveSubscribedRecipients(string(evt.Type))
		// Only outcome/decision events are forced to the requesting agent; the
		// initial request event is intended for coordinator review.
		switch string(evt.Type) {
		case "human_task.approved",
			"human_task.rejected",
			"human_task.deferred",
			"human_task.completed",
			"human_task.expired":
			recipients = append(recipients, eb.resolveHumanTaskRecipients(evt)...)
		}
		plan.Recipients = uniqueStrings(recipients)
		plan.PersistedRecipients = eb.persistableRecipients(ctx, plan.Recipients)
		return plan, nil
	}

	if !eb.isFactoryEvent(evt.Type) {
		// OpCo events are delivered via per-vertical routing tables to operating agents.
		// Holding/factory agents may also subscribe to OpCo event patterns and should receive
		// them live without bypassing the vertical routing contract.
		opcoRecipients := eb.resolveOpCoRecipients(evt)

		// Subscribers are filtered to avoid accidentally delivering to operating agents
		// in the same vertical that weren't routed (routing is the source of truth for OpCo).
		subscribed := eb.resolveSubscribedRecipients(string(evt.Type))
		subscribed = filterOutAgentIDs(subscribed, opcoRecipients)
		if strings.TrimSpace(evt.VerticalID) != "" {
			subscribed = filterOutVerticalScopedAgentIDs(subscribed, evt.VerticalID)
		}

		plan.Recipients = uniqueStrings(append(opcoRecipients, subscribed...))
		plan.PersistedRecipients = plan.Recipients
		plan.ExtraDetail = map[string]any{"opco_routed": len(opcoRecipients)}
		if len(plan.Recipients) == 0 {
			plan.ContradictionReason = "opco event resolved zero recipients"
		} else if len(opcoRecipients) == 0 {
			plan.ContradictionReason = "opco event resolved zero opco recipients"
		}
		return plan, nil
	}

	plan.Recipients = uniqueStrings(eb.resolveSubscribedRecipients(string(evt.Type)))
	plan.PersistedRecipients = eb.persistableRecipients(ctx, plan.Recipients)
	return plan, nil
}

func (eb *EventBus) persistEventRecord(ctx context.Context, evt events.Event, recipients []string) error {
	recipients = uniqueStrings(recipients)
	if atomicStore, ok := eb.store.(AtomicEventPersistence); ok {
		if err := atomicStore.PersistEventWithDeliveries(ctx, evt, recipients); err != nil {
			return fmt.Errorf("persist event transaction: %w", err)
		}
		return nil
	}
	if err := eb.store.AppendEvent(ctx, evt); err != nil {
		return fmt.Errorf("persist event: %w", err)
	}
	if len(recipients) == 0 {
		return nil
	}
	if err := eb.store.InsertEventDeliveries(ctx, evt.ID, recipients); err != nil {
		return fmt.Errorf("persist event deliveries: %w", err)
	}
	return nil
}

func (eb *EventBus) logDelivery(ctx context.Context, evt events.Event, recipients []string, extra map[string]any) {
	detail := map[string]any{"recipients_count": len(recipients)}
	for k, v := range extra {
		detail[k] = v
	}
	eb.logRuntime(ctx, RuntimeLogEntry{
		Level:      "debug",
		Component:  "eventbus",
		Action:     "delivered",
		EventID:    evt.ID,
		EventType:  string(evt.Type),
		VerticalID: evt.VerticalID,
		Detail:     detail,
	})
}

// PublishDirect persists an event and delivers it to the specified recipients
// regardless of routing tables or subscription patterns. This is the "message"
// primitive: explicit, point-to-point delivery.
func (eb *EventBus) PublishDirect(ctx context.Context, evt events.Event, recipients []string) (err error) {
	ctx = WithCurrentRuntimeEpoch(ctx)
	if err := ensurePublishEpoch(ctx); err != nil {
		return err
	}
	start := time.Now()
	persisted := false
	defer func() {
		if !persisted {
			return
		}
		status := "processed"
		errText := ""
		if err != nil {
			status = "error"
			errText = err.Error()
		}
		eb.markPipelineReceipt(ctx, evt.ID, status, errText)
	}()
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return errors.New("direct publish recipients are required")
	}
	if evt.Type == "" {
		return errors.New("event type is required")
	}
	if !isValidEventTypeName(string(evt.Type)) {
		return fmt.Errorf("invalid event type: %s", strings.TrimSpace(string(evt.Type)))
	}
	if evt.ID == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now()
	}
	if err := eb.persistEventRecord(ctx, evt, recipients); err != nil {
		return err
	}
	persisted = true
	eb.deliverToAgents(ctx, evt, recipients)
	eb.logRuntime(ctx, RuntimeLogEntry{
		Level:      "debug",
		Component:  "eventbus",
		Action:     "delivered",
		EventID:    evt.ID,
		EventType:  string(evt.Type),
		VerticalID: evt.VerticalID,
		DurationUS: int(time.Since(start) / time.Microsecond),
		Detail: map[string]any{
			"direct":           true,
			"recipients_count": len(recipients),
		},
	})
	return nil
}

func (eb *EventBus) Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event {
	ch := make(chan events.Event, 128)
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if existing, ok := eb.agentChans[agentID]; ok {
		ch = existing
	} else {
		eb.agentChans[agentID] = ch
	}

	// Track subscription patterns for wildcard delivery.
	for _, et := range eventTypes {
		eb.subscriptions[agentID] = appendUniqueEventType(eb.subscriptions[agentID], et)
	}

	for _, et := range eventTypes {
		if eb.channels[et] == nil {
			eb.channels[et] = make(map[string]chan events.Event)
		}
		eb.channels[et][agentID] = ch
	}
	return ch
}

// Unsubscribe removes an agent from all in-memory EventBus subscription maps.
// The existing channel is closed and discarded to prevent resource leaks after
// teardown/reconfigure cycles.
func (eb *EventBus) Unsubscribe(agentID string) {
	agentID = strings.TrimSpace(agentID)
	if eb == nil || agentID == "" {
		return
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if ch, ok := eb.agentChans[agentID]; ok {
		delete(eb.agentChans, agentID)
		close(ch)
	}
	delete(eb.subscriptions, agentID)
	for et := range eb.channels {
		delete(eb.channels[et], agentID)
		if len(eb.channels[et]) == 0 {
			delete(eb.channels, et)
		}
	}
}

func (eb *EventBus) SetRoutingTable(verticalID string, table *RoutingTable) error {
	if verticalID == "" || table == nil {
		return errors.New("verticalID and table are required")
	}
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.routingTable[verticalID] = table
	return nil
}

func (eb *EventBus) GetRoutingTable(verticalID string) *RoutingTable {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.routingTable[verticalID]
}

func (eb *EventBus) persistableRecipients(ctx context.Context, recipients []string) []string {
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return nil
	}
	lister, ok := eb.store.(ActiveAgentLister)
	if !ok {
		return recipients
	}
	ids, err := lister.ListActiveAgentIDs(ctx)
	if err != nil {
		return recipients
	}
	if len(ids) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id != "" {
			set[id] = struct{}{}
		}
	}
	out := make([]string, 0, len(recipients))
	for _, r := range recipients {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		if _, ok := set[r]; ok {
			out = append(out, r)
		}
	}
	return out
}

func (eb *EventBus) isFactoryEvent(eventType events.EventType) bool {
	name := strings.TrimSpace(string(eventType))
	if name == "" {
		return false
	}
	for _, prefix := range factoryEventPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func (eb *EventBus) resolveOpCoRecipients(evt events.Event) []string {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	table := eb.routingTable[evt.VerticalID]
	if table == nil {
		return nil
	}

	set := make(map[string]struct{})
	eventName := string(evt.Type)
	for _, r := range table.Routes {
		if r.Status != "" && r.Status != "active" {
			continue
		}
		if routeMatches(r.EventPattern, eventName) {
			set[r.SubscriberID] = struct{}{}
		}
	}

	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	return out
}

func (eb *EventBus) deliverToAgents(ctx context.Context, evt events.Event, agentIDs []string) {
	recipients := eb.snapshotRecipientChans(agentIDs)
	for _, recipient := range recipients {
		select {
		case recipient.ch <- evt:
		case <-ctx.Done():
			return
		case <-time.After(deliverySendTimeout):
			eb.logRuntime(ctx, RuntimeLogEntry{
				Level:      "warn",
				Component:  "eventbus",
				Action:     "delivery_timeout",
				EventID:    evt.ID,
				EventType:  string(evt.Type),
				AgentID:    recipient.agentID,
				VerticalID: evt.VerticalID,
				Detail: map[string]any{
					"timeout_ms": int(deliverySendTimeout / time.Millisecond),
				},
			})
		}
	}
}

type agentRecipient struct {
	agentID string
	ch      chan events.Event
}

func (eb *EventBus) snapshotRecipientChans(agentIDs []string) []agentRecipient {
	if eb == nil || len(agentIDs) == 0 {
		return nil
	}
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	out := make([]agentRecipient, 0, len(agentIDs))
	for _, id := range agentIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		ch, ok := eb.agentChans[id]
		if !ok {
			continue
		}
		out = append(out, agentRecipient{agentID: id, ch: ch})
	}
	return out
}

func (eb *EventBus) deliverByType(evt events.Event) {
	recipients := eb.resolveSubscribedRecipients(string(evt.Type))
	eb.deliverToAgents(context.Background(), evt, recipients)
}

func (eb *EventBus) resolveSubscribedRecipients(eventType string) []string {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	recipients := make([]string, 0, len(eb.subscriptions))
	for agentID, pats := range eb.subscriptions {
		for _, pat := range pats {
			if routeMatches(string(pat), eventType) {
				recipients = append(recipients, agentID)
				break
			}
		}
	}
	return recipients
}

func routeMatches(pattern, eventType string) bool {
	switch {
	case pattern == "", pattern == "*":
		return true
	default:
		// Support glob patterns like "opco.*.steady_state_reached" in addition to
		// the historical prefix-only semantics.
		if strings.Contains(pattern, "*") {
			if ok, err := path.Match(pattern, eventType); err == nil && ok {
				return true
			}
		}
		if strings.HasSuffix(pattern, "*") {
			return strings.HasPrefix(eventType, strings.TrimSuffix(pattern, "*"))
		}
		return pattern == eventType
	}
}

func appendUniqueEventType(in []events.EventType, v events.EventType) []events.EventType {
	if v == "" {
		return in
	}
	for _, x := range in {
		if x == v {
			return in
		}
	}
	return append(in, v)
}

func isValidEventTypeName(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	parts := strings.Split(raw, ".")
	if len(parts) == 0 {
		return false
	}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || !eventTypeTokenPattern.MatchString(p) {
			return false
		}
	}
	return true
}

func (eb *EventBus) resolveHumanTaskRecipients(evt events.Event) []string {
	if len(evt.Payload) == 0 {
		return nil
	}
	var payload struct {
		RequestingAgent string `json:"requesting_agent"`
	}
	_ = json.Unmarshal(evt.Payload, &payload)
	agentID := strings.TrimSpace(payload.RequestingAgent)
	if agentID == "" {
		return nil
	}
	return []string{agentID}
}

func uniqueStrings(in []string) []string {
	if len(in) <= 1 {
		return in
	}
	set := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, ok := set[v]; ok {
			continue
		}
		set[v] = struct{}{}
		out = append(out, v)
	}
	return out
}

func ensurePublishEpoch(ctx context.Context) error {
	epoch, ok := RuntimeEpochFromContext(ctx)
	if !ok || epoch <= 0 {
		return nil
	}
	if !IsCurrentRuntimeEpoch(epoch) {
		return ErrStaleRuntimeEpoch
	}
	return nil
}

func filterOutAgentIDs(in []string, disallow []string) []string {
	if len(in) == 0 || len(disallow) == 0 {
		return in
	}
	set := make(map[string]struct{}, len(disallow))
	for _, v := range disallow {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		set[v] = struct{}{}
	}
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if _, blocked := set[v]; blocked {
			continue
		}
		out = append(out, v)
	}
	return out
}

func filterOutVerticalScopedAgentIDs(in []string, verticalID string) []string {
	verticalID = strings.TrimSpace(verticalID)
	if len(in) == 0 || verticalID == "" {
		return in
	}
	suffix := "-" + verticalID
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if strings.HasSuffix(v, suffix) {
			continue
		}
		out = append(out, v)
	}
	return out
}

func (eb *EventBus) emitContradiction(ctx context.Context, source events.Event, reason string) error {
	payload := []byte(fmt.Sprintf(`{"event_id":"%s","reason":"%s","source_type":"%s"}`,
		source.ID, reason, source.Type))
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.contradiction_detected"),
		SourceAgent: "runtime",
		TaskID:      source.TaskID,
		VerticalID:  source.VerticalID,
		Payload:     payload,
		CreatedAt:   time.Now(),
	}
	if err := eb.store.AppendEvent(ctx, evt); err != nil {
		return fmt.Errorf("persist contradiction event: %w", err)
	}
	eb.logRuntime(ctx, RuntimeLogEntry{
		Level:      "warn",
		Component:  "guardrails",
		Action:     "violation",
		EventID:    source.ID,
		EventType:  string(source.Type),
		VerticalID: source.VerticalID,
		Detail: map[string]any{
			"reason": reason,
		},
	})
	return nil
}

func (eb *EventBus) markPipelineReceipt(ctx context.Context, eventID, status, errText string) {
	if eb == nil || eb.store == nil {
		return
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return
	}
	recorder, ok := eb.store.(PipelineReceiptPersistence)
	if !ok {
		return
	}
	_ = recorder.UpsertPipelineReceipt(ctx, eventID, status, errText)
}

func (eb *EventBus) logRuntime(ctx context.Context, entry RuntimeLogEntry) {
	if eb == nil {
		return
	}
	eb.mu.RLock()
	logger := eb.logger
	eb.mu.RUnlock()
	if logger == nil {
		return
	}
	logger.Log(ctx, entry)
}
