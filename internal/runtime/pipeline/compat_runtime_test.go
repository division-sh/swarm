package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

type BusEventStore interface {
	AppendEvent(ctx context.Context, evt events.Event) error
	InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error
}

type InMemoryEventStore struct{}

func (InMemoryEventStore) AppendEvent(context.Context, events.Event) error { return nil }
func (InMemoryEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}

type EventInterceptor interface {
	Intercept(ctx context.Context, evt events.Event) (passthrough bool, deferred []events.Event, err error)
}

type RuntimeLogger struct {
	db *sql.DB
}

func NewRuntimeLogger(db *sql.DB) *RuntimeLogger {
	return &RuntimeLogger{db: db}
}

func (l *RuntimeLogger) Log(ctx context.Context, e RuntimeLogEntry) {
	if l == nil || l.db == nil {
		return
	}
	level := strings.ToLower(strings.TrimSpace(e.Level))
	if level == "" {
		level = "info"
	}
	component := strings.TrimSpace(e.Component)
	if component == "" {
		component = "runtime"
	}
	action := strings.TrimSpace(e.Action)
	if action == "" {
		action = "unknown"
	}
	detail := []byte("{}")
	if e.Detail != nil {
		if encoded, err := json.Marshal(e.Detail); err == nil && len(encoded) > 0 {
			detail = encoded
		}
	}
	_, _ = l.db.ExecContext(withoutSQLTxContext(ctx), `
		INSERT INTO runtime_log (
			level, component, action,
			event_id, event_type, agent_id, vertical_id, campaign_id, scan_id, session_id,
			detail, error, duration_us
		)
		VALUES (
			$1, $2, $3,
			$4::uuid, NULLIF($5,''), NULLIF($6,''), $7::uuid, $8::uuid, $9::uuid, $10::uuid,
			$11::jsonb, NULLIF($12,''), NULLIF($13,0)
		)
	`,
		level,
		component,
		action,
		nullableUUIDString(e.EventID),
		strings.TrimSpace(e.EventType),
		strings.TrimSpace(e.AgentID),
		nullableUUIDString(e.VerticalID),
		nullableUUIDString(e.CampaignID),
		nullableUUIDString(e.ScanID),
		nullableUUIDString(e.SessionID),
		string(detail),
		strings.TrimSpace(e.Error),
		e.DurationUS,
	)
}

type EventBus struct {
	mu            sync.RWMutex
	channels      map[events.EventType]map[string]chan events.Event
	agentChans    map[string]chan events.Event
	subscriptions map[string][]events.EventType
	interceptors  []EventInterceptor
	store         BusEventStore
	logger        *RuntimeLogger
}

func NewEventBus(store BusEventStore) *EventBus {
	if store == nil {
		store = InMemoryEventStore{}
	}
	return &EventBus{
		channels:      make(map[events.EventType]map[string]chan events.Event),
		agentChans:    make(map[string]chan events.Event),
		subscriptions: make(map[string][]events.EventType),
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

func (eb *EventBus) Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event {
	ch := make(chan events.Event, 128)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if existing, ok := eb.agentChans[agentID]; ok {
		ch = existing
	} else {
		eb.agentChans[agentID] = ch
	}
	for _, et := range eventTypes {
		eb.subscriptions[agentID] = appendUniqueEventType(eb.subscriptions[agentID], et)
		if eb.channels[et] == nil {
			eb.channels[et] = make(map[string]chan events.Event)
		}
		eb.channels[et][agentID] = ch
	}
	return ch
}

func (eb *EventBus) Publish(ctx context.Context, evt events.Event) error {
	if eb == nil {
		return nil
	}
	if strings.TrimSpace(evt.ID) == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now().UTC()
	}
	passthrough := true
	deferred := make([]events.Event, 0, 4)
	eb.mu.RLock()
	interceptors := append([]EventInterceptor(nil), eb.interceptors...)
	eb.mu.RUnlock()
	for _, it := range interceptors {
		pass, out, err := it.Intercept(ctx, evt)
		if err != nil {
			return err
		}
		if !pass {
			passthrough = false
		}
		for _, d := range out {
			if strings.TrimSpace(d.ID) == "" {
				d.ID = uuid.NewString()
			}
			if d.CreatedAt.IsZero() {
				d.CreatedAt = time.Now().UTC()
			}
			deferred = append(deferred, d)
		}
	}
	if eb.store != nil {
		if err := eb.store.AppendEvent(ctx, evt); err != nil {
			return err
		}
	}
	if passthrough {
		recipients := eb.ResolveSubscribedRecipients(string(evt.Type))
		eb.deliver(evt, recipients)
		if eb.store != nil && len(recipients) > 0 {
			if err := eb.store.InsertEventDeliveries(ctx, evt.ID, recipients); err != nil {
				return err
			}
		}
	}
	for _, d := range deferred {
		if err := eb.publishDeferred(ctx, d); err != nil {
			return err
		}
	}
	return nil
}

func (eb *EventBus) publishDeferred(ctx context.Context, evt events.Event) error {
	if eb.store != nil {
		if err := eb.store.AppendEvent(ctx, evt); err != nil {
			return err
		}
	}
	recipients := eb.ResolveSubscribedRecipients(string(evt.Type))
	eb.deliver(evt, recipients)
	if eb.store != nil && len(recipients) > 0 {
		if err := eb.store.InsertEventDeliveries(ctx, evt.ID, recipients); err != nil {
			return err
		}
	}
	return nil
}

func (eb *EventBus) PublishDirect(ctx context.Context, evt events.Event, recipients []string) error {
	if eb == nil {
		return nil
	}
	if strings.TrimSpace(evt.ID) == "" {
		evt.ID = uuid.NewString()
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now().UTC()
	}
	if eb.store != nil {
		if err := eb.store.AppendEvent(ctx, evt); err != nil {
			return err
		}
		if len(recipients) > 0 {
			if err := eb.store.InsertEventDeliveries(ctx, evt.ID, recipients); err != nil {
				return err
			}
		}
	}
	eb.deliver(evt, recipients)
	return nil
}

func (eb *EventBus) ResolveSubscribedRecipients(eventType string) []string {
	trimmed := events.EventType(strings.TrimSpace(eventType))
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	recipients := make([]string, 0)
	for agentID, ch := range eb.channels[trimmed] {
		if agentID != "" && ch != nil {
			recipients = append(recipients, agentID)
		}
	}
	sort.Strings(recipients)
	return recipients
}

func (eb *EventBus) LogRuntime(ctx context.Context, entry RuntimeLogEntry) {
	eb.mu.RLock()
	logger := eb.logger
	eb.mu.RUnlock()
	if logger != nil {
		logger.Log(ctx, entry)
	}
}

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
}

func (eb *EventBus) deliver(evt events.Event, recipients []string) {
	eb.mu.RLock()
	channels := make([]chan events.Event, 0, len(recipients))
	for _, recipient := range recipients {
		if ch, ok := eb.agentChans[recipient]; ok && ch != nil {
			channels = append(channels, ch)
		}
	}
	eb.mu.RUnlock()
	for _, ch := range channels {
		select {
		case ch <- evt:
		default:
		}
	}
}

func appendUniqueEventType(existing []events.EventType, eventType events.EventType) []events.EventType {
	for _, current := range existing {
		if current == eventType {
			return existing
		}
	}
	return append(existing, eventType)
}

func nullableUUIDString(raw string) any {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	if _, err := uuid.Parse(trimmed); err != nil {
		return nil
	}
	return trimmed
}
