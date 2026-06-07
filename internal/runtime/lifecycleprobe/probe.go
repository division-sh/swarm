package lifecycleprobe

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Kind string

const (
	EventPersisted              Kind = "event_persisted"
	DeliveryPersisted           Kind = "delivery_persisted"
	DeliveryStatusChanged       Kind = "delivery_status_changed"
	HandlerStarted              Kind = "handler_started"
	HandlerCompleted            Kind = "handler_completed"
	PostCommitDispatchStarted   Kind = "post_commit_dispatch_started"
	PostCommitDispatchCompleted Kind = "post_commit_dispatch_completed"
)

type Observer interface {
	NotifyLifecycle(context.Context, Signal)
}

type Signal struct {
	Kind           Kind
	EventID        string
	EventType      string
	SubscriberType string
	SubscriberID   string
	Status         string
	At             time.Time
}

type Cursor struct {
	nextSequence uint64
}

type Probe struct {
	mu           sync.Mutex
	history      []recordedSignal
	waiters      map[uint64]waiter
	nextID       uint64
	nextSequence uint64
}

type waiter struct {
	after uint64
	want  Signal
	ch    chan recordedSignal
}

type recordedSignal struct {
	sequence uint64
	signal   Signal
}

const maxHistory = 4096

func New() *Probe {
	return &Probe{waiters: make(map[uint64]waiter)}
}

func (p *Probe) NotifyLifecycle(_ context.Context, signal Signal) {
	if p == nil {
		return
	}
	signal = signal.Normalized()
	if signal.Kind == "" || signal.EventID == "" {
		return
	}
	if signal.At.IsZero() {
		signal.At = time.Now().UTC()
	}
	p.mu.Lock()
	p.nextSequence++
	record := recordedSignal{
		sequence: p.nextSequence,
		signal:   signal,
	}
	p.history = append(p.history, record)
	if len(p.history) > maxHistory {
		p.history = append([]recordedSignal(nil), p.history[len(p.history)-maxHistory:]...)
	}
	for id, waiting := range p.waiters {
		if record.sequence >= waiting.after && signalMatches(waiting.want, signal) {
			delete(p.waiters, id)
			waiting.ch <- record
			close(waiting.ch)
		}
	}
	p.mu.Unlock()
}

func (p *Probe) Wait(ctx context.Context, want Signal) (Signal, error) {
	signal, _, err := p.WaitAfter(ctx, Cursor{}, want)
	return signal, err
}

func (p *Probe) WaitAfter(ctx context.Context, cursor Cursor, want Signal) (Signal, Cursor, error) {
	if p == nil {
		return Signal{}, Cursor{}, errors.New("lifecycle probe is nil")
	}
	want = want.Normalized()
	if want.Kind == "" {
		return Signal{}, Cursor{}, errors.New("lifecycle probe wait kind is required")
	}
	if want.EventID == "" {
		return Signal{}, Cursor{}, errors.New("lifecycle probe wait event_id is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	after := cursor.nextSequence
	if after == 0 {
		after = 1
	}
	p.mu.Lock()
	for _, record := range p.history {
		if record.sequence >= after && signalMatches(want, record.signal) {
			p.mu.Unlock()
			return record.signal, Cursor{nextSequence: record.sequence + 1}, nil
		}
	}
	p.nextID++
	id := p.nextID
	ch := make(chan recordedSignal, 1)
	if p.waiters == nil {
		p.waiters = make(map[uint64]waiter)
	}
	p.waiters[id] = waiter{after: after, want: want, ch: ch}
	p.mu.Unlock()

	select {
	case record := <-ch:
		return record.signal, Cursor{nextSequence: record.sequence + 1}, nil
	case <-ctx.Done():
		p.mu.Lock()
		delete(p.waiters, id)
		p.mu.Unlock()
		return Signal{}, Cursor{}, fmt.Errorf("wait for lifecycle %s event %s: %w", want.Kind, want.EventID, ctx.Err())
	}
}

func (p *Probe) WaitForDeliveryStatus(ctx context.Context, eventID, subscriberType, subscriberID, status string) (Signal, error) {
	return p.Wait(ctx, Signal{
		Kind:           DeliveryStatusChanged,
		EventID:        eventID,
		SubscriberType: subscriberType,
		SubscriberID:   subscriberID,
		Status:         status,
	})
}

func (p *Probe) WaitForHandlerStarted(ctx context.Context, eventID, nodeID string) (Signal, error) {
	return p.Wait(ctx, Signal{
		Kind:           HandlerStarted,
		EventID:        eventID,
		SubscriberType: "node",
		SubscriberID:   nodeID,
	})
}

func (p *Probe) WaitForHandlerCompleted(ctx context.Context, eventID, nodeID string) (Signal, error) {
	return p.Wait(ctx, Signal{
		Kind:           HandlerCompleted,
		EventID:        eventID,
		SubscriberType: "node",
		SubscriberID:   nodeID,
	})
}

func (p *Probe) WaitForPostCommitDispatchStarted(ctx context.Context, eventID string) (Signal, error) {
	return p.Wait(ctx, Signal{Kind: PostCommitDispatchStarted, EventID: eventID})
}

func (p *Probe) WaitForPostCommitDispatchCompleted(ctx context.Context, eventID string) (Signal, error) {
	return p.Wait(ctx, Signal{Kind: PostCommitDispatchCompleted, EventID: eventID})
}

func (s Signal) Normalized() Signal {
	return Signal{
		Kind:           Kind(strings.TrimSpace(string(s.Kind))),
		EventID:        strings.TrimSpace(s.EventID),
		EventType:      strings.TrimSpace(s.EventType),
		SubscriberType: strings.TrimSpace(s.SubscriberType),
		SubscriberID:   strings.TrimSpace(s.SubscriberID),
		Status:         strings.TrimSpace(s.Status),
		At:             s.At,
	}
}

func signalMatches(want, got Signal) bool {
	want = want.Normalized()
	got = got.Normalized()
	if want.Kind != "" && want.Kind != got.Kind {
		return false
	}
	if want.EventID != "" && want.EventID != got.EventID {
		return false
	}
	if want.EventType != "" && want.EventType != got.EventType {
		return false
	}
	if want.SubscriberType != "" && want.SubscriberType != got.SubscriberType {
		return false
	}
	if want.SubscriberID != "" && want.SubscriberID != got.SubscriberID {
		return false
	}
	if want.Status != "" && want.Status != got.Status {
		return false
	}
	return true
}
