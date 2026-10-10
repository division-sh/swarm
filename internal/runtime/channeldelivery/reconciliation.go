package channeldelivery

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ReconcileDemand schedules observation, never delivery or provider authority.
type ReconcileDemand uint8

const (
	ReconcileOrdinary ReconcileDemand = 1 << iota
	ReconcileNative
)

func (d ReconcileDemand) Valid() bool {
	return d != 0 && d&^(ReconcileOrdinary|ReconcileNative) == 0
}

func (d ReconcileDemand) Includes(scope ReconcileDemand) bool { return d&scope != 0 }

type ReconcileMark struct {
	owner        *ReconcileSignal
	Subscription uint64
	Sequence     uint64
	Pass         uint64
}

// StartedAfter compares opaque subscription ownership as well as ordinals.
// Counters from another selected store/process cannot provide pass credit.
func (m ReconcileMark) StartedAfter(cut ReconcileMark) bool {
	return m.owner != nil && m.owner == cut.owner && m.Subscription == cut.Subscription &&
		m.Pass > cut.Pass && m.Sequence >= cut.Sequence
}

// ReconcilePass is emitted only after its complete named scope succeeds.
type ReconcilePass struct {
	Scope ReconcileDemand
	Start ReconcileMark
}

type ReconcileCadence struct {
	Ordinary time.Duration
	Native   time.Duration
}

// ReconcileSignal holds one process-owned worker subscription, not durable work.
type ReconcileSignal struct {
	mu       sync.Mutex
	sequence uint64
	nextID   uint64
	current  *ReconcileSubscription
}

type ReconcileSubscription struct {
	owner   *ReconcileSignal
	ctx     context.Context
	id      uint64
	wake    chan struct{}
	pending ReconcileDemand
	passes  uint64
}

func (s *ReconcileSignal) Subscribe(ctx context.Context) (*ReconcileSubscription, error) {
	if s == nil || ctx == nil {
		return nil, errors.New("channel reconciliation requires signal and owned context")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.current != nil {
		return nil, errors.New("channel reconciliation worker is already subscribed")
	}
	s.nextID++
	s.current = &ReconcileSubscription{owner: s, ctx: ctx, id: s.nextID, wake: make(chan struct{}, 1)}
	return s.current, nil
}

// PublishAcknowledged is called by the named outer owner, after native COMMIT.
// A no-op is represented by demand zero; cleanup error is independent of ack.
func (s *ReconcileSignal) PublishAcknowledged(ack bool, demand ReconcileDemand) error {
	if demand != 0 && !demand.Valid() {
		return errors.New("invalid channel reconciliation demand")
	}
	if !ack || demand == 0 || s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sequence++
	current := s.current
	if current == nil || current.ctx.Err() != nil {
		return nil
	}
	current.pending |= demand
	select {
	case current.wake <- struct{}{}:
	default:
	}
	return nil
}

func (s *ReconcileSubscription) Wake() <-chan struct{} { return s.wake }

func (s *ReconcileSubscription) Mark() (ReconcileMark, bool) {
	if s == nil || s.owner == nil {
		return ReconcileMark{}, false
	}
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.owner.current != s || s.ctx.Err() != nil {
		return ReconcileMark{}, false
	}
	return ReconcileMark{owner: s.owner, Subscription: s.id, Sequence: s.owner.sequence, Pass: s.passes}, true
}

// BeginPass consumes pending hints before scanning. A concurrent/in-pass commit
// stays pending and cannot be erased by completion of the current scan.
func (s *ReconcileSubscription) BeginPass() (ReconcileDemand, ReconcileMark) {
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.owner.current != s || s.ctx.Err() != nil {
		return 0, ReconcileMark{}
	}
	select {
	case <-s.wake:
	default:
	}
	demand := s.pending
	s.pending = 0
	s.passes++
	return demand, ReconcileMark{owner: s.owner, Subscription: s.id, Sequence: s.owner.sequence, Pass: s.passes}
}

func (s *ReconcileSignal) Mark() (ReconcileMark, bool) {
	if s == nil {
		return ReconcileMark{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil || s.current.ctx.Err() != nil {
		return ReconcileMark{}, false
	}
	return ReconcileMark{owner: s, Subscription: s.current.id, Sequence: s.sequence, Pass: s.current.passes}, true
}

func (s *ReconcileSubscription) Close() {
	if s == nil || s.owner == nil {
		return
	}
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	if s.owner.current == s {
		s.owner.current = nil
	}
	// Do not close wake: a retired signal never owns another worker's channel.
}
