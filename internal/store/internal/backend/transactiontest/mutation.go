package transactiontest

import (
	"context"
	"time"
)

type MutationPhase uint8

const (
	MutationFence MutationPhase = iota + 1
	MutationDomain
	MutationFinalize
)

type MutationCounts struct {
	// Durations are summed wall time across attempts; concurrent spans overlap.
	FenceCalls         uint64
	FenceDuration      time.Duration
	DomainCalls        uint64
	DomainDuration     time.Duration
	FinalizeCalls      uint64
	FinalizeDuration   time.Duration
	ActivityOrderCalls uint64
	ActivityOrderWait  time.Duration
	ActivityOrderHold  time.Duration
}

func (c *MutationCounts) add(other MutationCounts) {
	c.FenceCalls += other.FenceCalls
	c.FenceDuration += other.FenceDuration
	c.DomainCalls += other.DomainCalls
	c.DomainDuration += other.DomainDuration
	c.FinalizeCalls += other.FinalizeCalls
	c.FinalizeDuration += other.FinalizeDuration
	c.ActivityOrderCalls += other.ActivityOrderCalls
	c.ActivityOrderWait += other.ActivityOrderWait
	c.ActivityOrderHold += other.ActivityOrderHold
}

func RecordActivityOrderLock(ctx context.Context, started, ended time.Time, acquired bool) {
	a, _ := ctx.Value(attemptKey{}).(*Attempt)
	if a == nil {
		return
	}
	a.mutationMu.Lock()
	defer a.mutationMu.Unlock()
	a.mutation.ActivityOrderCalls++
	a.mutation.ActivityOrderWait += ended.Sub(started)
	if acquired && a.activityOrderAcquired.IsZero() {
		a.activityOrderAcquired = ended
	}
}

type MutationSpan struct {
	attempt *Attempt
	phase   MutationPhase
	started time.Time
}

func BeginMutationPhase(ctx context.Context, phase MutationPhase) MutationSpan {
	a, _ := ctx.Value(attemptKey{}).(*Attempt)
	if a == nil {
		return MutationSpan{}
	}
	return MutationSpan{attempt: a, phase: phase, started: time.Now()}
}

func (s MutationSpan) End() {
	if s.attempt == nil {
		return
	}
	duration := time.Since(s.started)
	a := s.attempt
	a.mutationMu.Lock()
	defer a.mutationMu.Unlock()
	switch s.phase {
	case MutationFence:
		a.mutation.FenceCalls++
		a.mutation.FenceDuration += duration
	case MutationDomain:
		a.mutation.DomainCalls++
		a.mutation.DomainDuration += duration
	case MutationFinalize:
		a.mutation.FinalizeCalls++
		a.mutation.FinalizeDuration += duration
	}
}
