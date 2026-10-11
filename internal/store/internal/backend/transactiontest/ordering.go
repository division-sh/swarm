package transactiontest

import (
	"context"
	"time"
)

// AcquisitionDuration is caller-observed SQL acquisition time, not server lock
// wait. Holder durations end at acknowledged native settlement, before cleanup.
// Concurrent attempts overlap; these durations are not workload elapsed time.
type OrderingCounts struct {
	AcquisitionAttempts    uint64
	Acquired               uint64
	AcquisitionErrors      uint64
	IncompleteAcquisitions uint64
	Reuses                 uint64
	AcquisitionDuration    time.Duration
	CommittedHolders       uint64
	RolledBackHolders      uint64
	UnresolvedHolders      uint64
	CommitHoldDuration     time.Duration
	RollbackHoldDuration   time.Duration
}

func (c *OrderingCounts) add(other OrderingCounts) {
	c.AcquisitionAttempts += other.AcquisitionAttempts
	c.Acquired += other.Acquired
	c.AcquisitionErrors += other.AcquisitionErrors
	c.IncompleteAcquisitions += other.IncompleteAcquisitions
	c.Reuses += other.Reuses
	c.AcquisitionDuration += other.AcquisitionDuration
	c.CommittedHolders += other.CommittedHolders
	c.RolledBackHolders += other.RolledBackHolders
	c.UnresolvedHolders += other.UnresolvedHolders
	c.CommitHoldDuration += other.CommitHoldDuration
	c.RollbackHoldDuration += other.RollbackHoldDuration
}

type orderingSettlement uint8

const (
	orderingCommit orderingSettlement = iota + 1
	orderingRollback
)

type orderingObservation struct {
	counts     OrderingCounts
	acquiredAt time.Time
	settledAt  time.Time
	settlement orderingSettlement
}

type OrderingAcquisition struct {
	attempt *Attempt
	started time.Time
}

func BeginOrderingAcquisition(ctx context.Context) OrderingAcquisition {
	a, _ := ctx.Value(attemptKey{}).(*Attempt)
	if a == nil {
		return OrderingAcquisition{}
	}
	started := time.Now()
	a.orderingMu.Lock()
	a.ordering.counts.AcquisitionAttempts++
	a.orderingMu.Unlock()
	return OrderingAcquisition{attempt: a, started: started}
}

func (s OrderingAcquisition) End(err error) {
	if s.attempt == nil {
		return
	}
	ended := time.Now()
	a := s.attempt
	a.orderingMu.Lock()
	defer a.orderingMu.Unlock()
	a.ordering.counts.AcquisitionDuration += ended.Sub(s.started)
	if err != nil {
		a.ordering.counts.AcquisitionErrors++
		return
	}
	a.ordering.counts.Acquired++
	a.ordering.acquiredAt = ended
}

func OrderingReused(ctx context.Context) {
	a, _ := ctx.Value(attemptKey{}).(*Attempt)
	if a == nil {
		return
	}
	a.orderingMu.Lock()
	a.ordering.counts.Reuses++
	a.orderingMu.Unlock()
}

func (a *Attempt) OrderingCommitted() { a.settleOrdering(orderingCommit) }

// A rollback error, including ErrTxDone, does not prove this owner's physical
// rollback acknowledgement. Leave its acquired holder interval unresolved.
func (a *Attempt) OrderingRolledBack(err error) {
	if err == nil {
		a.settleOrdering(orderingRollback)
	}
}

func (a *Attempt) settleOrdering(outcome orderingSettlement) {
	if a == nil {
		return
	}
	settled := time.Now()
	a.orderingMu.Lock()
	defer a.orderingMu.Unlock()
	if a.ordering.settlement == 0 {
		a.ordering.settledAt = settled
		a.ordering.settlement = outcome
	}
}

func (a *Attempt) orderingCounts() OrderingCounts {
	a.orderingMu.Lock()
	defer a.orderingMu.Unlock()
	observation := a.ordering
	counts := observation.counts
	counts.IncompleteAcquisitions = counts.AcquisitionAttempts - counts.Acquired - counts.AcquisitionErrors
	if counts.Acquired == 0 {
		return counts
	}
	// Cancellation can settle a read transaction concurrently with acquisition.
	// An overlapping/missing acknowledgement is unknown, not a zero-length hold.
	if observation.settlement == 0 || observation.settledAt.Before(observation.acquiredAt) {
		counts.UnresolvedHolders = counts.Acquired
		return counts
	}
	duration := observation.settledAt.Sub(observation.acquiredAt)
	if observation.settlement == orderingCommit {
		counts.CommittedHolders = counts.Acquired
		counts.CommitHoldDuration = duration
	} else {
		counts.RolledBackHolders = counts.Acquired
		counts.RollbackHoldDuration = duration
	}
	return counts
}
