// Package transactiontest supplies opt-in, backend-local transaction receipts
// for storetest. It does not wrap SQL, alter admission, or persist telemetry.
package transactiontest

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

type Operation string

const (
	Other                        Operation = "other"
	FanOutClaim                  Operation = "fan_out_claim"
	FanOutChunk                  Operation = "fan_out_chunk"
	FanOutProducer               Operation = "fan_out_producer"
	FanOutLoad                   Operation = "fan_out_load"
	FanOutObservation            Operation = "fan_out_observation"
	FanOutRetry                  Operation = "fan_out_retry"
	FanOutBlock                  Operation = "fan_out_block"
	FanOutRelease                Operation = "fan_out_release"
	PipelineDecisionProcessed    Operation = "pipeline_decision_processed"
	PipelineSettlement           Operation = "pipeline_settlement"
	PipelineEligibility          Operation = "pipeline_eligibility"
	PipelinePublicationAdmission Operation = "pipeline_publication_admission"
	PipelineLoad                 Operation = "pipeline_load"
	RunExecutionInspection       Operation = "run_execution_inspection"
	SourceSetLoad                Operation = "source_set_load"
)

type DelayScope string

const (
	DelayAllCommits    DelayScope = "all_commits"
	DelayAllWrites     DelayScope = "all_writes"
	DelayServingWrites DelayScope = "serving_writes"
)

type Options struct {
	Delay      time.Duration
	DelayScope DelayScope
}

type Counts struct {
	BeginAttempts    uint64
	Begun            uint64
	ReadCommits      uint64
	WriteCommits     uint64
	Failed           uint64
	CommitAttempts   uint64
	CommitFailures   uint64
	RollbackAttempts uint64
	CleanupFailures  uint64
	DelayedCommits   uint64
	InjectedDelay    time.Duration
	Revision         RevisionCounts
	FirstCommitAt    time.Time
	LastCommitAt     time.Time
}

type Snapshot struct {
	Total         Counts
	ByOperation   map[Operation]Counts
	Retained      Counts
	Active        uint64
	ActiveByClass map[ActiveClass]uint64
}

type Collector struct {
	mu       sync.Mutex
	options  Options
	snapshot Snapshot
}

func (c *Collector) Snapshot() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := c.snapshot
	result.ByOperation = make(map[Operation]Counts, len(c.snapshot.ByOperation))
	for operation, counts := range c.snapshot.ByOperation {
		result.ByOperation[operation] = counts
	}
	result.ActiveByClass = make(map[ActiveClass]uint64, len(c.snapshot.ActiveByClass))
	for class, count := range c.snapshot.ActiveByClass {
		result.ActiveByClass[class] = count
	}
	return result
}

// Slot is owned by exactly one selected backend. Retained sessions reference
// this same slot; there is no database registry or process-global hook.
type Slot struct {
	mu        sync.Mutex
	collector *Collector
}

func (s *Slot) Install(options Options) (*Collector, func(), error) {
	if options.Delay < 0 {
		return nil, nil, errors.New("transaction probe delay cannot be negative")
	}
	if options.DelayScope == "" {
		options.DelayScope = DelayAllCommits
	}
	switch options.DelayScope {
	case DelayAllCommits, DelayAllWrites, DelayServingWrites:
	default:
		return nil, nil, errors.New("unknown transaction probe delay scope")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.collector != nil {
		return nil, nil, errors.New("selected backend already has a transaction probe")
	}
	c := &Collector{options: options, snapshot: Snapshot{ByOperation: make(map[Operation]Counts), ActiveByClass: make(map[ActiveClass]uint64)}}
	s.collector = c
	var once sync.Once
	restore := func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.collector == c {
				s.collector = nil
			}
		})
	}
	return c, restore, nil
}

type Attempt struct {
	collector                                               *Collector
	operation                                               atomic.Value
	readOnly, retained                                      bool
	begun, commitAttempted, acknowledged, rollbackAttempted bool
	delay                                                   time.Duration
	committedAt                                             time.Time
	revisionMu                                              sync.Mutex
	revision                                                RevisionCounts
	activeClass                                             ActiveClass
	finished                                                bool
}

func (s *Slot) Begin(readOnly, retained bool) *Attempt {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	c := s.collector
	s.mu.Unlock()
	if c == nil {
		return nil
	}
	a := &Attempt{collector: c, readOnly: readOnly, retained: retained}
	a.operation.Store(Other)
	a.activeClass = ActiveClass{Operation: Other, Phase: PhaseBeginning, ReadOnly: readOnly, Retained: retained}
	c.mu.Lock()
	c.snapshot.Active++
	c.snapshot.ActiveByClass[a.activeClass]++
	c.mu.Unlock()
	return a
}

type attemptKey struct{}

func WithAttempt(ctx context.Context, attempt *Attempt) context.Context {
	if attempt == nil && ctx.Value(attemptKey{}) == nil {
		return ctx
	}
	return context.WithValue(ctx, attemptKey{}, attempt)
}

// Mark labels the actual transaction executing a semantic owner callback. A
// nested SQL helper cannot replace an already assigned enclosing operation.
func Mark(ctx context.Context, operation Operation) {
	if a, ok := ctx.Value(attemptKey{}).(*Attempt); ok && a != nil {
		if a.operation.CompareAndSwap(Other, operation) {
			a.reclassify("")
		}
	}
}

func (a *Attempt) Begun() {
	if a != nil {
		a.begun = true
		a.reclassify(PhaseOperation)
	}
}

// BeforeCommit is called only after the transaction owner's existing final
// admission checks. The delay represents commit transport cost, not a new
// command clock or admission point; the owner still performs the real Commit.
func (a *Attempt) BeforeCommit() {
	if a == nil {
		return
	}
	op := a.operation.Load().(Operation)
	options := a.collector.options
	apply := options.DelayScope == DelayAllCommits ||
		(options.DelayScope == DelayAllWrites && !a.readOnly) ||
		(options.DelayScope == DelayServingWrites && !a.readOnly && (op == FanOutClaim || op == FanOutChunk))
	if apply && options.Delay > 0 {
		a.delay = options.Delay
		a.reclassify(PhaseCommitDelay)
		time.Sleep(a.delay)
	}
	a.commitAttempted = true
	a.reclassify(PhaseCommitCall)
}

func (a *Attempt) Committed() {
	if a != nil {
		a.committedAt = time.Now()
		a.acknowledged = true
		a.reclassify(PhaseAcknowledgedCleanup)
	}
}

func (a *Attempt) RollbackAttempted() {
	if a != nil {
		a.rollbackAttempted = true
		a.reclassify(PhaseRollbackCleanup)
	}
}

func (a *Attempt) Finish(finalErr error) {
	if a == nil {
		return
	}
	a.revisionMu.Lock()
	revision := a.revision
	a.revisionMu.Unlock()
	add := func(counts Counts) Counts {
		counts.Revision.add(revision)
		counts.BeginAttempts++
		if a.begun {
			counts.Begun++
		}
		if a.commitAttempted {
			counts.CommitAttempts++
		}
		if a.rollbackAttempted {
			counts.RollbackAttempts++
		}
		if a.delay > 0 {
			counts.DelayedCommits++
			counts.InjectedDelay += a.delay
		}
		if a.acknowledged {
			if counts.FirstCommitAt.IsZero() || a.committedAt.Before(counts.FirstCommitAt) {
				counts.FirstCommitAt = a.committedAt
			}
			if counts.LastCommitAt.IsZero() || a.committedAt.After(counts.LastCommitAt) {
				counts.LastCommitAt = a.committedAt
			}
			if a.readOnly {
				counts.ReadCommits++
			} else {
				counts.WriteCommits++
			}
			if finalErr != nil {
				counts.CleanupFailures++
			}
		} else {
			counts.Failed++
			if a.commitAttempted {
				counts.CommitFailures++
			}
		}
		return counts
	}
	c := a.collector
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.finished {
		return
	}
	a.finished = true
	op := a.operation.Load().(Operation)
	c.snapshot.Total = add(c.snapshot.Total)
	c.snapshot.ByOperation[op] = add(c.snapshot.ByOperation[op])
	if a.retained {
		c.snapshot.Retained = add(c.snapshot.Retained)
	}
	c.snapshot.Active--
	c.removeActive(a.activeClass)
}
