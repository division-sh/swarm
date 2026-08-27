package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"
)

const (
	mutationRetryBudget             = 5 * time.Second
	mutationBaseDelay               = 10 * time.Millisecond
	mutationMaximumDelay            = 100 * time.Millisecond
	mutationDiagnosticRingCapacity  = 256
	mutationDiagnosticLabelCapacity = 128
	mutationDiagnosticOverflowLabel = "<overflow>"
)

type mutationPhase string

const (
	mutationPhaseFirstAttempt mutationPhase = "first_attempt"
	mutationPhaseRetryAttempt mutationPhase = "retry_attempt"
)

type mutationOperation struct {
	label               string
	phase               mutationPhase
	attempt             int
	sequence            uint64
	enqueuedAt          time.Time
	acquiredAt          time.Time
	queueDepth          int
	predecessorSequence uint64
	predecessorLabel    string
	contended           bool
}

type mutationAttemptDisposition string

const (
	mutationAttemptActive             mutationAttemptDisposition = "active"
	mutationAttemptReleased           mutationAttemptDisposition = "released"
	mutationAttemptAdmissionCancelled mutationAttemptDisposition = "admission_cancelled"
)

type mutationAttemptTrace struct {
	Sequence            uint64
	Label               string
	Phase               mutationPhase
	Attempt             int
	EnqueuedAt          time.Time
	AcquiredAt          time.Time
	ReleasedAt          time.Time
	CancelledAt         time.Time
	WaitDuration        time.Duration
	HoldDuration        time.Duration
	QueueDepth          int
	PredecessorSequence uint64
	PredecessorLabel    string
	Disposition         mutationAttemptDisposition
}

type mutationLabelAggregate struct {
	Label          string
	Count          uint64
	ContendedCount uint64
	TotalWait      time.Duration
	MaxWait        time.Duration
	TotalHold      time.Duration
	MaxHold        time.Duration
}

type mutationActiveSnapshot struct {
	Attempt     mutationAttemptTrace
	ElapsedHold time.Duration
}

type mutationDiagnosticsSnapshot struct {
	Cancelled  mutationAttemptTrace
	Active     *mutationActiveSnapshot
	Waiting    int
	Aggregates []mutationLabelAggregate
	Succession []mutationAttemptTrace
}

func (b *Backend) RunTransaction(ctx context.Context, label string, operation func(context.Context, *sql.Tx) error) error {
	if !b.Valid() {
		return fmt.Errorf("sqlite backend is required")
	}
	if operation == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	label = mutationLabel(label)
	var recoveryDeadline time.Time
	var lastBusy error
	busyAttempts := 0
	for {
		current := mutationOperation{label: label, phase: mutationPhaseFirstAttempt, attempt: busyAttempts + 1}
		if err := ctx.Err(); err != nil {
			b.observeFirstCancellation(current, "before_attempt", err)
			return err
		}

		recovering := lastBusy != nil
		if recovering && !time.Now().Before(recoveryDeadline) {
			return mutationBudgetError(label, lastBusy)
		}

		attemptCtx := ctx
		cancelAttempt := func() {}
		phase := mutationPhaseFirstAttempt
		if recovering {
			attemptCtx, cancelAttempt = context.WithDeadline(ctx, recoveryDeadline)
			phase = mutationPhaseRetryAttempt
		}
		current = b.beginMutationAttempt(label, phase, busyAttempts+1)
		if err := b.acquireMutation(attemptCtx, current); err != nil {
			cancelAttempt()
			if callerErr := ctx.Err(); callerErr != nil {
				return callerErr
			}
			if recovering && errors.Is(err, context.DeadlineExceeded) {
				return mutationBudgetError(label, lastBusy)
			}
			return err
		}
		err := func() error {
			defer b.releaseMutation()
			return b.runTransactionOnce(attemptCtx, nil, operation)
		}()
		attemptErr := attemptCtx.Err()
		cancelAttempt()

		if callerErr := ctx.Err(); callerErr != nil {
			b.observeFirstCancellation(current, "attempt", callerErr)
			return callerErr
		}
		if err == nil {
			return nil
		}
		if attemptErr != nil {
			if recovering && errors.Is(attemptErr, context.DeadlineExceeded) {
				return mutationBudgetError(label, lastBusy)
			}
			return attemptErr
		}
		if !mutationBusyError(err) {
			return err
		}

		lastBusy = err
		if recoveryDeadline.IsZero() {
			recoveryDeadline = time.Now().Add(mutationRetryBudget)
			b.observeFirstBusy(current, err)
		}
		busyAttempts++
		delay := time.Duration(busyAttempts) * mutationBaseDelay
		if delay > mutationMaximumDelay {
			delay = mutationMaximumDelay
		}
		if err := waitMutationBackoff(ctx, recoveryDeadline, delay); err != nil {
			if callerErr := ctx.Err(); callerErr != nil {
				b.observeFirstCancellation(current, "backoff", callerErr)
				return callerErr
			}
			return mutationBudgetError(label, lastBusy)
		}
	}
}

// RunReadTransaction owns the lifecycle of one caller-scoped consistent read.
// Reads deliberately bypass mutation admission and busy-recovery accounting.
func (b *Backend) RunReadTransaction(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
	if !b.Valid() {
		return fmt.Errorf("sqlite backend is required")
	}
	if operation == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return b.runTransactionOnce(ctx, &sql.TxOptions{ReadOnly: true}, operation)
}

func (b *Backend) acquireMutation(ctx context.Context, operation mutationOperation) error {
	if operation.sequence == 0 {
		operation = b.beginMutationAttempt(operation.label, operation.phase, operation.attempt)
	}
	if err := ctx.Err(); err != nil {
		b.cancelMutationAdmission(operation, err)
		return err
	}
	select {
	case <-b.mutationToken:
	case <-ctx.Done():
		b.cancelMutationAdmission(operation, ctx.Err())
		return ctx.Err()
	default:
		operation = b.beginMutationWait(operation)
		select {
		case <-b.mutationToken:
		case <-ctx.Done():
			b.cancelMutationAdmission(operation, ctx.Err())
			return ctx.Err()
		}
	}
	// Cancellation and token availability can become ready together. The
	// caller must win before callback entry even when token selection wins.
	if err := ctx.Err(); err != nil {
		b.cancelMutationAdmission(operation, err)
		b.mutationToken <- struct{}{}
		return err
	}
	b.acquireMutationAttempt(operation)
	return nil
}

func (b *Backend) beginMutationAttempt(label string, phase mutationPhase, attempt int) mutationOperation {
	operation := mutationOperation{
		label:   mutationLabel(label),
		phase:   phase,
		attempt: attempt,
	}
	b.mutationState.Lock()
	b.mutationState.nextSequence++
	operation.sequence = b.mutationState.nextSequence
	operation.enqueuedAt = time.Now()
	key, aggregate := b.mutationAggregateLocked(operation.label)
	aggregate.Count++
	b.mutationState.labelActivity[key] = aggregate
	b.mutationState.Unlock()
	return operation
}

func (b *Backend) beginMutationWait(operation mutationOperation) mutationOperation {
	b.mutationState.Lock()
	b.mutationState.waiting++
	operation.contended = true
	operation.queueDepth = b.mutationState.waiting
	operation.predecessorSequence = b.mutationState.active.sequence
	operation.predecessorLabel = b.mutationState.active.label
	key, aggregate := b.mutationAggregateLocked(operation.label)
	aggregate.ContendedCount++
	b.mutationState.labelActivity[key] = aggregate
	b.mutationState.Unlock()
	return operation
}

func (b *Backend) acquireMutationAttempt(operation mutationOperation) {
	operation.acquiredAt = time.Now()
	b.mutationState.Lock()
	if operation.contended {
		b.mutationState.waiting--
	}
	wait := operation.acquiredAt.Sub(operation.enqueuedAt)
	key, aggregate := b.mutationAggregateLocked(operation.label)
	aggregate.TotalWait += wait
	if wait > aggregate.MaxWait {
		aggregate.MaxWait = wait
	}
	b.mutationState.labelActivity[key] = aggregate
	b.mutationState.active = operation
	b.mutationState.Unlock()
}

func (b *Backend) releaseMutation() {
	releasedAt := time.Now()
	b.mutationState.Lock()
	operation := b.mutationState.active
	b.mutationState.active = mutationOperation{}
	trace := mutationTrace(operation, mutationAttemptReleased, releasedAt)
	b.appendMutationTraceLocked(trace)
	key, aggregate := b.mutationAggregateLocked(operation.label)
	aggregate.TotalHold += trace.HoldDuration
	if trace.HoldDuration > aggregate.MaxHold {
		aggregate.MaxHold = trace.HoldDuration
	}
	b.mutationState.labelActivity[key] = aggregate
	b.mutationState.Unlock()
	b.mutationToken <- struct{}{}
}

func (b *Backend) cancelMutationAdmission(operation mutationOperation, cause error) {
	cancelledAt := time.Now()
	b.mutationState.Lock()
	if operation.contended {
		b.mutationState.waiting--
	}
	trace := mutationTrace(operation, mutationAttemptAdmissionCancelled, cancelledAt)
	b.appendMutationTraceLocked(trace)
	key, aggregate := b.mutationAggregateLocked(operation.label)
	aggregate.TotalWait += trace.WaitDuration
	if trace.WaitDuration > aggregate.MaxWait {
		aggregate.MaxWait = trace.WaitDuration
	}
	b.mutationState.labelActivity[key] = aggregate
	b.mutationState.Unlock()
	b.observeFirstAdmissionCancellation(trace, cause)
}

func mutationTrace(operation mutationOperation, disposition mutationAttemptDisposition, finishedAt time.Time) mutationAttemptTrace {
	trace := mutationAttemptTrace{
		Sequence:            operation.sequence,
		Label:               operation.label,
		Phase:               operation.phase,
		Attempt:             operation.attempt,
		EnqueuedAt:          operation.enqueuedAt,
		AcquiredAt:          operation.acquiredAt,
		QueueDepth:          operation.queueDepth,
		PredecessorSequence: operation.predecessorSequence,
		PredecessorLabel:    operation.predecessorLabel,
		Disposition:         disposition,
	}
	if !operation.acquiredAt.IsZero() {
		trace.WaitDuration = operation.acquiredAt.Sub(operation.enqueuedAt)
	}
	switch disposition {
	case mutationAttemptReleased:
		trace.ReleasedAt = finishedAt
		trace.HoldDuration = finishedAt.Sub(operation.acquiredAt)
	case mutationAttemptAdmissionCancelled:
		trace.CancelledAt = finishedAt
		trace.WaitDuration = finishedAt.Sub(operation.enqueuedAt)
	}
	return trace
}

func (b *Backend) appendMutationTraceLocked(trace mutationAttemptTrace) {
	b.mutationState.ring[b.mutationState.ringNext] = trace
	b.mutationState.ringNext = (b.mutationState.ringNext + 1) % mutationDiagnosticRingCapacity
	if b.mutationState.ringCount < mutationDiagnosticRingCapacity {
		b.mutationState.ringCount++
	}
}

func (b *Backend) mutationAggregateLocked(label string) (string, mutationLabelAggregate) {
	if b.mutationState.labelActivity == nil {
		b.mutationState.labelActivity = make(map[string]mutationLabelAggregate, mutationDiagnosticLabelCapacity)
	}
	key := label
	if _, exists := b.mutationState.labelActivity[key]; !exists && len(b.mutationState.labelActivity) >= mutationDiagnosticLabelCapacity-1 {
		key = mutationDiagnosticOverflowLabel
	}
	aggregate := b.mutationState.labelActivity[key]
	aggregate.Label = key
	return key, aggregate
}

func (b *Backend) mutationDiagnosticsSnapshot(cancelled mutationAttemptTrace) mutationDiagnosticsSnapshot {
	now := time.Now()
	b.mutationState.Lock()
	snapshot := mutationDiagnosticsSnapshot{Cancelled: cancelled, Waiting: b.mutationState.waiting}
	if active := b.mutationState.active; active.sequence != 0 {
		attempt := mutationTrace(active, mutationAttemptActive, time.Time{})
		snapshot.Active = &mutationActiveSnapshot{Attempt: attempt, ElapsedHold: now.Sub(active.acquiredAt)}
	}
	snapshot.Aggregates = make([]mutationLabelAggregate, 0, len(b.mutationState.labelActivity))
	for _, aggregate := range b.mutationState.labelActivity {
		snapshot.Aggregates = append(snapshot.Aggregates, aggregate)
	}
	snapshot.Succession = make([]mutationAttemptTrace, 0, b.mutationState.ringCount)
	start := (b.mutationState.ringNext - b.mutationState.ringCount + mutationDiagnosticRingCapacity) % mutationDiagnosticRingCapacity
	for index := 0; index < b.mutationState.ringCount; index++ {
		snapshot.Succession = append(snapshot.Succession, b.mutationState.ring[(start+index)%mutationDiagnosticRingCapacity])
	}
	b.mutationState.Unlock()
	sort.Slice(snapshot.Aggregates, func(i, j int) bool {
		return snapshot.Aggregates[i].Label < snapshot.Aggregates[j].Label
	})
	return snapshot
}

func (b *Backend) observeFirstAdmissionCancellation(cancelled mutationAttemptTrace, cause error) {
	b.firstAdmissionCancellationObservation.Do(func() {
		snapshot := b.mutationDiagnosticsSnapshot(cancelled)
		slog.Info("sqlite mutation admission cancelled",
			"cause", cause,
			"cancelled_attempt", snapshot.Cancelled,
			"active_holder", snapshot.Active,
			"waiting_operations", snapshot.Waiting,
			"label_aggregates", snapshot.Aggregates,
			"succession", snapshot.Succession,
		)
	})
}

func (b *Backend) observeFirstBusy(operation mutationOperation, cause error) {
	b.firstBusyObservation.Do(func() {
		slog.Info("sqlite mutation busy recovery started",
			"operation", operation.label,
			"sequence", operation.sequence,
			"phase", operation.phase,
			"attempt", operation.attempt,
			"cause", cause,
		)
	})
}

func (b *Backend) observeFirstCancellation(operation mutationOperation, stage string, cause error) {
	b.firstCancellationObservation.Do(func() {
		slog.Info("sqlite mutation caller cancelled",
			"operation", operation.label,
			"sequence", operation.sequence,
			"phase", operation.phase,
			"attempt", operation.attempt,
			"stage", stage,
			"cause", cause,
		)
	})
}

func waitMutationBackoff(ctx context.Context, deadline time.Time, delay time.Duration) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return context.DeadlineExceeded
	}
	if delay > remaining {
		delay = remaining
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return nil
	}
}

func (b *Backend) runTransactionOnce(ctx context.Context, opts *sql.TxOptions, operation func(context.Context, *sql.Tx) error) error {
	tx, err := b.db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	if operationErr := operation(ctx, tx); operationErr != nil {
		return errors.Join(operationErr, rollback(tx))
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return errors.Join(commitErr, rollback(tx))
	}
	return nil
}

func rollback(tx *sql.Tx) error {
	if tx == nil {
		return errors.New("SQLite transaction is missing")
	}
	err := tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

func mutationBudgetError(label string, lastErr error) error {
	label = mutationLabel(label)
	if lastErr == nil {
		return fmt.Errorf("%s busy recovery invariant violated: missing busy/locked cause", label)
	}
	return fmt.Errorf("%s retry budget %s exceeded: %w", label, mutationRetryBudget, lastErr)
}

func mutationLabel(label string) string {
	label = strings.TrimSpace(label)
	if label == "" {
		return "sqlite mutation"
	}
	return label
}

func mutationBusyError(err error) bool {
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "sqlite_busy") || strings.Contains(text, "sqlite_locked") ||
		strings.Contains(text, "database is locked") || strings.Contains(text, "database table is locked") ||
		strings.Contains(text, "database is busy")
}
