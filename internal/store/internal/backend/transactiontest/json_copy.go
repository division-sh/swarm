package transactiontest

import "context"

// CopyCounts measures serialized arguments at explicitly observed write sites, not
// stored JSON, WAL or whole-workload SQL. Success is the write call's returned
// result; only commit acknowledgment can attribute successful bytes as durable.
type CopyCounts struct {
	Calls, Copies, SubmittedBytes              uint64
	SucceededCalls, FailedCalls                uint64
	SucceededBytes, FailedBytes                uint64
	CommittedBytes, UncommittedBytes           uint64
	IndeterminateBytes, RollbackAttemptedBytes uint64
	UnfinishedBytes                            uint64
}

func (c *CopyCounts) add(other CopyCounts) {
	c.Calls += other.Calls
	c.Copies += other.Copies
	c.SubmittedBytes += other.SubmittedBytes
	c.SucceededCalls += other.SucceededCalls
	c.FailedCalls += other.FailedCalls
	c.SucceededBytes += other.SucceededBytes
	c.FailedBytes += other.FailedBytes
	c.CommittedBytes += other.CommittedBytes
	c.UncommittedBytes += other.UncommittedBytes
	c.IndeterminateBytes += other.IndeterminateBytes
	c.RollbackAttemptedBytes += other.RollbackAttemptedBytes
	c.UnfinishedBytes += other.UnfinishedBytes
}

func (c *CopyCounts) settle(acknowledged, commitAttempted, rollbackAttempted bool) {
	c.UnfinishedBytes = c.SubmittedBytes - c.SucceededBytes - c.FailedBytes
	switch {
	case acknowledged:
		c.CommittedBytes = c.SucceededBytes
	case commitAttempted:
		// A failed Commit return cannot establish whether persistence occurred.
		c.IndeterminateBytes = c.SucceededBytes
	default:
		c.UncommittedBytes = c.SucceededBytes
	}
	if rollbackAttempted {
		// This records an attempt, not proof that Rollback succeeded.
		c.RollbackAttemptedBytes = c.SucceededBytes
	}
}

type JSONCopyCounts struct {
	WorkflowHeader        CopyCounts
	EntityMetadata        CopyCounts
	Events                CopyCounts
	EventDeliveries       CopyCounts
	CommittedReplayScopes CopyCounts
	EventReceipts         CopyCounts
	DeadLetters           CopyCounts
}

func (c *JSONCopyCounts) add(other JSONCopyCounts) {
	c.WorkflowHeader.add(other.WorkflowHeader)
	c.EntityMetadata.add(other.EntityMetadata)
	c.Events.add(other.Events)
	c.EventDeliveries.add(other.EventDeliveries)
	c.CommittedReplayScopes.add(other.CommittedReplayScopes)
	c.EventReceipts.add(other.EventReceipts)
	c.DeadLetters.add(other.DeadLetters)
}

func (c *JSONCopyCounts) settle(acknowledged, commitAttempted, rollbackAttempted bool) {
	c.WorkflowHeader.settle(acknowledged, commitAttempted, rollbackAttempted)
	c.EntityMetadata.settle(acknowledged, commitAttempted, rollbackAttempted)
	c.Events.settle(acknowledged, commitAttempted, rollbackAttempted)
	c.EventDeliveries.settle(acknowledged, commitAttempted, rollbackAttempted)
	c.CommittedReplayScopes.settle(acknowledged, commitAttempted, rollbackAttempted)
	c.EventReceipts.settle(acknowledged, commitAttempted, rollbackAttempted)
	c.DeadLetters.settle(acknowledged, commitAttempted, rollbackAttempted)
}

type EventHistoryJSONFamily uint8

const (
	EventJSON EventHistoryJSONFamily = iota + 1
	DeliveryJSON
	ReplayScopeJSON
	ReceiptJSON
	DeadLetterJSON
)

// BeginEventHistoryJSON observes only the submitted event/delivery ledger
// arguments. It neither selects history nor creates transaction authority.
func BeginEventHistoryJSON(ctx context.Context, family EventHistoryJSONFamily, length, copies int) *JSONCopyWrite {
	a := revisionAttempt(ctx)
	if a == nil || copies == 0 {
		return nil
	}
	var counts *CopyCounts
	switch family {
	case EventJSON:
		counts = &a.jsonCopies.Events
	case DeliveryJSON:
		counts = &a.jsonCopies.EventDeliveries
	case ReplayScopeJSON:
		counts = &a.jsonCopies.CommittedReplayScopes
	case ReceiptJSON:
		counts = &a.jsonCopies.EventReceipts
	case DeadLetterJSON:
		counts = &a.jsonCopies.DeadLetters
	default:
		return nil
	}
	return beginJSONCopy(a, counts, length, copies)
}

type JSONCopyWrite struct {
	attempt *Attempt
	counts  *CopyCounts
	bytes   uint64
	ended   bool
}

func BeginWorkflowHeaderJSON(ctx context.Context, length int) *JSONCopyWrite {
	a := revisionAttempt(ctx)
	if a == nil {
		return nil
	}
	return beginJSONCopy(a, &a.jsonCopies.WorkflowHeader, length, 1)
}

func BeginEntityMetadataJSON(ctx context.Context, length, copies int) *JSONCopyWrite {
	a := revisionAttempt(ctx)
	if a == nil || copies == 0 {
		return nil
	}
	return beginJSONCopy(a, &a.jsonCopies.EntityMetadata, length, copies)
}

func beginJSONCopy(a *Attempt, counts *CopyCounts, length, copies int) *JSONCopyWrite {
	a.jsonMu.Lock()
	defer a.jsonMu.Unlock()
	counts.Calls++
	counts.Copies += uint64(copies)
	counts.SubmittedBytes += uint64(length)
	return &JSONCopyWrite{attempt: a, counts: counts, bytes: uint64(length)}
}

func (w *JSONCopyWrite) End(err error) {
	if w == nil {
		return
	}
	w.attempt.jsonMu.Lock()
	defer w.attempt.jsonMu.Unlock()
	if w.ended {
		return
	}
	w.ended = true
	if err == nil {
		w.counts.SucceededCalls++
		w.counts.SucceededBytes += w.bytes
	} else {
		w.counts.FailedCalls++
		w.counts.FailedBytes += w.bytes
	}
}
