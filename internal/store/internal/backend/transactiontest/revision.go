package transactiontest

import (
	"context"
	"time"
)

// RevisionCounts covers only the instrumented revision finalizer. LockDuration
// includes lock-acquisition SQL execution, not pure server-side lock wait.
// SQL calls are attempted interface invocations, including calls that fail.
type RevisionCounts struct {
	Finalizations        uint64
	Duration             time.Duration
	LockPhases           uint64
	LockDuration         time.Duration
	ExecCalls            uint64
	QueryCalls           uint64
	QueryRowCalls        uint64
	CommittedSQL         RevisionSQLCounts
	UncommittedSQL       RevisionSQLCounts
	IndeterminateSQL     RevisionSQLCounts
	RollbackAttemptedSQL RevisionSQLCounts
}

type RevisionSQLCounts struct {
	ExecCalls, QueryCalls, QueryRowCalls uint64
}

func (c *RevisionSQLCounts) add(other RevisionSQLCounts) {
	c.ExecCalls += other.ExecCalls
	c.QueryCalls += other.QueryCalls
	c.QueryRowCalls += other.QueryRowCalls
}

func (c *RevisionCounts) settle(acknowledged, commitAttempted, rollbackAttempted bool) {
	calls := RevisionSQLCounts{ExecCalls: c.ExecCalls, QueryCalls: c.QueryCalls, QueryRowCalls: c.QueryRowCalls}
	switch {
	case acknowledged:
		c.CommittedSQL = calls
	case commitAttempted:
		c.IndeterminateSQL = calls
	default:
		c.UncommittedSQL = calls
	}
	if rollbackAttempted {
		c.RollbackAttemptedSQL = calls
	}
}

func (c *RevisionCounts) add(other RevisionCounts) {
	c.Finalizations += other.Finalizations
	c.Duration += other.Duration
	c.LockPhases += other.LockPhases
	c.LockDuration += other.LockDuration
	c.ExecCalls += other.ExecCalls
	c.QueryCalls += other.QueryCalls
	c.QueryRowCalls += other.QueryRowCalls
	c.CommittedSQL.add(other.CommittedSQL)
	c.UncommittedSQL.add(other.UncommittedSQL)
	c.IndeterminateSQL.add(other.IndeterminateSQL)
	c.RollbackAttemptedSQL.add(other.RollbackAttemptedSQL)
}

func revisionAttempt(ctx context.Context) *Attempt {
	a, _ := ctx.Value(attemptKey{}).(*Attempt)
	return a
}

func RevisionEnabled(ctx context.Context) bool { return revisionAttempt(ctx) != nil }

type RevisionPhase struct {
	attempt *Attempt
	started time.Time
	lock    bool
}

func BeginRevision(ctx context.Context) RevisionPhase {
	return beginRevisionPhase(ctx, false)
}

func BeginRevisionLock(ctx context.Context) RevisionPhase {
	return beginRevisionPhase(ctx, true)
}

func beginRevisionPhase(ctx context.Context, lock bool) RevisionPhase {
	a := revisionAttempt(ctx)
	if a == nil {
		return RevisionPhase{}
	}
	return RevisionPhase{attempt: a, started: time.Now(), lock: lock}
}

func (p RevisionPhase) End() {
	if p.attempt == nil {
		return
	}
	elapsed := time.Since(p.started)
	a := p.attempt
	a.revisionMu.Lock()
	defer a.revisionMu.Unlock()
	if p.lock {
		a.revision.LockPhases++
		a.revision.LockDuration += elapsed
	} else {
		a.revision.Finalizations++
		a.revision.Duration += elapsed
	}
}

type RevisionSQLCall uint8

const (
	RevisionExec RevisionSQLCall = iota
	RevisionQuery
	RevisionQueryRow
)

func CountRevisionSQL(ctx context.Context, call RevisionSQLCall) {
	a := revisionAttempt(ctx)
	if a == nil {
		return
	}
	a.revisionMu.Lock()
	defer a.revisionMu.Unlock()
	switch call {
	case RevisionExec:
		a.revision.ExecCalls++
	case RevisionQuery:
		a.revision.QueryCalls++
	case RevisionQueryRow:
		a.revision.QueryRowCalls++
	}
}
