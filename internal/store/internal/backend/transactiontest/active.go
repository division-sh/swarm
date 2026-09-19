package transactiontest

type ActivePhase string

const (
	PhaseBeginning            ActivePhase = "beginning"
	PhaseOperation            ActivePhase = "operation"
	PhaseCommitDelay          ActivePhase = "injected_commit_delay"
	PhaseCommitCall           ActivePhase = "commit_call"
	PhaseAcknowledgedCleanup  ActivePhase = "acknowledged_cleanup"
	PhaseCommitFailureCleanup ActivePhase = "commit_failure_cleanup"
	PhaseRollbackCleanup      ActivePhase = "rollback_cleanup"
)

// ActiveClass describes an instrumented transaction owner, not its current SQL
// or database lock state. Beginning includes owner waits before BeginTx returns;
// operation includes callback SQL and final admission checks. Zero-count classes
// are removed, so memory is bounded by operation/phase/mode, never workload IDs.
type ActiveClass struct {
	Operation Operation
	Phase     ActivePhase
	ReadOnly  bool
	Retained  bool
}

// CommitFailed is called after an unsuccessful physical Commit return, before
// the owner discards the connection or cleans up its retained session.
func (a *Attempt) CommitFailed() {
	a.reclassify(PhaseCommitFailureCleanup)
}

func (a *Attempt) reclassify(phase ActivePhase) {
	if a == nil {
		return
	}
	c := a.collector
	c.mu.Lock()
	defer c.mu.Unlock()
	if a.finished {
		return
	}
	c.removeActive(a.activeClass)
	a.activeClass.Operation = a.operation.Load().(Operation)
	if phase != "" {
		a.activeClass.Phase = phase
	}
	c.snapshot.ActiveByClass[a.activeClass]++
}

// Caller holds the collector mutex.
func (c *Collector) removeActive(class ActiveClass) {
	if c.snapshot.ActiveByClass[class] <= 1 {
		delete(c.snapshot.ActiveByClass, class)
	} else {
		c.snapshot.ActiveByClass[class]--
	}
}
