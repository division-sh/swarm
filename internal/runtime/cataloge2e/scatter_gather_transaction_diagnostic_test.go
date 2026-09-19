package cataloge2e

import (
	"os"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store/storetest"
)

// Opt-in observation of the unchanged S03 fixture, without injected delays.
func scatterGatherTransactionDiagnostics(t *testing.T, h *runtimeHarness) func(string) func() {
	if os.Getenv("SWARM_S03_TRANSACTION_DIAGNOSTICS") != "1" {
		return func(string) func() { return func() {} }
	}
	var selected any = h.sqlite
	if h.pg != nil {
		selected = h.pg
	}
	collector := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	return func(event string) func() {
		before, started := collector.Snapshot(), time.Now()
		return func() {
			after := collector.Snapshot()
			t.Logf("S03 transaction phase=%s start=%s end=%s elapsed=%s active=%d active_classes=%+v", event, started.UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano), time.Since(started), after.Active, after.ActiveByClass)
			logCounts := func(label string, a, b storetest.TransactionCounts) {
				t.Logf("S03 transaction phase=%s operation=%s begin=%d reads=%d writes=%d failed=%d commit_attempts=%d commit_failures=%d rollbacks=%d revision_finalizations=%d revision_elapsed=%s revision_lock_phases=%d revision_lock_elapsed=%s revision_exec=%d revision_query=%d revision_queryrow=%d first_commit=%s last_commit=%s", event, label,
					a.BeginAttempts-b.BeginAttempts, a.ReadCommits-b.ReadCommits, a.WriteCommits-b.WriteCommits, a.Failed-b.Failed,
					a.CommitAttempts-b.CommitAttempts, a.CommitFailures-b.CommitFailures, a.RollbackAttempts-b.RollbackAttempts,
					a.Revision.Finalizations-b.Revision.Finalizations, a.Revision.Duration-b.Revision.Duration,
					a.Revision.LockPhases-b.Revision.LockPhases, a.Revision.LockDuration-b.Revision.LockDuration,
					a.Revision.ExecCalls-b.Revision.ExecCalls, a.Revision.QueryCalls-b.Revision.QueryCalls, a.Revision.QueryRowCalls-b.Revision.QueryRowCalls,
					a.FirstCommitAt.UTC().Format(time.RFC3339Nano), a.LastCommitAt.UTC().Format(time.RFC3339Nano))
			}
			logCounts("total", after.Total, before.Total)
			var operations []string
			for operation, counts := range after.ByOperation {
				if counts != before.ByOperation[operation] {
					operations = append(operations, string(operation))
				}
			}
			sort.Strings(operations)
			for _, operation := range operations {
				key := storetest.TransactionOperation(operation)
				logCounts(operation, after.ByOperation[key], before.ByOperation[key])
			}
		}
	}
}
