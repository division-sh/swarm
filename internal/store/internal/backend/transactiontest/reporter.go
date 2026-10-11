package transactiontest

import (
	"fmt"
	"time"
)

// ValidateReporterIssuance admits detachment from native settled receipts, not
// cursor visibility. Captured non-serving attempts may still be active.
func ValidateReporterIssuance(receipt Snapshot, submitted time.Time, chunks uint64, delay time.Duration) error {
	if chunks == 0 || delay <= 0 || submitted.IsZero() {
		return fmt.Errorf("reporter issuance requires a submission time, chunks and delay")
	}
	for _, operation := range []Operation{FanOutClaim, FanOutChunk} {
		counts := receipt.ByOperation[operation]
		if counts.Failed != 0 || counts.CommitFailures != 0 || counts.CleanupFailures != 0 {
			return fmt.Errorf("reporter issuance has failed %s attempts: %+v", operation, counts)
		}
		if counts.WriteCommits != chunks || counts.ReadCommits != 0 || counts.DelayedCommits != chunks || counts.InjectedDelay != time.Duration(chunks)*delay || counts.FirstCommitAt.Before(submitted) || counts.LastCommitAt.Before(counts.FirstCommitAt) || counts.LastCommitAt.IsZero() {
			return fmt.Errorf("reporter issuance has incomplete %s acknowledgements: %+v", operation, counts)
		}
	}
	// Failed counts every unacknowledged attempt, including read-only rollback
	// and refused speculative work. Keep those receipts, without inventing COMMITs.
	if receipt.ByOperation[FanOutRelease].WriteCommits != 0 || receipt.Total.CommitFailures != 0 || receipt.Total.CleanupFailures != 0 {
		return fmt.Errorf("reporter issuance has release or failed attempts: %+v", receipt)
	}
	if receipt.Total.DelayedCommits != 2*chunks || receipt.Total.InjectedDelay != time.Duration(2*chunks)*delay {
		return fmt.Errorf("reporter issuance delayed non-serving work: %+v", receipt.Total)
	}
	return nil
}

// ValidateReporterContinuation requires real mixed-batch commits in the
// successor, while preserving the detached collector's original acknowledgements.
func ValidateReporterContinuation(issuance, detached, successor Snapshot) error {
	for _, operation := range []Operation{FanOutClaim, FanOutChunk} {
		before, after := issuance.ByOperation[operation], detached.ByOperation[operation]
		if before != after || successor.ByOperation[operation].WriteCommits != 1 || successor.ByOperation[operation].Failed != 0 {
			return fmt.Errorf("reporter continuation lost or reassigned %s receipts: old=%+v final=%+v successor=%+v", operation, before, after, successor.ByOperation[operation])
		}
	}
	for _, receipt := range []Snapshot{detached, successor} {
		if receipt.Total.CommitFailures != 0 || receipt.Total.CleanupFailures != 0 {
			return fmt.Errorf("reporter continuation has failed attempts: %+v", receipt)
		}
	}
	if successor.Total.DelayedCommits != 0 || successor.Total.InjectedDelay != 0 || successor.Total.WriteCommits == 0 {
		return fmt.Errorf("reporter successor is absent or still delayed: %+v", successor)
	}
	return nil
}

// ValidateReporterJoined is evaluated after both execution and store cleanup,
// never on a pre-teardown snapshot of work still owned by a collector.
func ValidateReporterJoined(issuance, detached, successor Snapshot) error {
	if detached.Active != 0 || successor.Active != 0 {
		return fmt.Errorf("reporter collectors still own captured attempts: old=%+v successor=%+v", detached, successor)
	}
	return ValidateReporterContinuation(issuance, detached, successor)
}

// ValidateReporterDrain admits already-settled descendants or a named native
// settlement after issuance, including work captured by the detached collector.
func ValidateReporterDrain(issuance, detached, successor Snapshot, settledBeforeHandoff bool) error {
	old := detached.ByOperation[DeliverySettle]
	next := successor.ByOperation[DeliverySettle]
	if next.WriteCommits > 0 || old.WriteCommits > issuance.ByOperation[DeliverySettle].WriteCommits && old.LastCommitAt.After(issuance.ByOperation[FanOutChunk].LastCommitAt) {
		return nil
	}
	if settledBeforeHandoff {
		return nil
	}
	return fmt.Errorf("outstanding reporter descendants have no observed post-issuance settlement: old=%+v successor=%+v", old, next)
}

// ValidateReporterStress makes continued all-commit injection observable after
// issuance, including the mixed batch; retaining the old root name is not proof.
func ValidateReporterStress(issuance, final Snapshot, delay time.Duration) error {
	if final.Total.WriteCommits <= issuance.Total.WriteCommits || final.ByOperation[FanOutChunk].WriteCommits != 21 || final.ByOperation[FanOutClaim].WriteCommits != 21 {
		return fmt.Errorf("all-commit reporter stopped before mixed settlement: %+v", final)
	}
	for operation, counts := range final.ByOperation {
		commits := counts.ReadCommits + counts.WriteCommits
		if (operation == FanOutClaim || operation == FanOutChunk) && counts.Failed != 0 {
			return fmt.Errorf("all-commit reporter has failed %s serving attempts: %+v", operation, counts)
		}
		if counts.CommitFailures != 0 || counts.CleanupFailures != 0 || counts.DelayedCommits != commits || counts.InjectedDelay != time.Duration(commits)*delay {
			return fmt.Errorf("all-commit reporter lost post-issuance injection for %s: %+v", operation, counts)
		}
	}
	return nil
}

func ValidateReporterStressJoined(issuance, final Snapshot, delay time.Duration) error {
	if final.Active != 0 {
		return fmt.Errorf("all-commit reporter still owns captured attempts: %+v", final)
	}
	return ValidateReporterStress(issuance, final, delay)
}
