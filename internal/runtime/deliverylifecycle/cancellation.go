package deliverylifecycle

import "fmt"

// CancellationReason describes authored intent; it is not settlement authority.
// Operational shutdown, supersession and context cancellation use their own owners.
type CancellationReason string

const (
	CancellationTerminate   CancellationReason = "terminate"
	CancellationTurnTimeout CancellationReason = "turn_timeout"
)

func ParseCancellationReason(raw string) (CancellationReason, error) {
	reason := CancellationReason(raw)
	switch reason {
	case CancellationTerminate, CancellationTurnTimeout:
		return reason, nil
	default:
		return "", fmt.Errorf("authored cancellation reason %q is invalid", raw)
	}
}

// ValidateCanceledSnapshot is shared by live and immutable-history decoders.
// Intent alone is not a settled outcome; independent effect/session work can
// still block completion after the exact delivery owner acknowledges settlement.
func ValidateCanceledSnapshot(snapshot Snapshot) error {
	if snapshot.Status != StatusCanceled {
		return nil
	}
	if _, err := ParseCancellationReason(snapshot.ReasonCode); err != nil {
		return err
	}
	if snapshot.Failure != nil || snapshot.SettledAt.IsZero() || !snapshot.NextEligibleAt.IsZero() ||
		snapshot.RetryScheduled || snapshot.ClaimReclaimable || !snapshot.ClaimExpiresAt.IsZero() {
		return fmt.Errorf("canceled delivery must be settled without failure, retry or open claim")
	}
	return nil
}
