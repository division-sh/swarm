package deliverylifecycle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// CommittedRetryDecision is settled business evidence, not current claim authority.
type CommittedRetryDecision struct {
	ClaimVersion int64     `json:"claim_version"`
	SettledAt    time.Time `json:"settled_at"`
	EligibleAt   time.Time `json:"eligible_at"`
}

func (d *CommittedRetryDecision) UnmarshalJSON(raw []byte) error {
	type encoded CommittedRetryDecision
	var value encoded
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	decision := CommittedRetryDecision(value)
	if err := decision.Validate(); err != nil {
		return err
	}
	*d = decision
	return nil
}

func (d CommittedRetryDecision) Validate() error {
	if d.ClaimVersion <= 0 || d.SettledAt.IsZero() || d.EligibleAt.IsZero() || !d.EligibleAt.After(d.SettledAt) {
		return fmt.Errorf("%w: committed retry decision requires its exact settled generation and later due", ErrConflict)
	}
	return nil
}
