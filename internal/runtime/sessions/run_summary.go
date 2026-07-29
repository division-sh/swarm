package sessions

import (
	"fmt"
	"strings"
	"time"
)

// RunSummary is the session owner's selected-store-time view of execution
// leases that can still perform work for one run.
type RunSummary struct {
	RunID        string
	ActiveLeases int
	NextExpiry   time.Time
	ObservedAt   time.Time
}

func (s RunSummary) Validate() error {
	if strings.TrimSpace(s.RunID) == "" {
		return fmt.Errorf("session run summary requires run_id")
	}
	if s.ActiveLeases < 0 {
		return fmt.Errorf("session run summary active lease count cannot be negative")
	}
	if s.ObservedAt.IsZero() {
		return fmt.Errorf("session run summary requires selected-store observation time")
	}
	if s.ActiveLeases == 0 && !s.NextExpiry.IsZero() {
		return fmt.Errorf("settled session run summary forbids next expiry")
	}
	if s.ActiveLeases > 0 && s.NextExpiry.IsZero() {
		return fmt.Errorf("active session run summary requires next expiry")
	}
	return nil
}

func (s RunSummary) BlocksCompletion() bool {
	return s.ActiveLeases > 0
}
