package effects

import (
	"fmt"
	"strings"
)

// RunSummary is the external-effect owner's validated view of attempts and
// subordinate budget reservations that still carry durable work for one run.
type RunSummary struct {
	RunID              string
	ActiveAttempts     int
	OrphanReservations int
	MalformedBindings  int
}

func (s RunSummary) Validate() error {
	if strings.TrimSpace(s.RunID) == "" {
		return fmt.Errorf("external-effect run summary requires run_id")
	}
	if s.ActiveAttempts < 0 || s.OrphanReservations < 0 || s.MalformedBindings < 0 {
		return fmt.Errorf("external-effect run summary counts cannot be negative")
	}
	return nil
}

func (s RunSummary) BlocksCompletion() bool {
	return s.ActiveAttempts > 0 || s.OrphanReservations > 0 || s.MalformedBindings > 0
}
