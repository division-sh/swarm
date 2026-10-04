package startupownership

import (
	"errors"
	"time"
)

// PossessionObservation is a joined, momentary exclusion test, not authority
// or a promise that a later boot can acquire the store.
type PossessionObservation struct {
	Backend    string    `json:"backend"`
	Available  bool      `json:"available"`
	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
}

func (p PossessionObservation) Validate() error {
	if p.Backend == "" || p.StartedAt.IsZero() || p.FinishedAt.Before(p.StartedAt) {
		return errors.New("possession observation is incomplete")
	}
	return nil
}
